# Copyright (C) 2026 FORKTEX S.R.L.
#
# SPDX-License-Identifier: AGPL-3.0-or-later OR LicenseRef-ForkTex-Commercial
#
# This file is part of ForkTex Flow.
#
# For commercial licensing -- including use in proprietary products, SaaS
# deployments, or any context where AGPL obligations cannot be met -- you
# MUST obtain a commercial license from FORKTEX S.R.L. (info@forktex.com).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/>.


from __future__ import annotations

import asyncio
import contextlib
import traceback
from typing import TYPE_CHECKING

from forktex.error import BadRequestError
from forktex.log import get_logger
from forktex.types import JsonObject

from forktex_flow.domain.process import AdHocSubProcess, ServiceTask
from forktex_flow.domain.state import to_variables, validate_state
from forktex_flow.errors import WorkflowCancelled
from forktex_flow.persist import instances as _instances
from forktex_flow.persist import jobs as _jobs
from forktex_flow.persist.models import ElementRun, Instance
from forktex_flow.runtime.ctx import Ctx
from forktex_flow.runtime.engine import Advance, notify

if TYPE_CHECKING:
    from forktex_flow.domain.types import Job
    from forktex_flow.runtime.context import FlowRuntime

__all__ = ["run_job"]

logger = get_logger(__name__)


async def run_job(runtime: FlowRuntime, job: Job) -> None:
    """Cancelling the caller hands the job back as pending; it does not spend its attempt."""
    store = runtime.store
    async with store.transaction() as session:
        instance = await session.get(Instance, job.instance_id)
        if instance is None or instance.status != "active":
            # Cancelled or failed between the claim and now; its open elements were closed with it.
            return
        definition = await runtime.definition(session, instance.definition_id)
        run = await session.get(ElementRun, job.element_run_id)
        variables: JsonObject = {**instance.variables, **((run.input if run is not None else None) or {})}
        metadata: JsonObject = dict(instance.metadata_)

    found = definition.model.activity(job.element_id)
    element = found[2] if found is not None else definition.model.element(job.element_id)
    handler = runtime.registry.handler(job.task_type)
    if handler is None or not isinstance(element, ServiceTask | AdHocSubProcess):
        await _fail(runtime, job, f"no handler for task type {job.task_type!r} in this process")
        return

    state: object = variables
    if handler.state is not None:
        try:
            state = validate_state(handler.state, variables)
        except BadRequestError as exc:
            # Retrying cannot change the variables, so this fails the element at once.
            await _fail(runtime, job, exc.message)
            return

    ctx = Ctx(
        instance_id=job.instance_id,
        namespace=definition.namespace,
        key=definition.key,
        version=definition.version,
        element_id=job.element_id,
        task_type=job.task_type,
        attempt=job.attempts,
        max_attempts=job.max_attempts,
        metadata=metadata,
        activities=tuple(a.id for a in element.elements) if isinstance(element, AdHocSubProcess) else (),
    )
    ctx._store = store
    async with store.transaction() as session:
        _instances.emit(
            session, job.instance_id, "element_started", {"element": job.element_id, "attempt": job.attempts}
        )

    lost = asyncio.Event()
    body = asyncio.create_task(handler.fn(ctx, state), name=f"forktex_flow.job[{job.element_run_id}]")
    beat = asyncio.create_task(_heartbeat(runtime, job, body, lost))
    try:
        output = to_variables(await body)
    except asyncio.CancelledError:
        if lost.is_set():
            logger.info("job %s is no longer running; its handler was stopped", job.element_run_id)
            return
        await _jobs.release(store, job.element_run_id)
        raise
    except WorkflowCancelled as exc:
        async with store.transaction() as session:
            await _instances.cancel(session, job.instance_id, str(exc) or "cancelled by its handler")
        return
    except Exception as exc:
        await _failed(runtime, job, element, exc)
        return
    finally:
        beat.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await beat
    await _complete(runtime, job, output)


async def _complete(runtime: FlowRuntime, job: Job, output: JsonObject) -> None:
    async with runtime.store.transaction() as session:
        advance = Advance(runtime, session)
        await advance.complete(job.element_run_id, output)
    await notify(runtime, advance.finished)


async def _fail(runtime: FlowRuntime, job: Job, error: str) -> None:
    async with runtime.store.transaction() as session:
        advance = Advance(runtime, session)
        await advance.fail(job.element_run_id, error)
    await notify(runtime, advance.finished)


async def _failed(runtime: FlowRuntime, job: Job, element: ServiceTask | AdHocSubProcess, exc: Exception) -> None:
    error = "".join(traceback.format_exception(exc))
    if job.attempts >= job.max_attempts:
        await _fail(runtime, job, error)
        return
    # The last delay repeats once attempts run past the tuple, rather than collapsing back to the shortest.
    backoff = element.backoff or runtime.config.backoff
    delay = backoff[min(job.attempts - 1, len(backoff) - 1)]
    if await _jobs.retry_later(runtime.store, job.element_run_id, error, delay):
        async with runtime.store.transaction() as session:
            _instances.emit(
                session,
                job.instance_id,
                "element_retried",
                {"element": job.element_id, "attempt": job.attempts, "delay": delay, "error": str(exc)},
            )


async def _heartbeat(runtime: FlowRuntime, job: Job, body: asyncio.Task[object], lost: asyncio.Event) -> None:
    while True:
        await asyncio.sleep(runtime.config.heartbeat_interval)
        try:
            alive = await _jobs.heartbeat(runtime.store, job.element_run_id)
        except Exception:
            # A blip must not fail the job it watches; at worst the job looks stale and is reclaimed.
            logger.debug("heartbeat update failed", exc_info=True)
            continue
        if not alive:
            lost.set()
            body.cancel()
            return
