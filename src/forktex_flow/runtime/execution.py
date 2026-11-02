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
import hashlib
import json
import traceback
from datetime import date, datetime
from typing import TYPE_CHECKING
from uuid import UUID

from forktex.iso import to_date_iso, to_iso
from forktex.log import get_logger
from forktex.types import JsonObject

from forktex_flow.domain.state import apply_state_update
from forktex_flow.errors import (
    StateNotSerialisable,
    StepClaimLost,
    StepFailed,
    StepRetryRequested,
    WorkflowCancelled,
)
from forktex_flow.persist import runs as _runs
from forktex_flow.persist import steps as _steps
from forktex_flow.runtime.ctx import Ctx

if TYPE_CHECKING:
    from forktex_flow.domain.definition import NodeDef, WorkflowDefinition
    from forktex_flow.domain.types import RunInfo, TerminalHook
    from forktex_flow.extension import FlowExtension
    from forktex_flow.runtime.context import FlowRuntime

__all__ = ["execute_run"]

logger = get_logger(__name__)


async def execute_run(
    runtime: FlowRuntime,
    run_id: UUID,
    workflow_name: str,
    workflow_version: int,
) -> None:
    defn = runtime.registry.get(workflow_name, version=workflow_version)
    if defn is None:
        await _runs.update_run_status(
            runtime.store,
            run_id,
            status="failed",
            error=(
                f"workflow {workflow_name!r} version {workflow_version} is not registered "
                "in this process — refusing to dispatch"
            ),
        )
        return

    run_info = await _runs.fetch_run(runtime.store, run_id)
    if run_info is None:
        return  # the row was deleted between the claim and the dispatch

    # The run-level stamp covers what the step heartbeat cannot: between two
    # steps, before the first step row exists, and after the last one completes.
    alive = asyncio.create_task(_run_heartbeat(runtime, run_id), name=f"forktex_flow.run_heartbeat[{run_id}]")
    try:
        await _execute(runtime, run_id, defn, run_info)
    finally:
        alive.cancel()


async def _execute(
    runtime: FlowRuntime,
    run_id: UUID,
    defn: WorkflowDefinition,
    run_info: RunInfo,
) -> None:
    # Always start from run.input. run.output is written after every step for
    # observability, but it is NOT the resume point: resuming from a merged
    # state would feed a different state into the memoisation key and miss every
    # cached step, re-running side effects that already happened.
    state = dict(run_info.input or {})

    ctx = Ctx(
        _store=runtime.store,
        run_id=run_id,
        workflow_name=defn.name,
        workflow_version=defn.version,
        node_name="",
        attempt=0,
        metadata=dict(run_info.metadata),
    )

    try:
        await _runs.emit_run_event(runtime.store, run_id, "run_started", {})

        for index, node in enumerate(defn.nodes):
            await _raise_if_cancelled(runtime, run_id)
            delta = await _dispatch_node(runtime, ctx, node, state, index)
            state = apply_state_update(state, delta, defn.reducers)
            # One write per step, and it is not bookkeeping: `WorkflowInstance.state`
            # reads this column, so it is what `stream()` renders as a run
            # progresses. Without it a consumer watching a twelve-step deploy sees
            # the initial input until the very last step lands.
            await _runs.update_run_output(runtime.store, run_id, state)

        if not await _runs.update_run_status(runtime.store, run_id, status="completed", output=state):
            # The run reached a terminal status while this attempt was running —
            # a cancel, typically. It is not ours to announce as completed.
            logger.info("run %s finished in a terminal status already; not reporting completion", run_id)
            return
        await _runs.emit_run_event(runtime.store, run_id, "run_completed", {})
        await _notify(runtime.extensions, "after_complete", await _terminal(runtime, run_id, run_info), state)

    except StepClaimLost as exc:
        # Another executor owns the step this attempt tried to claim. Back off
        # in silence: whichever attempt won the compare-and-swap is responsible
        # for recording the outcome, and a status write here would race it.
        logger.info("run %s: step claim lost to a concurrent executor (%s); backing off", run_id, exc)

    except StepRetryRequested as exc:
        await _runs.update_run_status(runtime.store, run_id, status="pending")
        await _runs.emit_run_event(runtime.store, run_id, "run_pending_retry", {"reason": str(exc)})

    except WorkflowCancelled as exc:
        await _runs.update_run_status(runtime.store, run_id, status="cancelled", cancel_reason=str(exc))
        await _runs.emit_run_event(runtime.store, run_id, "run_cancelled", {"reason": str(exc)})
        await _notify(runtime.extensions, "after_fail", await _terminal(runtime, run_id, run_info), exc)

    except StepFailed as exc:
        await _runs.update_run_status(runtime.store, run_id, status="failed", error=str(exc))
        await _runs.emit_run_event(runtime.store, run_id, "run_failed", {"error": str(exc)})
        await _notify(runtime.extensions, "after_fail", await _terminal(runtime, run_id, run_info), exc)

    except Exception as exc:
        await _runs.update_run_status(runtime.store, run_id, status="failed", error=traceback.format_exc())
        await _runs.emit_run_event(runtime.store, run_id, "run_failed", {"error": str(exc)})
        await _notify(runtime.extensions, "after_fail", await _terminal(runtime, run_id, run_info), exc)


async def _raise_if_cancelled(runtime: FlowRuntime, run_id: UUID) -> None:
    info = await _runs.fetch_run(runtime.store, run_id)
    if info is not None and info.status == "cancelled":
        raise WorkflowCancelled("cancelled while running")


async def _dispatch_node(
    runtime: FlowRuntime,
    ctx: Ctx,
    node: NodeDef,
    state: JsonObject,
    index: int,
) -> JsonObject:
    store = runtime.store
    qualname = f"{ctx.workflow_name}:{node.name}"
    args_hash = _hash_state(state, ordinal=index)

    slot = await _steps.upsert_pending_step(
        store,
        run_id=ctx.run_id,
        step_name=node.name,
        step_qualname=qualname,
        step_index=index,
        args_hash=args_hash,
        max_attempts=node.max_attempts,
    )

    if slot.status == "completed":
        if slot.skipped:
            await _runs.emit_run_event(store, ctx.run_id, "node_skipped", {"node": node.name})
            return {}
        return slot.output if isinstance(slot.output, dict) else {}

    if node.when_fn is not None and not node.when_fn(state):
        await _steps.mark_step_running(store, slot.step_id)
        await _steps.mark_step_skipped(store, slot.step_id)
        await _runs.emit_run_event(store, ctx.run_id, "node_skipped", {"node": node.name})
        return {}

    await _steps.mark_step_running(store, slot.step_id)
    ctx.node_name = node.name
    ctx.attempt = await _steps.read_step_attempts(store, slot.step_id)

    await _runs.emit_run_event(store, ctx.run_id, "node_started", {"node": node.name, "step_id": str(slot.step_id)})

    heartbeat = asyncio.create_task(_heartbeat(runtime, slot.step_id))
    try:
        result = await node.fn(ctx, state)
        if result is None:
            result = {}
    except StepClaimLost:
        # Not this step's failure — see the handler in `_execute`.
        raise
    except WorkflowCancelled:
        await _steps.mark_step_failed(store, slot.step_id, "cancelled", final=True)
        raise
    except Exception as exc:
        await _record_failure(runtime, ctx, node, slot.step_id, exc)
        raise  # `_record_failure` always raises; this satisfies the type checker
    else:
        await _steps.mark_step_completed(store, slot.step_id, result)
        await _runs.emit_run_event(
            store, ctx.run_id, "node_completed", {"node": node.name, "step_id": str(slot.step_id)}
        )
        return result
    finally:
        heartbeat.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await heartbeat


async def _terminal(runtime: FlowRuntime, run_id: UUID, fallback: RunInfo) -> RunInfo:
    """The run as it ended, for a hook that is promised a terminal status. The snapshot taken before execution
    still says ``running``, and a hook that branches on status reads it as a run that never finished.
    """
    return await _runs.fetch_run(runtime.store, run_id) or fallback


async def _record_failure(
    runtime: FlowRuntime,
    ctx: Ctx,
    node: NodeDef,
    step_id: UUID,
    exc: Exception,
) -> None:
    """Mark a failed attempt and raise the outcome: retry, or final failure."""
    store = runtime.store
    tb = traceback.format_exc()
    attempts = await _steps.read_step_attempts(store, step_id)

    if attempts >= node.max_attempts:
        await _steps.mark_step_failed(store, step_id, tb, final=True)
        await _runs.emit_run_event(
            store,
            ctx.run_id,
            "node_failed",
            {"node": node.name, "attempts": attempts, "error": str(exc)},
        )
        raise StepFailed(f"step {node.name!r} failed after {attempts} attempts") from exc

    # The last backoff entry repeats once attempts run past the tuple's length,
    # rather than the delay collapsing back to the first (shortest) value.
    delay = node.backoff[min(attempts - 1, len(node.backoff) - 1)]
    next_at = await _steps.mark_step_failed(store, step_id, tb, final=False, retry_in_seconds=delay)
    await _runs.emit_run_event(
        store,
        ctx.run_id,
        "node_retried",
        {
            "node": node.name,
            "attempts": attempts,
            "next_attempt_at": None if next_at is None else to_iso(next_at),
            "error": str(exc),
        },
    )
    raise StepRetryRequested(f"step {node.name!r} retry scheduled (attempt {attempts}/{node.max_attempts})") from exc


async def _run_heartbeat(runtime: FlowRuntime, run_id: UUID) -> None:
    try:
        while True:
            await asyncio.sleep(runtime.config.heartbeat_interval)
            try:
                if not await _runs.heartbeat_run(runtime.store, run_id):
                    logger.info("run %s is no longer running; heartbeat stopping", run_id)
                    return
            except Exception:
                logger.debug("run heartbeat update failed", exc_info=True)
    except asyncio.CancelledError:
        return


async def _heartbeat(runtime: FlowRuntime, step_id: UUID) -> None:
    """Keep the step's ``heartbeat_at`` fresh until its body returns or raises."""
    try:
        while True:
            await asyncio.sleep(runtime.config.heartbeat_interval)
            try:
                if not await _steps.heartbeat_step(runtime.store, step_id):
                    logger.info("step %s is no longer running; heartbeat stopping", step_id)
                    return
            except Exception:
                # A blip here must not fail the step it is watching. Worst case
                # the step looks stale and gets reclaimed, which is recoverable.
                logger.debug("heartbeat update failed", exc_info=True)
    except asyncio.CancelledError:
        return


async def _notify(
    extensions: tuple[FlowExtension, ...],
    hook_name: TerminalHook,
    run_info: RunInfo,
    payload: object,
) -> None:
    """Call a lifecycle hook on every extension, isolating failures. ``hook_name`` is a :data:`TerminalHook` rather
    than a `str`: the dispatch is a `getattr` that treats a missing attribute as "this extension does not
    implement it": a misspelled name is indistinguishable from that, and silently notifies nobody.
    """
    for extension in extensions:
        hook = getattr(extension, hook_name, None)
        if hook is None:
            continue
        try:
            await hook(run_info, payload)
        except Exception:
            # An extension is an observer. It does not get to change the run's
            # outcome, so its failure is logged and swallowed here.
            logger.warning(
                "extension %s.%s raised; run outcome unaffected",
                type(extension).__name__,
                hook_name,
                exc_info=True,
            )


def _hash_state(state: JsonObject, *, ordinal: int) -> str:
    """The memoisation key for one step: its input state plus its position. State values must be JSON-serialisable
    — that is the durability constraint every engine of this kind has.
    """

    def _fallback(value: object) -> object:
        # Temporal values go through `iso`, not their own `.isoformat()`:
        # `to_iso` normalises to UTC first, so the same instant written with two
        # different offsets hashes the same. Without it a resume could miss a
        # cached step over a difference in the caller's tzinfo.
        # datetime before date — it is a subclass, and `to_iso` rejects a date.
        if isinstance(value, datetime):
            return to_iso(value)
        if isinstance(value, date):
            return to_date_iso(value)
        if isinstance(value, UUID):
            return str(value)
        raise StateNotSerialisable(
            f"state carries a {type(value).__name__}, which cannot be written to JSONB; "
            "carry an id and build the object inside the step body"
        )

    canonical = json.dumps({"state": state, "ordinal": ordinal}, sort_keys=True, default=_fallback)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
