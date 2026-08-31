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

"""Running one workflow to completion, durably.

The contract is the one DBOS, Temporal and Restate share: a run is executed from
the top every time it is picked up — first dispatch or post-crash resume — and
each step consults ``step_run`` for a completed row before doing anything. A hit
returns the recorded output without re-running the body, so side effects happen
once; a miss runs the step and records what it returned.

That is what makes resume free: the state at step N is rebuilt by replaying the
recorded outputs of steps 0..N-1 through the reducers, so it is byte-identical
to the first attempt's, and the memoisation key therefore still matches.

This file was called ``replay.py``, which read as an optional feature. It is the
executor.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import traceback
from datetime import date, datetime
from typing import TYPE_CHECKING, Any
from uuid import UUID

from forktex.iso import to_date_iso, to_iso
from forktex.log import get_logger

from forktex_flow.domain.state import apply_state_update
from forktex_flow.errors import StepClaimLost, StepFailed, StepRetryRequested, WorkflowCancelled
from forktex_flow.persist import runs as _runs
from forktex_flow.persist import steps as _steps
from forktex_flow.runtime.ctx import Ctx

if TYPE_CHECKING:
    from forktex_flow.domain.definition import NodeDef, WorkflowDefinition
    from forktex_flow.domain.types import RunInfo
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
    """Drive one run from its first step to a terminal status."""
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

    await _execute(runtime, run_id, defn, run_info)


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

        await _runs.update_run_status(runtime.store, run_id, status="completed", output=state)
        await _runs.emit_run_event(runtime.store, run_id, "run_completed", {})
        await _notify(runtime.extensions, "after_complete", run_info, state)

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
        await _notify(runtime.extensions, "after_fail", run_info, exc)

    except StepFailed as exc:
        await _runs.update_run_status(runtime.store, run_id, status="failed", error=str(exc))
        await _runs.emit_run_event(runtime.store, run_id, "run_failed", {"error": str(exc)})
        await _notify(runtime.extensions, "after_fail", run_info, exc)

    except Exception as exc:
        await _runs.update_run_status(runtime.store, run_id, status="failed", error=traceback.format_exc())
        await _runs.emit_run_event(runtime.store, run_id, "run_failed", {"error": str(exc)})
        await _notify(runtime.extensions, "after_fail", run_info, exc)


async def _raise_if_cancelled(runtime: FlowRuntime, run_id: UUID) -> None:
    """Stop a run that was cancelled while it was executing.

    Cancellation is cooperative and checked between steps: a step already
    running is left to finish, because the library has no way to interrupt
    arbitrary consumer code and pretending otherwise would leave half-finished
    side effects with no record of them.

    One extra read per step, on a loop that already writes once per step. The
    alternative — no check at all — is what made `WorkflowInstance.cancel()`
    silently ineffective against any run the driver had already picked up.
    """
    info = await _runs.fetch_run(runtime.store, run_id)
    if info is not None and info.status == "cancelled":
        raise WorkflowCancelled("cancelled while running")


async def _dispatch_node(
    runtime: FlowRuntime,
    ctx: Ctx,
    node: NodeDef,
    state: dict[str, Any],
    index: int,
) -> dict[str, Any]:
    """Run one step, or return what it returned last time.

    ``index`` is the step's position in the workflow, which is what makes the
    memoisation key deterministic across resumes: the same step in the same
    pipeline always carries the same ordinal, without a mutable counter whose
    value depends on how far the previous attempt got.
    """
    store = runtime.store
    qualname = f"{ctx.workflow_name}:{node.name}"
    args_hash = _hash_state(state, ordinal=index)

    step_id, status, cached_output = await _steps.upsert_pending_step(
        store,
        run_id=ctx.run_id,
        step_name=node.name,
        step_qualname=qualname,
        step_index=index,
        args_hash=args_hash,
        max_attempts=node.max_attempts,
    )

    if status == "completed":
        if cached_output == _steps.SKIPPED_OUTPUT:
            await _runs.emit_run_event(store, ctx.run_id, "node_skipped", {"node": node.name})
            return {}
        return cached_output if isinstance(cached_output, dict) else {}

    if node.when_fn is not None and not node.when_fn(state):
        await _steps.mark_step_running(store, step_id)
        await _steps.mark_step_skipped(store, step_id)
        await _runs.emit_run_event(store, ctx.run_id, "node_skipped", {"node": node.name})
        return {}

    await _steps.mark_step_running(store, step_id)
    ctx.node_name = node.name
    ctx.attempt = await _steps.read_step_attempts(store, step_id)

    await _runs.emit_run_event(store, ctx.run_id, "node_started", {"node": node.name, "step_id": str(step_id)})

    heartbeat = asyncio.create_task(_heartbeat(runtime, step_id))
    try:
        result = await node.fn(ctx, state)
        if result is None:
            result = {}
    except StepClaimLost:
        # Not this step's failure — see the handler in `_execute`.
        raise
    except WorkflowCancelled:
        await _steps.mark_step_failed(store, step_id, "cancelled", final=True)
        raise
    except Exception as exc:
        await _record_failure(runtime, ctx, node, step_id, exc)
        raise  # `_record_failure` always raises; this satisfies the type checker
    else:
        await _steps.mark_step_completed(store, step_id, result)
        await _runs.emit_run_event(store, ctx.run_id, "node_completed", {"node": node.name, "step_id": str(step_id)})
        return result
    finally:
        heartbeat.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await heartbeat


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


async def _heartbeat(runtime: FlowRuntime, step_id: UUID) -> None:
    """Keep the step's ``heartbeat_at`` fresh until its body returns or raises.

    A step that stops heartbeating for longer than ``stale_threshold`` is
    reclaimed by the driver and re-dispatched, so this loop is what tells the
    rest of the system that a long step is alive rather than abandoned.
    """
    try:
        while True:
            await asyncio.sleep(runtime.config.heartbeat_interval)
            try:
                await _steps.heartbeat_step(runtime.store, step_id)
            except Exception:
                # A blip here must not fail the step it is watching. Worst case
                # the step looks stale and gets reclaimed, which is recoverable.
                logger.debug("heartbeat update failed", exc_info=True)
    except asyncio.CancelledError:
        return


async def _notify(
    extensions: tuple[FlowExtension, ...],
    hook_name: str,
    run_info: RunInfo,
    payload: object,
) -> None:
    """Call a lifecycle hook on every extension, isolating failures.

    Awaited, not fire-and-forget. The previous version scheduled these on the
    loop without holding a reference, so a hook could be garbage-collected
    before it ran and was guaranteed to be lost if the process exited straight
    after the run finished — which is exactly when `after_complete` fires.
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


def _hash_state(state: dict[str, Any], *, ordinal: int) -> str:
    """The memoisation key for one step: its input state plus its position.

    State values must be JSON-serialisable — that is the durability constraint
    every engine of this kind has. A step that needs a session or a client
    builds it inside its own body from ids carried in state.
    """

    def _fallback(value: object) -> object:
        # Temporal values go through `iso`, not their own `.isoformat()`:
        # `to_iso` normalises to UTC first, so the same instant written with two
        # different offsets hashes the same. Without it a resume could miss a
        # cached step purely because the caller's tzinfo differed.
        # datetime before date — it is a subclass, and `to_iso` rejects a date.
        if isinstance(value, datetime):
            return to_iso(value)
        if isinstance(value, date):
            return to_date_iso(value)
        if isinstance(value, UUID):
            return str(value)
        return str(value)

    canonical = json.dumps({"state": state, "ordinal": ordinal}, sort_keys=True, default=_fallback)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
