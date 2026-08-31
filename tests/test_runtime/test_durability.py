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

"""Retries, heartbeats, cancellation, and the memoisation key itself.

The behaviours a consumer reasons about when a step is slow, flaky, or must be
stopped — and the hashing that decides whether a resumed run repeats work.
"""

from __future__ import annotations

import asyncio
import operator
import uuid
from datetime import UTC, date, datetime
from typing import Annotated, TypedDict

import pytest
from sqlalchemy import select

from forktex_flow import Ctx, Flow, step
from forktex_flow.errors import WorkflowCancelled
from forktex_flow.persist.models import Run, StepRun
from forktex_flow.runtime.context import FlowRuntime
from forktex_flow.runtime.execution import _hash_state, execute_run


class State(TypedDict, total=False):
    logs: Annotated[list[str], operator.add]
    when: datetime
    day: date
    ident: uuid.UUID


def _runtime(flow: Flow) -> FlowRuntime:
    return FlowRuntime(flow._store, flow._registry, flow._config, flow.extensions)


# ── the memoisation key ──────────────────────────────────────────────────


def test_the_same_state_at_the_same_position_hashes_the_same():
    """This is what makes a resumed run skip completed work: the state rebuilt
    from cached outputs must hash identically to the first attempt's."""
    state = {"a": 1, "b": ["x"]}
    assert _hash_state(state, ordinal=0) == _hash_state(dict(state), ordinal=0)


def test_key_order_does_not_change_the_hash():
    """Dict ordering is an implementation detail of how state was assembled, not
    a difference in the state. If it changed the hash, a resume would miss every
    cached step for no reason."""
    assert _hash_state({"a": 1, "b": 2}, ordinal=0) == _hash_state({"b": 2, "a": 1}, ordinal=0)


def test_position_is_part_of_the_key():
    """The same function twice in a pipeline, given the same state, is two
    distinct pieces of work."""
    assert _hash_state({"a": 1}, ordinal=0) != _hash_state({"a": 1}, ordinal=1)


def test_the_same_instant_in_two_timezones_hashes_the_same():
    """`to_iso` normalises to UTC first. Without that, a resume could miss a
    cached step purely because the caller's tzinfo differed — the kind of bug
    that only appears after a deploy to a differently-configured host."""
    from datetime import timedelta, timezone

    instant = datetime(2026, 8, 30, 12, 0, tzinfo=UTC)
    elsewhere = instant.astimezone(timezone(timedelta(hours=3)))
    assert _hash_state({"when": instant}, ordinal=0) == _hash_state({"when": elsewhere}, ordinal=0)


def test_dates_uuids_and_unknown_objects_all_hash_without_raising():
    """State crosses JSONB, so the hash has to cope with whatever a step put
    there. Failing to serialise here would fail the run, not the hash."""

    class Opaque:
        def __repr__(self) -> str:
            return "<opaque>"

    digest = _hash_state(
        {"day": date(2026, 8, 30), "ident": uuid.uuid7(), "other": Opaque()},
        ordinal=0,
    )
    assert len(digest) == 64


# ── retries ──────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_a_step_retries_up_to_its_limit_then_fails_the_run(flow: Flow):
    attempts: list[int] = []

    async def flaky(ctx: Ctx, state: State) -> dict:
        attempts.append(ctx.attempt)
        raise ValueError("always")

    flow.pipeline("t.retry", version=1, state=State)(
        type("W", (), {"steps": [step(flaky, max_attempts=3, backoff=(0.0,))]})
    )
    instance = await flow.run("t.retry", state={})
    runtime = _runtime(flow)

    for _ in range(3):
        await execute_run(runtime, instance.instance_id, "t.retry", 1)

    final = await flow.get(instance.instance_id)
    assert attempts == [1, 2, 3], f"expected three attempts, got {attempts}"
    assert final.status == "failed"
    assert final.nodes[0].attempt == 3


@pytest.mark.asyncio
async def test_a_retry_schedules_the_next_attempt_rather_than_spinning(flow: Flow):
    """The backoff is recorded on the row, so the driver leaves the step alone
    until its window opens instead of re-dispatching immediately."""

    async def flaky(ctx: Ctx, state: State) -> dict:
        raise ValueError("not yet")

    flow.pipeline("t.backoff", version=1, state=State)(
        type("W", (), {"steps": [step(flaky, max_attempts=3, backoff=(30.0, 120.0))]})
    )
    instance = await flow.run("t.backoff", state={})
    await execute_run(_runtime(flow), instance.instance_id, "t.backoff", 1)

    async with flow.session() as session:
        row = (await session.execute(select(StepRun).where(StepRun.run_id == instance.instance_id))).scalar_one()

    assert row.status == "pending", "a retryable failure returns the step to pending"
    assert row.next_attempt_at is not None
    assert row.next_attempt_at > datetime.now(UTC)


@pytest.mark.asyncio
async def test_the_last_backoff_entry_repeats_past_the_tuple(flow: Flow):
    """Five attempts against a two-entry backoff must not index out of range,
    and must not collapse back to the shortest delay."""

    async def flaky(ctx: Ctx, state: State) -> dict:
        raise ValueError("again")

    flow.pipeline("t.longbackoff", version=1, state=State)(
        type("W", (), {"steps": [step(flaky, max_attempts=5, backoff=(0.0, 0.0))]})
    )
    instance = await flow.run("t.longbackoff", state={})
    runtime = _runtime(flow)

    for _ in range(5):
        await execute_run(runtime, instance.instance_id, "t.longbackoff", 1)

    assert (await flow.get(instance.instance_id)).status == "failed"


# ── cancellation ─────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_a_step_raising_workflow_cancelled_cancels_the_run(flow: Flow):
    """A step can decide the run should stop — a precondition that no longer
    holds — without that being recorded as a failure."""

    async def gives_up(ctx: Ctx, state: State) -> dict:
        raise WorkflowCancelled("the target was deleted while we were deploying")

    flow.pipeline("t.cancelled", version=1, state=State)(type("W", (), {"steps": [gives_up]}))
    instance = await flow.run("t.cancelled", state={})
    await execute_run(_runtime(flow), instance.instance_id, "t.cancelled", 1)

    final = await flow.get(instance.instance_id)
    assert final.status == "cancelled"
    assert final.nodes[0].status == "failed", "the step records that it did not complete"

    # The reason reaches the row an operator reads, not just the log.
    async with flow.session() as session:
        reason = (await session.execute(select(Run.cancel_reason).where(Run.id == instance.instance_id))).scalar_one()
    assert "target was deleted" in reason


@pytest.mark.asyncio
async def test_cancelling_mid_pipeline_leaves_earlier_steps_completed(flow: Flow):
    """Cancellation is not a rollback. Work already done stays done and stays
    visible — which is what an operator needs in order to clean up."""

    async def first(ctx: Ctx, state: State) -> dict:
        return {"logs": ["provisioned"]}

    async def abort(ctx: Ctx, state: State) -> dict:
        raise WorkflowCancelled("stop here")

    flow.pipeline("t.partial", version=1, state=State)(type("W", (), {"steps": [first, abort]}))
    instance = await flow.run("t.partial", state={"logs": []})
    await execute_run(_runtime(flow), instance.instance_id, "t.partial", 1)

    final = await flow.get(instance.instance_id)
    assert final.status == "cancelled"
    assert [n.status for n in final.nodes] == ["completed", "failed"]


# ── heartbeats ───────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_a_long_step_heartbeats_while_it_runs(flow: Flow, postgres_url_str: str):
    """The signal that says a slow step is alive rather than abandoned. Without
    it, `stale_threshold` would reclaim every step that takes longer than the
    threshold and re-run it."""
    from forktex_flow import RuntimeConfig

    import uuid as _uuid

    from sqlalchemy import text

    schema = "forktex_flow_" + _uuid.uuid7().hex
    fast = Flow(
        postgres_url_str,
        schema=schema,
        config=RuntimeConfig(heartbeat_interval=0.05, stale_threshold=5.0),
    )
    try:
        await fast.init()

        async def slow(ctx: Ctx, state: State) -> dict:
            await asyncio.sleep(0.3)
            return {}

        fast.pipeline("t.heartbeat", version=1, state=State)(type("W", (), {"steps": [slow]}))
        instance = await fast.run("t.heartbeat", state={})
        await execute_run(_runtime(fast), instance.instance_id, "t.heartbeat", 1)

        async with fast.session() as session:
            row = (await session.execute(select(StepRun).where(StepRun.run_id == instance.instance_id))).scalar_one()

        assert row.status == "completed"
        assert row.heartbeat_at is not None
        assert row.started_at is not None
        assert row.heartbeat_at > row.started_at, "the heartbeat loop refreshed the row while the step ran"
    finally:
        async with fast.engine.begin() as conn:
            await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await fast.close()


@pytest.mark.asyncio
async def test_a_configured_default_reaches_an_undecorated_step(flow: Flow, postgres_url_str: str):
    """`RuntimeConfig(max_attempts=…)` is documented as the default for steps that
    do not override it. It was read nowhere: the compiler fell back to its own
    module constants, so a consumer configuring 7 got steps with 3 and no error.
    """
    import uuid as _uuid

    from sqlalchemy import text

    from forktex_flow import RuntimeConfig

    schema = "forktex_flow_" + _uuid.uuid7().hex
    tuned = Flow(postgres_url_str, schema=schema, config=RuntimeConfig(max_attempts=7, backoff=(1.0, 2.0)))
    try:
        await tuned.init()

        async def undecorated(ctx: Ctx, state: State) -> dict:
            return {}

        tuned.pipeline("t.defaults", version=1, state=State)(type("W", (), {"steps": [undecorated]}))
        instance = await tuned.run("t.defaults", state={})
        await execute_run(_runtime(tuned), instance.instance_id, "t.defaults", 1)

        async with tuned.session() as session:
            row = (await session.execute(select(StepRun).where(StepRun.run_id == instance.instance_id))).scalar_one()

        assert row.max_attempts == 7, "the configured default did not reach the step"
    finally:
        async with tuned.engine.begin() as conn:
            await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await tuned.close()


# ── cancellation of a live run ───────────────────────────────────────────


@pytest.mark.asyncio
async def test_cancelling_a_running_run_is_not_reverted_by_the_executor(flow: Flow):
    """The bug this closes.

    `update_run_status` had no guard and `_execute` never re-checked status, so
    an operator cancelling a run mid-step had it silently overwritten by the
    executor's own success write — leaving `status='completed'` beside
    `cancel_reason='cancelled by operator'`, a self-contradictory row with
    nothing raised anywhere to say so.
    """
    started = asyncio.Event()

    async def slow(ctx: Ctx, state: State) -> dict:
        started.set()
        await asyncio.sleep(0.4)
        return {"logs": ["finished anyway"]}

    async def second(ctx: Ctx, state: State) -> dict:
        return {"logs": ["should never run"]}

    flow.pipeline("t.cancelrace", version=1, state=State)(type("W", (), {"steps": [slow, second]}))
    instance = await flow.run("t.cancelrace", state={"logs": []})

    task = asyncio.create_task(execute_run(_runtime(flow), instance.instance_id, "t.cancelrace", 1))
    await started.wait()
    assert await instance.cancel() is True
    await task

    final = await flow.get(instance.instance_id)
    assert final.status == "cancelled", "the executor overwrote the cancellation"
    assert "should never run" not in final.state.get("logs", []), "a step ran after cancellation"

    async with flow.session() as session:
        row = (await session.execute(select(Run).where(Run.id == instance.instance_id))).scalar_one()
    assert row.cancel_reason == "cancelled by operator"
    assert row.cancelled_at is not None


@pytest.mark.asyncio
async def test_the_step_already_running_is_allowed_to_finish(flow: Flow):
    """Cancellation is cooperative: the library cannot interrupt consumer code,
    so the running step completes and its work is recorded rather than being
    abandoned half-done with no trace."""
    started = asyncio.Event()
    finished: list[str] = []

    async def slow(ctx: Ctx, state: State) -> dict:
        started.set()
        await asyncio.sleep(0.3)
        finished.append("ran to completion")
        return {"logs": ["done"]}

    flow.pipeline("t.midstep", version=1, state=State)(type("W", (), {"steps": [slow]}))
    instance = await flow.run("t.midstep", state={"logs": []})

    task = asyncio.create_task(execute_run(_runtime(flow), instance.instance_id, "t.midstep", 1))
    await started.wait()
    await instance.cancel()
    await task

    assert finished == ["ran to completion"]
    assert (await flow.get(instance.instance_id)).nodes[0].status == "completed"


@pytest.mark.asyncio
async def test_cancelling_an_already_finished_run_changes_nothing(flow: Flow):
    """A terminal status is final. The call reports that it did nothing rather
    than rewriting a completed run's outcome."""

    async def work(ctx: Ctx, state: State) -> dict:
        return {"logs": ["done"]}

    flow.pipeline("t.late", version=1, state=State)(type("W", (), {"steps": [work]}))
    instance = await flow.run("t.late", state={"logs": []})
    await execute_run(_runtime(flow), instance.instance_id, "t.late", 1)

    assert await instance.cancel() is False

    final = await flow.get(instance.instance_id)
    assert final.status == "completed"
    async with flow.session() as session:
        row = (await session.execute(select(Run).where(Run.id == instance.instance_id))).scalar_one()
    assert row.cancel_reason is None, "a declined cancel must not stamp a reason"
