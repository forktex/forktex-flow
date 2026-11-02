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

"""The paths that only run when something has already gone wrong.

A guard no test exercises is a guard that may not work, and these are the guards
that stand between a transient fault and a run that is silently lost or silently
executed twice.
"""

from __future__ import annotations

import asyncio
import contextlib
from datetime import UTC, datetime, timedelta

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine
from forktex_flow.domain.types import RunClaim, RunInfo
from forktex_flow.errors import FlowClosed
from tests.conftest import LoggingState, until
from sqlalchemy import select, update

from forktex_flow import Ctx, Flow, RuntimeConfig, step
from forktex_flow.errors import StepClaimLost
from forktex_flow.persist import runs as _runs
from forktex_flow.persist import steps as _steps
from forktex_flow.persist.models import Run, StepRun
from forktex_flow.runtime.context import FlowRuntime
from forktex_flow.runtime.driver import Driver
from forktex_flow.runtime.execution import execute_run


def _runtime(flow: Flow) -> FlowRuntime:
    return FlowRuntime(flow._store, flow._registry, flow._config, flow.extensions)


async def _register(flow: Flow, name: str, body) -> None:
    flow.pipeline(name, version=1, state=LoggingState)(type("W", (), {"steps": [body]}))


@pytest.mark.asyncio
async def test_a_step_returning_none_is_treated_as_an_empty_delta(flow: Flow):
    """A step that only causes a side effect has nothing to merge. Returning
    `None` must not blow up the merge or wipe the state."""

    async def silent(ctx: Ctx, state: LoggingState) -> dict:
        return None  # type: ignore[return-value]

    await _register(flow, "t.none", silent)
    instance = await flow.run("t.none", state={"logs": ["seed"]})
    await execute_run(_runtime(flow), instance.instance_id, "t.none", 1)

    final = await flow.get(instance.instance_id)
    assert final.status == "completed"
    assert final.state["logs"] == ["seed"]


@pytest.mark.asyncio
async def test_a_lost_step_claim_leaves_the_run_untouched(flow: Flow):
    """Another worker already owns this step. This attempt must abandon quietly:
    whichever attempt won the compare-and-swap is responsible for recording the
    outcome, and a status write here would race it."""

    async def contested(ctx: Ctx, state: LoggingState) -> dict:
        raise StepClaimLost("another executor holds this step")

    await _register(flow, "t.contested", contested)
    instance = await flow.run("t.contested", state={"logs": []})
    before = (await flow.get(instance.instance_id)).status

    await execute_run(_runtime(flow), instance.instance_id, "t.contested", 1)

    after = (await flow.get(instance.instance_id)).status
    assert after == before, f"a lost claim must leave the run's status alone, got {before!r} -> {after!r}"
    assert after not in {"failed", "completed", "cancelled"}


@pytest.mark.asyncio
async def test_a_stale_step_is_reclaimed_and_the_run_revived(flow: Flow):
    """The recovery path for a worker that died mid-step: the heartbeat goes
    stale, the step returns to `pending`, and the run — still `running` from the
    dead attempt — is swept back so it can be claimed again."""

    async def work(ctx: Ctx, state: LoggingState) -> dict:
        return {"logs": ["done"]}

    await _register(flow, "t.stale", work)
    instance = await flow.run("t.stale", state={"logs": []})

    # The dead worker's trace: a claimed step, running, whose heartbeat stopped.
    # `mark_step_running` always stamps `heartbeat_at`, so a stale one is an old
    # timestamp, never NULL — backdate it rather than clearing it.
    async with flow.transaction() as session:
        session.add(
            StepRun(
                run_id=instance.instance_id,
                step_name="work",
                step_qualname="t.stale:work",
                step_index=0,
                args_hash="x",
                status="running",
                max_attempts=3,
                heartbeat_at=datetime.now(UTC) - timedelta(minutes=10),
            )
        )
    await _runs.update_run_status(flow._store, instance.instance_id, status="running")

    reclaimed = await _steps.reclaim_stale_steps(flow._store, stale_threshold=60.0)
    assert reclaimed == 1, "a step whose heartbeat stopped ten minutes ago is abandoned"

    await _driver_for(flow)._revive_abandoned_runs()
    assert (await flow.get(instance.instance_id)).status == "pending", (
        "a run holding a reclaimed step must be swept back so the next tick re-claims it"
    )


def _driver_for(flow: Flow) -> Driver:
    return Driver(_runtime(flow))


@pytest.mark.asyncio
async def test_a_failing_tick_phase_does_not_stop_the_others(flow: Flow):
    """Each phase recovers a different kind of stuck work, so one throwing must
    not cost the tick the other two."""
    driver = _driver_for(flow)
    calls: list[str] = []

    async def boom() -> None:
        calls.append("boom")
        raise RuntimeError("phase failed")

    async def ok() -> None:
        calls.append("ok")

    driver._fire_due_schedules = boom  # type: ignore[method-assign]
    driver._reclaim_stale = ok  # type: ignore[method-assign]
    driver._revive_abandoned_runs = ok  # type: ignore[method-assign]

    await driver._tick()

    assert calls == ["boom", "ok", "ok"], "a raising phase must not abort the tick"


@pytest.mark.asyncio
async def test_stop_driver_is_idempotent_and_safe_before_start(flow: Flow):
    """Called from a shutdown handler that may run twice, or after a startup
    that failed before the driver began."""
    await flow.stop_driver()
    await flow.start_driver()
    assert flow._driver is not None
    await flow.stop_driver()
    await flow.stop_driver()
    assert flow._driver is None, "a stopped driver was left attached"


@pytest.mark.asyncio
async def test_draining_returns_when_there_is_nothing_in_flight(flow: Flow):
    """Shutdown with an idle driver must not wait out the drain timeout."""
    driver = _driver_for(flow)
    started = asyncio.get_running_loop().time()
    await driver._drain_tasks()
    assert asyncio.get_running_loop().time() - started < 1.0, "an empty drain waited anyway"


@pytest.mark.asyncio
async def test_draining_times_out_rather_than_hanging_shutdown(flow: Flow):
    """A step that will not finish must not hold the process open forever.

    `wait_for` cancels the gather it waits on, so giving up also cancels the
    runs: they are interrupted mid-step and reclaimed later, not left running.
    """
    driver = _driver_for(flow)

    async def never() -> None:
        await asyncio.sleep(60)

    task = asyncio.create_task(never())
    driver._tasks.add(task)
    try:
        started = asyncio.get_running_loop().time()
        await driver._drain_tasks(timeout=0.05)
        elapsed = asyncio.get_running_loop().time() - started
        assert elapsed < 5.0, f"the drain waited {elapsed:.1f}s on a task that never finishes"
        assert task.cancelled(), "a run the drain gave up on was left running"
    finally:
        task.cancel()


@pytest.mark.asyncio
async def test_a_second_start_driver_does_not_start_a_second_loop(flow: Flow):
    await flow.start_driver()
    first = flow._driver_task
    await flow.start_driver()
    assert flow._driver_task is first
    await flow.stop_driver()


@pytest.mark.asyncio
async def test_a_run_reclaimed_while_still_in_flight_is_not_dispatched_twice(flow: Flow):
    """The guard against the worst outcome in the system: the same step body
    running concurrently with itself because a heartbeat merely lagged."""

    async def work(ctx: Ctx, state: LoggingState) -> dict:
        return {}

    await _register(flow, "t.inflight", work)
    instance = await flow.run("t.inflight", state={})
    driver = _driver_for(flow)

    holder = asyncio.create_task(asyncio.sleep(5))
    driver._in_flight[instance.instance_id] = holder  # type: ignore[assignment]
    try:
        driver._dispatch(RunClaim(run_id=instance.instance_id, workflow_name="t.inflight", workflow_version=1))
        assert driver._tasks == set(), "a run already in flight must not be dispatched again"
    finally:
        holder.cancel()


@pytest.mark.asyncio
async def test_two_concurrent_start_driver_calls_leave_exactly_one_driver(flow: Flow) -> None:
    """`start_driver` checked `_driver_task`, then awaited `init()` — a yield
    point — then assigned. Both callers passed the guard and the first task was
    orphaned while still holding the leader lock, so neither `stop_driver()` nor
    `close()` could ever stop it."""
    # return_exceptions: two drivers racing one pool raise from Postgres, and
    # the task count is what this test is about.
    await asyncio.gather(flow.start_driver(), flow.start_driver(), return_exceptions=True)

    started = [t for t in asyncio.all_tasks() if t.get_name().startswith("forktex_flow.driver")]
    await flow.stop_driver()
    still = [t for t in started if not t.done()]

    assert len(started) == 1, f"{len(started)} driver tasks after two concurrent starts"
    assert still == [], f"a driver survived stop_driver(): {[t.get_name() for t in still]}"


@pytest.mark.asyncio
async def test_a_run_whose_worker_died_between_steps_is_recovered(flow: Flow) -> None:
    """Recovery could only see a run through its steps, so a run was invisible
    between two steps, before its first step row existed, and after its last
    step completed. A worker dying in any of those windows stranded it
    `running` forever."""

    async def only(ctx: Ctx, state: dict) -> dict:
        return {}

    flow.pipeline("strand.recover", version=1)(type("W", (), {"steps": [only]}))
    instance = await flow.run("strand.recover", version=1)
    run_id = instance.instance_id

    # exactly the state a worker leaves behind when it dies after claiming and
    # before writing any step row
    claimed = await _runs.claim_pending_runs(flow._store, limit=10)
    assert [c.run_id for c in claimed] == [run_id]
    async with flow.transaction() as session:
        await session.execute(
            update(Run).where(Run.id == run_id).values(heartbeat_at=sa.func.now() - timedelta(seconds=3600))
        )

    driver = Driver(FlowRuntime(flow._store, flow._registry, flow._config, flow.extensions))
    await driver._revive_abandoned_runs()

    async with flow.transaction() as session:
        row = (await session.execute(select(Run).where(Run.id == run_id))).scalar_one()
    assert row.status == "pending", "a run with no live step was left stranded"

    assert [c.run_id for c in await _runs.claim_pending_runs(flow._store, limit=10)] == [run_id]


@pytest.mark.asyncio
async def test_a_leader_whose_lock_was_released_stands_down(postgres_url_str: str) -> None:
    """The lock was taken once and never revalidated, so a terminated backend,
    a Postgres restart or a pooler recycle silently dropped leadership while the
    old leader kept ticking — two workers both believing they led.

    Its own lock key: every other Flow shares the default one, so a shared key
    would make the terminate below hit another test's connection.
    """
    import uuid as _uuid

    key = int.from_bytes(_uuid.uuid7().bytes[:4], "big") % (2**31)
    schema = "forktex_flow_" + _uuid.uuid7().hex
    flow = Flow(postgres_url_str, schema=schema, config=RuntimeConfig(leader_lock_key=key))
    try:
        await flow.init()
        driver = _driver_for(flow)
        held = asyncio.create_task(driver._compete_for_leadership())
        await asyncio.wait_for(until(lambda: driver.is_leader), timeout=5)
        assert await driver._still_leader()

        # From a throwaway engine, disposed immediately: issuing this on the
        # flow's own pool leaves the terminated connection in it, and a later
        # checkout of it fails with "can't reconnect until the invalid
        # transaction is rolled back".
        executioner = create_async_engine(postgres_url_str)
        try:
            async with executioner.begin() as conn:
                terminated = await conn.execute(
                    sa.text(
                        "SELECT pg_terminate_backend(pid) FROM pg_locks "
                        "WHERE locktype = 'advisory' AND objid = :k AND pid <> pg_backend_pid()"
                    ),
                    {"k": key},
                )
                assert terminated.first() is not None, "no backend held the lock, so nothing was tested"
        finally:
            await executioner.dispose()

        assert not await driver._still_leader(), "the driver still believes it holds a released lock"

        driver.shutdown.set()
        with contextlib.suppress(Exception):
            await asyncio.wait_for(held, timeout=5)
    finally:
        await flow.close()


@pytest.mark.asyncio
async def test_a_failure_hook_is_given_the_run_as_it_ended(postgres_url_str: str) -> None:
    """`after_fail` was handed the snapshot taken before execution, whose status
    is still `running`, so an extension branching on it saw a run that never
    finished.
    """
    import uuid as _uuid

    seen: list[str] = []

    class Watcher:
        async def after_fail(self, run: RunInfo, error: BaseException) -> None:
            seen.append(run.status)

    async def boom(ctx: Ctx, state: LoggingState) -> dict:
        raise RuntimeError("no")

    flow = Flow(
        postgres_url_str,
        schema="forktex_flow_" + _uuid.uuid7().hex,
        extensions=[Watcher()],  # type: ignore[list-item]
    )
    try:
        await flow.init()
        flow.pipeline("t.hookstatus", version=1, state=LoggingState)(
            type("W", (), {"steps": [step(boom, max_attempts=1)]})
        )
        await flow.run("t.hookstatus", state={"logs": []})
        await flow.start_driver()
        try:
            await until(lambda: bool(seen), timeout=20)
        finally:
            await flow.stop_driver()
    finally:
        await flow.close()

    assert seen == ["failed"], f"the hook saw {seen}, not the status the run ended in"


@pytest.mark.asyncio
async def test_a_closed_flow_refuses_to_be_used(postgres_url_str: str) -> None:
    """`close()` disposes the pool but left the handle working, so `run()`
    succeeded afterwards — SQLAlchemy builds a fresh pool on demand, which
    leaks one nothing will dispose.
    """
    import uuid as _uuid

    flow = Flow(postgres_url_str, schema="forktex_flow_" + _uuid.uuid7().hex)
    await flow.init()
    await flow.close()

    for call in (flow.run("t.whatever", state={}), flow.init(), flow.start_driver()):
        with pytest.raises(FlowClosed):
            await call
