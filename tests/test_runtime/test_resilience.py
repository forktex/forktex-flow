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
import operator
from datetime import UTC, datetime, timedelta
from typing import Annotated, TypedDict

import pytest

from forktex_flow import Ctx, Flow
from forktex_flow.errors import StepClaimLost
from forktex_flow.persist import runs as _runs
from forktex_flow.persist import steps as _steps
from forktex_flow.persist.models import StepRun
from forktex_flow.runtime.context import FlowRuntime
from forktex_flow.runtime.driver import Driver
from forktex_flow.runtime.execution import execute_run


class State(TypedDict, total=False):
    logs: Annotated[list[str], operator.add]


def _runtime(flow: Flow) -> FlowRuntime:
    return FlowRuntime(flow._store, flow._registry, flow._config, flow.extensions)


async def _register(flow: Flow, name: str, body) -> None:
    flow.pipeline(name, version=1, state=State)(type("W", (), {"steps": [body]}))


@pytest.mark.asyncio
async def test_a_step_returning_none_is_treated_as_an_empty_delta(flow: Flow):
    """A step that only causes a side effect has nothing to merge. Returning
    `None` must not blow up the merge or wipe the state."""

    async def silent(ctx: Ctx, state: State) -> dict:
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

    async def contested(ctx: Ctx, state: State) -> dict:
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

    async def work(ctx: Ctx, state: State) -> dict:
        return {"logs": ["done"]}

    await _register(flow, "t.stale", work)
    instance = await flow.run("t.stale", state={"logs": []})

    # The dead worker's trace: a claimed step, running, whose heartbeat stopped.
    # `mark_step_running` always stamps `heartbeat_at`, so a stale one is an old
    # timestamp, never NULL — backdate it rather than clearing it.
    async with flow.session() as session:
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
        await session.commit()
    await _runs.update_run_status(flow._store, instance.instance_id, status="running")

    reclaimed = await _steps.reclaim_stale_steps(flow._store, stale_threshold=60.0)
    assert reclaimed == 1, "a step whose heartbeat stopped ten minutes ago is abandoned"

    await _driver_for(flow)._revive_runs_with_pending_steps()
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
    driver._revive_runs_with_pending_steps = ok  # type: ignore[method-assign]

    await driver._tick()

    assert calls == ["boom", "ok", "ok"], "a raising phase must not abort the tick"


@pytest.mark.asyncio
async def test_stop_driver_is_idempotent_and_safe_before_start(flow: Flow):
    """Called from a shutdown handler that may run twice, or after a startup
    that failed before the driver began."""
    await flow.stop_driver()
    await flow.start_driver()
    await flow.stop_driver()
    await flow.stop_driver()


@pytest.mark.asyncio
async def test_draining_returns_when_there_is_nothing_in_flight(flow: Flow):
    driver = _driver_for(flow)
    await driver._drain_tasks()


@pytest.mark.asyncio
async def test_draining_times_out_rather_than_hanging_shutdown(flow: Flow):
    """A step that will not finish must not hold the process open forever."""
    driver = _driver_for(flow)

    async def never() -> None:
        await asyncio.sleep(60)

    task = asyncio.create_task(never())
    driver._tasks.add(task)
    try:
        await driver._drain_tasks(timeout=0.05)
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

    async def work(ctx: Ctx, state: State) -> dict:
        return {}

    await _register(flow, "t.inflight", work)
    instance = await flow.run("t.inflight", state={})
    driver = _driver_for(flow)

    holder = asyncio.create_task(asyncio.sleep(5))
    driver._in_flight[instance.instance_id] = holder  # type: ignore[assignment]
    try:
        driver._dispatch(instance.instance_id, "t.inflight", 1)
        assert driver._tasks == set(), "a run already in flight must not be dispatched again"
    finally:
        holder.cancel()
