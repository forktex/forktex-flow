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

"""The driver: leader election, dispatch, and the cron claim."""

from __future__ import annotations

import asyncio
import operator
from datetime import UTC, datetime, timedelta
from typing import Annotated, TypedDict

import pytest
from sqlalchemy import select, update

from forktex_flow import Ctx, Flow
from forktex_flow.persist.models import Run, ScheduledRun
from forktex_flow.runtime.context import FlowRuntime
from forktex_flow.runtime.driver import Driver


class State(TypedDict, total=False):
    logs: Annotated[list[str], operator.add]


def _driver(flow: Flow) -> Driver:
    return Driver(
        FlowRuntime(
            store=flow._store,
            registry=flow._registry,
            config=flow._config,
            extensions=flow.extensions,
        )
    )


@pytest.mark.asyncio
async def test_a_due_schedule_fires_exactly_once_under_two_competing_drivers(flow: Flow):
    """The bug this rewrite fixes.

    The previous claim SELECTed due rows, mutated the ORM objects and committed,
    while its docstring claimed `UPDATE … RETURNING` atomicity. Two drivers
    overlapping for one tick — which is exactly what leader handover is — both
    read the same due row and both fired it. The claim is now a compare-and-swap
    on `next_fire_at`, so precisely one of them matches a row.

    The row has to be backdated by hand: a freshly registered schedule's
    `next_fire_at` is always in the future, so a test that merely registers one
    and races two drivers asserts nothing at all.
    """

    async def scheduled_body(ctx: Ctx, state: State) -> dict:
        return {"logs": ["fired"]}

    flow.scheduled("t.cron", version=1, cron="* * * * *", state=State)(scheduled_body)

    first, second = _driver(flow), _driver(flow)
    await first._fire_due_schedules()  # creates the row, fires nothing yet

    async with flow.session() as session:
        assert (await session.execute(select(Run).where(Run.workflow_name == "t.cron"))).first() is None
        await session.execute(
            update(ScheduledRun)
            .where(ScheduledRun.workflow_name == "t.cron")
            .values(next_fire_at=datetime.now(UTC) - timedelta(minutes=5))
        )
        await session.commit()

    # Two leaders, same due row, at the same moment.
    await asyncio.gather(first._fire_due_schedules(), second._fire_due_schedules())

    async with flow.session() as session:
        runs = (await session.execute(select(Run).where(Run.workflow_name == "t.cron"))).scalars().all()

    assert len(runs) == 1, f"a due schedule fired {len(runs)} times; exactly one worker may win the claim"
    assert runs[0].triggered_by == "schedule"


@pytest.mark.asyncio
async def test_a_fired_schedule_advances_past_now_rather_than_backlogging(flow: Flow):
    """A leader that fell behind must not then work through every occurrence it
    slept past — the next fire is computed from now, not from the missed
    deadline."""

    async def body(ctx: Ctx, state: State) -> dict:
        return {}

    flow.scheduled("t.behind", version=1, cron="* * * * *", state=State)(body)
    driver = _driver(flow)
    await driver._fire_due_schedules()

    async with flow.session() as session:
        await session.execute(
            update(ScheduledRun)
            .where(ScheduledRun.workflow_name == "t.behind")
            .values(next_fire_at=datetime.now(UTC) - timedelta(hours=3))
        )
        await session.commit()

    await driver._fire_due_schedules()

    async with flow.session() as session:
        row = (await session.execute(select(ScheduledRun).where(ScheduledRun.workflow_name == "t.behind"))).scalar_one()
        runs = (await session.execute(select(Run).where(Run.workflow_name == "t.behind"))).scalars().all()

    assert len(runs) == 1, "three hours of missed minutes must not become 180 runs"
    assert row.next_fire_at > datetime.now(UTC) - timedelta(minutes=1)


@pytest.mark.asyncio
async def test_a_schedule_row_is_created_for_a_registered_cron_workflow(flow: Flow):
    async def body(ctx: Ctx, state: State) -> dict:
        return {}

    flow.scheduled("t.registered", version=2, cron="0 3 * * *", state=State)(body)
    await _driver(flow)._fire_due_schedules()

    async with flow.session() as session:
        row = (
            await session.execute(select(ScheduledRun).where(ScheduledRun.workflow_name == "t.registered"))
        ).scalar_one()

    assert row.cron == "0 3 * * *"
    assert row.enabled is True
    assert row.next_fire_at is not None


@pytest.mark.asyncio
async def test_editing_a_cron_in_code_updates_the_stored_schedule(flow: Flow):
    """A schedule row outlives the process that created it, so a cron changed in
    code has to reach the row — otherwise the old expression runs forever and
    the source of truth is a database nobody looks at."""

    async def body(ctx: Ctx, state: State) -> dict:
        return {}

    flow.scheduled("t.changing", version=1, cron="0 1 * * *", state=State)(body)
    await _driver(flow)._fire_due_schedules()

    revised = Flow(database=flow._db, schema=flow.schema)
    revised.scheduled("t.changing", version=1, cron="30 4 * * *", state=State)(body)
    await _driver(revised)._fire_due_schedules()

    async with flow.session() as session:
        row = (
            await session.execute(select(ScheduledRun).where(ScheduledRun.workflow_name == "t.changing"))
        ).scalar_one()

    assert row.cron == "30 4 * * *"


@pytest.mark.asyncio
async def test_a_schedule_registered_disabled_gets_a_row_and_does_not_fire(flow: Flow):
    """`enabled=False` registers the timer without starting it.

    The case this exists for is a cron whose first tick is destructive — a
    reaper that dispatches a destroy against expired infrastructure. Arming it
    the instant the process boots, and disarming afterwards, loses that race:
    the first tick can land before anyone runs the UPDATE.
    """

    async def body(ctx: Ctx, state: State) -> dict:
        return {"logs": ["fired"]}

    # A cron that is due every minute, so "did not fire" is a real assertion
    # rather than an artefact of nothing being due yet.
    flow.scheduled("t.disarmed", version=1, cron="* * * * *", state=State, enabled=False)(body)
    driver = _driver(flow)
    await driver._fire_due_schedules()
    await driver._fire_due_schedules()

    async with flow.session() as session:
        row = (
            await session.execute(select(ScheduledRun).where(ScheduledRun.workflow_name == "t.disarmed"))
        ).scalar_one()
        runs = (await session.execute(select(Run).where(Run.workflow_name == "t.disarmed"))).scalars().all()

    assert row.enabled is False, "the row exists but must not be armed"
    assert runs == [], "a disabled schedule dispatched a run"


@pytest.mark.asyncio
async def test_arming_a_schedule_survives_re_registration(flow: Flow):
    """Once armed, the database owns the switch.

    `enabled` is read on the first insert only. If re-registration wrote it
    back, every deploy would silently re-disarm a schedule an operator had
    turned on — or worse, re-arm one they had turned off, which is how a reaper
    comes back from the dead. The upsert updates `cron` and nothing else.
    """

    async def body(ctx: Ctx, state: State) -> dict:
        return {}

    flow.scheduled("t.armed", version=1, cron="0 1 * * *", state=State, enabled=False)(body)
    await _driver(flow)._fire_due_schedules()

    async with flow.session() as session:
        await session.execute(update(ScheduledRun).where(ScheduledRun.workflow_name == "t.armed").values(enabled=True))
        await session.commit()

    # A fresh process registering the same workflow — still declaring it
    # disabled, as the code always will.
    revised = Flow(database=flow._db, schema=flow.schema)
    revised.scheduled("t.armed", version=1, cron="0 2 * * *", state=State, enabled=False)(body)
    await _driver(revised)._fire_due_schedules()

    async with flow.session() as session:
        row = (await session.execute(select(ScheduledRun).where(ScheduledRun.workflow_name == "t.armed"))).scalar_one()

    assert row.enabled is True, "re-registration disarmed a schedule an operator had armed"
    assert row.cron == "0 2 * * *", "the cron should still track the code"


@pytest.mark.asyncio
async def test_the_driver_claims_and_completes_a_pending_run(flow: Flow):
    """The whole loop, once: a submitted run is claimed, dispatched, executed
    and marked completed without anyone calling the executor directly."""

    async def work(ctx: Ctx, state: State) -> dict:
        return {"logs": ["done"]}

    @flow.pipeline("t.driven", version=1, state=State)
    class Driven:
        steps = [work]

    instance = await flow.run("t.driven", state={"logs": []})

    driver = _driver(flow)
    await driver._tick()
    await asyncio.gather(*driver._tasks)

    final = await flow.get(instance.instance_id)
    assert final.status == "completed"
    assert final.state["logs"] == ["done"]


@pytest.mark.asyncio
async def test_only_one_of_two_drivers_becomes_leader(flow: Flow):
    """Leadership is a Postgres advisory lock, so this is really asserting that
    two Flows on one schema derive the same lock key."""
    first, second = _driver(flow), _driver(flow)
    first.shutdown.set()
    second.shutdown.set()

    results = await asyncio.gather(first._compete_for_leadership(), second._compete_for_leadership())
    assert results == [None, None]
    assert not (first.is_leader and second.is_leader)


@pytest.mark.asyncio
async def test_the_live_driver_loop_picks_up_and_completes_a_run(flow: Flow, postgres_url_str: str):
    """The whole thing running by itself: `start_driver()`, then a submitted run
    reaches `completed` with nobody calling the executor.

    Every other test drives phases directly so a failure points at the phase.
    This one asserts the loop that ties them together actually turns — election,
    tick, dispatch, drain — which is the part a consumer relies on and no unit
    test touches.
    """
    import uuid as _uuid

    from sqlalchemy import text

    from forktex_flow import RuntimeConfig

    schema = "forktex_flow_" + _uuid.uuid7().hex
    live = Flow(postgres_url_str, schema=schema, config=RuntimeConfig(poll_interval=0.05))
    try:
        await live.init()

        async def work(ctx: Ctx, state: State) -> dict:
            return {"logs": ["driven"]}

        live.pipeline("t.live", version=1, state=State)(type("W", (), {"steps": [work]}))
        instance = await live.run("t.live", state={"logs": []})

        await live.start_driver()
        try:
            final = await instance.wait(timeout=15)
        finally:
            await live.stop_driver()

        assert final.status == "completed", f"the driver loop did not complete the run: {final.status}"
        assert final.state["logs"] == ["driven"]
    finally:
        async with live.engine.begin() as conn:
            await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await live.close()


@pytest.mark.asyncio
async def test_a_second_worker_takes_over_when_the_leader_stops(flow: Flow, postgres_url_str: str):
    """Failover, end to end. The leader stops; the standby, which has been
    losing elections, wins the next one and drains the backlog. This is the
    property the advisory lock exists for, and the only test here that exercises
    both workers' loops at once.
    """
    import uuid as _uuid

    from sqlalchemy import text

    from forktex_flow import RuntimeConfig

    schema = "forktex_flow_" + _uuid.uuid7().hex
    # A short election interval so the takeover is prompt and the test is not
    # timing out on the default two-second backoff between elections.
    config = RuntimeConfig(poll_interval=0.05, election_interval=0.05)
    leader = Flow(postgres_url_str, schema=schema, config=config)
    standby = Flow(postgres_url_str, schema=schema, config=config)

    async def work(ctx: Ctx, state: State) -> dict:
        return {"logs": ["done"]}

    try:
        await leader.init()
        for engine in (leader, standby):
            engine.pipeline("t.failover", version=1, state=State)(type("W", (), {"steps": [work]}))

        await leader.start_driver()
        await standby.start_driver()
        await asyncio.sleep(0.3)  # let the election settle

        # Exactly one of them drives.
        assert not (leader._driver.is_leader and standby._driver.is_leader)

        # The leader goes away with work outstanding.
        instance = await standby.run("t.failover", state={"logs": []})
        await leader.stop_driver()
        await leader.close()

        final = await instance.wait(timeout=20)
        assert final.status == "completed", "the standby did not take over"
        assert final.state["logs"] == ["done"]
    finally:
        await standby.stop_driver()
        async with standby.engine.begin() as conn:
            await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await standby.close()
