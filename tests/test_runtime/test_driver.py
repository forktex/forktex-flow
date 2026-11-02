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

"""The driver's leader election and dispatch. Cron is `test_schedules.py`."""

from __future__ import annotations

import asyncio
import contextlib

import pytest

from forktex_flow import Ctx, Flow, RuntimeConfig
from tests.conftest import LoggingState, until
from forktex_flow.runtime.context import FlowRuntime
from forktex_flow.runtime.driver import Driver


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
async def test_the_driver_claims_and_completes_a_pending_run(flow: Flow):
    """The whole loop, once: a submitted run is claimed, dispatched, executed
    and marked completed without anyone calling the executor directly."""

    async def work(ctx: Ctx, state: LoggingState) -> dict:
        return {"logs": ["done"]}

    @flow.pipeline("t.driven", version=1, state=LoggingState)
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
    """Mutual exclusion while both are live.

    Setting `shutdown` before competing makes this vacuous: each loop exits at
    once and the `finally` clears `is_leader`, so both end up False and
    `not (first and second)` holds however the lock behaved. Both have to be
    inside the lock at the same instant for the assertion to mean anything.
    """
    first, second = _driver(flow), _driver(flow)
    assert first.runtime.config.leader_lock_key == second.runtime.config.leader_lock_key

    racing = [
        asyncio.create_task(first._compete_for_leadership()),
        asyncio.create_task(second._compete_for_leadership()),
    ]
    try:
        await until(lambda: first.is_leader or second.is_leader)
        assert first.is_leader != second.is_leader, "two drivers held the same lock at once"
        assert await (first if first.is_leader else second)._still_leader()
        assert not await (second if first.is_leader else first)._still_leader()
    finally:
        first.shutdown.set()
        second.shutdown.set()
        for task in racing:
            with contextlib.suppress(Exception):
                await asyncio.wait_for(task, timeout=5)


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

        async def work(ctx: Ctx, state: LoggingState) -> dict:
            return {"logs": ["driven"]}

        live.pipeline("t.live", version=1, state=LoggingState)(type("W", (), {"steps": [work]}))
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

    async def work(ctx: Ctx, state: LoggingState) -> dict:
        return {"logs": ["done"]}

    try:
        await leader.init()
        for engine in (leader, standby):
            engine.pipeline("t.failover", version=1, state=LoggingState)(type("W", (), {"steps": [work]}))

        await leader.start_driver()
        await standby.start_driver()

        # Exactly one of them drives. Asserting only that they are not *both*
        # leader passes trivially while neither has won yet, so wait for one.
        await until(lambda: leader._driver.is_leader or standby._driver.is_leader)
        assert leader._driver.is_leader != standby._driver.is_leader

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


@pytest.mark.asyncio
async def test_verify_probes_the_pool_without_touching_the_schema(postgres_url_str: str) -> None:
    """A readiness probe must not migrate: `init()` owns the schema, `verify()`
    only answers whether the pool can reach Postgres."""
    import uuid as _uuid

    from sqlalchemy import text

    from forktex.database import DatabaseUnreachableError

    schema = "forktex_flow_" + _uuid.uuid7().hex
    unmigrated = Flow(postgres_url_str, schema=schema)
    try:
        await unmigrated.verify()
        async with unmigrated.engine.connect() as conn:
            found = await conn.execute(
                text("SELECT 1 FROM information_schema.schemata WHERE schema_name = :s"), {"s": schema}
            )
            assert found.first() is None, "verify() created the schema it was only meant to probe"
    finally:
        await unmigrated.close()

    unreachable = Flow("postgresql+asyncpg://nobody:nobody@127.0.0.1:1/nothing", schema="probe")
    try:
        with pytest.raises(DatabaseUnreachableError, match="not reachable"):
            await unreachable.verify()
    finally:
        await unreachable.close()


@pytest.mark.asyncio
async def test_the_driver_does_not_claim_past_its_concurrency_cap(flow: Flow) -> None:
    """The batch bounded one claim, not the total. Ten runs were claimed every
    tick regardless of how many were still executing, so a backlog of slow runs
    accumulated in flight at the batch size per second.
    """
    gate = asyncio.Event()

    async def blocks(ctx: Ctx, state: LoggingState) -> dict:
        await gate.wait()
        return {"logs": ["done"]}

    flow.pipeline("t.cap", version=1, state=LoggingState)(type("W", (), {"steps": [blocks]}))
    driver = Driver(
        FlowRuntime(
            store=flow._store,
            registry=flow._registry,
            config=RuntimeConfig(max_concurrent_runs=2),
            extensions=flow.extensions,
        )
    )

    for _ in range(5):
        await flow.run("t.cap", state={"logs": []})

    try:
        await driver._claim_and_dispatch()
        await driver._claim_and_dispatch()
        assert len(driver._in_flight) == 2, f"claimed {len(driver._in_flight)} with a cap of 2"
    finally:
        gate.set()
        driver.shutdown.set()
        await driver._drain_tasks(timeout=10)


@pytest.mark.asyncio
async def test_two_flows_on_different_schemas_elect_independently(postgres_url_str: str) -> None:
    """Leader election is one advisory lock, and the key was folded from the
    literal `forktex_flow` rather than the instance's schema — so two unrelated
    tenants shared a leader and one of them never drove, its schedules never
    firing. A custom `RuntimeConfig` was worse: the default key only applied
    when no config was passed, leaving the key at 0 for everyone else.
    """
    import uuid as _uuid

    a = Flow(postgres_url_str, schema="tenant_a_" + _uuid.uuid7().hex[:12])
    b = Flow(postgres_url_str, schema="tenant_b_" + _uuid.uuid7().hex[:12])
    tuned = Flow(
        postgres_url_str,
        schema="tenant_c_" + _uuid.uuid7().hex[:12],
        config=RuntimeConfig(poll_interval=0.5),
    )
    keys = {a._config.leader_lock_key, b._config.leader_lock_key, tuned._config.leader_lock_key}
    assert len(keys) == 3, f"schemas share a lock key: {keys}"
    assert 0 not in keys, "a lock key was left at the derive sentinel"

    try:
        await a.init()
        await b.init()
        await a.start_driver()
        await b.start_driver()
        await until(lambda: a._driver.is_leader and b._driver.is_leader)
    finally:
        await a.close()
        await b.close()
        await tuned.close()
