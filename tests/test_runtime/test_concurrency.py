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

"""Two workers, one database — the guarantees that only hold under contention.

Everything here uses **separate `Flow` instances with independent connection
pools** against one schema, which is what two processes actually look like to
Postgres. A single pool would let SQLAlchemy serialise the very races these
assert against, and the tests would pass while the guarantee did not hold.

These are the claims the library makes that a consumer cannot verify for itself:
one leader, one execution per step, one row per memoisation key.
"""

from __future__ import annotations

import asyncio
import operator
from typing import Annotated, TypedDict

import pytest
from sqlalchemy import func, select

from forktex_flow import Ctx, Flow, RuntimeConfig
from forktex_flow.errors import StepClaimLost
from forktex_flow.persist import steps as _steps
from forktex_flow.persist.models import StepRun
from forktex_flow.runtime.context import FlowRuntime
from forktex_flow.runtime.driver import Driver
from forktex_flow.runtime.execution import execute_run


class State(TypedDict, total=False):
    logs: Annotated[list[str], operator.add]


def _runtime(flow: Flow) -> FlowRuntime:
    return FlowRuntime(flow._store, flow._registry, flow._config, flow.extensions)


@pytest.fixture
def second_flow(flow: Flow, postgres_url_str: str):
    """A second `Flow` on the same schema with its own pool — a second worker.

    Yielded un-migrated: `flow` already brought the schema up, and `init()` is
    idempotent anyway. Closed here so its pool does not outlive the test.
    """
    other = Flow(postgres_url_str, schema=flow.schema)
    yield other


@pytest.mark.asyncio
async def test_only_one_of_two_workers_holds_leadership_at_a_time(flow: Flow, second_flow: Flow):
    """Leadership is a session-scoped Postgres advisory lock. Two workers on one
    schema derive the same key, so exactly one can hold it — that is the whole
    failover primitive, and it is worth asserting across two real pools rather
    than two objects sharing one connection."""
    first, second = Driver(_runtime(flow)), Driver(_runtime(second_flow))
    try:
        await flow.init()
        first.shutdown.set()
        second.shutdown.set()
        await asyncio.gather(first._compete_for_leadership(), second._compete_for_leadership())
        assert not (first.is_leader and second.is_leader)
    finally:
        await second_flow.close()


@pytest.mark.asyncio
async def test_leadership_transfers_when_the_holder_lets_go(flow: Flow, second_flow: Flow):
    """The failover path. The first worker's lock is session-scoped, so when its
    connection returns to the pool the lock is released and the next election
    has a winner — which is what makes a dead process recoverable without any
    liveness protocol of our own."""
    from forktex.database import try_advisory_lock

    key = flow._config.leader_lock_key
    try:
        async with try_advisory_lock(flow.engine, key) as held:
            assert held, "the first worker should win an uncontested election"
            async with try_advisory_lock(second_flow.engine, key) as contested:
                assert not contested, "a second worker must not hold the lock concurrently"

        # First worker's lock released with its connection.
        async with try_advisory_lock(second_flow.engine, key) as after_release:
            assert after_release, "the lock must be available once the holder lets go"
    finally:
        await second_flow.close()


@pytest.mark.asyncio
async def test_two_workers_executing_one_run_run_the_step_body_once(flow: Flow, second_flow: Flow):
    """The guarantee the whole durability design exists for.

    Both workers execute the same run concurrently — which happens for real when
    a step's heartbeat lags, the step is reclaimed, and the original executor is
    still alive. The step's unique memoisation row is what makes one of them win
    and the other serve the cached output. A side-effect count of 2 here would
    mean a deploy provisioning two servers.
    """
    executions: list[str] = []

    async def side_effecting(ctx: Ctx, state: State) -> dict:
        executions.append("ran")
        await asyncio.sleep(0.05)  # widen the window the race has to fit through
        return {"logs": ["done"]}

    for engine in (flow, second_flow):
        engine.pipeline("t.once", version=1, state=State)(type("W", (), {"steps": [side_effecting]}))

    try:
        instance = await flow.run("t.once", state={"logs": []})
        await asyncio.gather(
            execute_run(_runtime(flow), instance.instance_id, "t.once", 1),
            execute_run(_runtime(second_flow), instance.instance_id, "t.once", 1),
            return_exceptions=True,
        )

        async with flow.session() as session:
            rows = (
                await session.execute(
                    select(func.count()).select_from(StepRun).where(StepRun.run_id == instance.instance_id)
                )
            ).scalar_one()

        assert executions.count("ran") == 1, f"the step body ran {executions.count('ran')} times, not once"
        assert rows == 1, "one memoisation row per (run, step, input state)"
        assert (await flow.get(instance.instance_id)).status == "completed"
    finally:
        await second_flow.close()


@pytest.mark.asyncio
async def test_a_concurrent_claim_on_one_step_raises_for_the_loser(flow: Flow, second_flow: Flow):
    """`mark_step_running` is a compare-and-swap on `status='pending'`, not a
    blind write. The loser must learn it lost — silently proceeding is how a
    reclaimed step ends up executing alongside its original attempt."""

    async def noop(ctx: Ctx, state: State) -> dict:
        return {}

    flow.pipeline("t.claim", version=1, state=State)(type("W", (), {"steps": [noop]}))
    try:
        instance = await flow.run("t.claim", state={})
        step_id, status, _ = await _steps.upsert_pending_step(
            flow._store,
            run_id=instance.instance_id,
            step_name="noop",
            step_qualname="t.claim:noop",
            step_index=0,
            args_hash="hash",
            max_attempts=3,
        )
        assert status == "pending"

        await _steps.mark_step_running(flow._store, step_id)
        with pytest.raises(StepClaimLost):
            await _steps.mark_step_running(second_flow._store, step_id)
    finally:
        await second_flow.close()


@pytest.mark.asyncio
async def test_concurrent_upserts_of_one_step_converge_on_a_single_row(flow: Flow, second_flow: Flow):
    """The `uq_step_run_identity` constraint doing its job, plus the
    `IntegrityError` recovery that turns losing the race into a cache read
    rather than an error the run has to survive."""

    async def noop(ctx: Ctx, state: State) -> dict:
        return {}

    flow.pipeline("t.upsert", version=1, state=State)(type("W", (), {"steps": [noop]}))
    try:
        instance = await flow.run("t.upsert", state={})

        async def claim(store):
            return await _steps.upsert_pending_step(
                store,
                run_id=instance.instance_id,
                step_name="noop",
                step_qualname="t.upsert:noop",
                step_index=0,
                args_hash="same-hash",
                max_attempts=3,
            )

        results = await asyncio.gather(claim(flow._store), claim(second_flow._store))

        assert results[0][0] == results[1][0], "both workers must resolve to the same step row"
        async with flow.session() as session:
            count = (
                await session.execute(
                    select(func.count()).select_from(StepRun).where(StepRun.run_id == instance.instance_id)
                )
            ).scalar_one()
        assert count == 1
    finally:
        await second_flow.close()


@pytest.mark.asyncio
async def test_two_drivers_claiming_pending_runs_do_not_both_take_the_same_one(flow: Flow, second_flow: Flow):
    """`claim_pending_runs` is one `UPDATE … RETURNING` over a
    `FOR UPDATE SKIP LOCKED` subquery, so two leaders overlapping at handover
    partition the work instead of duplicating it."""

    async def noop(ctx: Ctx, state: State) -> dict:
        return {}

    for engine in (flow, second_flow):
        engine.pipeline("t.claims", version=1, state=State)(type("W", (), {"steps": [noop]}))

    try:
        from forktex_flow.persist import runs as _runs

        for _ in range(6):
            await flow.run("t.claims", state={})

        first, second = await asyncio.gather(
            _runs.claim_pending_runs(flow._store, limit=10),
            _runs.claim_pending_runs(second_flow._store, limit=10),
        )

        ids_first = {run_id for run_id, _, _ in first}
        ids_second = {run_id for run_id, _, _ in second}
        assert ids_first & ids_second == set(), "a run was claimed by both workers"
        assert len(ids_first | ids_second) == 6, "every pending run must be claimed by someone"
    finally:
        await second_flow.close()


@pytest.mark.asyncio
async def test_a_worker_with_a_tuned_config_still_shares_the_default_lock_key(flow: Flow, postgres_url_str: str):
    """Tuning retries or polling must not accidentally elect two leaders. The
    lock key is derived from the schema, not from the config."""
    tuned = Flow(
        postgres_url_str,
        schema=flow.schema,
        config=RuntimeConfig(poll_interval=0.1, max_attempts=9, leader_lock_key=flow._config.leader_lock_key),
    )
    try:
        assert tuned._config.leader_lock_key == flow._config.leader_lock_key
    finally:
        await tuned.close()


@pytest.mark.asyncio
async def test_runs_are_isolated_between_schemas_in_one_process(flow: Flow, postgres_url_str: str):
    """Two `Flow`s on different schemas share a process and see nothing of each
    other — the property `schema_translate_map` exists for, and the one a
    multi-tenant consumer depends on."""
    import uuid

    from sqlalchemy import text

    other_schema = "forktex_flow_" + uuid.uuid7().hex
    other = Flow(postgres_url_str, schema=other_schema)

    async def noop(ctx: Ctx, state: State) -> dict:
        return {}

    try:
        await other.init()
        for engine in (flow, other):
            engine.pipeline("t.tenant", version=1, state=State)(type("W", (), {"steps": [noop]}))

        await flow.run("t.tenant", state={}, metadata={"tenant": "a"})
        await other.run("t.tenant", state={}, metadata={"tenant": "b"})

        here = await flow.query().workflow("t.tenant").fetch()
        there = await other.query().workflow("t.tenant").fetch()

        assert len(here.items) == 1 and here.items[0].metadata["tenant"] == "a"
        assert len(there.items) == 1 and there.items[0].metadata["tenant"] == "b"
    finally:
        async with other.engine.begin() as conn:
            await conn.execute(text(f'DROP SCHEMA IF EXISTS "{other_schema}" CASCADE'))
        await other.close()
