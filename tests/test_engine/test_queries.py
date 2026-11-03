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

"""Finding instances: the filters compose, pages never skip or repeat, and a stream ends on the terminal state."""

from __future__ import annotations

import pytest
import sqlalchemy as sa
from forktex import iso
from forktex.database import reflect
from forktex.error import BadRequestError

from forktex_flow import Activation, AdHocDecision, Flow, isolated_flow, make_ctx, run_to_completion
from forktex_flow.persist.models import InstanceEvent
from tests.test_engine.test_definitions import linear


@pytest.fixture
async def populated(flow: Flow) -> Flow:
    @flow.handler("work")
    async def work(ctx, variables):
        if variables.get("fail"):
            raise RuntimeError("no")
        return {"done": True}

    await flow.deploy(linear("orders", "work", retries=1))
    await flow.deploy(linear("refunds", "work", retries=1))
    for index in range(5):
        await run_to_completion(flow, "orders", variables={"n": index}, metadata={"shop": "a" if index % 2 else "b"})
    await run_to_completion(flow, "refunds", variables={"fail": True})
    await flow.start("orders", variables={"n": 99})
    return flow


@pytest.mark.asyncio
async def test_filters_narrow_by_key_status_metadata_variables_and_position(populated: Flow):
    query = populated.query

    assert await query().count() == 7
    assert await query().key("orders").count() == 6
    assert await query().status("failed").count() == 1
    assert await query().key("orders").metadata(shop="a").count() == 2
    assert await query().variables(n=3).count() == 1
    assert await query().current_element("work").count() == 1
    assert await query().triggered_by("schedule").count() == 0
    assert await query().since(iso.now()).count() == 0
    assert await query().until(iso.now()).count() == 7


@pytest.mark.asyncio
async def test_paging_visits_every_instance_once_newest_first(populated: Flow):
    seen, cursor = [], None
    while True:
        page = await populated.query().limit(3).fetch(cursor)
        seen.extend(instance.instance_id for instance in page.items)
        if not page.has_more:
            break
        cursor = page.next_cursor

    newest = await populated.query().first()
    assert len(seen) == len(set(seen)) == 7
    assert newest is not None and seen[0] == newest.instance_id


@pytest.mark.asyncio
async def test_a_summary_counts_by_status(populated: Flow):
    summary = await populated.query().summary()

    assert summary.total == 7
    assert summary.by_status == {"completed": 5, "failed": 1, "active": 1}
    assert summary.avg_duration_seconds is not None


def test_a_limit_must_be_positive(flow: Flow):
    with pytest.raises(BadRequestError):
        flow.query().limit(0)


@pytest.mark.asyncio
async def test_a_stream_yields_each_change_and_ends_on_the_terminal_snapshot(flow: Flow):
    @flow.handler("work")
    async def work(ctx, variables):
        return {}

    await flow.deploy(linear("k", "work"))
    done = await run_to_completion(flow, "k")

    snapshots = [snapshot async for snapshot in done.stream()]

    assert [s.status for s in snapshots] == ["completed"]


@pytest.mark.asyncio
async def test_a_handler_can_be_awaited_without_a_database():
    async def handler(ctx, variables):
        return {"seen": ctx.key, "attempt": ctx.attempt}

    ctx = make_ctx(key="orders", attempt=2)

    assert await handler(ctx, {}) == {"seen": "orders", "attempt": 2}
    with pytest.raises(BadRequestError):
        await ctx.emit("progress")


@pytest.mark.asyncio
async def test_a_decider_can_be_awaited_with_the_activities_it_may_choose():
    async def decide(ctx, variables):
        return AdHocDecision(activate=tuple(Activation(element_id=a) for a in ctx.activities))

    decision = await decide(make_ctx(namespace="org-1", activities=("search", "fetch")), {})

    assert [a.element_id for a in decision.activate] == ["search", "fetch"]


@pytest.mark.asyncio
async def test_a_handler_s_events_land_in_the_instance_log(flow: Flow):
    @flow.handler("work")
    async def work(ctx, variables):
        await ctx.emit("progress", {"percent": 50})
        return {}

    await flow.deploy(linear("k", "work"))
    done = await run_to_completion(flow, "k")

    async with flow.transaction() as session:
        rows = (
            await session.execute(
                sa.select(InstanceEvent.event_type, InstanceEvent.payload)
                .where(InstanceEvent.instance_id == done.instance_id)
                .order_by(InstanceEvent.id)
            )
        ).all()

    types = [row.event_type for row in rows]
    assert types[0] == "instance_started" and types[-1] == "instance_completed"
    assert ("progress", {"percent": 50}) in [(row.event_type, row.payload) for row in rows]


@pytest.mark.asyncio
async def test_isolated_flow_gives_a_throwaway_schema(postgres_url_str: str):
    async with isolated_flow(postgres_url_str) as flow:
        flow.handler("work")(lambda ctx, variables: _empty())
        await flow.deploy(linear("k", "work"))
        done = await run_to_completion(flow, "k")
        schema = flow.schema

    assert done.status == "completed"
    probe = Flow(postgres_url_str)
    async with probe.engine.connect() as conn:
        assert not await reflect.has_table(conn, "instance", schema=schema)
    await probe.close()


async def _empty() -> dict:
    return {}
