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

"""Finding runs: filters, keyset paging, and the aggregate."""

from __future__ import annotations

import operator
from typing import Annotated, TypedDict

import pytest
from sqlalchemy import text

from forktex_flow import Ctx, Flow


class State(TypedDict, total=False):
    logs: Annotated[list[str], operator.add]
    tenant: str


async def _register(flow: Flow, name: str = "t.query") -> None:
    async def work(ctx: Ctx, state: State) -> dict:
        return {}

    flow.pipeline(name, version=1, state=State)(type("W", (), {"steps": [work]}))


@pytest.mark.asyncio
async def test_a_query_filters_by_workflow_and_status(flow: Flow):
    await _register(flow, "t.a")
    await _register(flow, "t.b")
    await flow.run("t.a", state={})
    await flow.run("t.a", state={})
    await flow.run("t.b", state={})

    page = await flow.query().workflow("t.a").fetch()
    assert len(page.items) == 2
    assert {i.workflow_name for i in page.items} == {"t.a"}

    assert await flow.query().status("pending").count() == 3
    assert await flow.query().status("completed").count() == 0


@pytest.mark.asyncio
async def test_metadata_containment_filters(flow: Flow):
    await _register(flow)
    await flow.run("t.query", state={}, metadata={"org_id": "acme", "kind": "deploy"})
    await flow.run("t.query", state={}, metadata={"org_id": "other"})

    page = await flow.query().metadata(org_id="acme").fetch()
    assert len(page.items) == 1
    assert page.items[0].metadata["kind"] == "deploy"


@pytest.mark.asyncio
async def test_paging_walks_every_run_exactly_once(flow: Flow):
    """The property a keyset pager exists for. A boundary that skips or repeats
    a row still returns plausible pages, so the assertion has to be over the
    union across pages, not over any one page."""
    await _register(flow)
    for _ in range(7):
        await flow.run("t.query", state={})

    seen: list[str] = []
    cursor: str | None = None
    while True:
        page = await flow.query().limit(3).fetch(cursor)
        seen.extend(str(i.instance_id) for i in page.items)
        if not page.has_more:
            break
        cursor = page.next_cursor

    assert len(seen) == 7
    assert len(set(seen)) == 7, "a page boundary repeated a row"


@pytest.mark.asyncio
async def test_the_keyset_predicate_is_an_index_cond_not_a_filter(flow: Flow):
    """`database:keyset-row-comparison`, asserted the only way it can be: an
    OR-chain returns the same rows, so only the plan tells them apart. A Filter
    here means the query re-reads the index from the start on every page and the
    cost grows with depth — correct results, degrading silently."""
    await _register(flow)
    for _ in range(3):
        await flow.run("t.query", state={})

    first = await flow.query().limit(1).fetch()
    cursor = first.next_cursor
    assert cursor is not None

    boundary = first.items[0]
    async with flow.session() as session:
        plan = (
            await session.execute(
                text(
                    f'EXPLAIN (COSTS OFF) SELECT id FROM "{flow.schema}".run '  # noqa: S608 - our own schema
                    "WHERE (started_at, id) < (:ts, :rid) "
                    "ORDER BY started_at DESC, id DESC LIMIT 1"
                ).bindparams(ts=boundary.started_at, rid=boundary.instance_id)
            )
        ).all()

    rendered = "\n".join(row[0] for row in plan)
    assert "Index Cond" in rendered, f"keyset predicate is not pushed into the index:\n{rendered}"


@pytest.mark.asyncio
async def test_first_returns_the_newest_matching_run(flow: Flow):
    await _register(flow)
    await flow.run("t.query", state={}, metadata={"n": 1})
    newest = await flow.run("t.query", state={}, metadata={"n": 2})

    found = await flow.query().first()
    assert found is not None
    assert found.instance_id == newest.instance_id


@pytest.mark.asyncio
async def test_first_returns_none_when_nothing_matches(flow: Flow):
    await _register(flow)
    assert await flow.query().workflow("t.nothing").first() is None


@pytest.mark.asyncio
async def test_the_summary_honours_the_same_filters_as_the_page(flow: Flow):
    """The bug the single-filter-source rewrite fixes: the summary branch built
    its own `where` list and had drifted, so `.metadata(...).summary()` silently
    aggregated over every run in the table."""
    await _register(flow)
    await flow.run("t.query", state={}, metadata={"org_id": "acme"})
    await flow.run("t.query", state={}, metadata={"org_id": "acme"})
    await flow.run("t.query", state={}, metadata={"org_id": "other"})

    scoped = await flow.query().metadata(org_id="acme").summary()
    assert scoped.total == 2
    assert scoped.by_status == {"pending": 2}

    everything = await flow.query().summary()
    assert everything.total == 3


@pytest.mark.asyncio
async def test_a_bad_cursor_is_rejected_rather_than_restarting_at_page_one(flow: Flow):
    from forktex.error import BadRequestError

    await _register(flow)
    with pytest.raises(BadRequestError):
        await flow.query().fetch("not-a-cursor")


@pytest.mark.asyncio
async def test_limit_must_be_positive(flow: Flow):
    from forktex.error import BadRequestError

    with pytest.raises(BadRequestError):
        flow.query().limit(0)


@pytest.mark.asyncio
async def test_a_builder_is_mutated_by_its_filters_not_branched(flow: Flow):
    """Documented, and worth pinning: a filter narrows the builder in place.

    A consumer who expects `base.status(...)` to yield a second query would
    silently get a narrowed `base` and one result set where they wanted two.
    """
    await _register(flow, "t.branch")
    await flow.run("t.branch", state={}, metadata={"kind": "a"})
    await flow.run("t.branch", state={}, metadata={"kind": "b"})

    base = flow.query().workflow("t.branch")
    narrowed = base.metadata(kind="a")

    assert narrowed is base, "filters return the same builder"
    assert await base.count() == 1, "narrowing the branch narrowed the base"


@pytest.mark.asyncio
async def test_a_builder_can_be_awaited_more_than_once(flow: Flow):
    """`count()` then `fetch()` over one filter set is the common shape, so a
    terminal method must not consume the builder."""
    await _register(flow, "t.reuse")
    await flow.run("t.reuse", state={})

    query = flow.query().workflow("t.reuse")
    assert await query.count() == 1
    page = await query.fetch()
    assert len(page.items) == 1
    assert await query.count() == 1
