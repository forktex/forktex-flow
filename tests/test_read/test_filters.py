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

"""Every filter the query builder offers, exercised against real SQL.

A filter that compiles but selects the wrong rows is the failure mode here —
nothing raises, the page just quietly contains the wrong runs. So each of these
asserts on *which* runs came back, not merely that a query ran.
"""

from __future__ import annotations

import asyncio
import operator
from datetime import UTC, datetime, timedelta
from typing import Annotated, TypedDict

import pytest

from forktex_flow import Ctx, Flow
from forktex_flow.runtime.context import FlowRuntime
from forktex_flow.runtime.execution import execute_run


class State(TypedDict, total=False):
    logs: Annotated[list[str], operator.add]
    region: str


async def _register(flow: Flow, name: str = "t.filter") -> None:
    async def work(ctx: Ctx, state: State) -> dict:
        return {}

    flow.pipeline(name, version=1, state=State)(type("W", (), {"steps": [work]}))


@pytest.mark.asyncio
async def test_state_containment_filters_on_the_run_input(flow: Flow):
    """`.state(**kv)` matches against `run.input` — the state a run *started*
    with, which is what a consumer searching for "the deploy of server X" has."""
    await _register(flow)
    await flow.run("t.filter", state={"region": "eu", "logs": []})
    await flow.run("t.filter", state={"region": "us", "logs": []})

    page = await flow.query().state(region="eu").fetch()
    assert len(page.items) == 1
    assert page.items[0].state["region"] == "eu"


@pytest.mark.asyncio
async def test_triggered_by_separates_manual_runs_from_scheduled_ones(flow: Flow):
    await _register(flow)
    await flow.run("t.filter", state={})

    async def cron_body(ctx: Ctx, state: State) -> dict:
        return {}

    flow.scheduled("t.cron", version=1, cron="* * * * *", state=State)(cron_body)
    from sqlalchemy import update

    from forktex_flow.persist.models import ScheduledRun
    from forktex_flow.runtime.driver import Driver

    driver = Driver(FlowRuntime(flow._store, flow._registry, flow._config, flow.extensions))
    await driver._fire_due_schedules()
    async with flow.session() as session:
        await session.execute(update(ScheduledRun).values(next_fire_at=datetime.now(UTC) - timedelta(minutes=5)))
        await session.commit()
    await driver._fire_due_schedules()

    assert await flow.query().triggered_by("manual").count() == 1
    assert await flow.query().triggered_by("schedule").count() == 1
    assert await flow.query().triggered_by("manual", "schedule").count() == 2


@pytest.mark.asyncio
async def test_since_and_until_bound_the_window(flow: Flow):
    await _register(flow)
    await flow.run("t.filter", state={})
    boundary = datetime.now(UTC)
    await asyncio.sleep(0.01)
    await flow.run("t.filter", state={})

    assert await flow.query().since(boundary).count() == 1
    assert await flow.query().until(boundary).count() == 1
    assert await flow.query().since(boundary).until(datetime.now(UTC)).count() == 1


@pytest.mark.asyncio
async def test_current_node_finds_runs_stopped_inside_a_named_step(flow: Flow):
    """The filter an operator reaches for when a deploy is wedged: which runs are
    sitting in `provision_vps` right now?"""

    async def slow(ctx: Ctx, state: State) -> dict:
        return {}

    flow.pipeline("t.node", version=1, state=State)(type("W", (), {"steps": [slow]}))
    instance = await flow.run("t.node", state={})

    from forktex_flow.persist import steps as _steps

    step_id, _, _ = await _steps.upsert_pending_step(
        flow._store,
        run_id=instance.instance_id,
        step_name="slow",
        step_qualname="t.node:slow",
        step_index=0,
        args_hash="h",
        max_attempts=3,
    )
    await _steps.mark_step_running(flow._store, step_id)

    assert await flow.query().current_node("slow").count() == 1
    assert await flow.query().current_node("other").count() == 0


@pytest.mark.asyncio
async def test_workflow_version_narrows_within_a_name(flow: Flow):
    """Two versions of one workflow coexist — a run started under v1 keeps
    executing v1's steps — so a consumer must be able to ask about one."""

    async def work(ctx: Ctx, state: State) -> dict:
        return {}

    flow.pipeline("t.versioned", version=1, state=State)(type("V1", (), {"steps": [work]}))
    flow.pipeline("t.versioned", version=2, state=State)(type("V2", (), {"steps": [work]}))

    await flow.run("t.versioned", state={}, version=1)
    await flow.run("t.versioned", state={}, version=2)
    await flow.run("t.versioned", state={})  # unversioned -> highest

    assert await flow.query().workflow("t.versioned").count() == 3
    assert await flow.query().workflow("t.versioned", version=1).count() == 1
    assert await flow.query().workflow("t.versioned", version=2).count() == 2


@pytest.mark.asyncio
async def test_filters_compose_rather_than_replacing_each_other(flow: Flow):
    """A fluent builder that dropped an earlier filter would return a superset —
    plausible-looking, and wrong."""
    await _register(flow)
    await flow.run("t.filter", state={"region": "eu"}, metadata={"org": "acme"})
    await flow.run("t.filter", state={"region": "eu"}, metadata={"org": "other"})
    await flow.run("t.filter", state={"region": "us"}, metadata={"org": "acme"})

    both = await flow.query().state(region="eu").metadata(org="acme").count()
    assert both == 1


@pytest.mark.asyncio
async def test_repeated_metadata_calls_accumulate(flow: Flow):
    await _register(flow)
    await flow.run("t.filter", state={}, metadata={"org": "acme", "env": "prod"})
    await flow.run("t.filter", state={}, metadata={"org": "acme", "env": "dev"})

    assert await flow.query().metadata(org="acme").metadata(env="prod").count() == 1


@pytest.mark.asyncio
async def test_the_summary_of_an_empty_match_is_zeroed_not_an_error(flow: Flow):
    """A dashboard querying a workflow that has never run must render, not 500."""
    await _register(flow)
    summary = await flow.query().workflow("t.never").summary()
    assert summary.total == 0
    assert summary.by_status == {}
    assert summary.avg_duration_seconds is None
    assert summary.oldest_started_at is None


@pytest.mark.asyncio
async def test_the_summary_reports_durations_once_runs_finish(flow: Flow):
    await _register(flow)
    instance = await flow.run("t.filter", state={})
    await execute_run(
        FlowRuntime(flow._store, flow._registry, flow._config, flow.extensions),
        instance.instance_id,
        "t.filter",
        1,
    )

    summary = await flow.query().workflow("t.filter").summary()
    assert summary.total == 1
    assert summary.by_status == {"completed": 1}
    assert summary.avg_duration_seconds is not None
    assert summary.avg_duration_seconds >= 0


@pytest.mark.asyncio
async def test_current_node_matches_the_step_name_exactly(flow: Flow):
    """It used to splice the caller's string into a `LIKE` pattern
    (`step_qualname LIKE '%:' || name`), so `%` matched every running step and a
    name that was the tail of another workflow's step matched across workflows.

    `%` and `_` are the two LIKE metacharacters; both must be inert here, and a
    partial name must not match.
    """
    from forktex_flow.persist import steps as _steps

    async def validate_input(ctx: Ctx, state: State) -> dict:
        return {}

    flow.pipeline("t.exact", version=1, state=State)(type("W", (), {"steps": [validate_input]}))
    instance = await flow.run("t.exact", state={})

    step_id, _, _ = await _steps.upsert_pending_step(
        flow._store,
        run_id=instance.instance_id,
        step_name="validate_input",
        step_qualname="t.exact:validate_input",
        step_index=0,
        args_hash="h",
        max_attempts=3,
    )
    await _steps.mark_step_running(flow._store, step_id)

    assert await flow.query().current_node("validate_input").count() == 1
    assert await flow.query().current_node("%").count() == 0, "LIKE wildcard matched everything"
    assert await flow.query().current_node("validate_input_").count() == 0
    assert await flow.query().current_node("validate1input").count() == 0
    assert await flow.query().current_node("input").count() == 0, "a suffix must not match"
    assert await flow.query().current_node("nope", "validate_input").count() == 1, "any-of still works"
