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

"""End-to-end execution against a real Postgres.

These are the tests that would have caught the bugs the rewrite fixed, so they
are written against the behaviour rather than the implementation: run a
pipeline, crash it, resume it, and assert what the database holds.
"""

from __future__ import annotations

import operator
from typing import Annotated, TypedDict

import pytest

from forktex_flow import Ctx, Flow, step
from forktex_flow.runtime.context import FlowRuntime
from forktex_flow.runtime.execution import execute_run


class State(TypedDict, total=False):
    logs: Annotated[list[str], operator.add]
    enabled: bool
    marker: str


def _runtime(flow: Flow) -> FlowRuntime:
    """The runtime the driver would build, without starting the driver.

    Tests drive `execute_run` directly so a failure points at the executor
    rather than at a polling loop that may or may not have ticked yet.
    """
    return FlowRuntime(
        store=flow._store,
        registry=flow._registry,
        config=flow._config,
        extensions=flow.extensions,
    )


async def _run_to_completion(flow: Flow, name: str, state: dict) -> dict:
    instance = await flow.run(name, state=state)
    await execute_run(_runtime(flow), instance.instance_id, name, instance.workflow_version)
    return (await flow.get(instance.instance_id)).model_dump()


@pytest.mark.asyncio
async def test_a_pipeline_runs_its_steps_in_order(flow: Flow):
    async def first(ctx: Ctx, state: State) -> dict:
        return {"logs": ["first"]}

    async def second(ctx: Ctx, state: State) -> dict:
        return {"logs": ["second"]}

    @flow.pipeline("t.order", version=1, state=State)
    class Ordered:
        steps = [first, second]

    result = await _run_to_completion(flow, "t.order", {"logs": []})
    assert result["status"] == "completed"
    assert result["state"]["logs"] == ["first", "second"]


@pytest.mark.asyncio
async def test_reducers_accumulate_across_steps(flow: Flow):
    """The contract ForkTex Cloud's deploy pipelines are built on: every state
    class carries `logs: Annotated[list[str], operator.add]` and every step
    appends to it. An overwrite here loses all but the last step's output."""

    async def a(ctx: Ctx, state: State) -> dict:
        return {"logs": ["a"]}

    async def b(ctx: Ctx, state: State) -> dict:
        return {"logs": ["b"]}

    async def c(ctx: Ctx, state: State) -> dict:
        return {"logs": ["c"]}

    @flow.pipeline("t.reduce", version=1, state=State)
    class Accumulating:
        steps = [a, b, c]

    result = await _run_to_completion(flow, "t.reduce", {"logs": ["seed"]})
    assert result["state"]["logs"] == ["seed", "a", "b", "c"]


@pytest.mark.asyncio
async def test_a_when_guard_skips_its_step(flow: Flow):
    async def always(ctx: Ctx, state: State) -> dict:
        return {"logs": ["always"]}

    async def conditional(ctx: Ctx, state: State) -> dict:
        return {"logs": ["conditional"]}

    @flow.pipeline("t.guard", version=1, state=State)
    class Guarded:
        steps = [always, step(conditional, when=lambda s: s.get("enabled", False))]

    skipped = await _run_to_completion(flow, "t.guard", {"logs": [], "enabled": False})
    assert skipped["state"]["logs"] == ["always"]

    taken = await _run_to_completion(flow, "t.guard", {"logs": [], "enabled": True})
    assert taken["state"]["logs"] == ["always", "conditional"]


@pytest.mark.asyncio
async def test_a_completed_step_is_not_re_executed_on_resume(flow: Flow):
    """Durability, stated as a side effect count: the first attempt dies partway
    through, the second runs the whole pipeline from the top, and the step that
    already succeeded must not run its body again."""
    calls: list[str] = []

    async def succeeds(ctx: Ctx, state: State) -> dict:
        calls.append("succeeds")
        return {"logs": ["ok"]}

    async def fails_once(ctx: Ctx, state: State) -> dict:
        calls.append("fails_once")
        if len([c for c in calls if c == "fails_once"]) == 1:
            raise ValueError("first attempt dies")
        return {"logs": ["recovered"]}

    @flow.pipeline("t.resume", version=1, state=State)
    class Resumable:
        steps = [succeeds, step(fails_once, max_attempts=2, backoff=(0.0,))]

    instance = await flow.run("t.resume", state={"logs": []})
    runtime = _runtime(flow)

    await execute_run(runtime, instance.instance_id, "t.resume", 1)
    assert (await flow.get(instance.instance_id)).status == "pending"  # retry scheduled

    await execute_run(runtime, instance.instance_id, "t.resume", 1)
    final = await flow.get(instance.instance_id)

    assert final.status == "completed"
    assert calls.count("succeeds") == 1, "a completed step must be served from its cached output"
    assert calls.count("fails_once") == 2
    assert final.state["logs"] == ["ok", "recovered"]


@pytest.mark.asyncio
async def test_a_step_that_exhausts_its_retries_fails_the_run(flow: Flow):
    async def always_fails(ctx: Ctx, state: State) -> dict:
        raise ValueError("nope")

    @flow.pipeline("t.fail", version=1, state=State)
    class Failing:
        steps = [step(always_fails, max_attempts=1)]

    instance = await flow.run("t.fail", state={"logs": []})
    await execute_run(_runtime(flow), instance.instance_id, "t.fail", 1)

    final = await flow.get(instance.instance_id)
    assert final.status == "failed"
    assert final.nodes[0].status == "failed"
    assert final.nodes[0].attempt == 1


@pytest.mark.asyncio
async def test_an_unregistered_workflow_fails_the_run_rather_than_hanging(flow: Flow):
    """A run whose definition is not in this process must reach a terminal
    status. Leaving it `running` is how a queue silently accumulates work no
    worker will ever pick up."""

    async def noop(ctx: Ctx, state: State) -> dict:
        return {}

    @flow.pipeline("t.present", version=1, state=State)
    class Present:
        steps = [noop]

    instance = await flow.run("t.present", state={})
    await execute_run(_runtime(flow), instance.instance_id, "t.absent", 1)

    assert (await flow.get(instance.instance_id)).status == "failed"


@pytest.mark.asyncio
async def test_run_events_are_recorded_for_a_consumer_to_tail(flow: Flow):
    """ForkTex Cloud streams progress by tailing `run_event` with raw SQL, so
    the rows have to exist and carry the event types it renders."""

    async def one(ctx: Ctx, state: State) -> dict:
        await ctx.emit("custom", {"detail": "from inside the step"})
        return {"logs": ["one"]}

    @flow.pipeline("t.events", version=1, state=State)
    class Emitting:
        steps = [one]

    instance = await flow.run("t.events", state={"logs": []})
    await execute_run(_runtime(flow), instance.instance_id, "t.events", 1)

    from sqlalchemy import text

    async with flow.session() as session:
        rows = (
            await session.execute(
                text(
                    f'SELECT event_type FROM "{flow.schema}".run_event '  # noqa: S608 - schema is ours, not input
                    "WHERE run_id = :rid ORDER BY id ASC"
                ).bindparams(rid=instance.instance_id)
            )
        ).all()

    types = [r[0] for r in rows]
    assert types[0] == "run_started"
    assert "node_started" in types
    assert "custom" in types
    assert types[-1] == "run_completed"


@pytest.mark.asyncio
async def test_a_failing_step_body_does_not_lose_the_run(flow: Flow):
    """`StepFailed` carries the step's name so an operator knows what broke."""

    async def boom(ctx: Ctx, state: State) -> dict:
        raise RuntimeError("kaboom")

    @flow.pipeline("t.boom", version=1, state=State)
    class Boom:
        steps = [step(boom, max_attempts=1)]

    instance = await flow.run("t.boom", state={})
    await execute_run(_runtime(flow), instance.instance_id, "t.boom", 1)

    final = await flow.get(instance.instance_id)
    assert final.status == "failed"
    assert final.nodes[0].status == "failed"
    assert "kaboom" in (final.nodes[0].error or "")
