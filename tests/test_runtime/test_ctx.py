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

"""`Ctx` — what a step body is handed."""

from __future__ import annotations

import operator
import uuid
from typing import Annotated, TypedDict

import pytest
from forktex.error import BadRequestError
from sqlalchemy import text

from forktex_flow import Ctx, Flow
from forktex_flow.runtime.context import FlowRuntime
from forktex_flow.runtime.execution import execute_run


class State(TypedDict, total=False):
    logs: Annotated[list[str], operator.add]


def test_a_ctx_built_without_a_store_is_usable_for_a_unit_test():
    """A step should be testable by awaiting it directly, with no database. That
    only works if `Ctx` is constructible on its own."""
    ctx = Ctx(
        run_id=uuid.uuid7(),
        workflow_name="t.unit",
        workflow_version=1,
        node_name="step",
        attempt=1,
        metadata={"org": "acme"},
    )
    assert ctx.metadata["org"] == "acme"
    assert ctx.node_name == "step"


@pytest.mark.asyncio
async def test_emit_on_an_unbound_ctx_says_so_rather_than_failing_obscurely():
    ctx = Ctx(
        run_id=uuid.uuid7(),
        workflow_name="t.unit",
        workflow_version=1,
        node_name="step",
        attempt=1,
        metadata={},
    )
    with pytest.raises(BadRequestError, match="database-bound"):
        await ctx.emit("anything")


@pytest.mark.asyncio
async def test_sleep_is_available_and_does_not_need_a_store():
    ctx = Ctx(
        run_id=uuid.uuid7(),
        workflow_name="t.unit",
        workflow_version=1,
        node_name="step",
        attempt=1,
        metadata={},
    )
    await ctx.sleep(0.001)


@pytest.mark.asyncio
async def test_the_executor_rewrites_node_name_and_attempt_per_step(flow: Flow):
    """A step that logs `ctx.node_name` must report its own position, not the
    run's entry point — otherwise every log line in a 12-step pipeline says the
    same thing."""
    seen: list[tuple[str, int]] = []

    async def first(ctx: Ctx, state: State) -> dict:
        seen.append((ctx.node_name, ctx.attempt))
        return {}

    async def second(ctx: Ctx, state: State) -> dict:
        seen.append((ctx.node_name, ctx.attempt))
        return {}

    flow.pipeline("t.ctx", version=1, state=State)(type("W", (), {"steps": [first, second]}))
    instance = await flow.run("t.ctx", state={})
    await execute_run(
        FlowRuntime(flow._store, flow._registry, flow._config, flow.extensions),
        instance.instance_id,
        "t.ctx",
        1,
    )

    assert seen == [("first", 1), ("second", 1)]


@pytest.mark.asyncio
async def test_metadata_reaches_the_step_body(flow: Flow):
    """The channel a consumer uses to pass request context — org id, trace id —
    into a step without threading it through state."""
    captured: dict = {}

    async def reader(ctx: Ctx, state: State) -> dict:
        captured.update(ctx.metadata)
        return {}

    flow.pipeline("t.meta", version=1, state=State)(type("W", (), {"steps": [reader]}))
    instance = await flow.run("t.meta", state={}, metadata={"org_id": "acme", "trace": "abc"})
    await execute_run(
        FlowRuntime(flow._store, flow._registry, flow._config, flow.extensions),
        instance.instance_id,
        "t.meta",
        1,
    )

    assert captured == {"org_id": "acme", "trace": "abc"}


@pytest.mark.asyncio
async def test_emit_from_inside_a_step_lands_in_the_event_log(flow: Flow):
    """How a long step reports progress between its start and its return."""

    async def chatty(ctx: Ctx, state: State) -> dict:
        await ctx.emit("progress", {"percent": 50})
        return {}

    flow.pipeline("t.emit", version=1, state=State)(type("W", (), {"steps": [chatty]}))
    instance = await flow.run("t.emit", state={})
    await execute_run(
        FlowRuntime(flow._store, flow._registry, flow._config, flow.extensions),
        instance.instance_id,
        "t.emit",
        1,
    )

    async with flow.session() as session:
        payloads = (
            (
                await session.execute(
                    text(
                        f'SELECT payload FROM "{flow.schema}".run_event '  # noqa: S608 - our own schema
                        "WHERE run_id = :rid AND event_type = 'progress'"
                    ).bindparams(rid=instance.instance_id)
                )
            )
            .scalars()
            .all()
        )

    assert payloads == [{"percent": 50}]
