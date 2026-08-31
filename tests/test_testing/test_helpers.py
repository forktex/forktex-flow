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

"""`forktex_flow.testing`, used the way a consumer would use it.

These are written from the outside — importing only the public surface, never
reaching into the engine — because that is the whole point of the module. If a
test here needs a private attribute, the module is not doing its job.
"""

from __future__ import annotations

import operator
from typing import Annotated, TypedDict

import pytest
from forktex.error import BadRequestError

from forktex_flow import Ctx, RuntimeConfig, isolated_flow, make_ctx, run_to_completion, step


class DeployState(TypedDict, total=False):
    logs: Annotated[list[str], operator.add]
    server_id: str
    dns_enabled: bool


# ── make_ctx: a step is an ordinary callable ─────────────────────────────


@pytest.mark.asyncio
async def test_a_step_can_be_awaited_with_a_hand_built_ctx():
    """The claim `docs/flow.md` makes and never showed how to honour."""

    @step
    async def provision(ctx: Ctx, state: DeployState) -> dict:
        return {"server_id": "srv-1", "logs": [f"provisioned by {ctx.node_name}"]}

    result = await provision(make_ctx(node_name="provision"), {"logs": []})
    assert result == {"server_id": "srv-1", "logs": ["provisioned by provision"]}


@pytest.mark.asyncio
async def test_make_ctx_carries_metadata_into_the_step():
    """How a consumer unit-tests a step that branches on request context."""

    async def scoped(ctx: Ctx, state: DeployState) -> dict:
        return {"logs": [ctx.metadata["org_id"]]}

    assert await scoped(make_ctx(metadata={"org_id": "acme"}), {}) == {"logs": ["acme"]}


@pytest.mark.asyncio
async def test_an_unbound_ctx_refuses_to_emit_rather_than_pretending():
    """`make_ctx` has no database, and says so when asked for one — better than
    silently dropping an event a test believed it recorded."""
    ctx = make_ctx()
    with pytest.raises(BadRequestError, match="database-bound"):
        await ctx.emit("progress")


# ── isolated_flow + run_to_completion: the integration shape ─────────────


@pytest.mark.asyncio
async def test_a_consumer_can_run_a_whole_pipeline_and_assert_on_it(postgres_url_str: str):
    """The shape a consumer's own suite takes: register, run, assert."""

    async def provision(ctx: Ctx, state: DeployState) -> dict:
        return {"server_id": "srv-1", "logs": ["provisioned"]}

    async def configure_dns(ctx: Ctx, state: DeployState) -> dict:
        return {"logs": ["dns configured"]}

    async with isolated_flow(postgres_url_str) as flow:
        flow.pipeline("deploy.apply", version=1, state=DeployState)(
            type("DeployApply", (), {"steps": [provision, step(configure_dns, when=lambda s: s["dns_enabled"])]})
        )

        enabled = await run_to_completion(flow, "deploy.apply", state={"logs": [], "dns_enabled": True})
        assert enabled.status == "completed"
        assert enabled.state["logs"] == ["provisioned", "dns configured"]

        disabled = await run_to_completion(flow, "deploy.apply", state={"logs": [], "dns_enabled": False})
        assert disabled.state["logs"] == ["provisioned"]


@pytest.mark.asyncio
async def test_run_to_completion_drives_retries_to_a_terminal_state(postgres_url_str: str):
    """A run that schedules a retry must not be reported as finished. The helper
    keeps executing until the run is terminal, so a consumer testing a flaky
    step asserts on the outcome rather than on an intermediate `pending`."""
    attempts: list[int] = []

    async def flaky(ctx: Ctx, state: DeployState) -> dict:
        attempts.append(ctx.attempt)
        if len(attempts) < 3:
            raise RuntimeError("not yet")
        return {"logs": ["recovered"]}

    async with isolated_flow(postgres_url_str) as flow:
        flow.pipeline("t.flaky", version=1, state=DeployState)(
            type("W", (), {"steps": [step(flaky, max_attempts=3, backoff=(0.0,))]})
        )
        final = await run_to_completion(flow, "t.flaky", state={"logs": []})

    assert final.status == "completed"
    assert attempts == [1, 2, 3]
    assert final.state["logs"] == ["recovered"]


@pytest.mark.asyncio
async def test_run_to_completion_bounds_a_workflow_that_never_settles(postgres_url_str: str):
    """A step that always fails must fail the test, not hang it."""

    async def never(ctx: Ctx, state: DeployState) -> dict:
        raise RuntimeError("always")

    async with isolated_flow(postgres_url_str) as flow:
        flow.pipeline("t.never", version=1, state=DeployState)(
            type("W", (), {"steps": [step(never, max_attempts=99, backoff=(0.0,))]})
        )
        final = await run_to_completion(flow, "t.never", state={"logs": []}, max_attempts=3)

    assert final.status == "pending", "the helper returned rather than looping forever"


@pytest.mark.asyncio
async def test_two_isolated_flows_do_not_see_each_other(postgres_url_str: str):
    """Schema-per-test is what makes a consumer's suite safe against one
    database. Without it, a second test's runs show up in the first's queries."""

    async def work(ctx: Ctx, state: DeployState) -> dict:
        return {}

    async with isolated_flow(postgres_url_str) as first, isolated_flow(postgres_url_str) as second:
        for flow in (first, second):
            flow.pipeline("t.iso", version=1, state=DeployState)(type("W", (), {"steps": [work]}))
        await run_to_completion(first, "t.iso", state={})

        assert await first.query().count() == 1
        assert await second.query().count() == 0


@pytest.mark.asyncio
async def test_a_config_can_be_passed_through(postgres_url_str: str):
    """A consumer testing their own retry tuning needs the knob."""
    async with isolated_flow(postgres_url_str, config=RuntimeConfig(max_attempts=7)) as flow:

        async def work(ctx: Ctx, state: DeployState) -> dict:
            return {}

        flow.pipeline("t.cfg", version=1, state=DeployState)(type("W", (), {"steps": [work]}))
        final = await run_to_completion(flow, "t.cfg", state={})

    assert final.status == "completed"


@pytest.mark.asyncio
async def test_the_schema_is_dropped_on_the_way_out(postgres_url_str: str):
    """A suite that leaves a schema per test behind eventually cannot connect."""
    from sqlalchemy import text

    async with isolated_flow(postgres_url_str) as flow:
        schema = flow.schema
        engine = flow.engine
        async with engine.connect() as conn:
            found = (
                await conn.execute(
                    text("SELECT 1 FROM information_schema.schemata WHERE schema_name = :s").bindparams(s=schema)
                )
            ).first()
        assert found is not None

    probe = __import__("sqlalchemy.ext.asyncio", fromlist=["create_async_engine"]).create_async_engine(postgres_url_str)
    try:
        async with probe.connect() as conn:
            gone = (
                await conn.execute(
                    text("SELECT 1 FROM information_schema.schemata WHERE schema_name = :s").bindparams(s=schema)
                )
            ).first()
        assert gone is None, "the throwaway schema outlived the context manager"
    finally:
        await probe.dispose()
