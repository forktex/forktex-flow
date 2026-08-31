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

"""The README's promises, executed.

Documentation that has never run is a hypothesis. Each of these mirrors a claim
the README makes to a prospective consumer, so a change that quietly invalidates
one fails here rather than in someone's first hour with the library.
"""

from __future__ import annotations

import operator
from typing import Annotated, TypedDict

import pytest

from forktex_flow import Ctx, Flow, WorkflowCancelled, step
from forktex_flow.runtime.context import FlowRuntime
from forktex_flow.runtime.execution import execute_run


class DeployState(TypedDict, total=False):
    logs: Annotated[list[str], operator.add]
    server_id: str
    dns_enabled: bool


def _runtime(flow: Flow) -> FlowRuntime:
    return FlowRuntime(flow._store, flow._registry, flow._config, flow.extensions)


@pytest.mark.asyncio
async def test_the_headline_example_behaves_as_advertised(flow: Flow):
    """The README's opening claim, verbatim: if the worker dies after
    `provision` and before `configure_dns`, the run resumes, `provision` is not
    re-run, and no second server is created."""
    created: list[str] = []
    dns_calls: list[int] = []

    async def provision(ctx: Ctx, state: DeployState) -> dict:
        created.append("server")
        return {"server_id": "srv-1", "logs": ["provisioned srv-1"]}

    async def configure_dns(ctx: Ctx, state: DeployState) -> dict:
        dns_calls.append(1)
        if len(dns_calls) == 1:
            raise RuntimeError("the worker died here")
        return {"logs": ["dns configured"]}

    @flow.pipeline("deploy.apply", version=1, state=DeployState)
    class DeployApply:
        steps = [provision, step(configure_dns, when=lambda s: s["dns_enabled"], max_attempts=2, backoff=(0.0,))]

    instance = await flow.run("deploy.apply", state={"dns_enabled": True, "logs": []})
    runtime = _runtime(flow)

    await execute_run(runtime, instance.instance_id, "deploy.apply", 1)  # dies inside configure_dns
    assert (await flow.get(instance.instance_id)).status == "pending"

    await execute_run(runtime, instance.instance_id, "deploy.apply", 1)  # resumes
    final = await flow.get(instance.instance_id)

    assert final.status == "completed"
    assert created == ["server"], "provision ran twice — no second server may be created"
    assert len(dns_calls) == 2, "the step that did not finish must be retried"
    assert final.state["logs"] == ["provisioned srv-1", "dns configured"]


@pytest.mark.asyncio
async def test_a_guard_that_is_false_skips_its_step(flow: Flow):
    """ "A run visits every step once, in order, executing the ones whose guard
    passes."""

    async def always(ctx: Ctx, state: DeployState) -> dict:
        return {"logs": ["always"]}

    async def conditional(ctx: Ctx, state: DeployState) -> dict:
        return {"logs": ["conditional"]}

    @flow.pipeline("deploy.guarded", version=1, state=DeployState)
    class Guarded:
        steps = [always, step(conditional, when=lambda s: s["dns_enabled"])]

    instance = await flow.run("deploy.guarded", state={"dns_enabled": False, "logs": []})
    await execute_run(_runtime(flow), instance.instance_id, "deploy.guarded", 1)

    assert (await flow.get(instance.instance_id)).state["logs"] == ["always"]


@pytest.mark.asyncio
async def test_reducers_accumulate_and_plain_fields_overwrite(flow: Flow):
    """The README's state table: `logs` accumulates, `server_id` is last-write-wins."""

    async def first(ctx: Ctx, state: DeployState) -> dict:
        return {"logs": ["a"], "server_id": "old"}

    async def second(ctx: Ctx, state: DeployState) -> dict:
        return {"logs": ["b"], "server_id": "new"}

    @flow.pipeline("deploy.reduce", version=1, state=DeployState)
    class Reducing:
        steps = [first, second]

    instance = await flow.run("deploy.reduce", state={"logs": [], "dns_enabled": False})
    await execute_run(_runtime(flow), instance.instance_id, "deploy.reduce", 1)

    final = await flow.get(instance.instance_id)
    assert final.state["logs"] == ["a", "b"]
    assert final.state["server_id"] == "new"


@pytest.mark.asyncio
async def test_cancelling_from_inside_a_step_keeps_earlier_work_visible(flow: Flow):
    """ "Cancellation is not a rollback: work already done stays done and stays
    visible, which is what an operator needs in order to clean up."""

    async def provision(ctx: Ctx, state: DeployState) -> dict:
        return {"logs": ["provisioned"]}

    async def check_target(ctx: Ctx, state: DeployState) -> dict:
        raise WorkflowCancelled("the target was deleted while we were deploying")

    @flow.pipeline("deploy.cancel", version=1, state=DeployState)
    class Cancelling:
        steps = [provision, check_target]

    instance = await flow.run("deploy.cancel", state={"logs": [], "dns_enabled": False})
    await execute_run(_runtime(flow), instance.instance_id, "deploy.cancel", 1)

    final = await flow.get(instance.instance_id)
    assert final.status == "cancelled"
    assert [n.status for n in final.nodes] == ["completed", "failed"]


@pytest.mark.asyncio
async def test_a_step_can_be_awaited_directly_with_no_database(flow: Flow):
    """ "Bare `@step` returns the function unchanged, so a step stays an ordinary
    callable a unit test can await directly." The claim that makes steps testable
    without any of this machinery."""
    import uuid

    @step
    async def pure(ctx: Ctx, state: DeployState) -> dict:
        return {"logs": [f"ran as {ctx.node_name}"]}

    ctx = Ctx(
        run_id=uuid.uuid7(),
        workflow_name="unit",
        workflow_version=1,
        node_name="pure",
        attempt=1,
        metadata={},
    )
    assert await pure(ctx, {"logs": []}) == {"logs": ["ran as pure"]}
