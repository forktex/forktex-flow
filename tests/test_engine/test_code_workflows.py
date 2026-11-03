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
"""A code pipeline is a ready-made definition: it deploys as a process and runs on the same engine as one written
as JSON.
"""

from __future__ import annotations

import operator
from typing import Annotated

import pytest
from forktex.error import AlreadyExistsError, BadRequestError
from pydantic import BaseModel

from forktex_flow import Ctx, ExclusiveGateway, Flow, run_to_completion, step
from tests.conftest import FAST, LoggingState


async def _append(ctx: Ctx, state: dict) -> dict:
    return {"logs": [ctx.element_id]}


class Order(BaseModel):
    order_id: str
    express: bool = False
    charge_id: str | None = None
    logs: Annotated[list[str], operator.add] = []


@pytest.mark.asyncio
async def test_a_pipeline_runs_its_steps_in_order_and_merges_their_outputs(flow: Flow):
    async def provision(ctx: Ctx, state: dict) -> dict:
        return {"server_id": "srv-1", "logs": ["provision"]}

    async def configure(ctx: Ctx, state: dict) -> dict:
        return {"logs": [f"configure {state['server_id']}"]}

    flow.pipeline("deploy", state=LoggingState, steps=[provision, configure])
    instance = await run_to_completion(flow, "deploy", variables={"logs": ["queued"]})

    assert instance.status == "completed"
    assert instance.variables == {"logs": ["queued", "provision", "configure srv-1"], "server_id": "srv-1"}
    assert instance.input == {"logs": ["queued"]}
    assert [e.element_id for e in instance.elements] == ["start", "provision", "configure", "end"]
    assert flow.registered() == ["deploy"]


@pytest.mark.asyncio
async def test_a_typed_pipeline_hands_each_step_its_model(flow: Flow):
    seen: list[object] = []

    async def charge(ctx: Ctx, order: Order) -> dict:
        seen.append(order)
        return {"charge_id": f"ch_{order.order_id}", "logs": ["charged"]}

    flow.pipeline("orders", state=Order, steps=[charge])
    instance = await run_to_completion(flow, "orders", variables={"order_id": "A-1"})

    assert seen == [Order(order_id="A-1")]
    assert instance.variables == {"order_id": "A-1", "express": False, "charge_id": "ch_A-1", "logs": ["charged"]}


@pytest.mark.asyncio
async def test_variables_that_do_not_match_the_state_are_refused_at_start(flow: Flow):
    flow.pipeline("orders", state=Order, steps=[_append])
    with pytest.raises(BadRequestError, match=r"order_id: Field required; express: Input should be a valid boolean"):
        await flow.start("orders", variables={"express": "maybe"})


@pytest.mark.asyncio
async def test_a_returned_model_changes_only_the_fields_it_set(flow: Flow):
    async def charge(ctx: Ctx, order: Order) -> dict:
        return {"charge_id": "ch_1"}

    async def refund(ctx: Ctx, order: Order) -> Order:
        return Order.model_construct(charge_id=None)

    flow.pipeline("orders", state=Order, steps=[charge, refund])
    instance = await run_to_completion(flow, "orders", variables={"order_id": "A-1", "express": True})

    assert instance.variables["charge_id"] is None
    assert (instance.variables["order_id"], instance.variables["express"]) == ("A-1", True)


@pytest.mark.asyncio
async def test_a_step_whose_variables_stopped_matching_fails_without_retrying(flow: Flow):
    async def corrupt(ctx: Ctx, order: Order) -> dict:
        return {"express": "not a bool"}

    flow.pipeline("orders", state=Order, steps=[corrupt, step(_append, max_attempts=5)])
    instance = await run_to_completion(flow, "orders", variables={"order_id": "A-1"})

    assert instance.status == "failed"
    assert (instance.element("_append").status, instance.element("_append").attempts) == ("failed", 1)
    assert "express" in (instance.error or "")


@pytest.mark.asyncio
async def test_a_condition_compiles_to_a_gateway_and_a_false_one_never_visits_the_step(flow: Flow):
    async def upgrade(ctx: Ctx, order: Order) -> dict:
        return {"logs": ["upgraded"]}

    async def ship(ctx: Ctx, order: Order) -> dict:
        return {"logs": ["shipped"]}

    flow.pipeline("orders", state=Order, steps=[step(upgrade, when={"var": "express"}), ship])
    express = await run_to_completion(flow, "orders", variables={"order_id": "A", "express": True})
    standard = await run_to_completion(flow, "orders", variables={"order_id": "B"})

    gateway = (await flow.definition("orders")).model.element("upgrade.when")
    assert isinstance(gateway, ExclusiveGateway)
    assert express.variables["logs"] == ["upgraded", "shipped"]
    assert standard.variables["logs"] == ["shipped"]
    assert standard.element("upgrade") is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("variables", "visited"),
    [
        ({"a": True, "b": True}, ["first", "a", "b"]),
        ({"a": False, "b": True}, ["first", "b"]),
        ({"a": True, "b": False}, ["first", "a"]),
        ({}, ["first"]),
    ],
)
async def test_consecutive_conditions_each_decide_their_own_step(flow: Flow, variables: dict, visited: list[str]):
    async def first(ctx: Ctx, state: dict) -> dict:
        return {"logs": ["first"]}

    async def a(ctx: Ctx, state: dict) -> dict:
        return {"logs": ["a"]}

    async def b(ctx: Ctx, state: dict) -> dict:
        return {"logs": ["b"]}

    flow.pipeline("branches", state=LoggingState, steps=[first, step(a, when={"var": "a"}), step(b, when={"var": "b"})])
    instance = await run_to_completion(flow, "branches", variables=variables)

    assert instance.status == "completed"
    assert instance.variables["logs"] == visited


@pytest.mark.asyncio
async def test_a_pipeline_can_run_a_shared_handler_by_its_task_type(flow: Flow):
    @flow.handler("payments.charge", state=Order)
    async def charge(ctx: Ctx, order: Order) -> dict:
        return {"charge_id": f"ch_{order.order_id}"}

    flow.pipeline("orders", state=Order, steps=["payments.charge"])
    instance = await run_to_completion(flow, "orders", variables={"order_id": "A-1"})

    assert instance.variables["charge_id"] == "ch_A-1"


@pytest.mark.asyncio
async def test_a_definition_cannot_use_another_pipelines_private_step(flow: Flow):
    flow.pipeline("orders", steps=[_append])
    await flow.init()
    borrowed = {
        "key": "tenant.process",
        "elements": [
            {"id": "start", "type": "startEvent"},
            {"id": "steal", "type": "serviceTask", "taskType": "orders:_append"},
            {"id": "end", "type": "endEvent"},
        ],
        "flows": [{"id": "f1", "source": "start", "target": "steal"}, {"id": "f2", "source": "steal", "target": "end"}],
    }
    with pytest.raises(BadRequestError, match="only that pipeline may use it"):
        await flow.deploy(borrowed, namespace="org-1")


@pytest.mark.asyncio
async def test_a_pipeline_copied_into_a_namespace_runs_there_with_its_reducers(flow: Flow):
    flow.pipeline("deploy", state=LoggingState, steps=[_append])
    await flow.init()
    await flow.deploy((await flow.definition("deploy")).model, namespace="org-1")
    instance = await run_to_completion(flow, "deploy", namespace="org-1", variables={"logs": ["queued"]})

    assert (instance.namespace, instance.status) == ("org-1", "completed")
    assert instance.variables == {"logs": ["queued", "_append"]}


def test_a_scheduled_pipeline_must_start_from_no_variables(flow: Flow):
    with pytest.raises(BadRequestError, match="starts with no variables"):
        flow.pipeline("orders", state=Order, cron="0 3 * * *", steps=[_append])


def test_declaring_a_key_twice_is_refused(flow: Flow):
    flow.pipeline("nightly", cron="0 3 * * *", steps=[_append])
    with pytest.raises(AlreadyExistsError):
        flow.pipeline("nightly", cron="0 4 * * *", steps=[_append])


def test_a_bad_cron_is_refused_at_declaration(flow: Flow):
    with pytest.raises(BadRequestError, match="not a valid cron"):
        flow.pipeline("nightly", cron="every night", steps=[_append])


async def _first(ctx: Ctx, state: dict) -> dict:
    return {"logs": ["first"]}


async def _second(ctx: Ctx, state: dict) -> dict:
    return {"logs": ["second"]}


@pytest.mark.asyncio
async def test_redeploying_unchanged_code_keeps_its_version_and_a_change_makes_a_new_one(postgres_url_str, schema):
    versions = []
    for steps in ([_first], [_first], [_first, _second]):
        async with Flow(postgres_url_str, schema=schema) as flow:
            flow.pipeline("deploy", state=LoggingState, steps=steps)
            versions.append((await flow.start("deploy")).version)

    assert versions == [1, 1, 2]


@pytest.mark.asyncio
async def test_an_instance_keeps_the_version_it_started_on(postgres_url_str, schema):
    """A deploy mid-flight must not reroute an instance onto steps its version never had."""
    async with Flow(postgres_url_str, schema=schema) as before:
        before.pipeline("deploy", state=LoggingState, steps=[_first])
        started = await before.start("deploy")

    async with Flow(postgres_url_str, schema=schema, config=FAST) as after:
        after.pipeline("deploy", state=LoggingState, steps=[_first, _second])
        fresh = await run_to_completion(after, "deploy")
        await after.start_driver()
        resumed = await (await after.get(started.instance_id)).wait(timeout=10)

    assert (fresh.version, fresh.variables["logs"]) == (2, ["first", "second"])
    assert (resumed.version, resumed.variables["logs"]) == (1, ["first"])
    assert resumed.status == "completed"
