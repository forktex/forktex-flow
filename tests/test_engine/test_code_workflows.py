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

"""The decorators are ready-made definitions: a pipeline or a scheduled function deploys as a process and runs on
the same engine as one written as JSON.
"""

from __future__ import annotations

import pytest
from forktex.error import AlreadyExistsError, BadRequestError

from forktex_flow import Flow, ServiceTask, run_to_completion, step
from tests.conftest import FAST, LoggingState


async def _append(ctx, state: dict) -> dict:
    return {"logs": [ctx.element_id]}


@pytest.mark.asyncio
async def test_a_pipeline_runs_its_steps_in_order_and_merges_their_outputs(flow: Flow):
    async def provision(ctx, state: dict) -> dict:
        return {"server_id": "srv-1", "logs": ["provision"]}

    async def configure(ctx, state: dict) -> dict:
        return {"logs": [f"configure {state['server_id']}"]}

    @flow.pipeline("deploy", state=LoggingState)
    class Deploy:
        steps = [provision, configure]

    instance = await run_to_completion(flow, "deploy", variables={"logs": ["queued"]})

    assert instance.status == "completed"
    assert instance.variables == {"logs": ["queued", "provision", "configure srv-1"], "server_id": "srv-1"}
    assert instance.input == {"logs": ["queued"]}
    assert [e.element_id for e in instance.elements] == ["start", "provision", "configure", "end"]


@pytest.mark.asyncio
async def test_a_pipeline_is_stored_as_a_definition_like_any_other(flow: Flow):
    @flow.pipeline("deploy")
    class Deploy:
        steps = [step(_append, max_attempts=5, backoff=(1.0, 2.0))]

    await flow.init()
    definition = await flow.definition("deploy")

    task = definition.model.element("_append")
    assert isinstance(task, ServiceTask)
    assert (task.task_type, task.retries, task.backoff) == ("deploy:_append", 5, (1.0, 2.0))
    assert flow.registered() == ["deploy"]


@pytest.mark.asyncio
async def test_a_guarded_step_whose_guard_is_false_is_skipped(flow: Flow):
    @flow.pipeline("deploy", state=LoggingState)
    class Deploy:
        steps = [_append, step(_append, when=lambda state: state.get("dns", False))]

    instance = await run_to_completion(flow, "deploy")

    assert instance.variables["logs"] == ["_append"]
    assert instance.element("_append_1").status == "skipped"


@pytest.mark.asyncio
async def test_the_same_function_twice_becomes_two_elements(flow: Flow):
    @flow.pipeline("twice", state=LoggingState)
    class Twice:
        steps = [_append, _append]

    instance = await run_to_completion(flow, "twice")

    assert instance.variables["logs"] == ["_append", "_append_1"]


@pytest.mark.asyncio
async def test_a_scheduled_function_is_a_one_task_process(flow: Flow):
    @flow.scheduled("nightly", cron="0 3 * * *")
    async def prune(ctx, state: dict) -> dict:
        return {"pruned": 4}

    instance = await run_to_completion(flow, "nightly")

    assert instance.variables == {"pruned": 4}


async def _first(ctx, state):
    return {"logs": ["first"]}


async def _second(ctx, state):
    return {"logs": ["second"]}


def _deploy_pipeline(flow: Flow, steps: list) -> None:
    flow.pipeline("deploy", state=LoggingState)(type("Deploy", (), {"steps": steps}))


@pytest.mark.asyncio
async def test_redeploying_unchanged_code_keeps_its_version_and_a_change_makes_a_new_one(postgres_url_str, schema):
    versions = []
    for steps in ([_first], [_first], [_first, _second]):
        async with Flow(postgres_url_str, schema=schema) as flow:
            _deploy_pipeline(flow, steps)
            versions.append((await flow.start("deploy")).version)

    assert versions == [1, 1, 2]


@pytest.mark.asyncio
async def test_an_instance_keeps_the_version_it_started_on(postgres_url_str, schema):
    """A deploy mid-flight must not reroute an instance onto steps its version never had."""
    async with Flow(postgres_url_str, schema=schema) as before:
        _deploy_pipeline(before, [_first])
        started = await before.start("deploy")

    async with Flow(postgres_url_str, schema=schema, config=FAST) as after:
        _deploy_pipeline(after, [_first, _second])
        fresh = await run_to_completion(after, "deploy")
        await after.start_driver()
        resumed = await (await after.get(started.instance_id)).wait(timeout=10)

    assert (fresh.version, fresh.variables["logs"]) == (2, ["first", "second"])
    assert (resumed.version, resumed.variables["logs"]) == (1, ["first"])
    assert resumed.status == "completed"


def test_declaring_a_key_twice_is_refused(flow: Flow):
    flow.scheduled("nightly", cron="0 3 * * *")(_append)
    with pytest.raises(AlreadyExistsError):
        flow.scheduled("nightly", cron="0 4 * * *")(_append)


def test_a_bad_cron_is_refused_at_declaration(flow: Flow):
    with pytest.raises(BadRequestError, match="not a valid cron"):
        flow.scheduled("nightly", cron="every night")(_append)


def test_a_pipeline_without_steps_is_refused(flow: Flow):
    with pytest.raises(BadRequestError, match="declares no steps"):

        @flow.pipeline("empty")
        class Empty:
            steps = []


@pytest.mark.asyncio
async def test_a_pipeline_deployed_into_a_namespace_runs_there_with_its_reducers(flow: Flow):
    @flow.pipeline("deploy", state=LoggingState)
    class Deploy:
        steps = [_append]

    await flow.init()
    await flow.deploy((await flow.definition("deploy")).model, namespace="org-1")
    instance = await run_to_completion(flow, "deploy", namespace="org-1", variables={"logs": ["queued"]})

    assert (instance.namespace, instance.status) == ("org-1", "completed")
    assert instance.variables == {"logs": ["queued", "_append"]}
