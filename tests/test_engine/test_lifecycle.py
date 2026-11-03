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

"""Wiring a ``Flow``: one context manager brings it up, ``drive=True`` also runs jobs, and a closed one refuses."""

from __future__ import annotations

import logging

import pytest
from forktex.database import Database

from forktex_flow import Flow, FlowClosed, FlowExtension, WorkflowFailed, run_to_completion, step
from tests.conftest import FAST
from tests.test_engine.test_definitions import linear


async def _noop(ctx, variables):
    return {"ran": True}


@pytest.mark.asyncio
async def test_a_driving_flow_runs_what_it_is_started_with(postgres_url_str, schema):
    async with Flow(postgres_url_str, schema=schema, config=FAST, drive=True) as flow:
        flow.handler("noop")(_noop)
        await flow.deploy(linear("k", "noop"))
        done = await (await flow.start("k")).wait(timeout=10)

    assert done.variables == {"ran": True}


@pytest.mark.asyncio
async def test_without_a_driver_an_instance_waits_and_the_wait_says_why(postgres_url_str, schema, caplog):
    async with Flow(postgres_url_str, schema=schema, config=FAST) as flow:
        flow.handler("noop")(_noop)
        await flow.deploy(linear("k", "noop"))
        with caplog.at_level(logging.WARNING):
            waited = await (await flow.start("k")).wait(timeout=0.3)

    assert waited.status == "active"
    assert "no driver holds the leader lock" in caplog.text


@pytest.mark.asyncio
async def test_a_closed_flow_refuses_every_call(postgres_url_str, schema):
    flow = Flow(postgres_url_str, schema=schema)
    await flow.init()
    await flow.close()

    with pytest.raises(FlowClosed):
        await flow.start("anything")
    with pytest.raises(FlowClosed):
        await flow.init()


@pytest.mark.asyncio
async def test_a_flow_can_share_a_consumer_s_database_handle(postgres_url_str, schema):
    database = Database(postgres_url_str, schema_translate_map={"forktex_flow": schema})
    try:
        async with Flow(database=database, schema=schema) as flow:
            flow.handler("noop")(_noop)
            await flow.deploy(linear("k", "noop"))
            done = await run_to_completion(flow, "k")
        assert done.status == "completed"
    finally:
        await database.dispose()


def test_a_flow_needs_exactly_one_database(postgres_url_str):
    with pytest.raises(Exception, match="exactly one"):
        Flow()


class Recorder(FlowExtension):
    def __init__(self) -> None:
        self.events: list[tuple[str, str]] = []

    async def before_start(self, key, version, variables, metadata):
        return {"tenant": "acme"}

    async def after_complete(self, instance, variables):
        self.events.append(("completed", instance.key))

    async def after_fail(self, instance, error):
        assert isinstance(error, WorkflowFailed)
        self.events.append(("failed", str(error)))


class Broken(FlowExtension):
    async def after_complete(self, instance, variables):
        raise RuntimeError("observer bug")


@pytest.mark.asyncio
async def test_extensions_see_every_start_and_outcome_and_cannot_change_one(postgres_url_str, schema):
    recorder = Recorder()

    async def boom(ctx, variables):
        raise RuntimeError("down")

    async with Flow(postgres_url_str, schema=schema, extensions=[Broken(), recorder]) as flow:
        flow.pipeline("good")(type("Good", (), {"steps": [_noop]}))
        flow.pipeline("bad")(type("Bad", (), {"steps": [step(boom, max_attempts=1)]}))
        good = await run_to_completion(flow, "good")
        await run_to_completion(flow, "bad")

    assert good.metadata == {"tenant": "acme"}
    assert good.status == "completed"
    assert recorder.events[0] == ("completed", "good")
    assert recorder.events[1][0] == "failed" and "down" in recorder.events[1][1]
