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

"""Every driving ``Flow`` competes for the jobs of the task types it handles; none runs a job another holds."""

from __future__ import annotations

import asyncio
from collections import Counter

import pytest

from forktex_flow import Flow
from tests.conftest import FAST, until
from tests.test_engine.test_definitions import linear


@pytest.mark.asyncio
async def test_an_instance_moves_between_workers_by_task_type(postgres_url_str, schema):
    ran: list[tuple[str, str]] = []

    async with (
        Flow(postgres_url_str, schema=schema, config=FAST, drive=True) as billing,
        Flow(postgres_url_str, schema=schema, config=FAST, drive=True) as mailer,
    ):

        @billing.handler("issue")
        async def issue(ctx, variables):
            ran.append(("billing", ctx.element_id))
            return {"invoice": 1}

        @mailer.handler("send")
        async def send(ctx, variables):
            ran.append(("mailer", ctx.element_id))
            return {"sent": variables["invoice"]}

        await billing.deploy(linear("invoice", "issue", "send"))
        done = await (await billing.start("invoice")).wait(timeout=10)

    assert done.status == "completed"
    assert ran == [("billing", "issue"), ("mailer", "send")]


@pytest.mark.asyncio
async def test_two_workers_handling_one_type_never_run_the_same_job(postgres_url_str, schema):
    runs: Counter[str] = Counter()

    async def work(ctx, variables):
        runs[str(ctx.instance_id)] += 1
        await asyncio.sleep(0.01)
        return {}

    async with (
        Flow(postgres_url_str, schema=schema, config=FAST, drive=True) as one,
        Flow(postgres_url_str, schema=schema, config=FAST, drive=True) as two,
    ):
        one.handler("work")(work)
        two.handler("work")(work)
        await one.deploy(linear("batch", "work"))
        instances = [await one.start("batch") for _ in range(20)]
        finished = [await instance.wait(timeout=20) for instance in instances]

    assert {instance.status for instance in finished} == {"completed"}
    assert sorted(runs.values()) == [1] * 20


@pytest.mark.asyncio
async def test_a_flow_runs_at_most_max_concurrent_jobs_at_once(postgres_url_str, schema):
    running = 0
    peak = 0

    async def work(ctx, variables):
        nonlocal running, peak
        running += 1
        peak = max(peak, running)
        await asyncio.sleep(0.3)
        running -= 1
        return {}

    config = FAST.model_copy(update={"max_concurrent_jobs": 2})
    async with Flow(postgres_url_str, schema=schema, config=config, drive=True) as flow:
        flow.handler("work")(work)
        await flow.deploy(linear("batch", "work"))
        instances = [await flow.start("batch") for _ in range(6)]
        for instance in instances:
            await instance.wait(timeout=20)

    assert peak == 2


@pytest.mark.asyncio
async def test_a_flow_without_the_handler_leaves_the_job_for_one_that_has_it(postgres_url_str, schema):
    async with Flow(postgres_url_str, schema=schema, config=FAST, drive=True) as bystander:
        await bystander.deploy(linear("invoice", "issue"))
        instance = await bystander.start("invoice")
        await asyncio.sleep(0.2)
        still = await instance.refresh()

    assert still.status == "active"
    assert still.element("issue").status == "pending"


@pytest.mark.asyncio
async def test_a_worker_takes_the_next_step_without_waiting_for_its_poll(postgres_url_str, schema):
    async def work(ctx, variables):
        return {}

    slow_poll = FAST.model_copy(update={"poll_interval": 5.0})
    async with Flow(postgres_url_str, schema=schema, config=slow_poll, drive=True) as flow:
        for name in ("a", "b", "c", "d", "e"):
            flow.handler(name)(work)
        await flow.deploy(linear("chain", "a", "b", "c", "d", "e"))
        started = asyncio.get_running_loop().time()
        done = await (await flow.start("chain")).wait(timeout=10)
        elapsed = asyncio.get_running_loop().time() - started

    assert done.status == "completed"
    assert elapsed < 2.0


@pytest.mark.asyncio
async def test_the_leader_warns_about_jobs_no_running_process_claims(postgres_url_str, schema, caplog):
    async with Flow(postgres_url_str, schema=schema, config=FAST, drive=True) as flow:

        @flow.handler("a")
        async def a(ctx, variables):
            return {}

        await flow.deploy(linear("orphaned", "a", "removed"))
        instance = await flow.start("orphaned")
        await until(lambda: "of task type 'removed' are ready and unclaimed" in caplog.text)
        instance = await instance.refresh()

    assert [element.element_id for element in instance.current_elements] == ["removed"]
