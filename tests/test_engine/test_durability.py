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

"""What survives a crash, a retry, a cancel and a shutdown. Delivery is at-least-once: a completed element is never
repeated, and an interrupted one runs again.
"""

from __future__ import annotations

import asyncio

import pytest

from forktex_flow import Flow, RuntimeConfig, WorkflowCancelled, run_to_completion, step
import sqlalchemy as sa
from tests.conftest import FAST, until


def _pipeline(flow: Flow, *steps) -> None:
    flow.pipeline("deploy")(type("Deploy", (), {"steps": list(steps)}))


async def _abandon(flow: Flow, element_id: str) -> None:
    """Leave the job as a worker that claimed it and died leaves it."""
    async with flow.transaction() as session:
        await session.execute(
            sa.text(
                f"UPDATE \"{flow.schema}\".element_run SET status = 'running', attempts = attempts + 1, "
                "heartbeat_at = now() WHERE element_id = :element_id AND status = 'pending'"
            ),
            {"element_id": element_id},
        )


@pytest.mark.asyncio
async def test_a_job_abandoned_by_a_dead_worker_is_reclaimed_and_completed_steps_are_not_repeated(
    postgres_url_str, schema
):
    calls: list[str] = []

    async def provision(ctx, state):
        calls.append("provision")
        return {"server": "srv-1"}

    async def configure(ctx, state):
        calls.append("configure")
        return {"configured": state["server"]}

    async with Flow(postgres_url_str, schema=schema, config=FAST) as flow:
        _pipeline(flow, provision, configure)
        instance = await flow.start("deploy")

    # A worker that handles only the first step.
    async with Flow(postgres_url_str, schema=schema, config=FAST, drive=True) as first:
        first.handler("deploy:provision")(provision)
        await first.start_driver()
        await until(lambda: calls == ["provision"])

    # A worker that claimed `configure` and died before writing anything.
    async with Flow(postgres_url_str, schema=schema, config=FAST) as crashed:
        _pipeline(crashed, provision, configure)
        await _abandon(crashed, "configure")

    async with Flow(postgres_url_str, schema=schema, config=FAST, drive=True) as survivor:
        _pipeline(survivor, provision, configure)
        await survivor.init()
        await survivor.start_driver()
        done = await (await survivor.get(instance.instance_id)).wait(timeout=10)

    assert done.status == "completed"
    assert calls == ["provision", "configure"]
    assert done.element("configure").attempts == 2


@pytest.mark.asyncio
async def test_a_stale_job_with_no_attempts_left_fails_its_instance(postgres_url_str, schema):
    async def hang(ctx, state):
        return {}

    async with Flow(postgres_url_str, schema=schema, config=FAST) as flow:
        _pipeline(flow, step(hang, max_attempts=1))
        instance = await flow.start("deploy")
        await _abandon(flow, "hang")
        await flow.start_driver()
        done = await instance.wait(timeout=10)

    assert done.status == "failed"
    assert "stopped heartbeating" in (done.error or "")


@pytest.mark.asyncio
async def test_a_failing_step_is_retried_after_its_backoff_and_sees_its_final_attempt(postgres_url_str, schema):
    seen: list[tuple[int, bool]] = []

    async def flaky(ctx, state):
        seen.append((ctx.attempt, ctx.final_attempt))
        if ctx.attempt < 3:
            raise RuntimeError("try again")
        return {"ok": True}

    async with Flow(postgres_url_str, schema=schema, config=FAST) as flow:
        _pipeline(flow, step(flaky, max_attempts=3, backoff=(0.2,)))
        await flow.start_driver()
        loop = asyncio.get_running_loop()
        began = loop.time()
        done = await (await flow.start("deploy")).wait(timeout=10)
        elapsed = loop.time() - began

    assert done.status == "completed"
    assert seen == [(1, False), (2, False), (3, True)]
    assert elapsed >= 0.4, "two retries each waited their backoff"


@pytest.mark.asyncio
async def test_a_step_out_of_attempts_fails_the_instance_with_its_error(flow: Flow):
    async def broken(ctx, state):
        raise ValueError("bad credentials")

    _pipeline(flow, step(broken, max_attempts=2))
    done = await run_to_completion(flow, "deploy")

    assert done.status == "failed"
    assert "ValueError: bad credentials" in (done.error or "")
    assert done.element("broken").attempts == 2


@pytest.mark.asyncio
async def test_cancelling_an_instance_stops_its_running_handler(postgres_url_str, schema):
    started, stopped = asyncio.Event(), asyncio.Event()

    async def slow(ctx, state):
        started.set()
        try:
            await asyncio.sleep(30)
        finally:
            stopped.set()
        return {}

    async with Flow(postgres_url_str, schema=schema, config=FAST, drive=True) as flow:
        _pipeline(flow, slow)
        instance = await flow.start("deploy")
        await asyncio.wait_for(started.wait(), 10)
        assert await instance.cancel("no longer needed")
        await asyncio.wait_for(stopped.wait(), 10)
        done = await instance.refresh()

    assert done.status == "cancelled"
    assert done.element("slow").status == "cancelled"


@pytest.mark.asyncio
async def test_a_handler_can_cancel_its_own_instance(flow: Flow):
    async def guard(ctx, state):
        raise WorkflowCancelled("the order was withdrawn")

    _pipeline(flow, guard)
    done = await run_to_completion(flow, "deploy")

    assert (done.status, done.cancel_reason) == ("cancelled", "the order was withdrawn")


@pytest.mark.asyncio
async def test_stopping_waits_for_a_running_step_to_finish(postgres_url_str, schema):
    started = asyncio.Event()

    async def brief(ctx, state):
        started.set()
        await asyncio.sleep(0.2)
        return {"finished": True}

    async with Flow(postgres_url_str, schema=schema, config=FAST, drive=True) as flow:
        _pipeline(flow, brief)
        instance = await flow.start("deploy")
        await asyncio.wait_for(started.wait(), 10)
        await flow.stop_driver()
        done = await instance.refresh()

    assert done.status == "completed"


@pytest.mark.asyncio
async def test_a_step_still_running_when_the_drain_ends_is_handed_back_without_spending_an_attempt(
    postgres_url_str, schema
):
    started = asyncio.Event()

    async def endless(ctx, state):
        started.set()
        await asyncio.sleep(30)
        return {}

    config = FAST.model_copy(update={"drain_timeout": 0.1})
    async with Flow(postgres_url_str, schema=schema, config=config, drive=True) as flow:
        _pipeline(flow, step(endless, max_attempts=1))
        instance = await flow.start("deploy")
        await asyncio.wait_for(started.wait(), 10)
        await flow.stop_driver()
        handed_back = (await instance.refresh()).element("endless")

    assert handed_back is not None
    assert (handed_back.status, handed_back.attempts) == ("pending", 0)


@pytest.mark.asyncio
async def test_variables_that_jsonb_cannot_hold_fail_the_step(flow: Flow):
    async def leaky(ctx, state):
        return {"when": object()}

    _pipeline(flow, step(leaky, max_attempts=1))
    done = await run_to_completion(flow, "deploy")

    assert done.status == "failed"
    assert "cannot be written to JSONB" in (done.error or "")


def test_the_runtime_refuses_a_stale_threshold_inside_the_heartbeat():
    with pytest.raises(Exception, match="must exceed heartbeat_interval"):
        RuntimeConfig(heartbeat_interval=10, stale_threshold=5)
