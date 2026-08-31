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

"""The handle a caller holds on a run."""

from __future__ import annotations

import operator
from typing import Annotated, TypedDict

import pytest
from forktex.error import NotFoundError

from forktex_flow import Ctx, Flow, WorkflowInstance
from forktex_flow.runtime.context import FlowRuntime
from forktex_flow.runtime.execution import execute_run


class State(TypedDict, total=False):
    logs: Annotated[list[str], operator.add]


async def _register(flow: Flow) -> None:
    async def work(ctx: Ctx, state: State) -> dict:
        return {"logs": ["done"]}

    flow.pipeline("t.instance", version=1, state=State)(type("W", (), {"steps": [work]}))


def _runtime(flow: Flow) -> FlowRuntime:
    return FlowRuntime(flow._store, flow._registry, flow._config, flow.extensions)


@pytest.mark.asyncio
async def test_refresh_reflects_work_done_since_the_snapshot(flow: Flow):
    await _register(flow)
    instance = await flow.run("t.instance", state={"logs": []})
    assert instance.status == "pending"

    await execute_run(_runtime(flow), instance.instance_id, "t.instance", 1)

    assert (await instance.refresh()).status == "completed"
    assert instance.status == "pending", "refresh returns a new snapshot; it does not mutate the old one"


@pytest.mark.asyncio
async def test_wait_returns_immediately_on_an_already_terminal_run(flow: Flow):
    await _register(flow)
    instance = await flow.run("t.instance", state={"logs": []})
    await execute_run(_runtime(flow), instance.instance_id, "t.instance", 1)

    final = await instance.wait(timeout=5)
    assert final.status == "completed"
    assert final.state["logs"] == ["done"]


@pytest.mark.asyncio
async def test_wait_returns_the_snapshot_it_has_when_it_times_out(flow: Flow):
    """A timeout is not an error: raising would throw away the progress the run
    did make, and the caller can read `status` to see it has not finished."""
    await _register(flow)
    instance = await flow.run("t.instance", state={"logs": []})

    timed_out = await instance.wait(timeout=0.1)
    assert timed_out.status == "pending"


@pytest.mark.asyncio
async def test_cancel_moves_the_run_to_cancelled(flow: Flow):
    await _register(flow)
    instance = await flow.run("t.instance", state={"logs": []})

    await instance.cancel()
    assert (await instance.refresh()).status == "cancelled"


@pytest.mark.asyncio
async def test_stream_ends_on_the_terminal_snapshot(flow: Flow):
    """A consumer that renders each yield must never need one more fetch to
    learn how the run ended, so the terminal snapshot is always emitted."""
    await _register(flow)
    instance = await flow.run("t.instance", state={"logs": []})
    await execute_run(_runtime(flow), instance.instance_id, "t.instance", 1)

    snapshots = [snapshot async for snapshot in instance.stream()]
    assert snapshots
    assert snapshots[-1].status == "completed"


@pytest.mark.asyncio
async def test_getting_an_unknown_run_raises_not_found(flow: Flow):
    import uuid

    with pytest.raises(NotFoundError):
        await flow.get(uuid.uuid7())


@pytest.mark.asyncio
async def test_running_an_unregistered_workflow_raises_not_found(flow: Flow):
    with pytest.raises(NotFoundError):
        await flow.run("t.never-declared", state={})


def test_an_unbound_instance_refuses_live_operations():
    """An instance rebuilt from a wire payload has no database behind it. Saying
    so beats an AttributeError three frames deeper."""
    from datetime import UTC, datetime
    import uuid

    detached = WorkflowInstance(
        instance_id=uuid.uuid7(),
        workflow_name="t.detached",
        workflow_version=1,
        status="completed",
        state={},
        metadata={},
        current_node=None,
        started_at=datetime.now(UTC),
        finished_at=None,
        nodes=[],
    )
    with pytest.raises(NotFoundError):
        import asyncio

        asyncio.run(detached.cancel())
