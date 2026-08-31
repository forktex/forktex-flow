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

"""Registration: what a process declares, and what it refuses to."""

from __future__ import annotations

import operator
from typing import Annotated, TypedDict

import pytest
from forktex.error import AlreadyExistsError, NotFoundError

from forktex_flow import Ctx, Flow
from forktex_flow.runtime.registry import FlowRegistry


class State(TypedDict, total=False):
    logs: Annotated[list[str], operator.add]


async def _noop(ctx: Ctx, state: State) -> dict:
    return {}


def _pipeline(flow: Flow, name: str, version: int) -> None:
    flow.pipeline(name, version=version, state=State)(type("W", (), {"steps": [_noop]}))


@pytest.mark.asyncio
async def test_registering_the_same_name_and_version_twice_is_refused(flow: Flow):
    """Two declarations disagreeing about what a workflow is means a run resumed
    after a restart executes whichever module was imported second. Bump the
    version instead — that is why this raises rather than overwrites."""
    _pipeline(flow, "t.dupe", 1)
    with pytest.raises(AlreadyExistsError, match="already registered"):
        _pipeline(flow, "t.dupe", 1)


@pytest.mark.asyncio
async def test_two_versions_of_one_workflow_coexist(flow: Flow):
    """Required for a rolling deploy: runs started under v1 keep executing v1's
    steps while new dispatches take v2."""
    _pipeline(flow, "t.multi", 1)
    _pipeline(flow, "t.multi", 2)
    assert flow.registered() == ["t.multi"]
    assert flow._registry.get("t.multi", version=1) is not None
    assert flow._registry.get("t.multi", version=2) is not None


@pytest.mark.asyncio
async def test_an_unversioned_lookup_takes_the_highest(flow: Flow):
    _pipeline(flow, "t.latest", 1)
    _pipeline(flow, "t.latest", 3)
    _pipeline(flow, "t.latest", 2)
    found = flow._registry.get("t.latest")
    assert found is not None
    assert found.version == 3


@pytest.mark.asyncio
async def test_dispatching_an_unregistered_version_is_a_not_found(flow: Flow):
    _pipeline(flow, "t.v1only", 1)
    with pytest.raises(NotFoundError):
        await flow.run("t.v1only", state={}, version=99)


def test_an_empty_registry_answers_rather_than_raising():
    registry = FlowRegistry()
    assert registry.get("nothing") is None
    assert registry.names() == []
    assert registry.scheduled() == []


@pytest.mark.asyncio
async def test_registered_lists_names_once_regardless_of_versions(flow: Flow):
    _pipeline(flow, "t.a", 1)
    _pipeline(flow, "t.a", 2)
    _pipeline(flow, "t.b", 1)
    assert flow.registered() == ["t.a", "t.b"]


@pytest.mark.asyncio
async def test_only_cron_workflows_appear_as_scheduled(flow: Flow):
    _pipeline(flow, "t.ondemand", 1)
    flow.scheduled("t.cron", version=1, cron="0 * * * *", state=State)(_noop)

    scheduled = flow._registry.scheduled()
    assert [d.name for d in scheduled] == ["t.cron"]


@pytest.mark.asyncio
async def test_a_pipeline_may_also_carry_a_cron(flow: Flow):
    """`@pipeline(cron=…)` is how a multi-step workflow runs on a schedule — the
    two decorators differ only in whether the definition carries one."""
    flow.pipeline("t.both", version=1, state=State, cron="*/15 * * * *")(type("W", (), {"steps": [_noop]}))
    scheduled = flow._registry.scheduled()
    assert [d.name for d in scheduled] == ["t.both"]
