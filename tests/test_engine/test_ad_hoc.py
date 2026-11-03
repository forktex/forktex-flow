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

"""An ad-hoc subprocess hands control to a decider — an AI agent, typically — which picks the activities to run
each round from a fixed set, sees their results, and decides again until it is done.
"""

from __future__ import annotations

import pytest
from forktex.error import BadRequestError

from forktex_flow import Activation, AdHocDecision, Flow, ProcessModel, run_to_completion


def _agent(max_iterations: int = 5) -> dict:
    return {
        "key": "support.agent",
        "elements": [
            {"id": "start", "type": "startEvent"},
            {
                "id": "agent",
                "type": "adHocSubProcess",
                "taskType": "ai.decide",
                "maxIterations": max_iterations,
                "resultVariable": "answer",
                "elements": [
                    {"id": "search", "type": "serviceTask", "taskType": "tool.search", "resultVariable": "found"},
                    {"id": "mail", "type": "serviceTask", "taskType": "tool.mail", "resultVariable": "mailed"},
                ],
            },
            {"id": "end", "type": "endEvent"},
        ],
        "flows": [
            {"id": "a", "source": "start", "target": "agent"},
            {"id": "b", "source": "agent", "target": "end"},
        ],
    }


@pytest.fixture
def tools(flow: Flow) -> Flow:
    @flow.handler("tool.search")
    async def search(ctx, variables):
        return {"hits": [variables["query"]]}

    @flow.handler("tool.mail")
    async def mail(ctx, variables):
        return {"to": variables["to"]}

    return flow


@pytest.mark.asyncio
async def test_the_decider_runs_rounds_of_activities_until_it_is_done(tools: Flow):
    seen: list[tuple[tuple[str, ...], dict]] = []

    @tools.handler("ai.decide")
    async def decide(ctx, variables):
        seen.append((ctx.activities, {k: variables.get(k) for k in ("found", "mailed")}))
        if "found" not in variables:
            return AdHocDecision(
                activate=(
                    Activation(element_id="search", variables={"query": "refund policy"}),
                    Activation(element_id="search", variables={"query": "order 7"}),
                )
            )
        if "mailed" not in variables:
            return AdHocDecision(activate=(Activation(element_id="mail", variables={"to": "ana@example.com"}),))
        return AdHocDecision(done=True, output={"reply": "sent"})

    await tools.deploy(_agent())
    done = await run_to_completion(tools, "support.agent")

    assert done.status == "completed"
    assert done.variables["answer"] == {"reply": "sent"}
    assert done.variables["mailed"] == {"to": "ana@example.com"}
    assert seen[0] == (("search", "mail"), {"found": None, "mailed": None})
    assert len(seen) == 3
    activities = sorted(e.element_id for e in done.elements if "/" in e.element_id)
    assert activities == ["agent/1.0/search", "agent/1.1/search", "agent/2.0/mail"]
    assert done.element("agent/1.1/search").input == {"query": "order 7"}


@pytest.mark.asyncio
async def test_a_decider_that_never_finishes_fails_after_max_iterations(tools: Flow):
    @tools.handler("ai.decide")
    async def decide(ctx, variables):
        return AdHocDecision(activate=(Activation(element_id="search", variables={"query": "again"}),))

    await tools.deploy(_agent(max_iterations=2))
    done = await run_to_completion(tools, "support.agent")

    assert done.status == "failed"
    assert "did not finish within 2 rounds" in (done.error or "")


@pytest.mark.asyncio
async def test_a_decider_asking_for_an_activity_it_lacks_fails_the_instance(tools: Flow):
    @tools.handler("ai.decide")
    async def decide(ctx, variables):
        return {"activate": [{"elementId": "delete_everything"}]}

    await tools.deploy(_agent())
    done = await run_to_completion(tools, "support.agent")

    assert done.status == "failed"
    assert "'delete_everything'" in (done.error or "")


@pytest.mark.asyncio
async def test_a_decider_returning_something_else_fails_the_instance(tools: Flow):
    @tools.handler("ai.decide")
    async def decide(ctx, variables):
        return {"activate": "everything"}

    await tools.deploy(_agent())
    done = await run_to_completion(tools, "support.agent")

    assert done.status == "failed"
    assert "no valid AdHocDecision" in (done.error or "")


@pytest.mark.asyncio
async def test_cancelling_mid_round_cancels_the_round_s_activities(flow: Flow):
    @flow.handler("ai.decide")
    async def decide(ctx, variables):
        return AdHocDecision(activate=(Activation(element_id="search", variables={"query": "x"}),))

    await flow.deploy(_agent())
    waiting = await run_to_completion(flow, "support.agent")
    assert [e.element_id for e in waiting.current_elements] == ["agent", "agent/1.0/search"]

    await waiting.cancel()
    cancelled = await waiting.refresh()

    assert {e.status for e in cancelled.elements if e.element_id.startswith("agent")} == {"cancelled"}


def test_an_activity_id_may_not_use_the_separator():
    model = _agent()
    model["elements"][1]["elements"][0]["id"] = "web/search"
    with pytest.raises(BadRequestError, match="contains '/'"):
        ProcessModel.model_validate(model)
