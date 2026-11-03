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

"""A definition written as data: deployed into a namespace, run by the handlers a ``Flow`` registers by task type."""

from __future__ import annotations

import pytest
from forktex.error import BadRequestError, ConflictError, NotFoundError

from forktex_flow import Flow, run_pending, run_to_completion


def linear(key: str, *task_types: str, **task_extra: object) -> dict:
    ids = ["start", *task_types, "end"]
    return {
        "key": key,
        "elements": [
            {"id": "start", "type": "startEvent"},
            *({"id": t, "type": "serviceTask", "taskType": t, **task_extra} for t in task_types),
            {"id": "end", "type": "endEvent"},
        ],
        "flows": [{"id": f"{a}->{b}", "source": a, "target": b} for a, b in zip(ids, ids[1:], strict=False)],
    }


@pytest.mark.asyncio
async def test_a_deployed_definition_runs_on_the_handlers_registered_for_its_task_types(flow: Flow):
    @flow.handler("invoice.issue")
    async def issue(ctx, variables: dict) -> dict:
        return {"invoice": f"INV-{variables['order']}"}

    @flow.handler("invoice.send")
    async def send(ctx, variables: dict) -> dict:
        return {"sent_to": variables["email"], "invoice_seen": variables["invoice"]}

    definition = await flow.deploy(linear("billing.invoice", "invoice.issue", "invoice.send"))
    instance = await run_to_completion(flow, "billing.invoice", variables={"order": 7, "email": "a@example.com"})

    assert definition.version == 1
    assert instance.status == "completed"
    assert instance.variables == {
        "order": 7,
        "email": "a@example.com",
        "invoice": "INV-7",
        "sent_to": "a@example.com",
        "invoice_seen": "INV-7",
    }


@pytest.mark.asyncio
async def test_a_result_variable_nests_the_output_under_its_name(flow: Flow):
    @flow.handler("score")
    async def score(ctx, variables: dict) -> dict:
        return {"value": 9}

    await flow.deploy(linear("scoring", "score", resultVariable="score"))
    instance = await run_to_completion(flow, "scoring")

    assert instance.variables == {"score": {"value": 9}}


@pytest.mark.asyncio
async def test_namespaces_keep_the_same_key_apart(flow: Flow):
    """Two tenants may name a process the same; each gets its own versions, and neither starts the other's."""

    @flow.handler("greet")
    async def greet(ctx, variables: dict) -> dict:
        return {"greeted_in": ctx.namespace}

    await flow.deploy(linear("welcome", "greet"), namespace="org-a")
    await flow.deploy(linear("welcome", "greet", retries=2), namespace="org-b")
    await flow.deploy(linear("welcome", "greet", retries=3), namespace="org-b")

    a = await run_to_completion(flow, "welcome", namespace="org-a")
    b = await run_to_completion(flow, "welcome", namespace="org-b")

    assert (a.namespace, a.version, a.variables) == ("org-a", 1, {"greeted_in": "org-a"})
    assert (b.namespace, b.version) == ("org-b", 2)
    assert await flow.query().namespace("org-a").count() == 1
    with pytest.raises(NotFoundError):
        await flow.start("welcome")


@pytest.mark.asyncio
async def test_an_unchanged_redeploy_returns_the_existing_version(flow: Flow):
    first = await flow.deploy(linear("k", "t"))
    again = await flow.deploy(linear("k", "t"))

    assert again == first


@pytest.mark.asyncio
async def test_a_version_can_be_pinned(flow: Flow):
    @flow.handler("t")
    async def t(ctx, variables: dict) -> dict:
        return {}

    await flow.deploy(linear("k", "t"))
    await flow.deploy(linear("k", "t", retries=5))

    pinned = await run_to_completion(flow, "k", version=1)

    assert pinned.version == 1
    assert (await flow.definition("k")).version == 2


@pytest.mark.asyncio
async def test_a_definition_that_cannot_run_is_refused_at_deploy(flow: Flow):
    with pytest.raises(BadRequestError, match="cycle"):
        await flow.deploy(
            {
                "key": "loop",
                "elements": [
                    {"id": "start", "type": "startEvent"},
                    {"id": "a", "type": "serviceTask", "taskType": "a"},
                    {"id": "g", "type": "exclusiveGateway", "default": "out"},
                    {"id": "end", "type": "endEvent"},
                ],
                "flows": [
                    {"id": "s", "source": "start", "target": "a"},
                    {"id": "ag", "source": "a", "target": "g"},
                    {"id": "back", "source": "g", "target": "a", "condition": {"var": "again"}},
                    {"id": "out", "source": "g", "target": "end"},
                ],
            }
        )


def _routed() -> dict:
    return {
        "key": "approval",
        "elements": [
            {"id": "start", "type": "startEvent"},
            {"id": "route", "type": "exclusiveGateway", "default": "auto"},
            {"id": "review", "type": "userTask", "resultVariable": "review"},
            {"id": "decide", "type": "exclusiveGateway"},
            {"id": "pay", "type": "serviceTask", "taskType": "pay"},
            {"id": "end", "type": "endEvent"},
            {"id": "rejected", "type": "endEvent"},
        ],
        "flows": [
            {"id": "in", "source": "start", "target": "route"},
            {"id": "big", "source": "route", "target": "review", "condition": {">": [{"var": "amount"}, 1000]}},
            {"id": "auto", "source": "route", "target": "pay"},
            {"id": "reviewed", "source": "review", "target": "decide"},
            {
                "id": "approved",
                "source": "decide",
                "target": "pay",
                "condition": {"==": [{"var": "review.decision"}, "approve"]},
            },
            {
                "id": "denied",
                "source": "decide",
                "target": "rejected",
                "condition": {"!=": [{"var": "review.decision"}, "approve"]},
            },
            {"id": "paid", "source": "pay", "target": "end"},
        ],
    }


@pytest.fixture
def approvals(flow: Flow) -> Flow:
    @flow.handler("pay")
    async def pay(ctx, variables: dict) -> dict:
        return {"paid": variables["amount"]}

    return flow


@pytest.mark.asyncio
async def test_a_gateway_takes_its_default_when_no_condition_holds(approvals: Flow):
    await approvals.deploy(_routed())

    instance = await run_to_completion(approvals, "approval", variables={"amount": 50})

    assert instance.status == "completed"
    assert instance.variables["paid"] == 50
    assert instance.element("review") is None


@pytest.mark.asyncio
async def test_a_user_task_waits_until_it_is_completed(approvals: Flow):
    await approvals.deploy(_routed())

    waiting = await run_to_completion(approvals, "approval", variables={"amount": 5000})
    assert waiting.status == "active"
    assert [e.element_id for e in waiting.current_elements] == ["review"]
    assert await approvals.query().current_element("review").count() == 1

    await approvals.complete_task(waiting.instance_id, "review", {"decision": "approve", "by": "ana"})
    done = await run_pending(approvals, waiting.instance_id)

    assert done.status == "completed"
    assert done.variables["review"] == {"decision": "approve", "by": "ana"}
    assert done.variables["paid"] == 5000


@pytest.mark.asyncio
async def test_a_rejected_review_ends_on_the_other_branch(approvals: Flow):
    await approvals.deploy(_routed())
    waiting = await run_to_completion(approvals, "approval", variables={"amount": 5000})

    done = await approvals.complete_task(waiting.instance_id, "review", {"decision": "reject"})

    assert done.status == "completed"
    assert done.element("rejected") is not None
    assert "paid" not in done.variables


@pytest.mark.asyncio
async def test_a_user_task_completes_once(approvals: Flow):
    await approvals.deploy(_routed())
    waiting = await run_to_completion(approvals, "approval", variables={"amount": 5000})
    await approvals.complete_task(waiting.instance_id, "review", {"decision": "reject"})

    with pytest.raises(ConflictError):
        await approvals.complete_task(waiting.instance_id, "review", {"decision": "approve"})
    with pytest.raises(NotFoundError):
        await approvals.complete_task(waiting.instance_id, "pay", {})


@pytest.mark.asyncio
async def test_a_gateway_with_no_match_and_no_default_fails_the_instance(approvals: Flow):
    await approvals.deploy(_routed())
    waiting = await run_to_completion(approvals, "approval", variables={"amount": 5000})

    done = await approvals.complete_task(waiting.instance_id, "review", {})
    assert done.status == "completed"  # `!=` holds for a missing decision

    model = _routed()
    model["flows"][-2]["condition"] = {"==": [{"var": "review.decision"}, "reject"]}
    await approvals.deploy(model)
    waiting = await run_to_completion(approvals, "approval", variables={"amount": 5000})
    failed = await approvals.complete_task(waiting.instance_id, "review", {"decision": "maybe"})

    assert failed.status == "failed"
    assert "no flow out of gateway 'decide' matched" in (failed.error or "")
