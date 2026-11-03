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

"""A call activity runs a child instance and waits for it; the child's outcome is the element's."""

from __future__ import annotations

import pytest

from forktex_flow import Flow, run_to_completion


def _parent(called: str, version: int | None = None) -> dict:
    call = {"id": "escalate", "type": "callActivity", "calledElement": called, "resultVariable": "escalation"}
    if version is not None:
        call["version"] = version
    return {
        "key": "dunning",
        "elements": [{"id": "start", "type": "startEvent"}, call, {"id": "end", "type": "endEvent"}],
        "flows": [
            {"id": "a", "source": "start", "target": "escalate"},
            {"id": "b", "source": "escalate", "target": "end"},
        ],
    }


def _child(task_type: str = "notify") -> dict:
    return {
        "key": "escalation",
        "elements": [
            {"id": "start", "type": "startEvent"},
            {"id": "notify", "type": "serviceTask", "taskType": task_type},
            {"id": "end", "type": "endEvent"},
        ],
        "flows": [
            {"id": "a", "source": "start", "target": "notify"},
            {"id": "b", "source": "notify", "target": "end"},
        ],
    }


@pytest.mark.asyncio
async def test_a_call_activity_waits_for_its_child_and_takes_its_variables(flow: Flow):
    @flow.handler("notify")
    async def notify(ctx, variables: dict) -> dict:
        return {"notified": variables["customer"]}

    await flow.deploy(_child(), namespace="org")
    await flow.deploy(_parent("escalation"), namespace="org")

    parent = await run_to_completion(flow, "dunning", namespace="org", variables={"customer": "acme"})
    child = await flow.query().children_of(parent.instance_id).first()

    assert parent.status == "completed"
    assert parent.variables["escalation"] == {"customer": "acme", "notified": "acme"}
    assert child is not None
    assert (child.parent_instance_id, child.parent_element_id, child.triggered_by) == (
        parent.instance_id,
        "escalate",
        "call",
    )
    assert parent.element("escalate").child_instance_id == child.instance_id


@pytest.mark.asyncio
async def test_a_call_activity_runs_the_latest_child_unless_it_pins_one(flow: Flow):
    @flow.handler("notify")
    async def notify(ctx, variables: dict) -> dict:
        return {"child_version": ctx.version}

    await flow.deploy(_child())
    await flow.deploy(
        {
            **_child(),
            "elements": [*_child()["elements"][:1], {**_child()["elements"][1], "retries": 9}, _child()["elements"][2]],
        }
    )

    await flow.deploy(_parent("escalation"))
    latest = await run_to_completion(flow, "dunning")
    await flow.deploy(_parent("escalation", version=1))
    pinned = await run_to_completion(flow, "dunning")

    assert latest.variables["escalation"]["child_version"] == 2
    assert pinned.variables["escalation"]["child_version"] == 1


@pytest.mark.asyncio
async def test_a_failed_child_fails_its_parent(flow: Flow):
    @flow.handler("notify")
    async def notify(ctx, variables: dict) -> dict:
        raise RuntimeError("mail server down")

    await flow.deploy(
        {
            **_child(),
            "elements": [_child()["elements"][0], {**_child()["elements"][1], "retries": 1}, _child()["elements"][2]],
        }
    )
    await flow.deploy(_parent("escalation"))

    parent = await run_to_completion(flow, "dunning")

    assert parent.status == "failed"
    assert "mail server down" in (parent.error or "")
    assert parent.element("escalate").status == "failed"


@pytest.mark.asyncio
async def test_a_call_to_an_undeployed_process_fails_the_parent(flow: Flow):
    await flow.deploy(_parent("missing"))

    parent = await run_to_completion(flow, "dunning")

    assert parent.status == "failed"
    assert "'missing' is not deployed" in (parent.error or "")


@pytest.mark.asyncio
async def test_a_child_never_reaches_into_another_namespace(flow: Flow):
    await flow.deploy(_child(), namespace="org-a")
    await flow.deploy(_parent("escalation"), namespace="org-b")

    parent = await run_to_completion(flow, "dunning", namespace="org-b")

    assert parent.status == "failed"


@pytest.mark.asyncio
async def test_cancelling_a_parent_cancels_its_children(flow: Flow):
    """A child left running after its parent stopped would keep doing work nobody is waiting for."""
    await flow.deploy(_child("nobody-handles-this"))
    await flow.deploy(_parent("escalation"))

    parent = await flow.start("dunning")
    child = await flow.query().children_of(parent.instance_id).first()
    assert child is not None and child.status == "active"

    assert await parent.cancel("customer paid") is True
    child = await child.refresh()

    assert child.status == "cancelled"
    assert child.cancel_reason == "customer paid"
    assert await parent.cancel() is False
