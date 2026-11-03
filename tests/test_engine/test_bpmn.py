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

"""BPMN 2.0 XML — what Camunda Modeler saves — imports one way into the JSON model and runs on the same engine,
with the document kept verbatim as the version's source.
"""

from __future__ import annotations

import pytest
from forktex.error import BadRequestError

from forktex_flow import AdHocSubProcess, Flow, ServiceTask, parse_bpmn, run_pending, run_to_completion

HEAD = (
    '<?xml version="1.0" encoding="UTF-8"?>\n'
    '<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"'
    ' xmlns:bpmndi="http://www.omg.org/spec/BPMN/20100524/DI"'
    ' xmlns:zeebe="http://camunda.org/schema/zeebe/1.0"'
    ' xmlns:flow="https://forktex.com/schema/flow" id="defs">'
)

APPROVAL = (
    HEAD
    + """
  <bpmn:process id="expense" isExecutable="true">
    <bpmn:documentation>Expense approval</bpmn:documentation>
    <bpmn:startEvent id="start"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
    <bpmn:userTask id="review" flow:resultVariable="review"/>
    <bpmn:exclusiveGateway id="decide" default="rejected"/>
    <bpmn:serviceTask id="pay">
      <bpmn:extensionElements><zeebe:taskDefinition type="finance.pay" retries="5"/></bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:endEvent id="end"/>
    <bpmn:endEvent id="end_rejected"/>
    <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="review"/>
    <bpmn:sequenceFlow id="f2" sourceRef="review" targetRef="decide"/>
    <bpmn:sequenceFlow id="approved" sourceRef="decide" targetRef="pay">
      <bpmn:conditionExpression>{"==": [{"var": "review.decision"}, "approve"]}</bpmn:conditionExpression>
    </bpmn:sequenceFlow>
    <bpmn:sequenceFlow id="rejected" sourceRef="decide" targetRef="end_rejected"/>
    <bpmn:sequenceFlow id="f4" sourceRef="pay" targetRef="end"/>
  </bpmn:process>
  <bpmndi:BPMNDiagram id="diagram"/>
</bpmn:definitions>"""
)


@pytest.mark.asyncio
async def test_a_modeler_diagram_deploys_runs_and_keeps_its_source(flow: Flow):
    @flow.handler("finance.pay")
    async def pay(ctx, variables):
        return {"paid": variables["amount"], "attempts_allowed": ctx.max_attempts}

    definition = await flow.deploy(APPROVAL, namespace="org")
    waiting = await run_to_completion(flow, "expense", namespace="org", variables={"amount": 80})
    await flow.complete_task(waiting.instance_id, "review", {"decision": "approve"})
    done = await run_pending(flow, waiting.instance_id)

    assert definition.key == "expense"
    assert done.status == "completed"
    assert done.variables["paid"] == 80
    assert done.variables["attempts_allowed"] == 5
    assert definition.source == APPROVAL


@pytest.mark.asyncio
async def test_the_same_process_from_xml_and_from_json_is_one_version(flow: Flow):
    from_xml = parse_bpmn(APPROVAL)

    first = await flow.deploy(APPROVAL)
    second = await flow.deploy(from_xml.model_dump(mode="json", exclude_none=True))

    assert second.version == first.version


def test_call_activities_and_ad_hoc_subprocesses_import():
    model = parse_bpmn(
        HEAD
        + """
  <bpmn:process id="agentic">
    <bpmn:startEvent id="start"/>
    <bpmn:callActivity id="child">
      <bpmn:extensionElements><zeebe:calledElement processId="escalation"/></bpmn:extensionElements>
    </bpmn:callActivity>
    <bpmn:adHocSubProcess id="agent" flow:taskType="ai.decide" flow:maxIterations="4" flow:resultVariable="answer">
      <bpmn:serviceTask id="search" flow:taskType="tool.search"/>
    </bpmn:adHocSubProcess>
    <bpmn:endEvent id="end"/>
    <bpmn:sequenceFlow id="a" sourceRef="start" targetRef="child"/>
    <bpmn:sequenceFlow id="b" sourceRef="child" targetRef="agent"/>
    <bpmn:sequenceFlow id="c" sourceRef="agent" targetRef="end"/>
  </bpmn:process>
</bpmn:definitions>"""
    )

    agent = model.element("agent")
    assert isinstance(agent, AdHocSubProcess)
    assert (agent.task_type, agent.max_iterations, agent.result_variable) == ("ai.decide", 4, "answer")
    assert isinstance(agent.elements[0], ServiceTask) and agent.elements[0].task_type == "tool.search"
    assert model.element("child").called_element == "escalation"  # type: ignore[union-attr]


@pytest.mark.parametrize(
    ("body", "problem"),
    [
        pytest.param('<bpmn:parallelGateway id="split"/>', "not supported: ['parallelGateway']", id="parallel"),
        pytest.param(
            '<bpmn:intermediateCatchEvent id="wait"><bpmn:timerEventDefinition/></bpmn:intermediateCatchEvent>',
            "intermediateCatchEvent",
            id="timer",
        ),
    ],
)
def test_an_element_outside_the_subset_is_named(body: str, problem: str):
    with pytest.raises(BadRequestError, match=problem.replace("[", r"\[").replace("]", r"\]")):
        parse_bpmn(HEAD + f'<bpmn:process id="p"><bpmn:startEvent id="s"/>{body}</bpmn:process></bpmn:definitions>')


def test_an_event_definition_on_a_start_event_is_refused():
    with pytest.raises(BadRequestError, match="startEvent with timerEventDefinition"):
        parse_bpmn(
            HEAD
            + '<bpmn:process id="p"><bpmn:startEvent id="s"><bpmn:timerEventDefinition/></bpmn:startEvent>'
            + '<bpmn:endEvent id="e"/><bpmn:sequenceFlow id="f" sourceRef="s" targetRef="e"/>'
            + "</bpmn:process></bpmn:definitions>"
        )


def test_a_feel_condition_is_refused_with_the_reason():
    xml = APPROVAL.replace('{"==": [{"var": "review.decision"}, "approve"]}', '=review.decision = "approve"')
    with pytest.raises(BadRequestError, match="a condition is JSON Logic"):
        parse_bpmn(xml)


def test_a_doctype_is_refused():
    """A DOCTYPE is how an entity-expansion payload arrives; a process definition never needs one."""
    with pytest.raises(BadRequestError, match="DOCTYPE"):
        parse_bpmn('<?xml version="1.0"?><!DOCTYPE x [<!ENTITY a "aaaa">]>' + APPROVAL.split("?>", 1)[1])


def test_a_document_must_name_its_process_when_it_has_several():
    two = APPROVAL.replace("</bpmn:definitions>", '<bpmn:process id="other"/></bpmn:definitions>')

    with pytest.raises(BadRequestError, match="expected one process"):
        parse_bpmn(two)
    assert parse_bpmn(two, key="expense").key == "expense"


def test_malformed_xml_is_refused():
    with pytest.raises(BadRequestError, match="not well-formed"):
        parse_bpmn("<bpmn:definitions")
