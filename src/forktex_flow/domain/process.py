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


from __future__ import annotations

import hashlib
import json
from collections import deque
from typing import Annotated, Literal

from forktex.error import BadRequestError
from forktex.types import BaseWireValueObject, JsonObject
from pydantic import Field, model_validator

from forktex_flow.domain.jsonlogic import check_rule

__all__ = [
    "ACTIVITY_SEPARATOR",
    "Activation",
    "AdHocDecision",
    "AdHocSubProcess",
    "CallActivity",
    "Element",
    "EndEvent",
    "ExclusiveGateway",
    "ProcessModel",
    "SequenceFlow",
    "ServiceTask",
    "StartEvent",
    "UserTask",
]


class StartEvent(BaseWireValueObject):
    id: str
    type: Literal["startEvent"] = "startEvent"


class EndEvent(BaseWireValueObject):
    id: str
    type: Literal["endEvent"] = "endEvent"


class ServiceTask(BaseWireValueObject):
    id: str
    type: Literal["serviceTask"] = "serviceTask"
    task_type: str
    retries: int | None = Field(default=None, ge=1)
    backoff: tuple[float, ...] | None = None
    result_variable: str | None = None


class UserTask(BaseWireValueObject):
    id: str
    type: Literal["userTask"] = "userTask"
    result_variable: str | None = None


class ExclusiveGateway(BaseWireValueObject):
    id: str
    type: Literal["exclusiveGateway"] = "exclusiveGateway"
    default: str | None = None


class CallActivity(BaseWireValueObject):
    id: str
    type: Literal["callActivity"] = "callActivity"
    called_element: str
    version: int | None = Field(default=None, ge=1)
    result_variable: str | None = None


class AdHocSubProcess(BaseWireValueObject):
    id: str
    type: Literal["adHocSubProcess"] = "adHocSubProcess"
    task_type: str
    elements: tuple[ServiceTask, ...] = Field(min_length=1)
    max_iterations: int = Field(default=10, ge=1)
    retries: int | None = Field(default=None, ge=1)
    backoff: tuple[float, ...] | None = None
    result_variable: str | None = None

    def activity(self, element_id: str) -> ServiceTask | None:
        return next((element for element in self.elements if element.id == element_id), None)


class Activation(BaseWireValueObject):
    element_id: str
    variables: JsonObject = Field(default_factory=dict)


class AdHocDecision(BaseWireValueObject):
    activate: tuple[Activation, ...] = ()
    done: bool = False
    output: JsonObject = Field(default_factory=dict)


Element = Annotated[
    StartEvent | EndEvent | ServiceTask | UserTask | ExclusiveGateway | CallActivity | AdHocSubProcess,
    Field(discriminator="type"),
]

#: Separates an ad-hoc subprocess, its round and position, and its activity in a run's id: ``agent/2.1/search``.
ACTIVITY_SEPARATOR = "/"


class SequenceFlow(BaseWireValueObject):
    id: str
    source: str
    target: str
    #: A JSON Logic rule over the instance's variables. Only a gateway's outgoing flows carry one.
    condition: JsonObject | None = None


class ProcessModel(BaseWireValueObject):
    key: str = Field(min_length=1, max_length=255)
    elements: tuple[Element, ...]
    flows: tuple[SequenceFlow, ...] = ()

    @model_validator(mode="after")
    def _check(self) -> ProcessModel:
        problems = _problems(self)
        if problems:
            raise BadRequestError(f"process {self.key!r} cannot run: " + "; ".join(problems))
        return self

    @property
    def start(self) -> StartEvent:
        return next(element for element in self.elements if isinstance(element, StartEvent))

    def element(self, element_id: str) -> Element:
        """Raises ``BadRequestError`` for an id this model does not hold."""
        for element in self.elements:
            if element.id == element_id:
                return element
        raise BadRequestError(f"process {self.key!r} has no element {element_id!r}")

    def activity(self, run_element_id: str) -> tuple[AdHocSubProcess, int, ServiceTask] | None:
        parts = run_element_id.split(ACTIVITY_SEPARATOR)
        round_number = parts[1].split(".")[0] if len(parts) == 3 else ""
        if not round_number.isdigit():
            return None
        parent = self.element(parts[0])
        if not isinstance(parent, AdHocSubProcess):
            return None
        activity = parent.activity(parts[2])
        return None if activity is None else (parent, int(round_number), activity)

    def outgoing(self, element_id: str) -> tuple[SequenceFlow, ...]:
        return tuple(flow for flow in self.flows if flow.source == element_id)

    def checksum(self) -> str:
        canonical = json.dumps(self.model_dump(mode="json", exclude_none=True), sort_keys=True)
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _problems(model: ProcessModel) -> list[str]:
    ids = [element.id for element in model.elements]
    inner = [a.id for e in model.elements if isinstance(e, AdHocSubProcess) for a in e.elements]
    problems_inner = [f"id {i!r} contains {ACTIVITY_SEPARATOR!r}" for i in [*ids, *inner] if ACTIVITY_SEPARATOR in i]
    problems_inner += [
        f"activity id {i!r} is used twice inside one adHocSubProcess"
        for e in model.elements
        if isinstance(e, AdHocSubProcess)
        for i in sorted({a.id for a in e.elements if [b.id for b in e.elements].count(a.id) > 1})
    ]
    problems = [f"element id {i!r} is used twice" for i in sorted({i for i in ids if ids.count(i) > 1})]
    problems += problems_inner
    flow_ids = [flow.id for flow in model.flows]
    problems += [f"flow id {i!r} is used twice" for i in sorted({i for i in flow_ids if flow_ids.count(i) > 1})]
    known = set(ids)
    problems += [
        f"flow {flow.id!r} refers to unknown element {end!r}"
        for flow in model.flows
        for end in (flow.source, flow.target)
        if end not in known
    ]
    starts = [element for element in model.elements if isinstance(element, StartEvent)]
    if len(starts) != 1:
        problems.append(f"needs exactly one startEvent, has {len(starts)}")
    if not any(isinstance(element, EndEvent) for element in model.elements):
        problems.append("needs at least one endEvent")
    if problems:
        return problems  # the shape checks below assume ids resolve

    for element in model.elements:
        problems += _element_problems(model, element)
    problems += _graph_problems(model)
    return problems


def _element_problems(model: ProcessModel, element: Element) -> list[str]:
    outgoing = model.outgoing(element.id)
    incoming = [flow for flow in model.flows if flow.target == element.id]
    problems: list[str] = []
    if isinstance(element, StartEvent) and incoming:
        problems.append(f"startEvent {element.id!r} has incoming flows")
    if not isinstance(element, StartEvent) and not incoming:
        problems.append(f"{element.type} {element.id!r} has no incoming flow")
    if isinstance(element, EndEvent):
        if outgoing:
            problems.append(f"endEvent {element.id!r} has outgoing flows")
        return problems

    if not isinstance(element, ExclusiveGateway):
        if len(outgoing) != 1:
            # More than one would be BPMN's implicit parallel split, which this engine does not run.
            problems.append(f"{element.type} {element.id!r} needs exactly one outgoing flow, has {len(outgoing)}")
        problems += [f"flow {flow.id!r} has a condition but leaves no gateway" for flow in outgoing if flow.condition]
        return problems

    if not outgoing:
        problems.append(f"exclusiveGateway {element.id!r} has no outgoing flow")
    if element.default is not None and element.default not in {flow.id for flow in outgoing}:
        problems.append(f"exclusiveGateway {element.id!r}: default {element.default!r} is not one of its flows")
    for flow in outgoing:
        if flow.id == element.default:
            if flow.condition is not None:
                problems.append(f"flow {flow.id!r} is a default and cannot carry a condition")
        elif flow.condition is None:
            problems.append(f"flow {flow.id!r} leaves gateway {element.id!r} without a condition or being its default")
        else:
            try:
                check_rule(flow.condition)
            except BadRequestError as exc:
                problems.append(f"flow {flow.id!r}: {exc.message}")
    return problems


def _graph_problems(model: ProcessModel) -> list[str]:
    targets: dict[str, list[str]] = {element.id: [] for element in model.elements}
    indegree: dict[str, int] = dict.fromkeys(targets, 0)
    for flow in model.flows:
        targets[flow.source].append(flow.target)
        indegree[flow.target] += 1

    reachable: set[str] = set()
    pending = deque([model.start.id])
    while pending:
        current = pending.popleft()
        if current not in reachable:
            reachable.add(current)
            pending.extend(targets[current])

    ready = deque(i for i, degree in indegree.items() if degree == 0)
    ordered = 0
    while ready:
        current = ready.popleft()
        ordered += 1
        for target in targets[current]:
            indegree[target] -= 1
            if indegree[target] == 0:
                ready.append(target)

    problems = [f"element {i!r} is not reachable from the start event" for i in targets if i not in reachable]
    if ordered < len(targets):
        cyclic = sorted(i for i, degree in indegree.items() if degree > 0)
        problems.append(f"the flows form a cycle through {cyclic}; a process must be acyclic")
    return problems
