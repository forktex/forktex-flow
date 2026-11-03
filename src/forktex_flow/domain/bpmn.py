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

import json
import xml.etree.ElementTree as ET

from forktex.error import BadRequestError
from forktex.types import JsonObject

from forktex_flow.domain.process import ProcessModel

__all__ = ["BPMN_NS", "FLOW_NS", "ZEEBE_NS", "parse_bpmn"]

BPMN_NS = "http://www.omg.org/spec/BPMN/20100524/MODEL"
ZEEBE_NS = "http://camunda.org/schema/zeebe/1.0"
FLOW_NS = "https://forktex.com/schema/flow"

_ACTIVITIES = {"serviceTask", "userTask", "callActivity", "adHocSubProcess"}
_NODES = {"startEvent", "endEvent", "exclusiveGateway", *_ACTIVITIES}

#: Children of a process that describe it without executing: kept in the source, absent from the model.
_DESCRIPTIVE = {
    "documentation",
    "extensionElements",
    "laneSet",
    "textAnnotation",
    "association",
    "incoming",
    "outgoing",
}


def parse_bpmn(xml: str, *, key: str | None = None) -> ProcessModel:
    """The process ``key`` names, or the document's only one. Raises ``BadRequestError`` naming every element it
    cannot execute.
    """
    if "<!DOCTYPE" in xml.upper():
        raise BadRequestError("a BPMN document with a DOCTYPE is refused")
    try:
        root = ET.fromstring(xml)
    except ET.ParseError as exc:
        raise BadRequestError(f"not well-formed XML: {exc}") from exc

    processes = root.findall(_bpmn("process"))
    chosen = [p for p in processes if key is None or p.get("id") == key]
    if len(chosen) != 1:
        found = [p.get("id") for p in processes]
        raise BadRequestError(f"expected one process{'' if key is None else f' {key!r}'}, found {found}")
    process = chosen[0]

    unsupported = sorted(
        {_local(child.tag) for child in process if _local(child.tag) not in {*_NODES, "sequenceFlow", *_DESCRIPTIVE}}
    )
    elements = [_node(child, unsupported) for child in process if _local(child.tag) in _NODES]
    if unsupported:
        raise BadRequestError(f"BPMN elements not supported: {unsupported}")
    flows = [_flow(child) for child in process if _local(child.tag) == "sequenceFlow"]
    return ProcessModel.model_validate({"key": process.get("id"), "elements": elements, "flows": flows})


def _node(node: ET.Element, unsupported: list[str]) -> JsonObject:
    kind = _local(node.tag)
    out: JsonObject = {"id": node.get("id"), "type": kind}
    for child in node:
        if _local(child.tag).endswith("EventDefinition"):
            unsupported.append(f"{kind} with {_local(child.tag)}")
    if kind in {"serviceTask", "adHocSubProcess"}:
        definition = node.find(f"{_bpmn('extensionElements')}/{{{ZEEBE_NS}}}taskDefinition")
        out["taskType"] = node.get(f"{{{FLOW_NS}}}taskType") or (
            definition.get("type") if definition is not None else None
        )
        retries = definition.get("retries") if definition is not None else None
        if retries is not None:
            out["retries"] = int(retries)
    if kind == "exclusiveGateway" and node.get("default"):
        out["default"] = node.get("default")
    if kind == "callActivity":
        called = node.find(f"{_bpmn('extensionElements')}/{{{ZEEBE_NS}}}calledElement")
        out["calledElement"] = node.get("calledElement") or (called.get("processId") if called is not None else None)
        if node.get(f"{{{FLOW_NS}}}version"):
            out["version"] = int(node.get(f"{{{FLOW_NS}}}version") or 0)
    if kind == "adHocSubProcess":
        out["elements"] = [_node(child, unsupported) for child in node if _local(child.tag) == "serviceTask"]
        others = {_local(c.tag) for c in node} - {"serviceTask", *_DESCRIPTIVE}
        unsupported.extend(f"{other} inside adHocSubProcess" for other in sorted(others))
        if node.get(f"{{{FLOW_NS}}}maxIterations"):
            out["maxIterations"] = int(node.get(f"{{{FLOW_NS}}}maxIterations") or 0)
    if kind in _ACTIVITIES and node.get(f"{{{FLOW_NS}}}resultVariable"):
        out["resultVariable"] = node.get(f"{{{FLOW_NS}}}resultVariable")
    return {k: v for k, v in out.items() if v is not None}


def _flow(node: ET.Element) -> JsonObject:
    out: JsonObject = {"id": node.get("id"), "source": node.get("sourceRef"), "target": node.get("targetRef")}
    expression = node.find(_bpmn("conditionExpression"))
    if expression is not None and (expression.text or "").strip():
        try:
            out["condition"] = json.loads(expression.text or "")
        except json.JSONDecodeError as exc:
            raise BadRequestError(
                f"flow {node.get('id')!r}: a condition is JSON Logic, and {expression.text!r} is not JSON"
            ) from exc
    return out


def _bpmn(tag: str) -> str:
    return f"{{{BPMN_NS}}}{tag}"


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]
