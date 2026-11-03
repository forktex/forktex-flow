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

from datetime import datetime
from typing import Literal
from uuid import UUID

from forktex.types import BaseValueObject, BaseWireValueObject, JsonObject
from pydantic import Field

from forktex_flow.domain.process import ProcessModel

InstanceStatus = Literal["active", "completed", "failed", "cancelled"]
ElementStatus = Literal["pending", "running", "waiting", "completed", "failed", "cancelled"]
TriggeredBy = Literal["manual", "schedule", "call"]

#: The statuses an instance does not leave, and at which it acquires a ``finished_at``.
TERMINAL_STATUSES: frozenset[str] = frozenset({"completed", "failed", "cancelled"})

#: The element statuses that still hold the instance's token.
OPEN_ELEMENT_STATUSES: tuple[str, ...] = ("pending", "running", "waiting")

#: The namespace a code-declared workflow lives in, and the one every call defaults to.
DEFAULT_NAMESPACE = "default"


class DefinitionInfo(BaseWireValueObject):
    definition_id: UUID
    namespace: str
    key: str
    version: int
    checksum: str
    model: ProcessModel
    #: What was deployed, verbatim: BPMN XML as given, or the model's JSON.
    source: str
    created_at: datetime


class InstanceEventInfo(BaseWireValueObject):
    """One entry of an instance's history: the engine's own (``element_started``, ``instance_completed``, …) or
    one a handler emitted.
    """

    id: int
    instance_id: UUID
    ts: datetime
    event_type: str
    payload: JsonObject


class ElementRunInfo(BaseWireValueObject):
    element_run_id: UUID
    element_id: str
    element_type: str
    task_type: str | None
    status: ElementStatus
    output: JsonObject | None
    error: str | None
    attempts: int
    max_attempts: int
    next_attempt_at: datetime | None
    started_at: datetime
    finished_at: datetime | None
    heartbeat_at: datetime | None
    child_instance_id: UUID | None
    input: JsonObject | None = None


class InstanceInfo(BaseWireValueObject):
    instance_id: UUID
    namespace: str
    key: str
    version: int
    definition_id: UUID
    status: InstanceStatus
    input: JsonObject
    variables: JsonObject
    metadata: JsonObject
    error: str | None
    cancel_reason: str | None
    triggered_by: TriggeredBy
    parent_instance_id: UUID | None
    parent_element_id: str | None
    started_at: datetime
    finished_at: datetime | None
    elements: list[ElementRunInfo] = Field(default_factory=list)


class Job(BaseValueObject):
    element_run_id: UUID
    instance_id: UUID
    element_id: str
    task_type: str
    attempts: int
    max_attempts: int


class InstanceSummary(BaseWireValueObject):
    total: int
    by_status: dict[str, int]
    avg_duration_seconds: float | None
    p95_duration_seconds: float | None
    oldest_started_at: datetime | None
    newest_started_at: datetime | None


__all__ = [
    "DEFAULT_NAMESPACE",
    "OPEN_ELEMENT_STATUSES",
    "TERMINAL_STATUSES",
    "DefinitionInfo",
    "ElementRunInfo",
    "ElementStatus",
    "InstanceEventInfo",
    "InstanceInfo",
    "InstanceStatus",
    "InstanceSummary",
    "Job",
    "TriggeredBy",
]
