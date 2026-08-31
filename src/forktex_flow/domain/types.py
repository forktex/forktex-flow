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

"""The row shapes the read layer returns.

``BaseWireValueObject`` — frozen, and camelCase on the wire — because consumers
serialise these straight out of an HTTP route with ``model_dump()``. The typed
shape is the contract, so changing a field here changes someone's JSON.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from forktex.types import BaseWireValueObject, JsonValue
from pydantic import Field

#: Runs and steps share one status vocabulary — a step is a run's work in
#: miniature, and two lists that must stay identical are one list. Both names
#: are exported so a signature can say which it means; the CHECK constraints in
#: `v0001__schema.sql` are what actually enforce the values.
RunStatus = Literal["pending", "running", "completed", "failed", "cancelled"]
StepStatus = RunStatus

#: The statuses a run does not leave, and at which it acquires a ``finished_at``.
#: Polling and streaming stop here.
TERMINAL_STATUSES: frozenset[str] = frozenset({"completed", "failed", "cancelled"})


class StepRunInfo(BaseWireValueObject):
    """One step invocation within a run."""

    step_id: UUID
    step_name: str
    step_index: int
    status: StepStatus
    output: JsonValue
    error: str | None
    attempts: int
    started_at: datetime | None
    finished_at: datetime | None
    heartbeat_at: datetime | None


class RunInfo(BaseWireValueObject):
    """Full state of a workflow run, including per-step progress."""

    run_id: UUID
    workflow_name: str
    workflow_version: int
    status: RunStatus
    input: dict[str, Any]
    output: dict[str, Any] | None
    error: str | None
    metadata: dict[str, Any]
    started_at: datetime
    finished_at: datetime | None
    steps: list[StepRunInfo] = Field(default_factory=list)


class RunSummary(BaseWireValueObject):
    """Aggregate statistics over a matching set of runs.

    ``p95_duration_seconds`` is the worst per-status p95, not a true global one —
    computing that needs an ungrouped pass. Stated because the name suggests
    otherwise.
    """

    total: int
    by_status: dict[str, int]
    avg_duration_seconds: float | None
    p95_duration_seconds: float | None
    oldest_started_at: datetime | None
    newest_started_at: datetime | None


__all__ = [
    "TERMINAL_STATUSES",
    "RunInfo",
    "RunStatus",
    "RunSummary",
    "StepRunInfo",
    "StepStatus",
]
