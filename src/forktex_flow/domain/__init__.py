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

"""Flow's vocabulary: the process model, its conditions, and the value objects the engine reads and writes."""

from forktex_flow.domain.config import RuntimeConfig
from forktex_flow.domain.node import HandlerFn, StepSpec, check_handler, step
from forktex_flow.domain.process import (
    Activation,
    AdHocDecision,
    AdHocSubProcess,
    CallActivity,
    Element,
    EndEvent,
    ExclusiveGateway,
    ProcessModel,
    SequenceFlow,
    ServiceTask,
    StartEvent,
    UserTask,
)
from forktex_flow.domain.state import ReducerFn, apply_state_update, extract_reducers
from forktex_flow.domain.types import (
    DEFAULT_NAMESPACE,
    TERMINAL_STATUSES,
    DefinitionInfo,
    ElementRunInfo,
    ElementStatus,
    InstanceInfo,
    InstanceStatus,
    InstanceSummary,
)

__all__ = [
    "DEFAULT_NAMESPACE",
    "TERMINAL_STATUSES",
    "Activation",
    "AdHocDecision",
    "AdHocSubProcess",
    "CallActivity",
    "DefinitionInfo",
    "Element",
    "ElementRunInfo",
    "ElementStatus",
    "EndEvent",
    "ExclusiveGateway",
    "HandlerFn",
    "InstanceInfo",
    "InstanceStatus",
    "InstanceSummary",
    "ProcessModel",
    "ReducerFn",
    "RuntimeConfig",
    "SequenceFlow",
    "ServiceTask",
    "StartEvent",
    "StepSpec",
    "UserTask",
    "apply_state_update",
    "check_handler",
    "extract_reducers",
    "step",
]
