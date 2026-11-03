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

"""Postgres-native durable workflow execution, built on the ``forktex`` substrate."""

from forktex_flow.domain.bpmn import parse_bpmn
from forktex_flow.domain.config import RuntimeConfig
from forktex_flow.domain.node import StepSpec, step
from forktex_flow.domain.process import (
    Activation,
    AdHocDecision,
    AdHocSubProcess,
    CallActivity,
    EndEvent,
    ExclusiveGateway,
    ProcessModel,
    SequenceFlow,
    ServiceTask,
    StartEvent,
    UserTask,
)
from forktex_flow.domain.state import apply_state_update, extract_reducers
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
from forktex_flow.errors import FlowClosed, FlowError, StateNotSerialisable, WorkflowCancelled, WorkflowFailed
from forktex_flow.extension import FlowExtension
from forktex_flow.flow import Flow
from forktex_flow.persist.migrations import apply_migrations
from forktex_flow.read.instance import InstancePage, InstanceQuery, WorkflowInstance
from forktex_flow.runtime.ctx import Ctx
from forktex_flow.testing import isolated_flow, make_ctx, run_pending, run_to_completion

__all__ = [
    "DEFAULT_NAMESPACE",
    "TERMINAL_STATUSES",
    "Activation",
    "AdHocDecision",
    "AdHocSubProcess",
    "CallActivity",
    "Ctx",
    "DefinitionInfo",
    "ElementRunInfo",
    "ElementStatus",
    "EndEvent",
    "ExclusiveGateway",
    "Flow",
    "FlowClosed",
    "FlowError",
    "FlowExtension",
    "InstanceInfo",
    "InstancePage",
    "InstanceQuery",
    "InstanceStatus",
    "InstanceSummary",
    "ProcessModel",
    "RuntimeConfig",
    "SequenceFlow",
    "ServiceTask",
    "StartEvent",
    "StateNotSerialisable",
    "StepSpec",
    "UserTask",
    "WorkflowCancelled",
    "WorkflowFailed",
    "WorkflowInstance",
    "apply_migrations",
    "apply_state_update",
    "extract_reducers",
    "isolated_flow",
    "make_ctx",
    "parse_bpmn",
    "run_pending",
    "run_to_completion",
    "step",
]
