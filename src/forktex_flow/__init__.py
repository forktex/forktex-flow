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

from forktex_flow.domain.config import RuntimeConfig
from forktex_flow.domain.node import StepSpec, step
from forktex_flow.domain.state import apply_state_update, extract_reducers
from forktex_flow.domain.types import (
    TERMINAL_STATUSES,
    RunInfo,
    RunStatus,
    RunSummary,
    StepRunInfo,
    StepStatus,
)
from forktex_flow.errors import (
    FlowClosed,
    FlowError,
    StateNotSerialisable,
    StepFailed,
    WorkflowCancelled,
    WorkflowFailed,
)
from forktex_flow.extension import FlowExtension
from forktex_flow.flow import Flow
from forktex_flow.persist.migrations import apply_migrations
from forktex_flow.read.instance import (
    InstancePage,
    InstanceQuery,
    NodeInstance,
    WorkflowInstance,
)
from forktex_flow.runtime.ctx import Ctx
from forktex_flow.testing import isolated_flow, make_ctx, run_to_completion

__all__ = [
    "TERMINAL_STATUSES",
    "Ctx",
    "Flow",
    "FlowClosed",
    "FlowError",
    "FlowExtension",
    "InstancePage",
    "InstanceQuery",
    "NodeInstance",
    "RunInfo",
    "RunStatus",
    "RunSummary",
    "RuntimeConfig",
    "StateNotSerialisable",
    "StepFailed",
    "StepRunInfo",
    "StepSpec",
    "StepStatus",
    "WorkflowCancelled",
    "WorkflowFailed",
    "WorkflowInstance",
    "apply_migrations",
    "apply_state_update",
    "extract_reducers",
    "isolated_flow",
    "make_ctx",
    "run_to_completion",
    "step",
]
