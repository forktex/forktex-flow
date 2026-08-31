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

"""Flow's vocabulary: frozen value objects and the declarative workflow shapes.

The bottom of the package's DAG — nothing here imports anything else from
`forktex_flow`, and nothing here performs I/O. A reader can learn what a workflow
*is* from this layer alone, without meeting a session or an engine.

Four modules, split by what they describe: `types` the run/step row shapes and
statuses, `definition` the compiled workflow (an ordered list of steps), `node`
the authoring decorator, and `state` the reducer that merges a step's partial
update into the run state.
"""

from forktex_flow.domain.definition import (
    NodeDef,
    NodeFn,
    WhenFn,
    WorkflowDefinition,
)
from forktex_flow.domain.node import (
    StepMeta,
    StepSpec,
    has_step_meta,
    step,
    step_meta,
)
from forktex_flow.domain.state import ReducerFn, apply_state_update, extract_reducers
from forktex_flow.domain.types import (
    TERMINAL_STATUSES,
    RunInfo,
    RunStatus,
    StepRunInfo,
    StepStatus,
)

__all__ = [
    "TERMINAL_STATUSES",
    "NodeDef",
    "NodeFn",
    "ReducerFn",
    "RunInfo",
    "RunStatus",
    "StepMeta",
    "StepRunInfo",
    "StepSpec",
    "StepStatus",
    "WhenFn",
    "WorkflowDefinition",
    "apply_state_update",
    "extract_reducers",
    "has_step_meta",
    "step",
    "step_meta",
]
