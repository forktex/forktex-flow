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

"""ORM row → domain value object.

Both mappers live here rather than in the aggregate that owns the table, because
`to_run_info` embeds the step list: keeping them together is what stops `runs` and `steps`
importing each other's privates. The direction is one-way — persist knows about domain,
never the reverse.
"""

from __future__ import annotations

from typing import cast

from forktex_flow.domain.types import RunInfo, RunStatus, StepRunInfo, StepStatus
from forktex_flow.persist.models import Run, StepRun

__all__ = ["to_run_info", "to_step_info"]


def to_run_info(row: Run) -> RunInfo:
    return RunInfo(
        run_id=row.id,
        workflow_name=row.workflow_name,
        workflow_version=row.workflow_version,
        # The column is a plain VARCHAR; `ck_run_status` is what restricts it to
        # the five values `RunStatus` names, so the narrowing is guaranteed by the
        # schema rather than merely asserted here.
        status=cast(RunStatus, row.status),
        input=row.input or {},
        output=row.output,
        error=row.error,
        metadata=row.metadata_ or {},
        started_at=row.started_at,
        finished_at=row.finished_at,
        steps=[to_step_info(s) for s in row.steps],
    )


def to_step_info(row: StepRun) -> StepRunInfo:
    return StepRunInfo(
        step_id=row.id,
        step_name=row.step_name,
        step_index=row.step_index,
        status=cast(StepStatus, row.status),  # see the note in `to_run_info`
        output=row.output,
        error=row.error,
        attempts=row.attempts,
        started_at=row.started_at,
        finished_at=row.finished_at,
        heartbeat_at=row.heartbeat_at,
    )
