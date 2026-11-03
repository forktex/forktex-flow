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

"""ORM row → domain value object. One-way: persist knows about domain, never the reverse."""

from __future__ import annotations

from typing import cast

from forktex_flow.domain.process import ProcessModel
from forktex_flow.domain.types import (
    DefinitionInfo,
    ElementRunInfo,
    ElementStatus,
    InstanceInfo,
    InstanceStatus,
    TriggeredBy,
)
from forktex_flow.persist.models import Definition, ElementRun, Instance

__all__ = ["to_definition_info", "to_element_info", "to_instance_info"]


def to_definition_info(row: Definition) -> DefinitionInfo:
    return DefinitionInfo(
        definition_id=row.id,
        namespace=row.namespace,
        key=row.key,
        version=row.version,
        checksum=row.checksum,
        model=ProcessModel.model_validate(row.model),
        source=row.source,
        created_at=row.created_at,
    )


def to_instance_info(row: Instance) -> InstanceInfo:
    # The status columns are plain VARCHARs restricted by CHECK constraints built from the same Literals.
    return InstanceInfo(
        instance_id=row.id,
        namespace=row.namespace,
        key=row.key,
        version=row.version,
        definition_id=row.definition_id,
        status=cast(InstanceStatus, row.status),
        input=row.input or {},
        variables=row.variables or {},
        metadata=row.metadata_ or {},
        error=row.error,
        cancel_reason=row.cancel_reason,
        triggered_by=cast(TriggeredBy, row.triggered_by),
        parent_instance_id=row.parent_instance_id,
        parent_element_id=row.parent_element_id,
        started_at=row.started_at,
        finished_at=row.finished_at,
        elements=[to_element_info(element) for element in row.elements],
    )


def to_element_info(row: ElementRun) -> ElementRunInfo:
    return ElementRunInfo(
        element_run_id=row.id,
        element_id=row.element_id,
        element_type=row.element_type,
        task_type=row.task_type,
        status=cast(ElementStatus, row.status),
        output=row.output,
        error=row.error,
        attempts=row.attempts,
        max_attempts=row.max_attempts,
        next_attempt_at=row.next_attempt_at,
        started_at=row.started_at,
        finished_at=row.finished_at,
        heartbeat_at=row.heartbeat_at,
        child_instance_id=row.child_instance_id,
        input=row.input,
    )
