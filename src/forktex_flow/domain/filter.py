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

"""``RunFilter`` — what a caller is asking for, without any SQL.

Extracted so the read layer compiles a value object instead of reaching into a
query builder's private attributes, and so the same filter set can be applied to
the page query, the count and the summary. It previously could not be: each of
the three built its own ``where`` list, and the summary's list was missing the
metadata, state and current-node filters — so ``.metadata(k=v).summary()``
silently aggregated over every run in the table.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from forktex.types import BaseValueObject
from pydantic import Field


class RunFilter(BaseValueObject):
    """The predicate half of a run query. Frozen, and free of I/O.

    Every field is optional; an empty ``RunFilter`` matches every run.

    Attributes:
        workflow_name:    Exact match on the workflow's name.
        workflow_version: Exact match on its version. Only meaningful with a name.
        statuses:         Any-of match on run status.
        metadata_filter:  JSONB containment against ``run.metadata``.
        state_filter:     JSONB containment against ``run.input``.
        current_nodes:    Runs whose currently-running step is one of these.
        since:            ``started_at >= since``.
        until:            ``started_at < until``.
        triggered_by:     Any-of match on the trigger label.
    """

    workflow_name: str | None = None
    workflow_version: int | None = None
    statuses: tuple[str, ...] = ()
    metadata_filter: dict[str, Any] = Field(default_factory=dict)
    state_filter: dict[str, Any] = Field(default_factory=dict)
    current_nodes: tuple[str, ...] = ()
    since: datetime | None = None
    until: datetime | None = None
    triggered_by: tuple[str, ...] = ()


__all__ = ["RunFilter"]
