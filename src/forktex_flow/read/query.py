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

from typing import TYPE_CHECKING
from uuid import UUID

import sqlalchemy as sa
from forktex.database import Page, decode_cursor, encode_cursor
from forktex.iso import from_iso, to_iso
from sqlalchemy.dialects.postgresql import JSONB

from forktex_flow.domain.types import OPEN_ELEMENT_STATUSES, InstanceInfo, InstanceSummary
from forktex_flow.persist.mappers import to_instance_info
from forktex_flow.persist.models import ElementRun, Instance

if TYPE_CHECKING:
    from forktex_flow.domain.filter import InstanceFilter
    from forktex_flow.persist.store import FlowStore

__all__ = ["count_instances", "fetch_page", "summarise_instances"]


async def fetch_page(
    store: FlowStore, spec: InstanceFilter, *, limit: int, cursor: str | None = None
) -> Page[InstanceInfo]:
    statement = sa.select(Instance).where(*_clauses(spec)).order_by(Instance.started_at.desc(), Instance.id.desc())
    if cursor is not None:
        started_at, instance_id = decode_cursor(cursor, expected_length=2)
        boundary = sa.tuple_(
            sa.literal(from_iso(str(started_at)), type_=Instance.started_at.type),
            sa.literal(UUID(str(instance_id)), type_=Instance.id.type),
        )
        statement = statement.where(sa.tuple_(Instance.started_at, Instance.id) < boundary)

    async with store.transaction() as session:
        rows = (await session.scalars(statement.limit(limit + 1))).all()
        items = [to_instance_info(row) for row in rows[:limit]]

    has_more = len(rows) > limit
    last = items[-1] if has_more and items else None
    return Page[InstanceInfo](
        items=items,
        has_more=has_more,
        next_cursor=None if last is None else encode_cursor([to_iso(last.started_at), str(last.instance_id)]),
    )


async def count_instances(store: FlowStore, spec: InstanceFilter) -> int:
    async with store.transaction() as session:
        return (await session.execute(sa.select(sa.func.count(Instance.id)).where(*_clauses(spec)))).scalar_one()


async def summarise_instances(store: FlowStore, spec: InstanceFilter) -> InstanceSummary:
    duration = sa.extract("epoch", Instance.finished_at - Instance.started_at)
    statement = (
        sa.select(
            Instance.status,
            sa.func.count(Instance.id).label("total"),
            sa.func.avg(duration).label("avg_duration"),
            sa.func.percentile_cont(0.95).within_group(duration.asc()).label("p95_duration"),
            sa.func.min(Instance.started_at).label("oldest"),
            sa.func.max(Instance.started_at).label("newest"),
        )
        .where(*_clauses(spec))
        .group_by(Instance.status)
    )
    async with store.transaction() as session:
        rows = (await session.execute(statement)).all()

    timed = [row for row in rows if row.avg_duration is not None]
    return InstanceSummary(
        total=sum(row.total for row in rows),
        by_status={row.status: row.total for row in rows},
        avg_duration_seconds=(
            sum(float(row.avg_duration) * row.total for row in timed) / sum(row.total for row in timed)
            if timed
            else None
        ),
        p95_duration_seconds=max((float(row.p95_duration) for row in timed), default=None),
        oldest_started_at=min((row.oldest for row in rows), default=None),
        newest_started_at=max((row.newest for row in rows), default=None),
    )


def _clauses(spec: InstanceFilter) -> list[sa.ColumnElement[bool]]:
    clauses: list[sa.ColumnElement[bool]] = []
    if spec.namespace is not None:
        clauses.append(Instance.namespace == spec.namespace)
    if spec.key is not None:
        clauses.append(Instance.key == spec.key)
    if spec.version is not None:
        clauses.append(Instance.version == spec.version)
    if spec.statuses:
        clauses.append(Instance.status.in_(spec.statuses))
    if spec.triggered_by:
        clauses.append(Instance.triggered_by.in_(spec.triggered_by))
    if spec.parent_instance_id is not None:
        clauses.append(Instance.parent_instance_id == spec.parent_instance_id)
    if spec.since is not None:
        clauses.append(Instance.started_at >= spec.since)
    if spec.until is not None:
        clauses.append(Instance.started_at < spec.until)
    # Bound as JSONB so `@>` is containment rather than a text comparison.
    if spec.metadata_filter:
        clauses.append(Instance.metadata_.op("@>")(sa.bindparam(None, value=spec.metadata_filter, type_=JSONB)))
    if spec.variables_filter:
        clauses.append(Instance.variables.op("@>")(sa.bindparam(None, value=spec.variables_filter, type_=JSONB)))
    if spec.current_elements:
        holding = sa.select(ElementRun.instance_id).where(
            ElementRun.status.in_(OPEN_ELEMENT_STATUSES), ElementRun.element_id.in_(spec.current_elements)
        )
        clauses.append(Instance.id.in_(holding.scalar_subquery()))
    return clauses
