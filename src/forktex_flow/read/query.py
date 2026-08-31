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

"""Compiling a :class:`RunFilter` into SQL: one page, one count, one summary.

Kept out of :mod:`persist` because it composes several tables and answers a
*question*, where persist answers "write this row".

Three functions rather than one with a ``mode`` string. The single-entry version
returned ``Page | int | dict`` and forced every caller to ``cast()`` and then
re-check the type at runtime; worse, it built its ``where`` clauses three times
and the summary branch had drifted, silently ignoring the metadata, state and
current-node filters. :func:`_filter_clauses` is now the only place a filter
becomes SQL, so the three cannot disagree again.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import UUID

import sqlalchemy as sa
from forktex.database import Page, decode_cursor, encode_cursor
from forktex.iso import from_iso, to_iso
from forktex.types import JsonValue
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import selectinload

from forktex_flow.domain.types import RunInfo, RunSummary
from forktex_flow.persist.mappers import to_run_info
from forktex_flow.persist.models import Run, StepRun

if TYPE_CHECKING:
    from datetime import datetime

    from forktex_flow.domain.filter import RunFilter
    from forktex_flow.persist.store import FlowStore

__all__ = ["count_runs", "fetch_page", "summarise_runs"]

#: Paging is always newest-run-first over ``(started_at, id)``. Both columns are
#: NOT NULL and covered by `ix_run_started_at_id` in that order, which is what
#: lets the boundary be a row comparison instead of an OR-chain — the OR-chain
#: lands as a Filter and re-reads the index from the start on every page.
#: Offering other sort fields is what forced the general form: `finished_at` is
#: nullable, and a row comparison against NULL evaluates to NULL and drops the
#: row from the page without failing anything.
_SORT_COLUMNS = (Run.started_at, Run.id)


def _filter_clauses(spec: RunFilter) -> list[sa.ColumnElement[bool]]:
    """Every ``WHERE`` clause the filter implies. The single source for all
    three queries below."""
    clauses: list[sa.ColumnElement[bool]] = []

    if spec.workflow_name is not None:
        clauses.append(Run.workflow_name == spec.workflow_name)
    if spec.workflow_version is not None:
        clauses.append(Run.workflow_version == spec.workflow_version)
    if spec.statuses:
        clauses.append(Run.status.in_(spec.statuses))
    if spec.triggered_by:
        clauses.append(Run.triggered_by.in_(spec.triggered_by))
    if spec.since is not None:
        clauses.append(Run.started_at >= spec.since)
    if spec.until is not None:
        clauses.append(Run.started_at < spec.until)

    # Bound with an explicit JSONB type so asyncpg encodes the dict as jsonb and
    # `@>` resolves to containment rather than to a text comparison. The dict
    # goes across as a bound parameter; json.dumps plus a ::jsonb cast would be
    # re-implementing the driver's encoder.
    if spec.metadata_filter:
        clauses.append(Run.metadata_.op("@>")(sa.bindparam(None, value=spec.metadata_filter, type_=JSONB)))
    if spec.state_filter:
        clauses.append(Run.input.op("@>")(sa.bindparam(None, value=spec.state_filter, type_=JSONB)))

    if spec.current_nodes:
        # An exact match on `step_name`, not a LIKE against a suffix of
        # `step_qualname`. The pattern form spliced the caller's string into
        # `%:{name}` unescaped, so `current_node("%")` matched every running
        # step, and a name that happened to be the tail of another workflow's
        # step name matched across workflows. The column is already there.
        running_here = (
            select(StepRun.run_id)
            .where(StepRun.status == "running", StepRun.step_name.in_(spec.current_nodes))
            .scalar_subquery()
        )
        clauses.append(Run.id.in_(running_here))

    return clauses


async def fetch_page(
    store: FlowStore,
    spec: RunFilter,
    *,
    limit: int,
    cursor: str | None = None,
) -> Page[RunInfo]:
    """One keyset page of runs, newest first."""
    stmt = (
        select(Run)
        .options(selectinload(Run.steps))
        .where(*_filter_clauses(spec))
        .order_by(Run.started_at.desc(), Run.id.desc())
    )

    if cursor is not None:
        started_at, run_id = decode_cursor(cursor, expected_length=2)
        # A row comparison, matching the ORDER BY exactly: Postgres pushes
        # `ROW(started_at, id) < ROW(...)` into ix_run_started_at_id as an Index
        # Cond. Safe because both columns are NOT NULL.
        # `sa.literal(..., type_=)` rather than the bare Python values: the
        # boundary crosses as a bound parameter carrying the column's own type,
        # so the driver encodes the timestamp and the UUID itself instead of the
        # comparison relying on an implicit cast from text.
        boundary = sa.tuple_(
            sa.literal(_as_datetime(started_at), type_=Run.started_at.type),
            sa.literal(UUID(str(run_id)), type_=Run.id.type),
        )
        stmt = stmt.where(sa.tuple_(*_SORT_COLUMNS) < boundary)

    # limit + 1 to detect a further page without a second count query. Reading
    # `len(rows) == limit` instead reports has_more on an exactly-full last page.
    stmt = stmt.limit(limit + 1)

    async with store.session() as session:
        rows = (await session.execute(stmt)).scalars().all()

    has_more = len(rows) > limit
    page_rows = rows[:limit]

    next_cursor: str | None = None
    if has_more and page_rows:
        last = page_rows[-1]
        next_cursor = encode_cursor([to_iso(last.started_at), str(last.id)])

    return Page[RunInfo](
        items=[to_run_info(row) for row in page_rows],
        has_more=has_more,
        next_cursor=next_cursor,
    )


async def count_runs(store: FlowStore, spec: RunFilter) -> int:
    """How many runs match, without fetching them."""
    stmt = select(sa.func.count(Run.id)).where(*_filter_clauses(spec))
    async with store.session() as session:
        return (await session.execute(stmt)).scalar_one()


async def summarise_runs(store: FlowStore, spec: RunFilter) -> RunSummary:
    """Counts and duration percentiles over the matching runs, by status.

    Duration is computed in the database over finished runs only — an unfinished
    run has a NULL ``finished_at``, and Postgres's aggregates skip NULLs, so an
    in-flight run neither counts as zero-duration nor poisons the average.
    """
    duration = sa.extract("epoch", Run.finished_at - Run.started_at)
    stmt = (
        select(
            Run.status,
            sa.func.count(Run.id).label("total"),
            sa.func.avg(duration).label("avg_duration"),
            sa.func.percentile_cont(0.95).within_group(duration.asc()).label("p95_duration"),
            sa.func.min(Run.started_at).label("oldest"),
            sa.func.max(Run.started_at).label("newest"),
        )
        .where(*_filter_clauses(spec))
        .group_by(Run.status)
    )

    async with store.session() as session:
        rows = (await session.execute(stmt)).all()

    by_status = {row.status: row.total for row in rows}
    avgs = [row.avg_duration for row in rows if row.avg_duration is not None]
    p95s = [row.p95_duration for row in rows if row.p95_duration is not None]
    oldest = [row.oldest for row in rows if row.oldest is not None]
    newest = [row.newest for row in rows if row.newest is not None]

    return RunSummary(
        total=sum(by_status.values()),
        by_status=by_status,
        # A mean of per-status means, weighted by each status's count — the
        # previous code averaged the means unweighted, so one failed run
        # counted as much as a thousand completed ones.
        avg_duration_seconds=(
            sum(float(row.avg_duration) * row.total for row in rows if row.avg_duration is not None)
            / sum(row.total for row in rows if row.avg_duration is not None)
            if avgs
            else None
        ),
        # The worst per-status p95, not a true global p95: computing that needs
        # one ungrouped pass. Stated because the name suggests otherwise.
        p95_duration_seconds=max(float(p) for p in p95s) if p95s else None,
        oldest_started_at=min(oldest) if oldest else None,
        newest_started_at=max(newest) if newest else None,
    )


def _as_datetime(value: JsonValue) -> datetime:
    """Rehydrate the cursor's ISO-8601 timestamp into an aware ``datetime``.

    The cursor is JSON, so the boundary arrives as a string; comparing a string
    to a TIMESTAMPTZ column is the kind of implicit cast that works until a
    locale changes it.
    """
    return from_iso(str(value))
