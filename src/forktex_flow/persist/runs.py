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

"""Reads and writes on ``forktex_flow.run`` and ``forktex_flow.run_event``.

One aggregate per module: everything here is scoped to a run's own row and its
append-only event stream. Step-level state lives in :mod:`steps`.

``NOTIFY`` goes through ``sa.select(sa.func.pg_notify(channel, payload))`` rather than the
``NOTIFY`` statement, which takes only literals — so both the channel and the payload
travel as bind parameters instead of being spliced into SQL text.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from uuid import UUID

import sqlalchemy as sa
from forktex.log import get_logger
from sqlalchemy import select, update
from sqlalchemy.orm import selectinload

from forktex_flow.domain.types import TERMINAL_STATUSES, RunInfo, RunStatus
from forktex_flow.persist.mappers import to_run_info
from forktex_flow.persist.models import Run, RunEvent

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncConnection

    from forktex_flow.persist.store import FlowStore

logger = get_logger(__name__)

__all__ = [
    "claim_pending_runs",
    "emit_run_event",
    "fetch_run",
    "insert_run",
    "notify_run",
    "update_run_output",
    "update_run_status",
]


async def notify_run(conn: AsyncConnection, schema: str, run_id: UUID) -> None:
    """``NOTIFY`` consumers of ``flow.stream(run_id)``. The channel
    name is per-schema so multiple Flow instances on the same DB
    don't bleed updates.

    Uses ``pg_notify(channel, payload)`` rather than the ``NOTIFY`` statement:
    ``NOTIFY`` takes only literals, so both the channel and the payload had to
    be interpolated into the SQL string. ``pg_notify`` is an ordinary function,
    so both travel as bind parameters and nothing is spliced into SQL text.
    """
    await conn.execute(
        sa.select(sa.func.pg_notify(f"{schema}_run", str(run_id))),
    )


async def insert_run(
    store: FlowStore,
    *,
    workflow_name: str,
    workflow_version: int,
    input: dict[str, Any],
    metadata: dict[str, Any],
    triggered_by: str = "manual",
) -> UUID:
    """Insert a ``pending`` run and return its id.

    ``triggered_by`` is internal: ``"manual"`` for a caller's ``flow.run()`` and
    ``"schedule"`` for the driver firing a cron. ``ck_run_triggered_by`` permits
    nothing else.

    The id comes from the column's ``default=uuid.uuid7``, read back after the
    flush, rather than being minted at the call site — one place decides how run
    ids are made, and it is the same place for every caller.
    """
    async with store.session() as session:
        run = Run(
            workflow_name=workflow_name,
            workflow_version=workflow_version,
            status="pending",
            input=input or {},
            metadata_=metadata or {},
            triggered_by=triggered_by,
        )
        session.add(run)
        await session.flush()
        run_id = run.id
        await notify_run(await session.connection(), store.schema, run_id)
        await session.commit()
        return run_id


async def update_run_status(
    store: FlowStore,
    run_id: UUID,
    *,
    status: RunStatus,
    output: dict[str, Any] | None = None,
    error: str | None = None,
    cancel_reason: str | None = None,
) -> bool:
    """Move a run to ``status``, unless it has already finished.

    Returns whether a row was updated. ``False`` means the run was already
    terminal and this write was declined.

    The ``WHERE`` guard is the point. Without it a late writer overwrites a
    finished run: cancel a running run and the executor's own success write
    lands afterwards, leaving ``status='completed'`` beside
    ``cancel_reason='cancelled by operator'`` — a self-contradictory row, with
    nothing raised anywhere to say so. A terminal status is final; whoever gets
    there first owns the outcome.
    """
    values: dict[str, Any] = {"status": status}
    if output is not None:
        values["output"] = output
    if error is not None:
        values["error"] = error
    if cancel_reason is not None:
        values["cancel_reason"] = cancel_reason
    if status in TERMINAL_STATUSES:
        values["finished_at"] = sa.func.now()
    if status == "cancelled":
        values["cancelled_at"] = sa.func.now()

    async with store.session() as session:
        result = await session.execute(
            update(Run)
            .where(Run.id == run_id, Run.status.not_in(sorted(TERMINAL_STATUSES)))
            .values(**values)
            .returning(Run.id)
        )
        updated = result.first() is not None
        if updated:
            await session.flush()
            await notify_run(await session.connection(), store.schema, run_id)
        await session.commit()
        return updated


async def fetch_run(store: FlowStore, run_id: UUID) -> RunInfo | None:
    async with store.session() as session:
        row = (
            await session.execute(select(Run).options(selectinload(Run.steps)).where(Run.id == run_id))
        ).scalar_one_or_none()
        if row is None:
            return None
        return to_run_info(row)


async def update_run_output(
    store: FlowStore,
    run_id: UUID,
    state: dict[str, Any],
) -> None:
    """Write the accumulated state to ``run.output`` after each step completes.

    Observability only. It is deliberately **not** the resume point: a resumed
    run replays from ``run.input``, because starting from a merged state would
    feed a different value into the memoisation key and miss every cached step.
    """
    async with store.session() as session:
        await session.execute(update(Run).where(Run.id == run_id).values(output=state))
        await session.commit()


async def emit_run_event(
    store: FlowStore,
    run_id: UUID,
    event_type: str,
    payload: dict[str, Any] | None = None,
) -> None:
    async with store.session() as session:
        session.add(RunEvent(run_id=run_id, event_type=event_type, payload=payload or {}))
        await session.flush()
        await notify_run(await session.connection(), store.schema, run_id)
        await session.commit()


async def claim_pending_runs(store: FlowStore, limit: int) -> list[tuple[UUID, str, int]]:
    """Atomically pick up to ``limit`` pending runs: marks them
    ``running`` and returns ``(run_id, name, version)`` tuples for
    dispatch.

    One statement, so two leaders overlapping at handover partition the queue
    instead of racing over it: ``with_for_update(skip_locked=True)`` on a scalar
    subquery renders the locking clause *inside* the subselect, which is where
    Postgres needs it for the atomicity to hold.
    """
    claimable = (
        select(Run.id)
        .where(Run.status == "pending")
        .order_by(Run.started_at.asc())
        .limit(limit)
        .with_for_update(skip_locked=True)
        .scalar_subquery()
    )
    stmt = (
        update(Run)
        .where(Run.id.in_(claimable))
        .values(status="running")
        .returning(Run.id, Run.workflow_name, Run.workflow_version)
        .execution_options(synchronize_session=False)
    )
    async with store.session() as session:
        rows = (await session.execute(stmt)).all()
        return [(r.id, r.workflow_name, r.workflow_version) for r in rows]
