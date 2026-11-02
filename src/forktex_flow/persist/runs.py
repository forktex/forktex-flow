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

from typing import TYPE_CHECKING, Any
from uuid import UUID

import sqlalchemy as sa
from forktex.log import get_logger
from forktex.types import JsonObject
from sqlalchemy import select, update
from sqlalchemy.orm import selectinload

from forktex_flow.domain.types import TERMINAL_STATUSES, RunClaim, RunInfo, RunStatus
from forktex_flow.persist.mappers import to_run_info
from forktex_flow.persist.models import Run, RunEvent, StepRun
from forktex_flow.persist.store import rowcount

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession

    from forktex_flow.persist.store import FlowStore

logger = get_logger(__name__)

__all__ = [
    "claim_pending_runs",
    "emit_run_event",
    "fetch_run",
    "heartbeat_run",
    "insert_run",
    "notify_run",
    "update_run_output",
    "update_run_status",
]


async def notify_run(conn: AsyncConnection, schema: str, run_id: UUID) -> None:
    await conn.execute(
        sa.select(sa.func.pg_notify(f"{schema}_run", str(run_id))),
    )


async def insert_run(
    store: FlowStore,
    *,
    workflow_name: str,
    workflow_version: int,
    input: JsonObject,
    metadata: JsonObject,
    triggered_by: str = "manual",
) -> UUID:
    async with store.transaction() as session:
        return await insert_run_in(
            session,
            store.schema,
            workflow_name=workflow_name,
            workflow_version=workflow_version,
            input=input,
            metadata=metadata,
            triggered_by=triggered_by,
        )


async def insert_run_in(
    session: AsyncSession,
    schema: str,
    *,
    workflow_name: str,
    workflow_version: int,
    input: JsonObject,
    metadata: JsonObject,
    triggered_by: str = "manual",
) -> UUID:
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
    await notify_run(await session.connection(), schema, run_id)
    return run_id


async def update_run_status(
    store: FlowStore,
    run_id: UUID,
    *,
    status: RunStatus,
    output: JsonObject | None = None,
    error: str | None = None,
    cancel_reason: str | None = None,
) -> bool:
    # A SQLAlchemy column -> value mapping, not JSON: it carries `sa.func.now()`
    # alongside the JSON-typed columns.
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

    async with store.transaction() as session:
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
        return updated


async def heartbeat_run(store: FlowStore, run_id: UUID) -> bool:
    """Stamp the run alive. ``False`` when it is no longer ``running`` — another executor owns it, or it reached a
    terminal status, and the caller must stop.
    """
    async with store.transaction() as session:
        result = await session.execute(
            update(Run).where(Run.id == run_id, Run.status == "running").values(heartbeat_at=sa.func.now())
        )
        return rowcount(result) > 0


async def fetch_run(store: FlowStore, run_id: UUID) -> RunInfo | None:
    async with store.transaction() as session:
        row = (
            await session.execute(select(Run).options(selectinload(Run.steps)).where(Run.id == run_id))
        ).scalar_one_or_none()
        if row is None:
            return None
        return to_run_info(row)


async def update_run_output(
    store: FlowStore,
    run_id: UUID,
    state: JsonObject,
) -> None:
    """Write the accumulated state to ``run.output`` after each step completes. Declined once the run is terminal,
    so the step that was still running when an operator cancelled cannot write a successful-looking result over
    it.
    """
    async with store.transaction() as session:
        await session.execute(
            update(Run).where(Run.id == run_id, Run.status.not_in(TERMINAL_STATUSES)).values(output=state)
        )


async def emit_run_event(
    store: FlowStore,
    run_id: UUID,
    event_type: str,
    payload: JsonObject | None = None,
) -> None:
    async with store.transaction() as session:
        session.add(RunEvent(run_id=run_id, event_type=event_type, payload=payload or {}))
        await session.flush()
        await notify_run(await session.connection(), store.schema, run_id)


async def claim_pending_runs(store: FlowStore, limit: int) -> list[RunClaim]:
    """Atomically pick up to ``limit`` pending runs: marks them ``running`` and returns a :class:`RunClaim` for
    each, ready to dispatch. A fresh run with no step rows, and a resumed run whose next step has not been
    created yet, both still claim — the guard must not turn a backoff into a stall.
    """
    blocked_until_later = (
        select(StepRun.id)
        .where(
            StepRun.run_id == Run.id,
            StepRun.status == "pending",
            StepRun.next_attempt_at.isnot(None),
            StepRun.next_attempt_at > sa.func.now(),
        )
        .exists()
    )
    ready_now = (
        select(StepRun.id)
        .where(
            StepRun.run_id == Run.id,
            StepRun.status == "pending",
            sa.or_(
                StepRun.next_attempt_at.is_(None),
                StepRun.next_attempt_at <= sa.func.now(),
            ),
        )
        .exists()
    )
    claimable = (
        select(Run.id)
        .where(Run.status == "pending", sa.or_(~blocked_until_later, ready_now))
        .order_by(Run.started_at.asc())
        .limit(limit)
        .with_for_update(skip_locked=True)
        .scalar_subquery()
    )
    stmt = (
        update(Run)
        .where(Run.id.in_(claimable))
        .values(status="running", heartbeat_at=sa.func.now())
        .returning(Run.id, Run.workflow_name, Run.workflow_version)
        .execution_options(synchronize_session=False)
    )
    async with store.transaction() as session:
        rows = (await session.execute(stmt)).all()
        return [RunClaim(run_id=r.id, workflow_name=r.workflow_name, workflow_version=r.workflow_version) for r in rows]
