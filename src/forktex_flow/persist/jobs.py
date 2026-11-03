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

from collections.abc import Collection
from datetime import timedelta
from typing import TYPE_CHECKING

import sqlalchemy as sa
from forktex import iso

from forktex_flow.domain.types import Job
from forktex_flow.persist.models import ElementRun
from forktex_flow.persist.store import rowcount

if TYPE_CHECKING:
    from uuid import UUID

    from forktex_flow.persist.store import FlowStore

__all__ = ["claim", "exhausted_stale", "heartbeat", "reclaim_stale", "release", "retry_later", "unclaimed"]


async def claim(
    store: FlowStore,
    task_types: Collection[str],
    limit: int,
    *,
    instance_ids: Collection[UUID] | None = None,
    ignore_backoff: bool = False,
) -> list[Job]:
    """Take up to ``limit`` pending jobs of these types, each counted as an attempt. Concurrent callers never take
    the same job.
    """
    if not task_types or limit <= 0:
        return []
    ready = sa.select(ElementRun.id).where(ElementRun.status == "pending", ElementRun.task_type.in_(task_types))
    if not ignore_backoff:
        ready = ready.where(sa.or_(ElementRun.next_attempt_at.is_(None), ElementRun.next_attempt_at <= sa.func.now()))
    if instance_ids is not None:
        ready = ready.where(ElementRun.instance_id.in_(instance_ids))
    ready = ready.order_by(ElementRun.id).limit(limit).with_for_update(skip_locked=True)
    statement = (
        sa.update(ElementRun)
        .where(ElementRun.id.in_(ready.scalar_subquery()))
        .values(status="running", attempts=ElementRun.attempts + 1, heartbeat_at=sa.func.now(), next_attempt_at=None)
        .returning(
            ElementRun.id,
            ElementRun.instance_id,
            ElementRun.element_id,
            ElementRun.task_type,
            ElementRun.attempts,
            ElementRun.max_attempts,
        )
        .execution_options(synchronize_session=False)
    )
    async with store.transaction() as session:
        rows = (await session.execute(statement)).all()
    return [
        Job(
            element_run_id=row.id,
            instance_id=row.instance_id,
            element_id=row.element_id,
            task_type=row.task_type,
            attempts=row.attempts,
            max_attempts=row.max_attempts,
        )
        for row in rows
    ]


async def heartbeat(store: FlowStore, element_run_id: UUID) -> bool:
    async with store.transaction() as session:
        result = await session.execute(
            sa.update(ElementRun)
            .where(ElementRun.id == element_run_id, ElementRun.status == "running")
            .values(heartbeat_at=sa.func.now())
            .execution_options(synchronize_session=False)
        )
        return rowcount(result) == 1


async def retry_later(store: FlowStore, element_run_id: UUID, error: str, delay: float) -> bool:
    async with store.transaction() as session:
        result = await session.execute(
            sa.update(ElementRun)
            .where(ElementRun.id == element_run_id, ElementRun.status == "running")
            .values(
                status="pending",
                error=error,
                heartbeat_at=None,
                next_attempt_at=iso.now() + timedelta(seconds=delay),
            )
            .execution_options(synchronize_session=False)
        )
        return rowcount(result) == 1


async def release(store: FlowStore, element_run_id: UUID) -> None:
    async with store.transaction() as session:
        await session.execute(
            sa.update(ElementRun)
            .where(ElementRun.id == element_run_id, ElementRun.status == "running")
            .values(
                status="pending",
                attempts=sa.func.greatest(ElementRun.attempts - 1, 0),
                heartbeat_at=None,
                next_attempt_at=sa.func.now(),
            )
            .execution_options(synchronize_session=False)
        )


async def exhausted_stale(store: FlowStore, stale_threshold: float) -> list[UUID]:
    async with store.transaction() as session:
        return list(
            await session.scalars(
                sa.select(ElementRun.id).where(_stale(stale_threshold), ElementRun.attempts >= ElementRun.max_attempts)
            )
        )


async def reclaim_stale(store: FlowStore, stale_threshold: float) -> int:
    async with store.transaction() as session:
        result = await session.execute(
            sa.update(ElementRun)
            .where(_stale(stale_threshold), ElementRun.attempts < ElementRun.max_attempts)
            .values(status="pending", heartbeat_at=None, next_attempt_at=sa.func.now())
            .execution_options(synchronize_session=False)
        )
        return rowcount(result)


async def unclaimed(store: FlowStore, older_than: float) -> dict[str, int]:
    """Task type → how many of its jobs have been ready for longer than ``older_than`` seconds while none of that
    type is running.
    """
    ready_since = sa.func.coalesce(ElementRun.next_attempt_at, ElementRun.started_at)
    busy = sa.select(ElementRun.task_type).where(ElementRun.status == "running").distinct()
    statement = (
        sa.select(ElementRun.task_type, sa.func.count(ElementRun.id))
        .where(
            ElementRun.status == "pending",
            ElementRun.task_type.is_not(None),
            ready_since < sa.func.now() - _seconds(older_than),
            ElementRun.task_type.not_in(busy.scalar_subquery()),
        )
        .group_by(ElementRun.task_type)
    )
    async with store.transaction() as session:
        rows = (await session.execute(statement)).all()
    return {task_type: count for task_type, count in rows if task_type is not None}


def _seconds(seconds: float) -> sa.Cast[timedelta]:
    return sa.cast(sa.func.make_interval(0, 0, 0, 0, 0, 0, seconds), sa.Interval)


def _stale(stale_threshold: float) -> sa.ColumnElement[bool]:
    cutoff = sa.func.now() - _seconds(stale_threshold)
    return sa.and_(ElementRun.status == "running", ElementRun.heartbeat_at < cutoff)
