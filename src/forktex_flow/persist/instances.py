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

import sqlalchemy as sa
from forktex import iso
from forktex.types import JsonObject

from forktex_flow.domain.types import OPEN_ELEMENT_STATUSES, InstanceInfo
from forktex_flow.persist.mappers import to_instance_info
from forktex_flow.persist.models import ElementRun, Instance, InstanceEvent

if TYPE_CHECKING:
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncSession

    from forktex_flow.persist.store import FlowStore

__all__ = ["cancel", "close_open_elements", "emit", "fetch", "lock"]


def emit(session: AsyncSession, instance_id: UUID, event_type: str, payload: JsonObject | None = None) -> None:
    session.add(InstanceEvent(instance_id=instance_id, event_type=event_type, payload=payload or {}))


async def fetch(store: FlowStore, instance_id: UUID) -> InstanceInfo | None:
    async with store.transaction() as session:
        row = await session.get(Instance, instance_id)
        return None if row is None else to_instance_info(row)


async def lock(session: AsyncSession, instance_id: UUID) -> Instance | None:
    return await session.scalar(
        sa.select(Instance)
        .where(Instance.id == instance_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )


async def close_open_elements(session: AsyncSession, instance_id: UUID, status: str) -> None:
    await session.execute(
        sa.update(ElementRun)
        .where(ElementRun.instance_id == instance_id, ElementRun.status.in_(OPEN_ELEMENT_STATUSES))
        .values(status=status, finished_at=sa.func.now(), heartbeat_at=None)
        .execution_options(synchronize_session=False)
    )


async def cancel(session: AsyncSession, instance_id: UUID, reason: str) -> bool:
    row = await lock(session, instance_id)
    if row is None or row.status != "active":
        return False
    row.status = "cancelled"
    row.cancel_reason = reason
    row.finished_at = iso.now()
    await close_open_elements(session, instance_id, "cancelled")
    emit(session, instance_id, "instance_cancelled", {"reason": reason})
    children = await session.scalars(
        sa.select(Instance.id).where(Instance.parent_instance_id == instance_id, Instance.status == "active")
    )
    for child in list(children):
        await cancel(session, child, reason)
    return True
