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

from datetime import datetime
from typing import TYPE_CHECKING
from uuid import UUID

import sqlalchemy as sa
from croniter import croniter
from forktex.database import advisory_key, xact_lock
from sqlalchemy.dialects.postgresql import insert as pg_insert

from forktex_flow.domain.types import DefinitionInfo
from forktex_flow.persist.mappers import to_definition_info
from forktex_flow.persist.models import Definition, Schedule

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

    from forktex_flow.domain.process import ProcessModel

__all__ = ["by_id", "deploy", "find", "upsert_schedule"]


async def deploy(
    session: AsyncSession,
    model: ProcessModel,
    *,
    schema: str,
    namespace: str,
    source: str,
) -> tuple[DefinitionInfo, bool]:
    await xact_lock(session, advisory_key(schema, "deploy", namespace, model.key))
    checksum = model.checksum()
    existing = await session.scalar(
        sa.select(Definition).where(
            Definition.namespace == namespace, Definition.key == model.key, Definition.checksum == checksum
        )
    )
    if existing is not None:
        return to_definition_info(existing), False
    latest = await session.scalar(
        sa.select(sa.func.max(Definition.version)).where(Definition.namespace == namespace, Definition.key == model.key)
    )
    row = Definition(
        namespace=namespace,
        key=model.key,
        version=(latest or 0) + 1,
        checksum=checksum,
        model=model.model_dump(mode="json", exclude_none=True),
        source=source,
    )
    session.add(row)
    await session.flush()
    await session.refresh(row)
    return to_definition_info(row), True


async def find(session: AsyncSession, namespace: str, key: str, version: int | None = None) -> DefinitionInfo | None:
    statement = sa.select(Definition).where(Definition.namespace == namespace, Definition.key == key)
    if version is not None:
        statement = statement.where(Definition.version == version)
    row = await session.scalar(statement.order_by(Definition.version.desc()).limit(1))
    return None if row is None else to_definition_info(row)


async def by_id(session: AsyncSession, definition_id: UUID) -> DefinitionInfo | None:
    row = await session.get(Definition, definition_id)
    return None if row is None else to_definition_info(row)


async def upsert_schedule(
    session: AsyncSession, namespace: str, key: str, *, cron: str, enabled: bool, now: datetime
) -> None:
    await session.execute(
        pg_insert(Schedule)
        .values(
            namespace=namespace,
            key=key,
            cron=cron,
            enabled=enabled,
            next_fire_at=croniter(cron, now).get_next(datetime),
        )
        .on_conflict_do_update(index_elements=[Schedule.namespace, Schedule.key], set_={"cron": cron})
    )
