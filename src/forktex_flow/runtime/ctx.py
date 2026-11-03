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

import asyncio
from typing import TYPE_CHECKING
from uuid import UUID

from forktex.error import BadRequestError
from forktex.types import JsonObject
from pydantic import BaseModel, PrivateAttr

from forktex_flow.persist import instances as _instances

if TYPE_CHECKING:
    from forktex_flow.persist.store import FlowStore

__all__ = ["Ctx"]


class Ctx(BaseModel):
    instance_id: UUID
    namespace: str
    key: str
    version: int
    element_id: str
    task_type: str
    attempt: int
    max_attempts: int
    metadata: JsonObject
    #: For an ad-hoc decider: the ids of the activities it may activate.
    activities: tuple[str, ...] = ()

    _store: FlowStore | None = PrivateAttr(default=None)

    @property
    def final_attempt(self) -> bool:
        return self.attempt >= self.max_attempts

    async def emit(self, event_type: str, payload: JsonObject | None = None) -> None:
        """Raises ``BadRequestError`` on a ``Ctx`` built without a database, such as ``make_ctx()``'s."""
        if self._store is None:
            raise BadRequestError("Ctx.emit() needs a database-bound Ctx; this one was constructed without a store")
        async with self._store.transaction() as session:
            _instances.emit(session, self.instance_id, event_type, payload)

    async def sleep(self, seconds: float) -> None:
        """Not durable: a worker that dies mid-sleep leaves the job to be retried, and the sleep starts over."""
        await asyncio.sleep(seconds)
