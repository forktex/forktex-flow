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

from forktex_flow.persist import runs as _runs

if TYPE_CHECKING:
    from forktex_flow.persist.store import FlowStore

__all__ = ["Ctx"]


class Ctx(BaseModel):
    run_id: UUID
    workflow_name: str
    workflow_version: int
    node_name: str
    attempt: int
    metadata: JsonObject

    _store: FlowStore | None = PrivateAttr(default=None)
    """Set by the executor. ``None`` on a ``Ctx`` a test constructed directly,
    which is why :meth:`emit` checks it — a step under unit test should be able
    to take a bare ``Ctx`` without a database."""

    def __init__(self, _store: FlowStore | None = None, **data: object) -> None:
        """Accept ``_store`` positionally: pydantic's generated ``__init__`` only takes declared public fields."""
        super().__init__(**data)
        if _store is not None:
            self._store = _store

    async def emit(self, event_type: str, payload: JsonObject | None = None) -> None:
        if self._store is None:
            raise BadRequestError("Ctx.emit() needs a database-bound Ctx; this one was constructed without a store")
        await _runs.emit_run_event(self._store, self.run_id, event_type, payload or {})

    async def sleep(self, seconds: float) -> None:
        """Wait, in process. Not durable: if the worker dies mid-sleep the run is re-executed from the top,
        completed steps are served from their cached outputs, and this sleep starts over. For a delay that must
        survive a restart, end the step and let the retry backoff carry the wait.
        """
        await asyncio.sleep(seconds)
