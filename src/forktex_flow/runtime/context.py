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
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from forktex.error import NotFoundError

from forktex_flow.persist import definitions as _definitions

if TYPE_CHECKING:
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncSession

    from forktex_flow.domain.config import RuntimeConfig
    from forktex_flow.domain.types import DefinitionInfo
    from forktex_flow.extension import FlowExtension
    from forktex_flow.persist.store import FlowStore
    from forktex_flow.runtime.registry import FlowRegistry

__all__ = ["FlowRuntime"]


@dataclass(frozen=True, slots=True)
class FlowRuntime:
    store: FlowStore
    registry: FlowRegistry
    config: RuntimeConfig
    extensions: tuple[FlowExtension, ...] = ()
    #: Set whenever this process changes an instance, so its driver claims at once instead of at the next poll.
    wake: asyncio.Event = field(default_factory=asyncio.Event)
    #: Definitions are immutable once deployed, so one read per id serves the process for good.
    _definitions: dict[UUID, DefinitionInfo] = field(default_factory=dict)

    async def definition(self, session: AsyncSession, definition_id: UUID) -> DefinitionInfo:
        cached = self._definitions.get(definition_id)
        if cached is None:
            cached = await _definitions.by_id(session, definition_id)
            if cached is None:
                raise NotFoundError(f"definition {definition_id} not found")
            self._definitions[definition_id] = cached
        return cached
