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

from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from forktex.database import Database
    from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

__all__ = ["FlowStore", "rowcount"]


def rowcount(result: object) -> int:
    """How many rows an UPDATE touched. Every backend this library targets supports ``rowcount`` on an UPDATE, but
    pyright's stub does not expose it on the base ``Result`` type, so the access needs a typed fallback.
    """
    return getattr(result, "rowcount", 0) or 0


@dataclass(frozen=True, slots=True)
class FlowStore:
    database: Database
    schema: str

    def session(self) -> AbstractAsyncContextManager[AsyncSession]:
        """One transaction per block: commits on clean exit, rolls back on error. Callers do not commit —
        ``transactional_session`` owns the boundary and raises ``TransactionBoundaryError`` on
        ``commit``/``rollback``/``begin``, so a write-without-commit cannot be introduced.
        """
        return self.database.transactional_session()

    @property
    def engine(self) -> AsyncEngine:
        return self.database.engine
