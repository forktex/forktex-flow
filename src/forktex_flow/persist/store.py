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

"""``FlowStore`` — the database handle every persistence function takes.

This is what replaced passing ``Flow`` itself down the stack. Every function in
``persist``, ``read`` and ``runtime`` used to accept the whole engine in order to
reach two of its attributes, which made each lower layer import the facade for
its type and inverted the package's dependency direction. A store holds exactly
what a write needs: a pool and the schema it writes to.
"""

from __future__ import annotations

from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from forktex.database import Database
    from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

__all__ = ["FlowStore"]


@dataclass(frozen=True, slots=True)
class FlowStore:
    """A pool plus the schema its statements resolve against.

    ``database`` is a ``forktex.database.Database`` whose
    ``schema_translate_map`` rewrites the literal ``forktex_flow`` in the ORM
    models to ``schema``. The two are carried together because using one without
    the other silently writes to the wrong schema.
    """

    database: Database
    schema: str

    def session(self) -> AbstractAsyncContextManager[AsyncSession]:
        """A transactional session: commits on success, rolls back on error."""
        return self.database.session()

    @property
    def engine(self) -> AsyncEngine:
        """The underlying engine — for DDL, advisory locks and migrations."""
        return self.database.engine
