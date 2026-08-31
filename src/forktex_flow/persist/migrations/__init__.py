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

"""Bring the ``forktex_flow`` schema up to date.

Each ``v{NNNN}__{description}.sql`` file in this directory carries one
migration. Forward-only: a bad migration is fixed by the next version, never by
a downgrade — the same convention DBOS, Loki and Sentry follow.

The walking, ordering, advisory locking and version bookkeeping are
:class:`forktex.database.SchemaMigrationRunner`'s, not ours; its docstring names
``forktex_flow`` as its worked example. All this module adds is the two
flow-specific arguments.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from forktex.database import SchemaMigrationRunner

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine

__all__ = ["apply_migrations"]

_MIGRATIONS_DIR = Path(__file__).parent

#: Not the runner's default of ``schema_version``. Deployed databases already
#: carry this table under this name; the runner has to be told.
_VERSION_TABLE = "flow_schema_version"


async def apply_migrations(engine: AsyncEngine, schema: str = "forktex_flow") -> None:
    """Apply every unapplied migration for ``schema``.

    Idempotent and safe to call from every worker on startup — concurrent
    callers serialise on an advisory lock, the first applies and the rest skip.
    ``Flow.init()`` calls this; call it directly when a consumer drives its own
    migration pipeline.
    """
    await SchemaMigrationRunner(
        engine,
        schema=schema,
        migrations_dir=_MIGRATIONS_DIR,
        version_table=_VERSION_TABLE,
    ).apply()
