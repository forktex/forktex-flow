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
"""The SQL in `docs/operating.md`, executed against a migrated schema."""

from __future__ import annotations

import pathlib
import re

import pytest
from sqlalchemy import text

from forktex_flow import Flow

_CANONICAL_SCHEMA = "forktex_flow"


@pytest.mark.asyncio
async def test_the_sql_in_the_operating_guide_runs(postgres_url_str: str) -> None:
    """Operator SQL is copy-pasted into a psql session against production, so a
    stale column name there is found by whoever is mid-incident. Running it
    against a migrated schema is the only way the page cannot drift.
    """
    guide = (pathlib.Path(__file__).resolve().parents[2] / "docs" / "operating.md").read_text(encoding="utf-8")
    blocks = re.findall(r"```sql\n(.*?)```", guide, re.S)
    assert blocks, "docs/operating.md no longer carries any SQL to check"

    statements = [s.strip() for block in blocks for s in block.split(";") if s.strip()]
    # The documented schema name is literal, so this runs on the canonical one.
    flow = Flow(postgres_url_str, schema=_CANONICAL_SCHEMA)
    await flow.init()
    try:
        async with flow.engine.begin() as conn:
            for statement in statements:
                if statement.upper().startswith("DROP "):
                    continue  # documented recovery, not something to execute here
                await conn.execute(text(statement))
    finally:
        await flow.close()
