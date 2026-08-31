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

"""Schema bring-up, and the columns an extension adds to it."""

from __future__ import annotations

import uuid

import pytest
import sqlalchemy as sa
from forktex.database import reflect
from sqlalchemy import text

from forktex_flow import Flow

EXPECTED_TABLES = {"workflow", "run", "step_run", "run_event", "scheduled_run", "flow_schema_version"}


@pytest.mark.asyncio
async def test_init_creates_every_table(flow: Flow):
    async with flow.engine.connect() as conn:
        missing = [t for t in sorted(EXPECTED_TABLES) if not await reflect.has_table(conn, t, schema=flow.schema)]
    assert missing == [], f"missing tables: {missing}"


@pytest.mark.asyncio
async def test_init_is_idempotent(flow: Flow):
    """Every worker calls this on startup, so the second call through has to be
    a no-op rather than a duplicate-object error."""
    await flow.init()
    await flow.init()


@pytest.mark.asyncio
async def test_the_version_table_keeps_its_deployed_name(flow: Flow):
    """`flow_schema_version`, not the migration runner's default of
    `schema_version`. A deployed database already has the former, and a runner
    that looks for the latter finds no applied versions and re-runs v0001."""
    async with flow.engine.connect() as conn:
        assert await reflect.has_table(conn, "flow_schema_version", schema=flow.schema)
        assert not await reflect.has_table(conn, "schema_version", schema=flow.schema)


@pytest.mark.asyncio
async def test_the_run_status_check_constraint_is_enforced_by_the_database(flow: Flow):
    """`database:constraints-in-the-schema`. An application guard holds only for
    callers that went through that code path, and there is always another one."""
    async with flow.engine.begin() as conn:
        with pytest.raises(sa.exc.IntegrityError):
            await conn.execute(
                text(
                    f'INSERT INTO "{flow.schema}".run '  # noqa: S608 - our own schema
                    "(id, workflow_name, workflow_version, status) "
                    "VALUES (:id, 'x', 1, 'not-a-status')"
                ).bindparams(id=uuid.uuid7())
            )


@pytest.mark.asyncio
async def test_the_step_memoisation_key_is_unique_in_the_database(flow: Flow):
    """Two workers racing the same step must not both get a row: the uniqueness
    that makes memoisation safe has to hold under concurrency."""
    run_id = uuid.uuid7()
    async with flow.engine.begin() as conn:
        await conn.execute(
            text(
                f'INSERT INTO "{flow.schema}".run '  # noqa: S608 - our own schema
                "(id, workflow_name, workflow_version, status) VALUES (:id, 'x', 1, 'running')"
            ).bindparams(id=run_id)
        )
        insert = text(
            f'INSERT INTO "{flow.schema}".step_run '  # noqa: S608 - our own schema
            "(id, run_id, step_name, step_qualname, step_index, args_hash, status, max_attempts) "
            "VALUES (:id, :run_id, 's', 'x:s', 0, 'hash', 'pending', 3)"
        )
        await conn.execute(insert.bindparams(id=uuid.uuid7(), run_id=run_id))
        with pytest.raises(sa.exc.IntegrityError):
            await conn.execute(insert.bindparams(id=uuid.uuid7(), run_id=run_id))
