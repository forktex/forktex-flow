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

EXPECTED_TABLES = {"definition", "instance", "element_run", "instance_event", "schedule", "schema_version"}


@pytest.mark.asyncio
async def test_init_creates_every_table(flow: Flow):
    async with flow.engine.connect() as conn:
        missing = [t for t in sorted(EXPECTED_TABLES) if not await reflect.has_table(conn, t, schema=flow.schema)]
    assert missing == [], f"missing tables: {missing}"


@pytest.mark.asyncio
async def test_init_is_idempotent(flow: Flow):
    """Every worker calls this on startup, so the second call through has to be a no-op."""
    await flow.init()
    await flow.init()

    async with flow.engine.connect() as conn:
        applied = (
            await conn.execute(sa.text(f'SELECT count(*), count(DISTINCT version) FROM "{flow.schema}".schema_version'))
        ).one()
    assert applied[0] == applied[1], "the second init() recorded a duplicate version row"


@pytest.mark.asyncio
async def test_an_element_runs_at_most_once_per_instance(flow: Flow):
    """Two workers racing to record one element must not both get a row: that uniqueness is what stops a resume
    from repeating a completed element."""
    definition, instance = uuid.uuid7(), uuid.uuid7()
    async with flow.engine.begin() as conn:
        await conn.execute(
            text(
                f'INSERT INTO "{flow.schema}".definition '  # noqa: S608 - our own schema
                "(id, namespace, key, version, checksum, model, source) "
                "VALUES (:id, 'default', 'k', 1, 'c', '{}'::jsonb, '')"
            ).bindparams(id=definition)
        )
        await conn.execute(
            text(
                f'INSERT INTO "{flow.schema}".instance '  # noqa: S608 - our own schema
                "(id, namespace, key, version, definition_id, status) VALUES (:id, 'default', 'k', 1, :d, 'active')"
            ).bindparams(id=instance, d=definition)
        )
        insert = text(
            f'INSERT INTO "{flow.schema}".element_run '  # noqa: S608 - our own schema
            "(id, instance_id, element_id, element_type, status) VALUES (:id, :i, 'a', 'serviceTask', 'pending')"
        )
        await conn.execute(insert.bindparams(id=uuid.uuid7(), i=instance))
        with pytest.raises(sa.exc.IntegrityError):
            await conn.execute(insert.bindparams(id=uuid.uuid7(), i=instance))
