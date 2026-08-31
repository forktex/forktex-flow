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

"""The ORM models and the migration SQL describe the same tables.

Two files, one schema, and nothing but human memory keeping them in step —
`persist/models.py` says so in its own docstring ("must be changed together").
That is the convention that failed in a sibling project: a squashed migration
left upgraded databases missing a column, the ORM kept selecting it, and it
surfaced months later as `UndefinedColumnError` in production queries.

So this migrates a real database and compares what is actually there against
what the models expect. Table names were already checked; columns were not, and
columns are where drift hides.
"""

from __future__ import annotations

import pytest
from forktex.database import reflect

from forktex_flow import Flow
from forktex_flow.persist.models import Run, RunEvent, ScheduledRun, StepRun, Workflow

MAPPED = (Workflow, Run, StepRun, RunEvent, ScheduledRun)


@pytest.mark.parametrize("model", MAPPED, ids=lambda m: m.__tablename__)
@pytest.mark.asyncio
async def test_every_mapped_column_exists_in_the_migrated_schema(flow: Flow, model: type):
    """A column the ORM selects but the migration never created is an
    `UndefinedColumnError` the first time that query runs — which may be long
    after the deploy that introduced it."""
    async with flow.engine.connect() as conn:
        actual = await reflect.columns(conn, model.__tablename__, schema=flow.schema)

    expected = {column.name for column in model.__table__.columns}
    missing = sorted(expected - actual)
    assert missing == [], f"{model.__tablename__}: the ORM expects columns the migration does not create: {missing}"


@pytest.mark.parametrize("model", MAPPED, ids=lambda m: m.__tablename__)
@pytest.mark.asyncio
async def test_the_migration_creates_no_column_the_models_do_not_know(flow: Flow, model: type):
    """The other direction. A column in the database that no model maps is dead
    weight at best, and at worst a `NOT NULL` the ORM will fail to populate on
    the next insert."""
    async with flow.engine.connect() as conn:
        actual = await reflect.columns(conn, model.__tablename__, schema=flow.schema)

    expected = {column.name for column in model.__table__.columns}
    unmapped = sorted(actual - expected)
    assert unmapped == [], f"{model.__tablename__}: the migration creates columns no model maps: {unmapped}"


@pytest.mark.asyncio
async def test_every_mapped_table_is_created_by_the_migration(flow: Flow):
    """A model whose table the migration forgot fails on first use, not at init."""
    async with flow.engine.connect() as conn:
        missing = [
            model.__tablename__
            for model in MAPPED
            if not await reflect.has_table(conn, model.__tablename__, schema=flow.schema)
        ]
    assert missing == [], f"mapped tables absent from the migrated schema: {missing}"


@pytest.mark.asyncio
async def test_the_indexes_the_query_layer_depends_on_exist(flow: Flow):
    """Not cosmetic. `ix_run_started_at_id` is what makes the keyset boundary a
    row comparison Postgres pushes into the index; without it paging still
    returns the right rows and quietly degrades with depth — the failure mode
    `database:keyset-row-comparison` exists to prevent, and one no test that
    only checks results would ever catch.
    """
    async with flow.engine.connect() as conn:
        run_indexes = await reflect.indexes(conn, "run", schema=flow.schema)
        step_indexes = await reflect.indexes(conn, "step_run", schema=flow.schema)

    assert "ix_run_started_at_id" in run_indexes, "the keyset index is missing; paging will degrade with depth"
    assert "ix_run_metadata_gin" in run_indexes, "metadata containment filters have no index"
    assert "ix_step_run_status_heartbeat" in step_indexes, "the stale-step reclaim scan has no index"
