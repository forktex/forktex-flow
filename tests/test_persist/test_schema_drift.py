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

from typing import get_args

import pytest
from sqlalchemy import insert
from sqlalchemy.exc import DBAPIError
from forktex import iso
from forktex.database import reflect
from forktex.types import JsonValue

from forktex_flow import Flow
from forktex_flow.domain.types import RunStatus
from forktex_flow.persist import runs as _runs
from forktex_flow.persist import steps as _steps
from forktex_flow.persist.models import Run, RunEvent, ScheduledRun, StepRun, Workflow

MAPPED = (Workflow, Run, StepRun, RunEvent, ScheduledRun)

#: The schema the models name literally, and the runner's version table, which
#: is the runner's business rather than the models'.
_CANONICAL_SCHEMA = "forktex_flow"
_VERSION_TABLE = "flow_schema_version"


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


@pytest.mark.asyncio
async def test_the_models_and_the_migrated_schema_agree_completely(postgres_url_str: str) -> None:
    """Alembic's own comparator against the migrated schema: tables, columns,
    types, nullability, server defaults, constraints and indexes, by name and by
    shape. The column-name checks above cannot see a `TEXT` column the model
    maps as an `int`, or an index the model declares that nothing creates.

    Runs on the canonical `forktex_flow` schema, because the models name it
    literally and the comparator reflects what is actually there.
    """
    from alembic.autogenerate import compare_metadata
    from alembic.migration import MigrationContext

    flow = Flow(postgres_url_str, schema=_CANONICAL_SCHEMA)
    await flow.init()

    def diff(sync_conn: object) -> list[object]:
        context = MigrationContext.configure(
            sync_conn,  # type: ignore[arg-type]
            opts={
                "include_schemas": True,
                # The version table belongs to the runner, not to the models.
                "include_name": lambda name, type_, _parent: (
                    name == _CANONICAL_SCHEMA if type_ == "schema" else name != _VERSION_TABLE
                ),
            },
        )
        return compare_metadata(context, Run.__table__.metadata)

    try:
        async with flow.engine.connect() as conn:
            differences = await conn.run_sync(diff)
    finally:
        await flow.close()

    assert differences == [], "the models and the migration have drifted:\n" + "\n".join(
        f"  {item}" for item in differences
    )


@pytest.mark.asyncio
async def test_the_database_accepts_exactly_the_statuses_the_type_declares(flow: Flow) -> None:
    """`RunStatus` is what the package is written against; the CHECK constraint
    in the shipped migration is what the database enforces. A shipped migration
    cannot be edited, so adding a status to the type without an incremental
    migration would be rejected at runtime, per row.
    """
    declared = set(get_args(RunStatus))

    async with flow.transaction() as session:
        await session.execute(insert(Workflow).values(name="t.statuses", version=1, registered_at=iso.now()))
        for status in declared:
            await session.execute(
                insert(Run).values(
                    workflow_name="t.statuses", workflow_version=1, status=status, input={}, metadata_={}
                )
            )

        with pytest.raises(DBAPIError):
            async with flow.transaction() as rejecting:
                await rejecting.execute(
                    insert(Run).values(
                        workflow_name="t.statuses",
                        workflow_version=1,
                        status="not-a-status",
                        input={},
                        metadata_={},
                    )
                )


@pytest.mark.asyncio
async def test_a_replayed_step_whose_output_was_the_old_skip_marker_keeps_it(flow: Flow) -> None:
    """The skip was carried in the output as `{"__skipped__": true}`, which is a
    value a step body may return as its whole delta. On replay that output read
    as the marker, so the step's real delta became `{}` and a `node_skipped`
    event was recorded for a step that had executed.
    """
    collides: dict[str, JsonValue] = {"__skipped__": True}

    async with flow.transaction() as session:
        await session.execute(insert(Workflow).values(name="t.marker", version=1, registered_at=iso.now()))
    run_id = await _runs.insert_run(flow._store, workflow_name="t.marker", workflow_version=1, input={}, metadata={})

    first = await _steps.upsert_pending_step(
        flow._store,
        run_id=run_id,
        step_name="work",
        step_qualname="t.marker:work",
        step_index=0,
        args_hash="hash",
        max_attempts=1,
    )
    await _steps.mark_step_running(flow._store, first.step_id)
    await _steps.mark_step_completed(flow._store, first.step_id, collides)

    replayed = await _steps.upsert_pending_step(
        flow._store,
        run_id=run_id,
        step_name="work",
        step_qualname="t.marker:work",
        step_index=0,
        args_hash="hash",
        max_attempts=1,
    )

    assert replayed.status == "completed"
    assert replayed.skipped is False, "an executed step was replayed as a skip"
    assert replayed.output == collides, "the step's own output was discarded as a skip marker"


@pytest.mark.asyncio
async def test_a_step_cannot_be_skipped_without_being_completed(flow: Flow) -> None:
    """`skipped` arrived without a constraint while `status` beside it has one,
    so the pair admitted a `running` step marked skipped — a state the executor
    cannot interpret. Only one caller writes the column and it always writes
    `completed` too, which made the invariant a convention rather than a rule.
    """
    async with flow.transaction() as session:
        await session.execute(insert(Workflow).values(name="t.ck", version=1, registered_at=iso.now()))
    run_id = await _runs.insert_run(flow._store, workflow_name="t.ck", workflow_version=1, input={}, metadata={})

    with pytest.raises(DBAPIError, match="ck_step_run_skipped_is_completed"):
        async with flow.transaction() as session:
            await session.execute(
                insert(StepRun).values(
                    run_id=run_id,
                    step_name="s",
                    step_qualname="t.ck:s",
                    step_index=0,
                    args_hash="h",
                    status="running",
                    skipped=True,
                    max_attempts=1,
                )
            )
