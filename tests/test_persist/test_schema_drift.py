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

"""The ORM models and the migrated schema describe the same tables."""

from __future__ import annotations

from typing import get_args

import pytest
from sqlalchemy import insert
from sqlalchemy.exc import DBAPIError

from forktex_flow import Flow
from forktex_flow.domain.types import ElementStatus, InstanceStatus
from forktex_flow.persist.models import Definition, ElementRun, Instance

_CANONICAL_SCHEMA = "forktex_flow"
_VERSION_TABLE = "schema_version"


@pytest.mark.asyncio
async def test_the_models_and_the_migrated_schema_agree_completely(postgres_url_str: str) -> None:
    from alembic.autogenerate import compare_metadata
    from alembic.migration import MigrationContext

    flow = Flow(postgres_url_str, schema=_CANONICAL_SCHEMA)
    await flow.init()

    def diff(sync_conn: object) -> list[object]:
        context = MigrationContext.configure(
            sync_conn,  # type: ignore[arg-type]
            opts={
                "include_schemas": True,
                "include_name": lambda name, type_, _parent: (
                    name == _CANONICAL_SCHEMA if type_ == "schema" else name != _VERSION_TABLE
                ),
            },
        )
        return compare_metadata(context, Instance.__table__.metadata)

    try:
        async with flow.engine.connect() as conn:
            differences = await conn.run_sync(diff)
    finally:
        await flow.close()

    assert differences == [], "the models and the migration have drifted:\n" + "\n".join(
        f"  {item}" for item in differences
    )


@pytest.mark.asyncio
async def test_the_database_accepts_exactly_the_statuses_the_types_declare(flow: Flow) -> None:
    async with flow.transaction() as session:
        definition = Definition(namespace="default", key="t", version=1, checksum="c", model={}, source="")
        session.add(definition)
        await session.flush()
        instances = []
        for status in get_args(InstanceStatus):
            row = Instance(namespace="default", key="t", version=1, definition_id=definition.id, status=status)
            session.add(row)
            instances.append(row)
        await session.flush()
        for index, status in enumerate(get_args(ElementStatus)):
            session.add(
                ElementRun(
                    instance_id=instances[0].id, element_id=f"e{index}", element_type="serviceTask", status=status
                )
            )
        definition_id = definition.id

    for table, values in (
        (Instance, {"namespace": "default", "key": "t", "version": 1, "definition_id": definition_id}),
        (ElementRun, {"instance_id": instances[0].id, "element_id": "bad", "element_type": "serviceTask"}),
    ):
        with pytest.raises(DBAPIError):
            async with flow.transaction() as rejecting:
                await rejecting.execute(insert(table).values(status="not-a-status", **values))
