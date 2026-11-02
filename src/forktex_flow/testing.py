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

import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from forktex.types import JsonObject
from sqlalchemy import text

from forktex_flow.domain.config import RuntimeConfig
from forktex_flow.extension import FlowExtension
from forktex_flow.flow import Flow
from forktex_flow.read.instance import WorkflowInstance
from forktex_flow.runtime.ctx import Ctx
from forktex_flow.runtime.execution import execute_run

__all__ = ["isolated_flow", "make_ctx", "run_to_completion"]


def make_ctx(
    *,
    workflow_name: str = "test",
    workflow_version: int = 1,
    node_name: str = "step",
    attempt: int = 1,
    metadata: JsonObject | None = None,
    run_id: uuid.UUID | None = None,
) -> Ctx:
    """A ``Ctx`` for awaiting a step directly:: Not bound to a database, so ``Ctx.emit()`` raises: a step whose
    logic you are testing should not need one, and a step that does belongs in :func:`run_to_completion`.
    """
    return Ctx(
        run_id=run_id or uuid.uuid7(),
        workflow_name=workflow_name,
        workflow_version=workflow_version,
        node_name=node_name,
        attempt=attempt,
        metadata=metadata or {},
    )


@asynccontextmanager
async def isolated_flow(
    database_url: str,
    *,
    config: RuntimeConfig | None = None,
    extensions: list[FlowExtension] | None = None,
) -> AsyncIterator[Flow]:
    schema = "forktex_flow_test_" + uuid.uuid7().hex
    flow = Flow(database_url, schema=schema, config=config, extensions=extensions)
    await flow.init()
    try:
        yield flow
    finally:
        async with flow.engine.begin() as conn:
            await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await flow.close()


async def run_to_completion(
    flow: Flow,
    name: str,
    *,
    state: JsonObject | None = None,
    metadata: JsonObject | None = None,
    version: int | None = None,
    max_attempts: int = 10,
) -> WorkflowInstance:
    instance = await flow.run(name, state=state, metadata=metadata, version=version)
    runtime = flow._execution_context()

    for _ in range(max_attempts):
        await execute_run(runtime, instance.instance_id, name, instance.workflow_version)
        current = await flow.get(instance.instance_id)
        if current.status != "pending":
            return current

    return await flow.get(instance.instance_id)
