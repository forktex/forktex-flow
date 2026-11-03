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

import sqlalchemy as sa
from forktex.types import JsonObject
from sqlalchemy import text

from forktex_flow.domain.config import RuntimeConfig
from forktex_flow.domain.types import DEFAULT_NAMESPACE
from forktex_flow.extension import FlowExtension
from forktex_flow.flow import Flow
from forktex_flow.persist import jobs as _jobs
from forktex_flow.persist.models import Instance
from forktex_flow.read.instance import WorkflowInstance
from forktex_flow.runtime.ctx import Ctx
from forktex_flow.runtime.worker import run_job

__all__ = ["isolated_flow", "make_ctx", "run_pending", "run_to_completion"]


def make_ctx(
    *,
    namespace: str = DEFAULT_NAMESPACE,
    key: str = "test",
    version: int = 1,
    element_id: str = "step",
    attempt: int = 1,
    max_attempts: int = 3,
    metadata: JsonObject | None = None,
    instance_id: uuid.UUID | None = None,
    activities: tuple[str, ...] = (),
) -> Ctx:
    """A ``Ctx`` for awaiting a handler directly. ``emit()`` raises on it: a handler that needs the database
    belongs in :func:`run_to_completion`.
    """
    return Ctx(
        instance_id=instance_id or uuid.uuid7(),
        namespace=namespace,
        key=key,
        version=version,
        element_id=element_id,
        task_type=f"{key}:{element_id}",
        attempt=attempt,
        max_attempts=max_attempts,
        metadata=metadata or {},
        activities=activities,
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
    key: str,
    *,
    variables: JsonObject | None = None,
    metadata: JsonObject | None = None,
    namespace: str = DEFAULT_NAMESPACE,
    version: int | None = None,
) -> WorkflowInstance:
    instance = await flow.start(key, variables=variables, metadata=metadata, namespace=namespace, version=version)
    return await run_pending(flow, instance.instance_id)


async def run_pending(flow: Flow, instance_id: uuid.UUID, *, max_jobs: int = 1000) -> WorkflowInstance:
    """Run the instance's jobs, and its children's, in this task. A retry does not wait for its backoff. Returns
    once the instance is terminal or waits on something this Flow cannot do, such as a user task.
    """
    runtime = flow._runtime
    for _ in range(max_jobs):
        tree = await _tree(flow, instance_id)
        jobs = await _jobs.claim(flow._store, runtime.registry.task_types(), 1, instance_ids=tree, ignore_backoff=True)
        if not jobs:
            break
        await run_job(runtime, jobs[0])
    return await flow.get(instance_id)


async def _tree(flow: Flow, root: uuid.UUID) -> list[uuid.UUID]:
    found, frontier = [root], [root]
    async with flow.transaction() as session:
        while frontier:
            frontier = list(
                await session.scalars(sa.select(Instance.id).where(Instance.parent_instance_id.in_(frontier)))
            )
            found.extend(frontier)
    return found
