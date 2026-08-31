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

"""Helpers for testing workflows you wrote against this library.

Three things, and they exist because writing them yourself is the first thing
every consumer would otherwise have to do:

- :func:`make_ctx` builds a ``Ctx`` so a step can be awaited directly, with no
  database and no run. Most of what a step does is worth testing this way.
- :func:`isolated_flow` gives a migrated ``Flow`` on a schema of its own, torn
  down afterwards. Wrap it in a fixture; the schema-per-test shape is what makes
  a suite safe to run in parallel against one database.
- :func:`run_to_completion` dispatches a run and executes it inline. No driver,
  so the test is deterministic — it does not poll a background loop and hope.

This module is part of the public surface and follows the same stability rules
as the rest of it. It needs a real Postgres: ``pip install "forktex-flow[testing]"``
brings in ``testcontainers``, or point ``isolated_flow`` at any database you like.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

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
    metadata: dict[str, Any] | None = None,
    run_id: uuid.UUID | None = None,
) -> Ctx:
    """A ``Ctx`` for awaiting a step directly::

        result = await provision(make_ctx(metadata={"org_id": "acme"}), {"logs": []})
        assert result == {"server_id": "srv-1", "logs": ["provisioned srv-1"]}

    Not bound to a database, so ``Ctx.emit()`` raises. That is deliberate: a
    step whose logic you are testing should not need one, and a step that does
    belongs in :func:`run_to_completion`.
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
    """A migrated ``Flow`` on a throwaway schema, dropped on the way out.

    Isolation is per schema rather than per database, so a suite can run against
    one Postgres without tests seeing each other's runs::

        @pytest_asyncio.fixture
        async def flow(postgres_url):
            async with isolated_flow(postgres_url) as f:
                register_my_workflows(f)
                yield f

    The driver is **not** started. A test that wants one starts it explicitly,
    so nothing races a background loop it did not ask for; most tests want
    :func:`run_to_completion` instead.

    ``config`` and ``extensions`` are named rather than forwarded as ``**kwargs``:
    they are the two a test actually varies, and a typed signature says so.
    """
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
    state: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
    version: int | None = None,
    max_attempts: int = 10,
) -> WorkflowInstance:
    """Dispatch a run and execute it here, returning the final snapshot.

    Inline rather than through the driver, so a test asserts on a result instead
    of polling and hoping the tick landed. A run that schedules a retry is
    re-executed until it is terminal or ``max_attempts`` passes are spent — set
    a zero backoff on the steps you expect to retry, or the wait is real.

    ``max_attempts`` bounds the loop so a workflow that retries forever fails
    the test rather than hanging it.
    """
    instance = await flow.run(name, state=state, metadata=metadata, version=version)
    runtime = flow._execution_context()

    for _ in range(max_attempts):
        await execute_run(runtime, instance.instance_id, name, instance.workflow_version)
        current = await flow.get(instance.instance_id)
        if current.status != "pending":
            return current

    return await flow.get(instance.instance_id)
