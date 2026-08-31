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

"""Consumer hooks: metadata injection and terminal notification."""

from __future__ import annotations

from typing import Any, TypedDict

import pytest

from forktex_flow import Ctx, Flow, RunInfo, step
from forktex_flow.runtime.context import FlowRuntime
from forktex_flow.runtime.execution import execute_run


class State(TypedDict, total=False):
    marker: str


class Recorder:
    """A consumer extension, in the shape ForkTex Cloud's actually has."""

    def __init__(self) -> None:
        self.completed: list[str] = []
        self.failed: list[str] = []

    async def before_start(
        self, name: str, version: int, input: dict[str, Any], metadata: dict[str, Any]
    ) -> dict[str, Any]:
        return {"injected_by": "recorder"}

    async def after_complete(self, run: RunInfo, output: Any) -> None:
        self.completed.append(run.workflow_name)

    async def after_fail(self, run: RunInfo, error: BaseException) -> None:
        self.failed.append(run.workflow_name)


def _runtime(flow: Flow) -> FlowRuntime:
    return FlowRuntime(flow._store, flow._registry, flow._config, flow.extensions)


@pytest.mark.asyncio
async def test_before_start_metadata_reaches_the_stored_run(postgres_url_str: str):
    """The hook exists so a consumer can stamp a run with its tenant. If the
    result did not persist, every scoped query would come back empty."""
    import uuid

    from sqlalchemy import text

    recorder = Recorder()
    schema = "forktex_flow_" + uuid.uuid7().hex
    flow = Flow(postgres_url_str, schema=schema, extensions=[recorder])
    try:
        await flow.init()

        async def work(ctx: Ctx, state: State) -> dict:
            return {}

        flow.pipeline("t.ext", version=1, state=State)(type("W", (), {"steps": [work]}))
        instance = await flow.run("t.ext", state={}, metadata={"mine": "kept"})

        assert instance.metadata["injected_by"] == "recorder"
        assert instance.metadata["mine"] == "kept"
    finally:
        async with flow.engine.begin() as conn:
            await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await flow.close()


@pytest.mark.asyncio
async def test_after_complete_is_awaited_not_fire_and_forget(postgres_url_str: str):
    """The bug this rewrite fixes: the hooks were scheduled on the loop without
    a reference held, so one could be garbage-collected before it ran and was
    guaranteed lost if the process exited right after the run finished — which
    is exactly when `after_complete` fires."""
    import uuid

    from sqlalchemy import text

    recorder = Recorder()
    schema = "forktex_flow_" + uuid.uuid7().hex
    flow = Flow(postgres_url_str, schema=schema, extensions=[recorder])
    try:
        await flow.init()

        async def work(ctx: Ctx, state: State) -> dict:
            return {}

        flow.pipeline("t.done", version=1, state=State)(type("W", (), {"steps": [work]}))
        instance = await flow.run("t.done", state={})
        await execute_run(_runtime(flow), instance.instance_id, "t.done", 1)

        # No sleep, no polling: by the time execute_run returns the hook has run.
        assert recorder.completed == ["t.done"]
    finally:
        async with flow.engine.begin() as conn:
            await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await flow.close()


@pytest.mark.asyncio
async def test_a_raising_extension_does_not_change_the_run_outcome(postgres_url_str: str):
    """An extension is an observer. It does not get to fail a run that succeeded."""
    import uuid

    from sqlalchemy import text

    class Hostile:
        async def after_complete(self, run: RunInfo, output: Any) -> None:
            raise RuntimeError("this must not reach the run")

    schema = "forktex_flow_" + uuid.uuid7().hex
    flow = Flow(postgres_url_str, schema=schema, extensions=[Hostile()])
    try:
        await flow.init()

        async def work(ctx: Ctx, state: State) -> dict:
            return {"marker": "ok"}

        flow.pipeline("t.hostile", version=1, state=State)(type("W", (), {"steps": [work]}))
        instance = await flow.run("t.hostile", state={})
        await execute_run(_runtime(flow), instance.instance_id, "t.hostile", 1)

        final = await flow.get(instance.instance_id)
        assert final.status == "completed"
        assert final.state["marker"] == "ok"
    finally:
        async with flow.engine.begin() as conn:
            await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await flow.close()


@pytest.mark.asyncio
async def test_an_extension_may_implement_only_the_hooks_it_needs(postgres_url_str: str):
    """Every hook is optional. An extension that only wants terminal
    notifications should not have to stub out the column declarations."""
    import uuid

    from sqlalchemy import text

    seen: list[str] = []

    class OnlyComplete:
        async def after_complete(self, run: RunInfo, output: Any) -> None:
            seen.append(run.workflow_name)

    schema = "forktex_flow_" + uuid.uuid7().hex
    flow = Flow(postgres_url_str, schema=schema, extensions=[OnlyComplete()])
    try:
        await flow.init()

        async def work(ctx: Ctx, state: State) -> dict:
            return {}

        flow.pipeline("t.partial", version=1, state=State)(type("W", (), {"steps": [work]}))
        instance = await flow.run("t.partial", state={})
        await execute_run(_runtime(flow), instance.instance_id, "t.partial", 1)

        assert seen == ["t.partial"]
    finally:
        async with flow.engine.begin() as conn:
            await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await flow.close()


@pytest.mark.asyncio
async def test_after_fail_fires_and_receives_the_error(postgres_url_str: str):
    """The audit path. A run that failed must reach the extension with the
    exception, or a consumer's alerting sees only successes."""
    import uuid

    from sqlalchemy import text

    recorder = Recorder()
    schema = "forktex_flow_" + uuid.uuid7().hex
    flow = Flow(postgres_url_str, schema=schema, extensions=[recorder])
    try:
        await flow.init()

        async def boom(ctx: Ctx, state: State) -> dict:
            raise RuntimeError("deliberate")

        flow.pipeline("t.failing", version=1, state=State)(type("W", (), {"steps": [step(boom, max_attempts=1)]}))
        instance = await flow.run("t.failing", state={})
        await execute_run(_runtime(flow), instance.instance_id, "t.failing", 1)

        assert recorder.failed == ["t.failing"]
        assert recorder.completed == []
    finally:
        async with flow.engine.begin() as conn:
            await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await flow.close()
