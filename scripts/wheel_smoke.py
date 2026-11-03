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

"""Run a workflow from the installed wheel; refuses to run against a source checkout."""

from __future__ import annotations

import asyncio
import operator
import pathlib
import sys
import uuid
from typing import Annotated, TypedDict
from urllib.parse import urlparse


class State(TypedDict, total=False):
    logs: Annotated[list[str], operator.add]
    enabled: bool


def _assert_running_from_site_packages() -> None:
    import forktex_flow

    location = pathlib.Path(forktex_flow.__file__).resolve()
    if "site-packages" not in location.parts:
        raise SystemExit(
            f"wheel-smoke must run against an installed package, not {location}.\n"
            "Run it from a venv with only the built wheel installed."
        )
    print(f"  running from {location.parent}")


async def main() -> int:
    _assert_running_from_site_packages()

    from testcontainers.community.postgres import PostgresContainer

    from forktex_flow import Ctx, Flow, RuntimeConfig, step

    container = PostgresContainer("postgres:17-alpine")
    container.start()
    parsed = urlparse(container.get_connection_url())
    url = f"postgresql+asyncpg://{parsed.username}:{parsed.password}@{parsed.hostname}:{parsed.port}{parsed.path}"
    schema = "forktex_flow_" + uuid.uuid7().hex
    config = RuntimeConfig(poll_interval=0.05, election_interval=0.05, heartbeat_interval=0.5, stale_threshold=2)
    flow = Flow(url, schema=schema, config=config, drive=True)

    async def first(ctx: Ctx, state: State) -> dict:
        return {"logs": ["first"]}

    async def guarded(ctx: Ctx, state: State) -> dict:
        return {"logs": ["guarded"]}

    flow.pipeline("smoke.pipeline", state=State, steps=[first, step(guarded, when={"var": "enabled"})])

    try:
        # Proves the .sql migrations shipped AND that the runner finds them through the installed package.
        async with flow:
            print("  schema migrated from the packaged .sql; driver started")
            instance = await flow.start("smoke.pipeline", variables={"logs": [], "enabled": True})
            final = await instance.wait(timeout=30)

            assert final.status == "completed", final.status
            assert final.variables["logs"] == ["first", "guarded"], final.variables["logs"]
            print(f"  ran a 2-step pipeline: {final.status}, logs={final.variables['logs']}")

            page = await flow.query().key("smoke.pipeline").fetch()
            assert len(page.items) == 1
            print("  query round-trips")
    finally:
        from sqlalchemy import text

        cleanup = Flow(url, schema=schema)
        async with cleanup.engine.begin() as conn:
            await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await cleanup.close()
        container.stop()

    print("wheel-smoke: the installed package runs a workflow end to end")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
