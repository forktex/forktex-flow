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

"""Flow migrates and runs on the oldest PostgreSQL it supports."""

from __future__ import annotations

import operator
from typing import Annotated, TypedDict

import pytest
import sqlalchemy as sa

from forktex_flow import Ctx, Flow, step
from forktex_flow.testing import run_to_completion

from .._containers import start_postgres

FLOOR = 14


class FloorState(TypedDict):
    logs: Annotated[list[str], operator.add]
    run_guarded: bool


async def _first(ctx: Ctx, state: FloorState) -> dict:
    return {"logs": ["first"]}


async def _guarded(ctx: Ctx, state: FloorState) -> dict:
    return {"logs": ["guarded"]}


@pytest.mark.asyncio
async def test_the_advertised_postgres_floor_runs_a_workflow() -> None:
    container, url = start_postgres(f"postgres:{FLOOR}-alpine")
    try:
        flow = Flow(database_url=url.render_as_string(hide_password=False))
        flow.pipeline("floor.check", state=FloorState)(
            type("FloorPipeline", (), {"steps": [_first, step(_guarded, when=lambda s: s["run_guarded"])]})
        )
        await flow.init()

        async with flow.transaction() as session:
            server_major = int((await session.execute(sa.text("SHOW server_version"))).scalar_one().split(".")[0])
        assert server_major == FLOOR

        final = await run_to_completion(flow, "floor.check", variables={"logs": [], "run_guarded": True})
        assert final.status == "completed"
        assert final.variables["logs"] == ["first", "guarded"]

        page = await flow.query().key("floor.check", version=1).metadata().variables(run_guarded=True).limit(5).fetch()
        assert len(page.items) == 1
        assert await flow.query().current_element("_guarded").count() == 0

        await flow.close()
    finally:
        container.stop()
