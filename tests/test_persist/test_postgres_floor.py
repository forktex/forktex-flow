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

"""The advertised PostgreSQL floor is exercised, not asserted.

The README tells a reader which PostgreSQL they need. Every other test in this
suite runs on the version we develop against, so that sentence was the one
compatibility claim in the project with nothing behind it — and the failure it
guards against is silent: a consumer on the floor version hits an error inside
`Flow.init()`, not a resolver message.

The floor is read out of the README rather than repeated here, so raising the
claim without checking it fails instead of quietly becoming untrue.
"""

from __future__ import annotations

import operator
import re
from pathlib import Path
from typing import Annotated, TypedDict

import pytest
import sqlalchemy as sa

from forktex_flow import Ctx, Flow, step
from forktex_flow.testing import run_to_completion

from .._containers import start_postgres

README = Path(__file__).resolve().parents[2] / "README.md"


def advertised_floor() -> int:
    """The major version the README promises, e.g. 14 from 'PostgreSQL 14+'."""
    match = re.search(r"PostgreSQL (\d+)\+", README.read_text(encoding="utf-8"))
    assert match, "README no longer states a 'PostgreSQL N+' floor"
    return int(match.group(1))


class FloorState(TypedDict):
    logs: Annotated[list[str], operator.add]
    run_guarded: bool


async def _first(ctx: Ctx, state: FloorState) -> dict:
    return {"logs": ["first"]}


async def _guarded(ctx: Ctx, state: FloorState) -> dict:
    return {"logs": ["guarded"]}


@pytest.mark.asyncio
async def test_the_advertised_postgres_floor_runs_a_workflow() -> None:
    """`package:claims-are-checked`. Migrate and run on the oldest supported server.

    Covers the features whose availability is version-dependent and which a
    newer server would hide: the migration's DDL (partial and `jsonb_path_ops`
    GIN indexes, the row-comparison keyset index), `ON CONFLICT ... DO UPDATE`
    on the memoisation constraint, and the JSONB containment filters.
    """
    floor = advertised_floor()
    container, url = start_postgres(f"postgres:{floor}-alpine")
    try:
        flow = Flow(database_url=url.render_as_string(hide_password=False))
        flow.pipeline("floor.check", version=1, state=FloorState)(
            type("FloorPipeline", (), {"steps": [_first, step(_guarded, when=lambda s: s["run_guarded"])]})
        )
        await flow.init()

        async with flow.session() as session:
            server_major = int((await session.execute(sa.text("SHOW server_version"))).scalar_one().split(".")[0])
        assert server_major == floor, "the container is not the version the README advertises"

        final = await run_to_completion(flow, "floor.check", state={"logs": [], "run_guarded": True})
        assert final.status == "completed"
        assert final.state["logs"] == ["first", "guarded"]

        # The two JSONB filters and the keyset page, which lean on index types
        # whose operator classes differ across versions.
        page = await flow.query().workflow("floor.check", version=1).limit(5).fetch()
        assert len(page.items) == 1
        assert await flow.query().current_node("_guarded").count() == 0

        await flow.close()
    finally:
        container.stop()
