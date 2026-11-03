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

"""A cron on a definition key starts its latest version. A schedule declared disarmed stays disarmed until an
operator arms it, and no later deploy changes that.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest
import sqlalchemy as sa
from forktex import iso

from forktex_flow import Flow
from forktex_flow.persist.models import Schedule
from tests.conftest import FAST


async def _rows(flow: Flow) -> list[Schedule]:
    async with flow.transaction() as session:
        return list(await session.scalars(sa.select(Schedule)))


async def _make_due(flow: Flow) -> None:
    async with flow.transaction() as session:
        await session.execute(sa.update(Schedule).values(next_fire_at=iso.now() - timedelta(seconds=1)))


async def _prune(ctx, state):
    return {"pruned": True}


@pytest.mark.asyncio
async def test_a_due_schedule_starts_an_instance_once(postgres_url_str, schema):
    async with Flow(postgres_url_str, schema=schema, config=FAST) as flow:
        flow.pipeline("prune", cron="0 3 * * *", steps=[_prune])
        await flow.init()
        await _make_due(flow)
        await flow.start_driver()
        fired = None
        for _ in range(200):
            fired = await flow.query().key("prune").first()
            if fired is not None:
                break
            await asyncio.sleep(0.02)
        assert fired is not None
        done = await fired.wait(timeout=10)
        count = await flow.query().key("prune").count()
        (row,) = await _rows(flow)

    assert (done.status, done.triggered_by, done.variables) == ("completed", "schedule", {"pruned": True})
    assert count == 1
    assert row.next_fire_at > iso.now()


@pytest.mark.asyncio
async def test_a_disarmed_schedule_does_not_fire_and_a_redeploy_keeps_an_operator_s_choice(postgres_url_str, schema):
    async with Flow(postgres_url_str, schema=schema, config=FAST) as flow:
        flow.pipeline("reap", cron="*/5 * * * *", enabled=False, steps=[_prune])
        await flow.init()
        await _make_due(flow)
        await flow.start_driver()
        await asyncio.sleep(0.2)
        assert await flow.query().key("reap").count() == 0
        async with flow.transaction() as session:
            await session.execute(sa.update(Schedule).values(enabled=True))

    async with Flow(postgres_url_str, schema=schema) as redeployed:
        redeployed.pipeline("reap", cron="*/10 * * * *", enabled=False, steps=[_prune])
        await redeployed.init()
        (row,) = await _rows(redeployed)

    assert (row.enabled, row.cron) == (True, "*/10 * * * *")
