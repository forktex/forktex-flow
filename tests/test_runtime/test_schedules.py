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

"""Scheduled workflows: seeding a row, arming it, and claiming an occurrence."""

from __future__ import annotations

import asyncio
import contextlib
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
import sqlalchemy as sa
from sqlalchemy import select, update

from forktex import iso
from forktex_flow import Ctx, Flow
from tests.conftest import LoggingState
from forktex_flow.persist import runs as _driver_runs
from forktex_flow.persist.models import Run, ScheduledRun
from forktex_flow.runtime.context import FlowRuntime
from forktex_flow.runtime.driver import Driver


def _driver(flow: Flow) -> Driver:
    return Driver(
        FlowRuntime(
            store=flow._store,
            registry=flow._registry,
            config=flow._config,
            extensions=flow.extensions,
        )
    )


@pytest.mark.asyncio
async def test_a_due_schedule_fires_exactly_once_under_two_competing_drivers(flow: Flow):
    """The bug this rewrite fixes.

    The previous claim SELECTed due rows, mutated the ORM objects and committed,
    while its docstring claimed `UPDATE … RETURNING` atomicity. Two drivers
    overlapping for one tick — which is exactly what leader handover is — both
    read the same due row and both fired it. The claim is now a compare-and-swap
    on `next_fire_at`, so precisely one of them matches a row.

    The row has to be backdated by hand: a freshly registered schedule's
    `next_fire_at` is always in the future, so a test that merely registers one
    and races two drivers asserts nothing at all.
    """

    async def scheduled_body(ctx: Ctx, state: LoggingState) -> dict:
        return {"logs": ["fired"]}

    flow.scheduled("t.cron", version=1, cron="* * * * *", state=LoggingState)(scheduled_body)

    first, second = _driver(flow), _driver(flow)
    await first._fire_due_schedules()  # creates the row, fires nothing yet

    async with flow.session() as session:
        assert (await session.execute(select(Run).where(Run.workflow_name == "t.cron"))).first() is None
        await session.execute(
            update(ScheduledRun)
            .where(ScheduledRun.workflow_name == "t.cron")
            .values(next_fire_at=datetime.now(UTC) - timedelta(minutes=5))
        )

    # Two leaders, same due row, at the same moment.
    await asyncio.gather(first._fire_due_schedules(), second._fire_due_schedules())

    async with flow.session() as session:
        runs = (await session.execute(select(Run).where(Run.workflow_name == "t.cron"))).scalars().all()

    assert len(runs) == 1, f"a due schedule fired {len(runs)} times; exactly one worker may win the claim"
    assert runs[0].triggered_by == "schedule"


@pytest.mark.asyncio
async def test_a_fired_schedule_advances_past_now_rather_than_backlogging(flow: Flow):
    """A leader that fell behind must not then work through every occurrence it
    slept past — the next fire is computed from now, not from the missed
    deadline."""

    async def body(ctx: Ctx, state: LoggingState) -> dict:
        return {}

    flow.scheduled("t.behind", version=1, cron="* * * * *", state=LoggingState)(body)
    driver = _driver(flow)
    await driver._fire_due_schedules()

    async with flow.session() as session:
        await session.execute(
            update(ScheduledRun)
            .where(ScheduledRun.workflow_name == "t.behind")
            .values(next_fire_at=datetime.now(UTC) - timedelta(hours=3))
        )

    await driver._fire_due_schedules()

    async with flow.session() as session:
        row = (await session.execute(select(ScheduledRun).where(ScheduledRun.workflow_name == "t.behind"))).scalar_one()
        runs = (await session.execute(select(Run).where(Run.workflow_name == "t.behind"))).scalars().all()

    assert len(runs) == 1, "three hours of missed minutes must not become 180 runs"
    assert row.next_fire_at > datetime.now(UTC) - timedelta(minutes=1)


@pytest.mark.asyncio
async def test_a_schedule_row_is_created_for_a_registered_cron_workflow(flow: Flow):
    async def body(ctx: Ctx, state: LoggingState) -> dict:
        return {}

    flow.scheduled("t.registered", version=2, cron="0 3 * * *", state=LoggingState)(body)
    await _driver(flow)._fire_due_schedules()

    async with flow.session() as session:
        row = (
            await session.execute(select(ScheduledRun).where(ScheduledRun.workflow_name == "t.registered"))
        ).scalar_one()

    assert row.cron == "0 3 * * *"
    assert row.enabled is True
    assert row.next_fire_at is not None


@pytest.mark.asyncio
async def test_editing_a_cron_in_code_updates_the_stored_schedule(flow: Flow):
    """A schedule row outlives the process that created it, so a cron changed in
    code has to reach the row — otherwise the old expression runs forever and
    the source of truth is a database nobody looks at."""

    async def body(ctx: Ctx, state: LoggingState) -> dict:
        return {}

    flow.scheduled("t.changing", version=1, cron="0 1 * * *", state=LoggingState)(body)
    await _driver(flow)._fire_due_schedules()

    revised = Flow(database=flow._db, schema=flow.schema)
    revised.scheduled("t.changing", version=1, cron="30 4 * * *", state=LoggingState)(body)
    await _driver(revised)._fire_due_schedules()

    async with flow.session() as session:
        row = (
            await session.execute(select(ScheduledRun).where(ScheduledRun.workflow_name == "t.changing"))
        ).scalar_one()

    assert row.cron == "30 4 * * *"


@pytest.mark.asyncio
async def test_a_schedule_registered_disabled_gets_a_row_and_does_not_fire(flow: Flow):
    """`enabled=False` registers the timer without starting it.

    The case this exists for is a cron whose first tick is destructive — a
    reaper that dispatches a destroy against expired infrastructure. Arming it
    the instant the process boots, and disarming afterwards, loses that race:
    the first tick can land before anyone runs the UPDATE.
    """

    async def body(ctx: Ctx, state: LoggingState) -> dict:
        return {"logs": ["fired"]}

    # A cron that is due every minute, so "did not fire" is a real assertion
    # rather than an artefact of nothing being due yet.
    flow.scheduled("t.disarmed", version=1, cron="* * * * *", state=LoggingState, enabled=False)(body)
    driver = _driver(flow)
    await driver._fire_due_schedules()
    await driver._fire_due_schedules()

    async with flow.session() as session:
        row = (
            await session.execute(select(ScheduledRun).where(ScheduledRun.workflow_name == "t.disarmed"))
        ).scalar_one()
        runs = (await session.execute(select(Run).where(Run.workflow_name == "t.disarmed"))).scalars().all()

    assert row.enabled is False, "the row exists but must not be armed"
    assert runs == [], "a disabled schedule dispatched a run"


@pytest.mark.asyncio
async def test_arming_a_schedule_survives_re_registration(flow: Flow):
    """Once armed, the database owns the switch.

    `enabled` is read on the first insert only. If re-registration wrote it
    back, every deploy would silently re-disarm a schedule an operator had
    turned on — or worse, re-arm one they had turned off, which is how a reaper
    comes back from the dead. The upsert updates `cron` and nothing else.
    """

    async def body(ctx: Ctx, state: LoggingState) -> dict:
        return {}

    flow.scheduled("t.armed", version=1, cron="0 1 * * *", state=LoggingState, enabled=False)(body)
    await _driver(flow)._fire_due_schedules()

    async with flow.session() as session:
        await session.execute(update(ScheduledRun).where(ScheduledRun.workflow_name == "t.armed").values(enabled=True))

    # A fresh process registering the same workflow — still declaring it
    # disabled, as the code always will.
    revised = Flow(database=flow._db, schema=flow.schema)
    revised.scheduled("t.armed", version=1, cron="0 2 * * *", state=LoggingState, enabled=False)(body)
    await _driver(revised)._fire_due_schedules()

    async with flow.session() as session:
        row = (await session.execute(select(ScheduledRun).where(ScheduledRun.workflow_name == "t.armed"))).scalar_one()

    assert row.enabled is True, "re-registration disarmed a schedule an operator had armed"
    assert row.cron == "0 2 * * *", "the cron should still track the code"


@pytest.mark.asyncio
async def test_a_schedule_is_not_consumed_when_its_run_cannot_be_inserted(
    flow: Flow, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The advance committed in its own transaction and the insert followed in
    another, with its failure swallowed — so one transient blip consumed the
    occurrence and the nightly job simply did not happen, while `last_fired_at`
    claimed it had. Fault-injected: nothing short of a failing insert shows it."""

    async def nightly(ctx: Ctx, state: LoggingState) -> dict:
        return {"logs": ["ran"]}

    flow.scheduled("cron.atomic", version=1, cron="* * * * *", state=LoggingState)(nightly)
    driver = _driver(flow)
    await driver._ensure_schedule_rows(flow._registry.scheduled(), iso.now())

    async with flow.session() as session:
        await session.execute(
            update(ScheduledRun)
            .where(ScheduledRun.workflow_name == "cron.atomic")
            .values(next_fire_at=iso.now() - timedelta(minutes=1))
        )

    async def exploding_insert(*_a: object, **_k: object) -> UUID:
        raise RuntimeError("transient blip inserting the run")

    monkeypatch.setattr(_driver_runs, "insert_run_in", exploding_insert)
    with contextlib.suppress(Exception):
        await driver._fire_due_schedules()

    async with flow.session() as session:
        row = (
            await session.execute(select(ScheduledRun).where(ScheduledRun.workflow_name == "cron.atomic"))
        ).scalar_one()
        runs = (
            await session.execute(select(sa.func.count()).select_from(Run).where(Run.workflow_name == "cron.atomic"))
        ).scalar_one()

    assert runs == 0
    assert row.last_fired_at is None, "the occurrence was consumed without a run being created"
    assert row.next_fire_at < iso.now(), "the schedule advanced past an occurrence that never ran"
