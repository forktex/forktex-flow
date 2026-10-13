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


"""A declared backoff must actually be waited.

A step that asks to be retried sets ``next_attempt_at`` on its step row and the
run goes straight back to ``pending`` (``execution.py``, on
``StepRetryRequested``). ``claim_pending_runs`` selected on ``Run.status ==
"pending"`` and nothing else, so the driver re-dispatched the run on its very
next tick — a second later — and the delay the step declared was never waited.

The consequence is not "slightly early". ``@step(max_attempts=720,
backoff=(15, 30, 60))`` reads as roughly half a day of patience and delivered
about twelve minutes of busy-looping. Every wait built on top of retries — a
DNS record a human still has to create, an approval gate holding a deploy until
someone reads the plan — was really an unadvertised twelve-minute timeout, plus
720 pointless round-trips at whatever the step was polling.

``_revive_runs_with_pending_steps`` already gated on the due time, which is why
this looked correct on a reading: that sweep is for runs stuck at ``running``,
and a retrying run never passes through it.
"""

from __future__ import annotations

import operator
from datetime import timedelta
from typing import Annotated, TypedDict

import pytest
import sqlalchemy as sa

from forktex_flow import Ctx, Flow
from forktex_flow.persist import runs as _runs
from forktex_flow.persist import steps as _steps
from forktex_flow.persist.models import Run, StepRun


class State(TypedDict, total=False):
    logs: Annotated[list[str], operator.add]


async def _pending_run(flow: Flow, name: str):
    async def noop(ctx: Ctx, state: State) -> dict:
        return {}

    flow.pipeline(name, version=1, state=State)(type("W", (), {"steps": [noop]}))
    return await flow.run(name, state={})


async def _schedule_retry(flow: Flow, run_id, *, seconds: int) -> None:
    """Put the run in the state a retrying step leaves behind."""
    step_id, _, _ = await _steps.upsert_pending_step(
        flow._store,
        run_id=run_id,
        step_name="noop",
        step_qualname="t.backoff:noop",
        step_index=0,
        args_hash="h",
        max_attempts=720,
    )
    await _steps.mark_step_failed(flow._store, step_id, "boom", final=False, retry_in_seconds=seconds)
    async with flow.session() as session:
        await session.execute(sa.update(Run).where(Run.id == run_id).values(status="pending"))
        await session.commit()


@pytest.mark.asyncio
async def test_a_run_waiting_on_a_backoff_is_not_claimed(flow: Flow):
    """The bug, stated once. A minute of backoff must not be a second."""
    instance = await _pending_run(flow, "t.backoff.waiting")
    await _schedule_retry(flow, instance.instance_id, seconds=60)

    claimed = await _runs.claim_pending_runs(flow._store, limit=10)

    assert instance.instance_id not in {run_id for run_id, _, _ in claimed}


@pytest.mark.asyncio
async def test_it_is_claimed_once_the_backoff_has_elapsed(flow: Flow):
    """A backoff is a delay, not a cancellation.

    Getting the predicate slightly wrong turns every retry into a permanently
    stuck run, which is worse than the bug it replaces.
    """
    instance = await _pending_run(flow, "t.backoff.elapsed")
    await _schedule_retry(flow, instance.instance_id, seconds=60)

    # Reach back in time rather than sleeping for a minute.
    async with flow.session() as session:
        await session.execute(
            sa.update(StepRun)
            .where(StepRun.run_id == instance.instance_id)
            .values(next_attempt_at=sa.func.now() - timedelta(seconds=1))
        )
        await session.commit()

    claimed = await _runs.claim_pending_runs(flow._store, limit=10)

    assert instance.instance_id in {run_id for run_id, _, _ in claimed}


@pytest.mark.asyncio
async def test_a_fresh_run_with_no_steps_is_still_claimed(flow: Flow):
    """The common case by a wide margin, and the one a naive guard breaks.

    A run that has never dispatched has no step rows at all, so a predicate
    written as "has a due pending step" would never claim anything.
    """
    instance = await _pending_run(flow, "t.backoff.fresh")

    claimed = await _runs.claim_pending_runs(flow._store, limit=10)

    assert instance.instance_id in {run_id for run_id, _, _ in claimed}


@pytest.mark.asyncio
async def test_a_run_between_steps_is_still_claimed(flow: Flow):
    """Resumed mid-pipeline: completed rows, and the next step not created yet.

    "Has a due pending step" is false here too, and stalling this is how a
    backoff guard silently becomes a deadlock for every multi-step workflow.
    """
    instance = await _pending_run(flow, "t.backoff.between")
    step_id, _, _ = await _steps.upsert_pending_step(
        flow._store,
        run_id=instance.instance_id,
        step_name="noop",
        step_qualname="t.backoff.between:noop",
        step_index=0,
        args_hash="h",
        max_attempts=3,
    )
    await _steps.mark_step_completed(flow._store, step_id, {})
    async with flow.session() as session:
        await session.execute(sa.update(Run).where(Run.id == instance.instance_id).values(status="pending"))
        await session.commit()

    claimed = await _runs.claim_pending_runs(flow._store, limit=10)

    assert instance.instance_id in {run_id for run_id, _, _ in claimed}


@pytest.mark.asyncio
async def test_a_due_step_beats_a_waiting_one_in_the_same_run(flow: Flow):
    """A run can hold both. It is blocked only when nothing in it is ready."""
    instance = await _pending_run(flow, "t.backoff.mixed")
    await _schedule_retry(flow, instance.instance_id, seconds=60)

    ready_id, _, _ = await _steps.upsert_pending_step(
        flow._store,
        run_id=instance.instance_id,
        step_name="other",
        step_qualname="t.backoff.mixed:other",
        step_index=1,
        args_hash="h2",
        max_attempts=3,
    )
    await _steps.mark_step_failed(flow._store, ready_id, "boom", final=False, retry_in_seconds=0)

    claimed = await _runs.claim_pending_runs(flow._store, limit=10)

    assert instance.instance_id in {run_id for run_id, _, _ in claimed}
