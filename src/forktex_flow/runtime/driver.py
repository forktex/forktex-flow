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

"""Driver loop — the leader-elected dispatcher.

One driver per ``Flow``. Each competes for a session-scoped Postgres advisory
lock and only the holder ticks; when the holder's process dies its connection
drops, the lock releases, and another worker wins the next election. That is the
whole failover mechanism.

A tick, in order:

  1. Fire any ``@scheduled`` workflow whose ``next_fire_at`` has passed.
  2. Reclaim steps whose heartbeat went stale.
  3. Revive runs that are ``running`` but hold a ``pending`` step.
  4. Claim up to ten pending runs and dispatch each as a task.

Runs execute concurrently inside the one leader — they are coroutines, bounded
by the event loop, which is a few hundred in-flight I/O-bound runs. Scaling past
that needs a sharded lock; deliberately not built, because nothing is near it.
"""

from __future__ import annotations

import asyncio
import contextlib
from datetime import datetime
from typing import TYPE_CHECKING
from uuid import UUID

import sqlalchemy as sa
from croniter import croniter
from forktex import iso
from forktex.database import try_advisory_lock
from forktex.log import get_logger
from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from forktex_flow.errors import FlowError
from forktex_flow.persist import runs as _runs
from forktex_flow.persist import steps as _steps
from forktex_flow.persist.models import Run, ScheduledRun, StepRun, Workflow
from forktex_flow.runtime.execution import execute_run

if TYPE_CHECKING:
    from forktex_flow.domain.definition import WorkflowDefinition
    from forktex_flow.runtime.context import FlowRuntime

__all__ = ["Driver"]

logger = get_logger(__name__)

#: Runs claimed per tick. Small on purpose: a claim marks rows `running`, so a
#: large batch that the worker then dies holding takes a stale-reclaim cycle to
#: come back.
_CLAIM_BATCH = 10


class Driver:
    """Per-process driver. Competes for the leader lock; ticks while it holds it."""

    def __init__(self, runtime: FlowRuntime) -> None:
        self.runtime = runtime
        self.shutdown = asyncio.Event()
        self.is_leader = False
        #: In-flight run tasks, so shutdown can join them.
        self._tasks: set[asyncio.Task[None]] = set()
        #: run_id → task. A step whose heartbeat merely lagged (a GC pause, a
        #: drained pool) can be reclaimed to `pending` while its original
        #: `execute_run` task is still alive, and the run row follows it. Without
        #: this map the next tick would start a second `execute_run` for the same
        #: run while the first is inside a step body — silent double execution of
        #: anything not idempotent.
        self._in_flight: dict[UUID, asyncio.Task[None]] = {}

    async def run(self) -> None:
        """Alternate between standing for election and ticking, until shutdown."""
        while not self.shutdown.is_set():
            try:
                await self._compete_for_leadership()
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("driver loop crashed; retrying after backoff")
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(self.shutdown.wait(), timeout=5.0)
        await self._drain_tasks()

    async def _compete_for_leadership(self) -> None:
        """Hold the advisory lock for as long as this worker is leader."""
        async with try_advisory_lock(self.runtime.store.engine, self.runtime.config.leader_lock_key) as is_leader:
            if not is_leader:
                self.is_leader = False
                # A timeout here is the normal case — not the leader, so wait a
                # beat rather than hot-looping on the election.
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(self.shutdown.wait(), timeout=self.runtime.config.election_interval)
                return

            self.is_leader = True
            logger.info("forktex_flow.driver: this worker is now leader (schema=%s)", self.runtime.store.schema)
            try:
                while not self.shutdown.is_set():
                    await self._tick()
                    with contextlib.suppress(TimeoutError):
                        await asyncio.wait_for(self.shutdown.wait(), timeout=self.runtime.config.poll_interval)
            finally:
                self.is_leader = False

    async def _tick(self) -> None:
        """One pass. Each phase is isolated: a failure in one must not stop the
        others, because they recover independent kinds of stuck work."""
        for phase in (self._fire_due_schedules, self._reclaim_stale, self._revive_runs_with_pending_steps):
            try:
                await phase()
            except Exception:
                logger.exception("driver phase %s failed", phase.__name__)

        try:
            claims = await _runs.claim_pending_runs(self.runtime.store, limit=_CLAIM_BATCH)
        except Exception:
            logger.exception("claim_pending_runs failed")
            return

        for run_id, name, version in claims:
            self._dispatch(run_id, name, version)

    def _dispatch(self, run_id: UUID, name: str, version: int) -> None:
        if run_id in self._in_flight:
            logger.warning(
                "forktex_flow.driver: run %s was reclaimed while still in flight; skipping duplicate dispatch",
                run_id,
            )
            return
        task = asyncio.create_task(
            execute_run(self.runtime, run_id, name, version),
            name=f"forktex_flow.run[{run_id}]",
        )
        self._tasks.add(task)
        self._in_flight[run_id] = task
        task.add_done_callback(self._tasks.discard)
        task.add_done_callback(lambda _t, rid=run_id: self._in_flight.pop(rid, None))

    async def _reclaim_stale(self) -> None:
        reclaimed = await _steps.reclaim_stale_steps(self.runtime.store, self.runtime.config.stale_threshold)
        if reclaimed:
            logger.info("forktex_flow.driver: reclaimed %d stale steps", reclaimed)

    async def _fire_due_schedules(self) -> None:
        """Start a run for every scheduled workflow whose time has come."""
        definitions = self.runtime.registry.scheduled()
        if not definitions and not await self._has_schedule_rows():
            # Most consumers register no scheduled workflows. Without this the
            # driver pays two queries a tick to learn that every tick.
            return

        now = iso.now()
        await self._ensure_schedule_rows(definitions, now)

        for row in await self._due_schedules(now):
            if not await self._claim_schedule(row, now):
                continue
            try:
                await _runs.insert_run(
                    self.runtime.store,
                    workflow_name=row.workflow_name,
                    workflow_version=row.workflow_version,
                    input={},
                    metadata={},
                    triggered_by="schedule",
                )
            except Exception:
                logger.exception("scheduled run submit failed for %s v%d", row.workflow_name, row.workflow_version)

    async def _has_schedule_rows(self) -> bool:
        async with self.runtime.store.session() as session:
            return (await session.execute(select(ScheduledRun.workflow_name).limit(1))).first() is not None

    async def _ensure_schedule_rows(self, definitions: list[WorkflowDefinition], now: datetime) -> None:
        """Give every registered scheduled workflow a row, once."""
        if not definitions:
            return
        async with self.runtime.store.session() as session:
            for defn in definitions:
                cron = defn.schedule
                if cron is None:
                    # `registry.scheduled()` filters on `schedule is not None`,
                    # so this is a registry bug, not a data case. Fail loudly
                    # rather than skip the schedule in silence.
                    raise FlowError(f"scheduled workflow {defn.name!r} carries no cron expression")
                await session.execute(
                    pg_insert(Workflow)
                    .values(name=defn.name, version=defn.version)
                    .on_conflict_do_nothing(index_elements=[Workflow.name, Workflow.version])
                )
                await session.execute(
                    pg_insert(ScheduledRun)
                    .values(
                        workflow_name=defn.name,
                        workflow_version=defn.version,
                        cron=cron,
                        enabled=True,
                        next_fire_at=croniter(cron, now).get_next(datetime),
                    )
                    # The cron is updated on conflict so editing a schedule in
                    # code takes effect; `next_fire_at` is left alone, so the
                    # new expression applies from the next fire rather than
                    # retroactively moving a deadline that may already be due.
                    .on_conflict_do_update(
                        index_elements=[ScheduledRun.workflow_name, ScheduledRun.workflow_version],
                        set_={"cron": cron},
                    )
                )
            await session.commit()

    async def _due_schedules(self, now: datetime) -> list[ScheduledRun]:
        async with self.runtime.store.session() as session:
            rows = (
                await session.execute(
                    select(ScheduledRun).where(
                        ScheduledRun.enabled.is_(True),
                        ScheduledRun.next_fire_at <= now,
                    )
                )
            ).scalars()
            return list(rows)

    async def _claim_schedule(self, row: ScheduledRun, now: datetime) -> bool:
        """Advance one schedule's ``next_fire_at``, and report whether we won.

        A compare-and-swap: the WHERE clause names the ``next_fire_at`` this
        worker saw, so exactly one of two workers racing at leader handover
        matches a row and the other updates nothing. The previous version
        SELECTed the due rows, mutated the ORM objects and committed — its
        docstring claimed `UPDATE … RETURNING` atomicity it did not have, and
        two leaders overlapping for one tick fired every cron twice.

        The next fire is computed from ``now``, not from the missed deadline, so
        a leader that fell behind does not then work through a backlog of every
        occurrence it slept past.
        """
        statement = (
            update(ScheduledRun)
            .where(
                ScheduledRun.workflow_name == row.workflow_name,
                ScheduledRun.workflow_version == row.workflow_version,
                ScheduledRun.next_fire_at == row.next_fire_at,
                ScheduledRun.enabled.is_(True),
            )
            .values(next_fire_at=croniter(row.cron, now).get_next(datetime), last_fired_at=now)
            .returning(ScheduledRun.workflow_name)
            .execution_options(synchronize_session=False)
        )
        async with self.runtime.store.session() as session:
            won = (await session.execute(statement)).first() is not None
            await session.commit()
        return won

    async def _revive_runs_with_pending_steps(self) -> None:
        """Flip back to ``pending`` any run that is ``running`` but holds a step
        ready to go.

        That state is normal after a reclaim or a retry: the step went back to
        ``pending`` while the run row still said ``running`` from the attempt
        that just ended. Without this sweep the run is never re-claimed and sits
        there forever.
        """
        ready = (
            select(StepRun.run_id)
            .where(
                StepRun.status == "pending",
                sa.or_(
                    StepRun.next_attempt_at.is_(None),
                    StepRun.next_attempt_at <= sa.func.now(),
                ),
            )
            .scalar_subquery()
        )
        async with self.runtime.store.session() as session:
            await session.execute(
                update(Run).where(Run.status == "running", Run.id.in_(ready)).values(status="pending")
            )
            await session.commit()

    async def _drain_tasks(self, timeout: float = 30.0) -> None:
        """Wait for in-flight runs to finish, with a bound."""
        if not self._tasks:
            return
        try:
            await asyncio.wait_for(asyncio.gather(*self._tasks, return_exceptions=True), timeout=timeout)
        except TimeoutError:
            logger.warning("forktex_flow.driver: %d tasks still in flight at shutdown", len(self._tasks))
