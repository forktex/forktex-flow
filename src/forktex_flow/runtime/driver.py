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

"""Driver loop — the leader-elected dispatcher. Scaling past that needs a sharded lock, which this does not
implement.
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

from forktex_flow.domain.types import RunClaim
from forktex_flow.errors import FlowError
from forktex_flow.persist import runs as _runs
from forktex_flow.persist import steps as _steps
from forktex_flow.persist.models import Run, ScheduledRun, StepRun, Workflow
from forktex_flow.persist.store import rowcount
from forktex_flow.runtime.execution import execute_run

if TYPE_CHECKING:
    from forktex_flow.domain.definition import WorkflowDefinition
    from forktex_flow.runtime.context import FlowRuntime

__all__ = ["Driver"]

logger = get_logger(__name__)

#: Runs claimed per tick. Small on purpose: a claim marks rows `running`, so a
#: large batch that the worker then dies holding takes a stale-reclaim cycle to
#: come back.


class Driver:
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
        #: Cleared whenever leadership is lost, so each new leader seeds once.
        self._schedules_seeded = False

    async def run(self) -> None:
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
                    if not await self._still_leader():
                        logger.warning("forktex_flow.driver: leader lock was released; standing down")
                        return
                    await self._tick()
                    with contextlib.suppress(TimeoutError):
                        await asyncio.wait_for(self.shutdown.wait(), timeout=self.runtime.config.poll_interval)
            finally:
                self.is_leader = False
                self._schedules_seeded = False

    async def _still_leader(self) -> bool:
        """Whether this worker's advisory lock is still held. An advisory lock is session-scoped, so a second non-
        blocking attempt must fail while we hold it. Succeeding means Postgres released ours — the backend was
        terminated, restarted or recycled by a pooler — and the lock's own context manager cannot tell us,
        reporting only whether the acquire succeeded. A failed acquire only proves *somebody* holds the lock, so
        a worker that never led must answer for itself rather than read the probe.
        """
        if not self.is_leader:
            return False
        async with try_advisory_lock(self.runtime.store.engine, self.runtime.config.leader_lock_key) as acquired:
            return not acquired

    async def _tick(self) -> None:
        """One pass. Each phase is isolated: a failure in one must not stop the others, which recover independent
        kinds of stuck work.
        """
        phases = (
            self._fire_due_schedules,
            self._reclaim_stale,
            self._revive_abandoned_runs,
            self._claim_and_dispatch,
        )
        for phase in phases:
            try:
                await phase()
            except Exception:
                logger.exception("driver phase %s failed", phase.__name__)

    async def _claim_and_dispatch(self) -> None:
        headroom = self.runtime.config.max_concurrent_runs - len(self._in_flight)
        if headroom <= 0:
            return
        for claim in await _runs.claim_pending_runs(self.runtime.store, limit=headroom):
            self._dispatch(claim)

    def _dispatch(self, claim: RunClaim) -> None:
        run_id = claim.run_id
        if run_id in self._in_flight:
            logger.warning(
                "forktex_flow.driver: run %s was reclaimed while still in flight; skipping duplicate dispatch",
                run_id,
            )
            return
        task = asyncio.create_task(
            execute_run(self.runtime, run_id, claim.workflow_name, claim.workflow_version),
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
        definitions = self.runtime.registry.scheduled()
        if not definitions and not await self._has_schedule_rows():
            # Most consumers register no scheduled workflows. Without this the
            # driver pays two queries a tick to learn that every tick.
            return

        now = iso.now()
        if not self._schedules_seeded:
            await self._ensure_schedule_rows(definitions, now)
            self._schedules_seeded = True

        for row in await self._due_schedules(now):
            try:
                await self._fire_schedule(row, now)
            except Exception:
                logger.exception("scheduled run submit failed for %s v%d", row.workflow_name, row.workflow_version)

    async def _has_schedule_rows(self) -> bool:
        async with self.runtime.store.transaction() as session:
            return (await session.execute(select(ScheduledRun.workflow_name).limit(1))).first() is not None

    async def _ensure_schedule_rows(self, definitions: list[WorkflowDefinition], now: datetime) -> None:
        """Give every registered scheduled workflow a row. Two upserts per schedule, so this runs once per
        leadership term rather than once per tick: the registry is fixed before the driver starts, and every
        call after the first is a no-op the database still has to parse and lock.
        """
        if not definitions:
            return
        async with self.runtime.store.transaction() as session:
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
                        enabled=defn.enabled,
                        next_fire_at=croniter(cron, now).get_next(datetime),
                    )
                    # The cron is updated on conflict so editing a schedule in
                    # code takes effect; `next_fire_at` is left alone, so the
                    # new expression applies from the next fire rather than
                    # retroactively moving a deadline that may already be due.
                    # `enabled` is absent by design: it is set from the
                    # definition on the first insert and owned by the database
                    # afterwards, so an operator arming a schedule keeps it
                    # armed across every subsequent deploy.
                    .on_conflict_do_update(
                        index_elements=[ScheduledRun.workflow_name, ScheduledRun.workflow_version],
                        set_={"cron": cron},
                    )
                )

    async def _due_schedules(self, now: datetime) -> list[ScheduledRun]:
        async with self.runtime.store.transaction() as session:
            rows = (
                await session.execute(
                    select(ScheduledRun).where(
                        ScheduledRun.enabled.is_(True),
                        ScheduledRun.next_fire_at <= now,
                    )
                )
            ).scalars()
            return list(rows)

    async def _fire_schedule(self, row: ScheduledRun, now: datetime) -> bool:
        """Advance one schedule and insert its run in one transaction, reporting whether this worker won the
        occurrence. Separately, a failed insert consumes the occurrence: the schedule has moved on and
        ``last_fired_at`` claims a run that does not exist. The next fire is computed from ``now``, not from the
        missed deadline, so a leader that fell behind does not work through a backlog of every occurrence it
        slept past.
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
        async with self.runtime.store.transaction() as session:
            if (await session.execute(statement)).first() is None:
                return False
            await _runs.insert_run_in(
                session,
                self.runtime.store.schema,
                workflow_name=row.workflow_name,
                workflow_version=row.workflow_version,
                input={},
                metadata={},
                triggered_by="schedule",
            )
        return True

    async def _revive_abandoned_runs(self) -> None:
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
        holds_a_live_step = select(StepRun.id).where(StepRun.run_id == Run.id, StepRun.status == "running").exists()
        cutoff = sa.func.now() - sa.cast(
            sa.func.make_interval(0, 0, 0, 0, 0, 0, self.runtime.config.stale_threshold),
            sa.Interval,
        )
        async with self.runtime.store.transaction() as session:
            result = await session.execute(
                update(Run)
                .where(
                    Run.status == "running",
                    sa.or_(
                        Run.id.in_(ready),
                        sa.and_(
                            ~holds_a_live_step,
                            sa.or_(Run.heartbeat_at.is_(None), Run.heartbeat_at < cutoff),
                        ),
                    ),
                )
                .values(status="pending")
            )
            revived = rowcount(result)
        if revived:
            logger.info("forktex_flow.driver: revived %d abandoned run(s)", revived)

    async def _drain_tasks(self, timeout: float = 30.0) -> None:
        if not self._tasks:
            return
        try:
            await asyncio.wait_for(asyncio.gather(*self._tasks, return_exceptions=True), timeout=timeout)
        except TimeoutError:
            # `wait_for` cancels the gather it was waiting on, and cancelling a
            # gather cancels its children — so these runs were interrupted
            # mid-step. Their rows stay `running` until a heartbeat goes stale
            # and the next leader reclaims them.
            cancelled = [task for task in self._tasks if task.cancelled()]
            logger.warning(
                "forktex_flow.driver: %d of %d runs were still in flight after %.0fs and were "
                "cancelled mid-step; they will be reclaimed once their heartbeat goes stale",
                len(cancelled),
                len(self._tasks),
                timeout,
            )
