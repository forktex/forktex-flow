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

from forktex_flow.persist import definitions as _definitions
from forktex_flow.persist import jobs as _jobs
from forktex_flow.persist.models import Schedule
from forktex_flow.runtime.engine import Advance, notify
from forktex_flow.runtime.worker import run_job

if TYPE_CHECKING:
    from forktex_flow.domain.types import Job
    from forktex_flow.runtime.context import FlowRuntime

__all__ = ["Driver"]

logger = get_logger(__name__)


class Driver:
    def __init__(self, runtime: FlowRuntime) -> None:
        self.runtime = runtime
        self.shutdown = asyncio.Event()
        self.is_leader = False
        #: element_run id → its task. A job reclaimed while its handler is merely slow must not run twice here.
        self._in_flight: dict[UUID, asyncio.Task[None]] = {}
        self._unclaimed: set[str] = set()

    async def run(self) -> None:
        leader = asyncio.create_task(self._lead(), name="forktex_flow.leader")
        claiming = _Outage("claiming jobs")
        try:
            while not self.shutdown.is_set():
                self.runtime.wake.clear()
                full = False
                try:
                    full = await self._claim_and_dispatch()
                    claiming.ended()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    claiming.failed()
                if not full:
                    await self._pause(self.runtime.config.poll_interval, self.runtime.wake)
        except asyncio.CancelledError:
            pass
        finally:
            self.shutdown.set()
            leader.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await leader
            await self._drain()

    async def _claim_and_dispatch(self) -> bool:
        """Whether it claimed as many jobs as it had room for, so more may be waiting."""
        headroom = self.runtime.config.max_concurrent_jobs - len(self._in_flight)
        jobs = await _jobs.claim(self.runtime.store, self.runtime.registry.task_types(), headroom)
        for job in jobs:
            self._dispatch(job)
        return headroom > 0 and len(jobs) == headroom

    def _dispatch(self, job: Job) -> None:
        if job.element_run_id in self._in_flight:
            logger.warning("forktex_flow.driver: job %s was reclaimed while still in flight here", job.element_run_id)
            return
        task = asyncio.create_task(self._run(job), name=f"forktex_flow.job[{job.element_run_id}]")
        self._in_flight[job.element_run_id] = task
        task.add_done_callback(lambda _task, run_id=job.element_run_id: self._in_flight.pop(run_id, None))

    async def _run(self, job: Job) -> None:
        try:
            await run_job(self.runtime, job)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # The outcome did not commit; the job stays running until reclaim hands it back.
            logger.warning(
                "forktex_flow.driver: job %s could not record its outcome; it will be reclaimed: %s",
                job.element_run_id,
                str(exc).splitlines()[0] if str(exc) else type(exc).__name__,
            )
            logger.debug("forktex_flow.driver: job %s outcome failure", job.element_run_id, exc_info=True)

    async def _lead(self) -> None:
        leading = _Outage("leading")
        while not self.shutdown.is_set():
            try:
                await self._compete()
                leading.ended()
            except asyncio.CancelledError:
                raise
            except Exception:
                leading.failed()
                await self._pause(5.0)

    async def _compete(self) -> None:
        key = self.runtime.config.leader_lock_key
        async with try_advisory_lock(self.runtime.store.engine, key) as acquired:
            if not acquired:
                await self._pause(self.runtime.config.election_interval)
                return
            self.is_leader = True
            logger.info("forktex_flow.driver: this worker now leads schema %s", self.runtime.store.schema)
            phases = (self._fire_due_schedules, self._reclaim_stale, self._report_unclaimed)
            outages = {phase: _Outage(phase.__name__.strip("_").replace("_", " ")) for phase in phases}
            try:
                while not self.shutdown.is_set() and await self._still_leader():
                    for phase in phases:
                        try:
                            await phase()
                            outages[phase].ended()
                        except Exception:
                            outages[phase].failed()
                    await self._pause(self.runtime.config.poll_interval)
            finally:
                self.is_leader = False

    async def _still_leader(self) -> bool:
        """A second non-blocking acquire must fail while this session holds the lock; succeeding means Postgres
        released it — a terminated backend, a recycled pooler connection.
        """
        async with try_advisory_lock(self.runtime.store.engine, self.runtime.config.leader_lock_key) as acquired:
            if acquired:
                logger.warning("forktex_flow.driver: the leader lock was released; standing down")
            return not acquired

    async def _reclaim_stale(self) -> None:
        store, threshold = self.runtime.store, self.runtime.config.stale_threshold
        for element_run_id in await _jobs.exhausted_stale(store, threshold):
            async with store.transaction() as session:
                advance = Advance(self.runtime, session)
                await advance.fail(element_run_id, "the job stopped heartbeating with no attempts left")
            await notify(self.runtime, advance.finished)
        reclaimed = await _jobs.reclaim_stale(store, threshold)
        if reclaimed:
            logger.info("forktex_flow.driver: reclaimed %d stale job(s)", reclaimed)

    async def _report_unclaimed(self) -> None:
        unclaimed = await _jobs.unclaimed(self.runtime.store, self.runtime.config.stale_threshold)
        for task_type, count in unclaimed.items():
            if task_type not in self._unclaimed:
                logger.warning(
                    "forktex_flow.driver: %d job(s) of task type %r are ready and unclaimed; "
                    "no running process may handle it",
                    count,
                    task_type,
                )
        self._unclaimed = set(unclaimed)

    async def _fire_due_schedules(self) -> None:
        now = iso.now()
        async with self.runtime.store.transaction() as session:
            due = list(
                await session.scalars(
                    sa.select(Schedule).where(Schedule.enabled.is_(True), Schedule.next_fire_at <= now)
                )
            )
        for row in due:
            try:
                await self._fire(row, now)
            except Exception:
                logger.exception("forktex_flow.driver: firing schedule %s/%s failed", row.namespace, row.key)

    async def _fire(self, row: Schedule, now: datetime) -> None:
        """Advance the schedule and start its instance in one transaction. The next fire counts from ``now``, so a
        leader that fell behind does not work through every occurrence it slept past.
        """
        won = (
            sa.update(Schedule)
            .where(
                Schedule.namespace == row.namespace,
                Schedule.key == row.key,
                Schedule.next_fire_at == row.next_fire_at,
                Schedule.enabled.is_(True),
            )
            .values(next_fire_at=croniter(row.cron, now).get_next(datetime), last_fired_at=now)
            .returning(Schedule.key)
            .execution_options(synchronize_session=False)
        )
        async with self.runtime.store.transaction() as session:
            if (await session.execute(won)).first() is None:
                return
            definition = await _definitions.find(session, row.namespace, row.key)
            if definition is None:
                logger.warning("forktex_flow.driver: schedule %s/%s has no deployed definition", row.namespace, row.key)
                return
            advance = Advance(self.runtime, session)
            await advance.start(definition, variables={}, metadata={}, triggered_by="schedule")
        await notify(self.runtime, advance.finished)

    async def _pause(self, seconds: float, wake: asyncio.Event | None = None) -> None:
        waits = [asyncio.ensure_future(event.wait()) for event in (self.shutdown, wake) if event is not None]
        try:
            await asyncio.wait(waits, timeout=seconds, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for waiter in waits:
                waiter.cancel()

    async def _drain(self) -> None:
        # A snapshot: each task's done-callback removes it from `_in_flight`.
        in_flight = list(self._in_flight.values())
        if not in_flight:
            return
        timeout = self.runtime.config.drain_timeout
        _done, pending = await asyncio.wait(in_flight, timeout=timeout)
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.wait(pending)
            logger.warning(
                "forktex_flow.driver: %d of %d jobs were still running after %.0fs; they were handed back as pending",
                len(pending),
                len(in_flight),
                timeout,
            )


class _Outage:
    """A stretch of failures logs once with its traceback and once when it ends, not once per poll."""

    def __init__(self, what: str) -> None:
        self.what = what
        self.failing = False

    def failed(self) -> None:
        if self.failing:
            logger.debug("forktex_flow.driver: %s still failing", self.what, exc_info=True)
            return
        self.failing = True
        logger.warning("forktex_flow.driver: %s failed; retrying quietly until it succeeds", self.what, exc_info=True)

    def ended(self) -> None:
        if self.failing:
            self.failing = False
            logger.info("forktex_flow.driver: %s works again", self.what)
