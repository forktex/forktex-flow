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

"""Reads and writes on ``forktex_flow.step_run`` — the durable step-attempt table.

A step row is the unit replay keys off: `upsert_pending_step` is the idempotence boundary
(same run + qualname + args hash returns the cached outcome), and the `mark_step_*`
transitions record what happened. Stale-attempt reclaim compares against the **database**
clock, never the worker's, so a skewed worker cannot reclaim live work.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any
from uuid import UUID

import sqlalchemy as sa
from forktex.log import get_logger
from forktex.types import JsonValue
from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from forktex_flow.errors import StepClaimLost
from forktex_flow.persist.models import StepRun

if TYPE_CHECKING:
    from forktex_flow.persist.store import FlowStore

logger = get_logger(__name__)

#: How much of a step's error text is kept. A traceback is worth storing; an
#: unbounded one is a row an operator cannot read and a page that will not load.
#: Applied to every write of `error`, including the reclaim note, so a step
#: reclaimed in a loop cannot grow the column without limit.
_MAX_ERROR_CHARS = 5000

__all__ = [
    "SKIPPED_OUTPUT",
    "heartbeat_step",
    "mark_step_completed",
    "mark_step_failed",
    "mark_step_running",
    "mark_step_skipped",
    "read_step_attempts",
    "reclaim_stale_steps",
    "upsert_pending_step",
]

#: What a skipped step records as its output. The status stays ``completed``:
#: the CHECK constraint allows five statuses and "skipped" is not one of them,
#: and more importantly a skipped step *must* leave a completed row, or a resume
#: re-evaluates its guard against a state that has since moved on.
SKIPPED_OUTPUT: dict[str, bool] = {"__skipped__": True}


def _rowcount(result: object) -> int:
    """How many rows an UPDATE touched.

    Every backend this library targets supports `rowcount` on an UPDATE, but
    pyright's stub does not expose it on the base `Result` type — so the access
    needs a typed fallback, and it needed one in two places with the same
    comment beside each.
    """
    return getattr(result, "rowcount", 0) or 0


async def upsert_pending_step(
    store: FlowStore,
    *,
    run_id: UUID,
    step_name: str,
    step_qualname: str,
    step_index: int,
    args_hash: str,
    max_attempts: int,
) -> tuple[UUID, str, JsonValue]:
    """Get or create the row for one step invocation, in a single statement.

    Returns ``(step_id, status, output)``. A ``completed`` status is the cache
    hit that lets a resumed run skip the body and reuse the recorded output.

    ``ON CONFLICT DO UPDATE … RETURNING`` rather than select-then-insert. The
    previous shape was a SELECT, an INSERT, and — when two workers raced — an
    `IntegrityError`, a rollback and a second SELECT: three round trips and a
    window between the read and the write that concurrency tests rarely
    reproduce and production reliably hits. The conflict target is
    ``uq_step_run_identity``, so the losing writer is handed the winner's row
    instead of an exception to recover from.

    The `DO UPDATE` is a deliberate no-op write of ``step_name`` to itself:
    `ON CONFLICT DO NOTHING` returns no row at all on a conflict, which would
    put the second SELECT straight back.
    """
    statement = (
        pg_insert(StepRun)
        .values(
            run_id=run_id,
            step_name=step_name,
            step_qualname=step_qualname,
            step_index=step_index,
            args_hash=args_hash,
            status="pending",
            max_attempts=max_attempts,
            attempts=0,
        )
        .on_conflict_do_update(
            constraint="uq_step_run_identity",
            set_={"step_name": StepRun.step_name},
        )
        .returning(StepRun.id, StepRun.status, StepRun.output)
    )
    async with store.session() as session:
        row = (await session.execute(statement)).one()
        await session.commit()
        return row.id, row.status, row.output


async def mark_step_running(store: FlowStore, step_id: UUID) -> None:
    """Claim a pending step by flipping it to ``running`` — a compare-and-swap,
    not a blind write. ``WHERE status='pending'`` is the guard: without it, a
    step reclaimed by :func:`reclaim_stale_steps` while its original executor
    is still (merely slowly) running would let both executors believe they
    hold it, and a non-idempotent step body would run twice concurrently.

    Raises :class:`StepClaimLost` when the row was not ``pending`` — this
    executor lost the race and must abort without touching run/step state.
    """
    async with store.session() as session:
        result = await session.execute(
            update(StepRun)
            .where(StepRun.id == step_id, StepRun.status == "pending")
            .values(
                status="running",
                attempts=StepRun.attempts + 1,
                started_at=sa.func.coalesce(StepRun.started_at, sa.func.now()),
                heartbeat_at=sa.func.now(),
            )
        )
        await session.commit()
        if _rowcount(result) == 0:
            raise StepClaimLost(f"step {step_id} is no longer pending; claim lost to a concurrent executor")


async def heartbeat_step(store: FlowStore, step_id: UUID) -> None:
    async with store.session() as session:
        await session.execute(update(StepRun).where(StepRun.id == step_id).values(heartbeat_at=sa.func.now()))
        await session.commit()


async def mark_step_completed(store: FlowStore, step_id: UUID, output: JsonValue) -> None:
    async with store.session() as session:
        await session.execute(
            update(StepRun)
            .where(StepRun.id == step_id)
            .values(
                status="completed",
                output=output,
                finished_at=sa.func.now(),
            )
        )
        await session.commit()


async def mark_step_failed(
    store: FlowStore,
    step_id: UUID,
    error: str,
    *,
    final: bool,
    retry_in_seconds: float | None = None,
) -> datetime | None:
    """Mark a step failed; when not ``final``, schedule its retry.

    ``retry_in_seconds`` is resolved against the **database** clock
    (``now() + interval``) rather than the app's, because the driver selects
    due retries with ``next_attempt_at <= sa.func.now()`` — also the DB clock.
    Computing the deadline app-side put two clocks on either side of that
    comparison, so retries fired early or late by whatever the app↔Postgres
    skew happened to be.

    Returns the stored ``next_attempt_at`` (as the database computed it) so
    callers can report the real value instead of an app-side estimate.
    """
    new_status = "failed" if final else "pending"
    values: dict[str, Any] = {
        "status": new_status,
        "error": error[:_MAX_ERROR_CHARS],
        "next_attempt_at": (None if retry_in_seconds is None else sa.func.now() + timedelta(seconds=retry_in_seconds)),
    }
    if final:
        values["finished_at"] = sa.func.now()
    async with store.session() as session:
        result = await session.execute(
            update(StepRun).where(StepRun.id == step_id).values(**values).returning(StepRun.next_attempt_at)
        )
        stored = result.scalar_one_or_none()
        await session.commit()
        return stored


async def mark_step_skipped(store: FlowStore, step_id: UUID) -> None:
    """Record that a step's ``when_fn`` guard declined it.

    Written as ``completed`` with :data:`SKIPPED_OUTPUT`, so a resume treats it
    as already-decided rather than re-evaluating the guard.
    """
    async with store.session() as session:
        await session.execute(
            update(StepRun)
            .where(StepRun.id == step_id)
            .values(
                status="completed",
                output=SKIPPED_OUTPUT,
                finished_at=sa.func.now(),
            )
        )
        await session.commit()


async def reclaim_stale_steps(store: FlowStore, stale_threshold: float) -> int:
    """Reset any ``running`` step_run rows whose heartbeat is older
    than ``stale_threshold`` to ``pending``.

    The cutoff is computed on the **database** clock, not the app's.
    ``heartbeat_at`` is written with ``sa.func.now()`` (see
    :func:`mark_step_running` / :func:`heartbeat_step`), so comparing it
    against a Python-side ``iso.now()`` cutoff put two different clocks on
    either side of the comparison: with the app clock even slightly ahead of
    Postgres, a *live* step could be reclaimed to ``pending`` and executed a
    second time concurrently.
    """
    cutoff = sa.func.now() - timedelta(seconds=stale_threshold)
    note = sa.literal("\n[reclaimed: heartbeat stale]")
    async with store.session() as session:
        result = await session.execute(
            update(StepRun)
            .where(
                StepRun.status == "running",
                StepRun.heartbeat_at < cutoff,
            )
            .values(
                status="pending",
                # Left-truncated to the same cap every other write of `error`
                # uses: a step that is reclaimed repeatedly would otherwise
                # append this note forever, and the column is TEXT so nothing
                # would ever complain.
                error=sa.func.left(sa.func.coalesce(StepRun.error, "") + note, _MAX_ERROR_CHARS),
            )
        )
        await session.commit()
        return _rowcount(result)


async def read_step_attempts(store: FlowStore, step_id: UUID) -> int:
    """The step's current ``attempts`` count.

    Read back from the row rather than tracked in memory: the increment happens
    in the database, and a reclaimed step carries attempts from an executor that
    is no longer running.
    """
    async with store.session() as session:
        attempts = (await session.execute(select(StepRun.attempts).where(StepRun.id == step_id))).scalar_one()
        return attempts or 0
