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

"""SQLAlchemy ORM mappings for the ``forktex_flow.*`` tables.

``migrations/v0001__schema.sql`` owns the DDL; these mappings exist so the read
and write paths are typed and testable. The two MUST stay in sync — the
migration is what a deployed database actually has.

Built on :func:`forktex.database.substrate_base`, which gives flow its own
``MetaData`` while inheriting ``BaseDBModel``'s conventions. The separate
registry is load-bearing: ``BaseDBModel.metadata`` belongs to the consumer,
whose ``create_all`` must not try to build this library's tables in schemas it
never asked for.

``forktex_flow`` is a **physical Postgres schema name in deployed consumer
databases**, and it is also this distribution's import name. It is not a stale
``forktex_core`` reference and renaming it orphans data. SQLAlchemy's
``schema_translate_map`` — configured on the ``Database`` handle in
:class:`forktex_flow.flow.Flow` — rewrites it per instance at runtime, which is
how tests get an isolated schema without forking these definitions.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

import sqlalchemy as sa
from forktex.database import substrate_base
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

_SCHEMA = "forktex_flow"

#: The statuses a run or a step may hold. Declared once and rendered into a
#: CHECK constraint on both tables, so the database — not a caller — is what
#: guarantees the column never holds a value the executor cannot interpret.
_STATUSES: tuple[str, ...] = ("pending", "running", "completed", "failed", "cancelled")
_STATUS_CHECK = "status IN (" + ", ".join(f"'{s}'" for s in _STATUSES) + ")"

#: Flow's own declarative registry — every table defaults to ``_SCHEMA``.
_FlowBase = substrate_base(_SCHEMA)


class Workflow(_FlowBase):
    __tablename__ = "workflow"

    name: Mapped[str] = mapped_column(sa.String(255), primary_key=True)
    version: Mapped[int] = mapped_column(sa.Integer, primary_key=True)
    ast_hash: Mapped[str | None] = mapped_column(sa.String(64), nullable=True)
    registered_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True),
        server_default=sa.func.now(),
        nullable=False,
    )


class Run(_FlowBase):
    __tablename__ = "run"

    id: Mapped[uuid.UUID] = mapped_column(sa.UUID(as_uuid=True), primary_key=True, default=uuid.uuid7)
    workflow_name: Mapped[str] = mapped_column(sa.String(255), nullable=False, index=True)
    workflow_version: Mapped[int] = mapped_column(sa.Integer, nullable=False)
    status: Mapped[str] = mapped_column(sa.String(16), nullable=False, index=True)
    input: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, server_default=sa.text("'{}'::jsonb"))
    output: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    error: Mapped[str | None] = mapped_column(sa.Text, nullable=True)
    metadata_: Mapped[dict[str, Any]] = mapped_column(
        "metadata",
        JSONB,
        nullable=False,
        server_default=sa.text("'{}'::jsonb"),
    )
    triggered_by: Mapped[str] = mapped_column(sa.String(32), nullable=False, server_default=sa.text("'manual'"))
    # NOT NULL and indexed because it is half of the keyset sort key
    # `(started_at, id)`. A row comparison against a NULL evaluates to NULL and
    # silently drops the row from the page, so a nullable sort key is a
    # correctness bug, not a style choice.
    started_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
        index=True,
    )
    finished_at: Mapped[datetime | None] = mapped_column(sa.TIMESTAMP(timezone=True), nullable=True)
    cancelled_at: Mapped[datetime | None] = mapped_column(sa.TIMESTAMP(timezone=True), nullable=True)
    cancel_reason: Mapped[str | None] = mapped_column(sa.Text, nullable=True)

    steps: Mapped[list[StepRun]] = relationship(
        "StepRun",
        back_populates="run",
        cascade="all, delete-orphan",
        lazy="selectin",
        order_by="StepRun.step_index, StepRun.started_at",
    )

    __table_args__ = (sa.CheckConstraint(_STATUS_CHECK, name="ck_run_status"),)


class StepRun(_FlowBase):
    __tablename__ = "step_run"

    id: Mapped[uuid.UUID] = mapped_column(sa.UUID(as_uuid=True), primary_key=True, default=uuid.uuid7)
    run_id: Mapped[uuid.UUID] = mapped_column(
        sa.UUID(as_uuid=True),
        sa.ForeignKey(f"{_SCHEMA}.run.id", ondelete="CASCADE"),
        nullable=False,
    )
    step_name: Mapped[str] = mapped_column(sa.String(255), nullable=False)
    step_qualname: Mapped[str] = mapped_column(sa.String(512), nullable=False)
    step_index: Mapped[int] = mapped_column(sa.Integer, nullable=False)
    args_hash: Mapped[str] = mapped_column(sa.String(64), nullable=False)
    status: Mapped[str] = mapped_column(sa.String(16), nullable=False)
    output: Mapped[Any | None] = mapped_column(JSONB, nullable=True)
    error: Mapped[str | None] = mapped_column(sa.Text, nullable=True)
    attempts: Mapped[int] = mapped_column(sa.Integer, nullable=False, server_default=sa.text("0"))
    max_attempts: Mapped[int] = mapped_column(sa.Integer, nullable=False)
    next_attempt_at: Mapped[datetime | None] = mapped_column(sa.TIMESTAMP(timezone=True), nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(sa.TIMESTAMP(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(sa.TIMESTAMP(timezone=True), nullable=True)
    heartbeat_at: Mapped[datetime | None] = mapped_column(sa.TIMESTAMP(timezone=True), nullable=True)

    run: Mapped[Run] = relationship("Run", back_populates="steps")

    __table_args__ = (
        # The memoisation key. A completed row here is what makes a re-run skip
        # a step instead of repeating its side effects, so the uniqueness has to
        # hold under concurrency — which only a database constraint does.
        sa.UniqueConstraint("run_id", "step_qualname", "args_hash", name="uq_step_run_identity"),
        sa.CheckConstraint(_STATUS_CHECK, name="ck_step_run_status"),
        sa.CheckConstraint("attempts >= 0", name="ck_step_run_attempts_non_negative"),
        sa.CheckConstraint("max_attempts >= 1", name="ck_step_run_max_attempts_positive"),
    )


class RunEvent(_FlowBase):
    """Append-only progress log for a run.

    **External dependence:** ForkTex Cloud's SSE endpoint tails this table by
    raw SQL — ``SELECT id, ts, event_type, payload FROM forktex_flow.run_event
    WHERE run_id = :rid AND id > :last_id ORDER BY id ASC``. The table name,
    those four column names and the monotonic ``id`` are a cross-repo contract;
    changing any of them breaks a consumer that does not import this package.
    """

    __tablename__ = "run_event"

    id: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=True)
    run_id: Mapped[uuid.UUID] = mapped_column(
        sa.UUID(as_uuid=True),
        sa.ForeignKey(f"{_SCHEMA}.run.id", ondelete="CASCADE"),
        nullable=False,
    )
    ts: Mapped[datetime] = mapped_column(sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False)
    event_type: Mapped[str] = mapped_column(sa.String(64), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, server_default=sa.text("'{}'::jsonb"))


class ScheduledRun(_FlowBase):
    __tablename__ = "scheduled_run"

    workflow_name: Mapped[str] = mapped_column(sa.String(255), primary_key=True)
    workflow_version: Mapped[int] = mapped_column(sa.Integer, primary_key=True)
    cron: Mapped[str] = mapped_column(sa.String(128), nullable=False)
    enabled: Mapped[bool] = mapped_column(sa.Boolean, nullable=False, server_default=sa.true())
    last_fired_at: Mapped[datetime | None] = mapped_column(sa.TIMESTAMP(timezone=True), nullable=True)
    next_fire_at: Mapped[datetime] = mapped_column(sa.TIMESTAMP(timezone=True), nullable=False, index=True)

    __table_args__ = (
        sa.ForeignKeyConstraint(
            ["workflow_name", "workflow_version"],
            [f"{_SCHEMA}.workflow.name", f"{_SCHEMA}.workflow.version"],
            ondelete="CASCADE",
        ),
    )


__all__ = [
    "Run",
    "RunEvent",
    "ScheduledRun",
    "StepRun",
    "Workflow",
]
