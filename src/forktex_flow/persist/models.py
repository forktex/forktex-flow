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

import uuid
from datetime import datetime
from typing import get_args

import sqlalchemy as sa
from forktex.database import substrate_base
from forktex.types import JsonObject
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from forktex_flow.domain.types import ElementStatus, InstanceStatus, TriggeredBy

_SCHEMA = "forktex_flow"
_FlowBase = substrate_base(_SCHEMA)


_IN = "{} IN ({})"
_INSTANCE_STATUS_CHECK = _IN.format("status", ", ".join(f"'{v}'" for v in get_args(InstanceStatus)))
_ELEMENT_STATUS_CHECK = _IN.format("status", ", ".join(f"'{v}'" for v in get_args(ElementStatus)))
_TRIGGERED_BY_CHECK = _IN.format("triggered_by", ", ".join(f"'{v}'" for v in get_args(TriggeredBy)))


_TS = sa.TIMESTAMP(timezone=True)


class Definition(_FlowBase):
    __tablename__ = "definition"

    id: Mapped[uuid.UUID] = mapped_column(sa.UUID(as_uuid=True), primary_key=True, default=uuid.uuid7)
    namespace: Mapped[str] = mapped_column(sa.String(255), nullable=False)
    key: Mapped[str] = mapped_column(sa.String(255), nullable=False)
    version: Mapped[int] = mapped_column(sa.Integer, nullable=False)
    checksum: Mapped[str] = mapped_column(sa.String(64), nullable=False)
    model: Mapped[JsonObject] = mapped_column(JSONB, nullable=False)
    source: Mapped[str] = mapped_column(sa.Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(_TS, nullable=False, server_default=sa.func.now())

    __table_args__ = (
        sa.CheckConstraint("version >= 1", name="ck_definition_version_positive"),
        sa.UniqueConstraint("namespace", "key", "version", name="uq_definition_version"),
        sa.UniqueConstraint("namespace", "key", "checksum", name="uq_definition_checksum"),
    )


class Instance(_FlowBase):
    __tablename__ = "instance"

    id: Mapped[uuid.UUID] = mapped_column(sa.UUID(as_uuid=True), primary_key=True, default=uuid.uuid7)
    namespace: Mapped[str] = mapped_column(sa.String(255), nullable=False)
    key: Mapped[str] = mapped_column(sa.String(255), nullable=False)
    version: Mapped[int] = mapped_column(sa.Integer, nullable=False)
    definition_id: Mapped[uuid.UUID] = mapped_column(
        sa.UUID(as_uuid=True), sa.ForeignKey(f"{_SCHEMA}.definition.id"), nullable=False
    )
    status: Mapped[str] = mapped_column(sa.String(16), nullable=False)
    input: Mapped[JsonObject] = mapped_column(JSONB, nullable=False, server_default=sa.text("'{}'::jsonb"))
    variables: Mapped[JsonObject] = mapped_column(JSONB, nullable=False, server_default=sa.text("'{}'::jsonb"))
    metadata_: Mapped[JsonObject] = mapped_column(
        "metadata", JSONB, nullable=False, server_default=sa.text("'{}'::jsonb")
    )
    error: Mapped[str | None] = mapped_column(sa.Text, nullable=True)
    cancel_reason: Mapped[str | None] = mapped_column(sa.Text, nullable=True)
    triggered_by: Mapped[str] = mapped_column(sa.String(16), nullable=False, server_default=sa.text("'manual'"))
    parent_instance_id: Mapped[uuid.UUID | None] = mapped_column(
        sa.UUID(as_uuid=True), sa.ForeignKey(f"{_SCHEMA}.instance.id", ondelete="CASCADE"), nullable=True
    )
    parent_element_id: Mapped[str | None] = mapped_column(sa.String(255), nullable=True)
    started_at: Mapped[datetime] = mapped_column(_TS, nullable=False, server_default=sa.func.now())
    finished_at: Mapped[datetime | None] = mapped_column(_TS, nullable=True)

    elements: Mapped[list[ElementRun]] = relationship(
        "ElementRun",
        back_populates="instance",
        cascade="all, delete-orphan",
        lazy="selectin",
        foreign_keys="ElementRun.instance_id",
        order_by="ElementRun.started_at, ElementRun.id",
    )

    __table_args__ = (
        sa.CheckConstraint(_INSTANCE_STATUS_CHECK, name="ck_instance_status"),
        sa.CheckConstraint(_TRIGGERED_BY_CHECK, name="ck_instance_triggered_by"),
        sa.CheckConstraint("(parent_instance_id IS NULL) = (parent_element_id IS NULL)", name="ck_instance_parent"),
        sa.Index("ix_instance_namespace_key", "namespace", "key"),
        sa.Index("ix_instance_status", "status"),
        sa.Index("ix_instance_parent", "parent_instance_id"),
        sa.Index("ix_instance_metadata_gin", "metadata", postgresql_using="gin"),
        sa.Index(
            "ix_instance_variables_gin",
            "variables",
            postgresql_using="gin",
            postgresql_ops={"variables": "jsonb_path_ops"},
        ),
        sa.Index("ix_instance_started_at_id", sa.desc("started_at"), sa.desc("id")),
    )


class ElementRun(_FlowBase):
    __tablename__ = "element_run"

    id: Mapped[uuid.UUID] = mapped_column(sa.UUID(as_uuid=True), primary_key=True, default=uuid.uuid7)
    instance_id: Mapped[uuid.UUID] = mapped_column(
        sa.UUID(as_uuid=True), sa.ForeignKey(f"{_SCHEMA}.instance.id", ondelete="CASCADE"), nullable=False
    )
    element_id: Mapped[str] = mapped_column(sa.String(255), nullable=False)
    element_type: Mapped[str] = mapped_column(sa.String(32), nullable=False)
    task_type: Mapped[str | None] = mapped_column(sa.String(255), nullable=True)
    status: Mapped[str] = mapped_column(sa.String(16), nullable=False)
    output: Mapped[JsonObject | None] = mapped_column(JSONB, nullable=True)
    error: Mapped[str | None] = mapped_column(sa.Text, nullable=True)
    attempts: Mapped[int] = mapped_column(sa.Integer, nullable=False, server_default=sa.text("0"))
    max_attempts: Mapped[int] = mapped_column(sa.Integer, nullable=False, server_default=sa.text("1"))
    next_attempt_at: Mapped[datetime | None] = mapped_column(_TS, nullable=True)
    started_at: Mapped[datetime] = mapped_column(_TS, nullable=False, server_default=sa.func.now())
    finished_at: Mapped[datetime | None] = mapped_column(_TS, nullable=True)
    heartbeat_at: Mapped[datetime | None] = mapped_column(_TS, nullable=True)
    input: Mapped[JsonObject | None] = mapped_column(JSONB, nullable=True)
    child_instance_id: Mapped[uuid.UUID | None] = mapped_column(
        sa.UUID(as_uuid=True), sa.ForeignKey(f"{_SCHEMA}.instance.id", ondelete="SET NULL"), nullable=True
    )

    instance: Mapped[Instance] = relationship("Instance", back_populates="elements", foreign_keys=[instance_id])

    __table_args__ = (
        sa.CheckConstraint(_ELEMENT_STATUS_CHECK, name="ck_element_run_status"),
        sa.CheckConstraint("attempts >= 0 AND max_attempts >= 1", name="ck_element_run_attempts"),
        sa.UniqueConstraint("instance_id", "element_id", name="uq_element_run_identity"),
        sa.Index(
            "ix_element_run_job",
            "task_type",
            "next_attempt_at",
            postgresql_where=sa.column("status") == "pending",
        ),
        sa.Index("ix_element_run_running", "heartbeat_at", postgresql_where=sa.column("status") == "running"),
    )


class InstanceEvent(_FlowBase):
    __tablename__ = "instance_event"

    id: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=True)
    instance_id: Mapped[uuid.UUID] = mapped_column(
        sa.UUID(as_uuid=True), sa.ForeignKey(f"{_SCHEMA}.instance.id", ondelete="CASCADE"), nullable=False
    )
    ts: Mapped[datetime] = mapped_column(_TS, nullable=False, server_default=sa.func.now())
    event_type: Mapped[str] = mapped_column(sa.String(64), nullable=False)
    payload: Mapped[JsonObject] = mapped_column(JSONB, nullable=False, server_default=sa.text("'{}'::jsonb"))

    __table_args__ = (sa.Index("ix_instance_event_instance_id", "instance_id", "id"),)


class Schedule(_FlowBase):
    __tablename__ = "schedule"

    namespace: Mapped[str] = mapped_column(sa.String(255), primary_key=True)
    key: Mapped[str] = mapped_column(sa.String(255), primary_key=True)
    cron: Mapped[str] = mapped_column(sa.String(128), nullable=False)
    enabled: Mapped[bool] = mapped_column(sa.Boolean, nullable=False, server_default=sa.true())
    last_fired_at: Mapped[datetime | None] = mapped_column(_TS, nullable=True)
    next_fire_at: Mapped[datetime] = mapped_column(_TS, nullable=False)

    __table_args__ = (sa.Index("ix_schedule_due", "next_fire_at", postgresql_where=sa.column("enabled")),)


__all__ = ["Definition", "ElementRun", "Instance", "InstanceEvent", "Schedule"]
