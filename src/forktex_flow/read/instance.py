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
import time
from collections.abc import AsyncIterator
from datetime import datetime
from typing import TYPE_CHECKING
from uuid import UUID

import sqlalchemy as sa
from forktex.database import Page
from forktex.error import BadRequestError, NotFoundError
from forktex.log import get_logger
from forktex.types import JsonValue
from pydantic import PrivateAttr

from forktex_flow.domain.filter import InstanceFilter
from forktex_flow.domain.types import (
    OPEN_ELEMENT_STATUSES,
    TERMINAL_STATUSES,
    ElementRunInfo,
    InstanceEventInfo,
    InstanceInfo,
    InstanceSummary,
)
from forktex_flow.persist import instances as _instances
from forktex_flow.read import query as _query

if TYPE_CHECKING:
    from forktex_flow.persist.store import FlowStore

__all__ = ["InstancePage", "InstanceQuery", "WorkflowInstance", "bind_instance"]

#: How often `wait` and `stream` re-read the instance.
_POLL_SECONDS = 0.5

logger = get_logger(__name__)


class WorkflowInstance(InstanceInfo):
    _store: FlowStore | None = PrivateAttr(default=None)

    @property
    def current_elements(self) -> list[ElementRunInfo]:
        return [element for element in self.elements if element.status in OPEN_ELEMENT_STATUSES]

    def element(self, element_id: str) -> ElementRunInfo | None:
        return next((element for element in self.elements if element.element_id == element_id), None)

    async def cancel(self, reason: str = "cancelled by operator") -> bool:
        async with self._bound("cancel").transaction() as session:
            return await _instances.cancel(session, self.instance_id, reason)

    async def events(self, *, after: int = 0, limit: int = 100) -> list[InstanceEventInfo]:
        """The instance's history, oldest first. Pass the last ``id`` seen as ``after`` to read on from there."""
        if limit < 1:
            raise BadRequestError("limit must be at least 1")
        return await _instances.events(self._bound("read events"), self.instance_id, after=after, limit=limit)

    async def refresh(self) -> WorkflowInstance:
        store = self._bound("refresh")
        return bind_instance(await _fetch_or_raise(store, self.instance_id), store)

    async def wait(self, timeout: float | None = None) -> WorkflowInstance:
        """Return once the instance is terminal or ``timeout`` elapses; a timeout is not an error. An instance
        waiting on a user task stays active until someone completes it.
        """
        store = self._bound("wait")
        deadline = None if timeout is None else time.monotonic() + timeout
        warned = False
        while True:
            info = await _fetch_or_raise(store, self.instance_id)
            if info.status in TERMINAL_STATUSES or (deadline is not None and time.monotonic() >= deadline):
                return bind_instance(info, store)
            if not warned:
                warned = True
                if not await _driver_is_running(store):
                    logger.warning(
                        "forktex_flow: waiting on instance %s, but no driver holds the leader lock for schema %r; "
                        "its jobs run only once a process calls start_driver()",
                        self.instance_id,
                        store.schema,
                    )
            await asyncio.sleep(_POLL_SECONDS)

    async def stream(self) -> AsyncIterator[WorkflowInstance]:
        store = self._bound("stream")
        previous: object = None
        while True:
            info = await _fetch_or_raise(store, self.instance_id)
            fingerprint = (info.status, tuple((e.element_id, e.status, e.attempts) for e in info.elements))
            if fingerprint != previous:
                previous = fingerprint
                yield bind_instance(info, store)
            if info.status in TERMINAL_STATUSES:
                return
            await asyncio.sleep(_POLL_SECONDS)

    def _bound(self, action: str) -> FlowStore:
        if self._store is None:
            raise NotFoundError(f"this WorkflowInstance is not bound to a database — cannot {action}")
        return self._store


InstancePage = Page[WorkflowInstance]


class InstanceQuery:
    def __init__(self, store: FlowStore) -> None:
        self._store = store
        self._filter = InstanceFilter()
        self._limit = 50

    def namespace(self, namespace: str) -> InstanceQuery:
        return self._with(namespace=namespace)

    def key(self, key: str, version: int | None = None) -> InstanceQuery:
        return self._with(key=key, version=version)

    def status(self, *statuses: str) -> InstanceQuery:
        return self._with(statuses=statuses)

    def metadata(self, **kv: JsonValue) -> InstanceQuery:
        return self._with(metadata_filter={**self._filter.metadata_filter, **kv})

    def variables(self, **kv: JsonValue) -> InstanceQuery:
        return self._with(variables_filter={**self._filter.variables_filter, **kv})

    def current_element(self, *element_ids: str) -> InstanceQuery:
        return self._with(current_elements=element_ids)

    def children_of(self, instance_id: UUID) -> InstanceQuery:
        return self._with(parent_instance_id=instance_id)

    def since(self, dt: datetime) -> InstanceQuery:
        return self._with(since=dt)

    def until(self, dt: datetime) -> InstanceQuery:
        return self._with(until=dt)

    def triggered_by(self, *triggers: str) -> InstanceQuery:
        return self._with(triggered_by=triggers)

    def limit(self, n: int) -> InstanceQuery:
        if n <= 0:
            raise BadRequestError(f"limit must be > 0, got {n!r}")
        self._limit = n
        return self

    async def fetch(self, cursor: str | None = None) -> InstancePage:
        page = await _query.fetch_page(self._store, self._filter, limit=self._limit, cursor=cursor)
        return InstancePage(
            items=[bind_instance(info, self._store) for info in page.items],
            has_more=page.has_more,
            next_cursor=page.next_cursor,
            total=page.total,
        )

    async def count(self) -> int:
        return await _query.count_instances(self._store, self._filter)

    async def first(self) -> WorkflowInstance | None:
        page = await _query.fetch_page(self._store, self._filter, limit=1)
        return bind_instance(page.items[0], self._store) if page.items else None

    async def summary(self) -> InstanceSummary:
        return await _query.summarise_instances(self._store, self._filter)

    def _with(self, **changes: object) -> InstanceQuery:
        self._filter = self._filter.model_copy(update=changes)
        return self


def bind_instance(info: InstanceInfo, store: FlowStore) -> WorkflowInstance:
    instance = WorkflowInstance.model_validate(info.model_dump())
    instance._store = store
    return instance


async def _fetch_or_raise(store: FlowStore, instance_id: UUID) -> InstanceInfo:
    info = await _instances.fetch(store, instance_id)
    if info is None:
        raise NotFoundError(f"instance {instance_id} not found")
    return info


_PG_LOCKS = sa.table(
    "pg_locks",
    sa.column("locktype"),
    sa.column("granted"),
    sa.column("classid"),
    sa.column("objid"),
    sa.column("objsubid"),
    sa.column("database"),
)
_PG_DATABASE = sa.table("pg_database", sa.column("oid"), sa.column("datname"))


async def _driver_is_running(store: FlowStore) -> bool:
    if not store.leader_lock_key:
        return True
    # A bigint advisory key is listed in pg_locks as its high and low 32 bits.
    key = store.leader_lock_key & 0xFFFFFFFFFFFFFFFF
    current_database = (
        sa.select(_PG_DATABASE.c.oid).where(_PG_DATABASE.c.datname == sa.func.current_database()).scalar_subquery()
    )
    held = sa.exists().where(
        _PG_LOCKS.c.locktype == "advisory",
        _PG_LOCKS.c.granted.is_(True),
        _PG_LOCKS.c.classid == key >> 32,
        _PG_LOCKS.c.objid == key & 0xFFFFFFFF,
        _PG_LOCKS.c.objsubid == 1,
        _PG_LOCKS.c.database == current_database,
    )
    async with store.engine.connect() as conn:
        return bool(await conn.scalar(sa.select(held)))
