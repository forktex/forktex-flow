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
from datetime import datetime, timedelta
from typing import TYPE_CHECKING
from uuid import UUID

from forktex.database import Page
from forktex.error import BadRequestError, NotFoundError
from forktex.types import BaseWireValueObject, JsonObject, JsonValue
from pydantic import PrivateAttr

from forktex_flow.domain.filter import RunFilter
from forktex_flow.domain.types import TERMINAL_STATUSES, RunInfo, RunSummary, StepRunInfo
from forktex_flow.persist import runs as _runs
from forktex_flow.read import query as _query

if TYPE_CHECKING:
    from forktex_flow.persist.store import FlowStore

__all__ = [
    "InstancePage",
    "InstanceQuery",
    "NodeInstance",
    "WorkflowInstance",
    "bind_instance",
]

#: How often a wait/stream poll re-reads the run. `persist.runs` emits
#: ``pg_notify('<schema>_run', run_id)`` for an external listener; `wait` and
#: `stream` poll rather than LISTEN, so a run is seen within this interval.
_POLL_SECONDS = 0.5


class NodeInstance(BaseWireValueObject):
    name: str
    status: str
    """One of ``pending``, ``running``, ``completed``, ``failed``, ``cancelled``."""

    attempt: int
    """Current (or final) attempt number."""

    started_at: datetime | None
    finished_at: datetime | None

    duration: timedelta | None
    """``finished_at - started_at`` when both are set; else ``None``."""

    state_delta: JsonObject | None
    """The partial state update this step returned, or ``None`` if it has not
    completed."""

    error: str | None


class WorkflowInstance(BaseWireValueObject):
    instance_id: UUID
    workflow_name: str
    workflow_version: int
    status: str
    state: JsonObject
    """The accumulated state — ``run.output`` once a step has written one, else
    the original ``run.input``."""
    metadata: JsonObject
    current_node: str | None
    """Name of the step currently executing, or ``None``."""
    started_at: datetime
    finished_at: datetime | None
    nodes: list[NodeInstance]

    _store: FlowStore | None = PrivateAttr(default=None)
    """Set by :meth:`bind_instance`; not serialised. ``None`` on an instance
    built from a wire payload rather than read from the database, which is why
    every live method checks it."""

    def __init__(self, _store: FlowStore | None = None, **data: object) -> None:
        """Accept ``_store`` positionally as well: pydantic's generated ``__init__`` only takes declared public
        fields, and naming it here keeps the private attribute's type through the bridge.
        """
        super().__init__(**data)
        if _store is not None:
            self._store = _store

    def _bound_store(self, action: str) -> FlowStore:
        if self._store is None:
            raise NotFoundError(f"this WorkflowInstance is not bound to a database — cannot {action}")
        return self._store

    async def cancel(self) -> bool:
        """Ask this run to stop, and report whether it was still stoppable. A run that has not been picked up stops
        before its first step; a run mid-flight stops before its *next* step: the library cannot interrupt
        arbitrary consumer code and will not pretend to — the step already running completes, and its work is
        recorded. Returns ``False`` when the run has already finished, changing nothing: a terminal status is
        final, so a late cancel must not overwrite it.
        """
        return await _runs.update_run_status(
            self._bound_store("cancel"),
            self.instance_id,
            status="cancelled",
            cancel_reason="cancelled by operator",
        )

    async def refresh(self) -> WorkflowInstance:
        store = self._bound_store("refresh")
        return bind_instance(await _fetch_or_raise(store, self.instance_id), store)

    async def wait(self, timeout: float | None = None) -> WorkflowInstance:
        """Block until the run is terminal, or ``timeout`` elapses. A timeout is
        not an error: the snapshot comes back either way and ``status`` says
        whether it finished.
        """
        store = self._bound_store("wait")
        deadline = (time.monotonic() + timeout) if timeout is not None else None
        while True:
            info = await _fetch_or_raise(store, self.instance_id)
            if info.status in TERMINAL_STATUSES:
                return bind_instance(info, store)
            if deadline is not None and time.monotonic() >= deadline:
                return bind_instance(info, store)
            await asyncio.sleep(_POLL_SECONDS)

    async def stream(self) -> AsyncIterator[WorkflowInstance]:
        """Yield a fresh snapshot whenever the run or any of its steps moves, ending once the run is terminal. The
        terminal snapshot is always yielded, so a consumer that renders each yield never has to fetch once more
        to learn how the run ended.
        """
        store = self._bound_store("stream")
        previous: tuple[str, tuple[tuple[str, str, int], ...]] | None = None
        while True:
            info = await _fetch_or_raise(store, self.instance_id)
            fingerprint = (
                info.status,
                tuple((s.step_name, s.status, s.attempts) for s in info.steps),
            )
            if fingerprint != previous:
                previous = fingerprint
                yield bind_instance(info, store)
            if info.status in TERMINAL_STATUSES:
                return
            await asyncio.sleep(_POLL_SECONDS)


#: One page of instances — the library-wide :class:`forktex.database.Page`, not a
#: parallel shape.
InstancePage = Page[WorkflowInstance]


class InstanceQuery:
    """Fluent finder for runs. Each filter **mutates this object** and returns it, so a builder cannot be branched:
    ``base = flow.query().workflow("x")`` followed by ``base.status("failed")`` narrows ``base`` itself, and
    there is no second query.
    """

    def __init__(self, store: FlowStore) -> None:
        self._store = store
        self._filter = RunFilter()
        self._limit_value = 50

    def _with(self, **changes: object) -> InstanceQuery:
        self._filter = self._filter.model_copy(update=changes)
        return self

    def workflow(self, name: str, version: int | None = None) -> InstanceQuery:
        return self._with(workflow_name=name, workflow_version=version)

    def status(self, *statuses: str) -> InstanceQuery:
        return self._with(statuses=tuple(statuses))

    def metadata(self, **kv: JsonValue) -> InstanceQuery:
        return self._with(metadata_filter={**self._filter.metadata_filter, **kv})

    def state(self, **kv: JsonValue) -> InstanceQuery:
        return self._with(state_filter={**self._filter.state_filter, **kv})

    def current_node(self, *node_names: str) -> InstanceQuery:
        return self._with(current_nodes=tuple(node_names))

    def since(self, dt: datetime) -> InstanceQuery:
        return self._with(since=dt)

    def until(self, dt: datetime) -> InstanceQuery:
        return self._with(until=dt)

    def triggered_by(self, *triggers: str) -> InstanceQuery:
        return self._with(triggered_by=tuple(triggers))

    def limit(self, n: int) -> InstanceQuery:
        if n <= 0:
            raise BadRequestError(f"limit must be > 0, got {n!r}")
        self._limit_value = n
        return self

    async def fetch(self, cursor: str | None = None) -> InstancePage:
        page = await _query.fetch_page(self._store, self._filter, limit=self._limit_value, cursor=cursor)
        return InstancePage(
            items=[bind_instance(info, self._store) for info in page.items],
            has_more=page.has_more,
            next_cursor=page.next_cursor,
            total=page.total,
        )

    async def count(self) -> int:
        return await _query.count_runs(self._store, self._filter)

    async def first(self) -> WorkflowInstance | None:
        page = await _query.fetch_page(self._store, self._filter, limit=1)
        return bind_instance(page.items[0], self._store) if page.items else None

    async def summary(self) -> RunSummary:
        return await _query.summarise_runs(self._store, self._filter)


def bind_instance(run_info: RunInfo, store: FlowStore) -> WorkflowInstance:
    current_node: str | None = None
    for step in run_info.steps:
        if step.status == "running":
            current_node = step.step_name
            break

    return WorkflowInstance(
        _store=store,
        instance_id=run_info.run_id,
        workflow_name=run_info.workflow_name,
        workflow_version=run_info.workflow_version,
        status=run_info.status,
        state=dict(run_info.output) if run_info.output else dict(run_info.input),
        metadata=run_info.metadata,
        current_node=current_node,
        started_at=run_info.started_at,
        finished_at=run_info.finished_at,
        nodes=[_to_node_instance(step) for step in run_info.steps],
    )


def _to_node_instance(step: StepRunInfo) -> NodeInstance:
    duration: timedelta | None = None
    if step.started_at is not None and step.finished_at is not None:
        duration = step.finished_at - step.started_at

    return NodeInstance(
        name=step.step_name,
        status=step.status,
        attempt=step.attempts,
        started_at=step.started_at,
        finished_at=step.finished_at,
        duration=duration,
        state_delta=step.output if isinstance(step.output, dict) else None,
        error=step.error,
    )


async def _fetch_or_raise(store: FlowStore, run_id: UUID) -> RunInfo:
    info = await _runs.fetch_run(store, run_id)
    if info is None:
        raise NotFoundError(f"run {run_id} not found")
    return info
