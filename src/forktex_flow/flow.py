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
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from typing import TYPE_CHECKING, Any

from forktex.database import Database, advisory_key
from forktex.error import BadRequestError, NotFoundError
from forktex.log import get_logger
from forktex.types import JsonObject

from forktex_flow.domain.config import RuntimeConfig
from forktex_flow.errors import FlowClosed
from forktex_flow.persist import runs as _runs
from forktex_flow.persist.migrations import apply_migrations
from forktex_flow.persist.store import FlowStore
from forktex_flow.read.instance import InstanceQuery, WorkflowInstance, bind_instance
from forktex_flow.runtime.compiler import compile_pipeline, compile_scheduled
from forktex_flow.runtime.context import FlowRuntime
from forktex_flow.runtime.driver import Driver
from forktex_flow.runtime.registry import FlowRegistry

if TYPE_CHECKING:
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

    from forktex_flow.extension import FlowExtension

__all__ = ["Flow"]

logger = get_logger(__name__)

#: The schema literal the ORM models are declared against. Not configurable —
#: `schema_translate_map` rewrites it per instance at execution time, which is
#: what lets two Flows run against different schemas in one process without
#: forking the model definitions.
_MODEL_SCHEMA = "forktex_flow"

#: ``leader_lock_key`` left at this value means "derive it from the schema".
#: `advisory_key` never returns 0, so a derived key cannot collide with it.
_DERIVE_LEADER_LOCK_KEY = 0


class Flow:
    """Durable workflow runtime. Its ``schema_translate_map`` must remap ``forktex_flow`` to ``schema``."""

    def __init__(
        self,
        database_url: str | None = None,
        *,
        database: Database | None = None,
        schema: str = _MODEL_SCHEMA,
        extensions: list[FlowExtension] | None = None,
        config: RuntimeConfig | None = None,
        echo: bool = False,
        **engine_kwargs: object,
    ) -> None:
        if (database_url is None) == (database is None):
            raise BadRequestError("pass exactly one of database_url or database")

        if database is not None:
            if database.schema_translate_map != {_MODEL_SCHEMA: schema}:
                logger.warning(
                    "the supplied database handle does not remap %s to %r; flow's tables "
                    "will resolve to whatever its map says",
                    _MODEL_SCHEMA,
                    schema,
                    extra={"schema": schema, "map": database.schema_translate_map},
                )
            self._db = database
            self._owns_db = False
        else:
            # `Database` owns connection management. Construction is lazy:
            # nothing connects until something executes.
            self._db = Database(
                database_url,  # type: ignore[arg-type]  # guarded above
                echo=echo,
                schema_translate_map={_MODEL_SCHEMA: schema},
                **engine_kwargs,
            )
            self._owns_db = True

        self.extensions: tuple[FlowExtension, ...] = tuple(extensions or ())
        self._closed = False
        self._driver_lock = asyncio.Lock()
        self._store = FlowStore(database=self._db, schema=schema)
        self._registry = FlowRegistry()
        self._config = _with_lock_key(config or RuntimeConfig(), schema)
        self._runtime = FlowRuntime(
            store=self._store,
            registry=self._registry,
            config=self._config,
            extensions=self.extensions,
        )
        self._driver: Driver | None = None
        self._driver_task: asyncio.Task[None] | None = None

    # ── Wiring ───────────────────────────────────────────────────────────

    @property
    def schema(self) -> str:
        return self._store.schema

    @property
    def engine(self) -> AsyncEngine:
        return self._store.engine

    def transaction(self) -> AbstractAsyncContextManager[AsyncSession]:
        """A read transaction on this Flow's pool. The engine owns the boundary: ``commit()`` raises
        ``TransactionBoundaryError``.
        """
        return self._store.transaction()

    async def init(self) -> None:
        """Bring the schema up to date. Idempotent, and safe to call from every worker: concurrent callers
        serialise on an advisory lock.
        """
        self._refuse_if_closed()
        await apply_migrations(self.engine, self.schema)

    async def verify(self) -> None:
        """Probe the pool, creating nothing. Raises ``DatabaseUnreachableError``."""
        self._refuse_if_closed()
        await self._db.verify()

    async def start_driver(self) -> None:
        self._refuse_if_closed()
        async with self._driver_lock:
            if self._driver_task is not None and not self._driver_task.done():
                return
            await self.init()
            self._driver = Driver(self._runtime)
            self._driver_task = asyncio.create_task(
                self._driver.run(),
                name=f"forktex_flow.driver[{self.schema}]",
            )

    async def stop_driver(self) -> None:
        """Stop the driver loop cleanly. Idempotent."""
        if self._driver is not None:
            self._driver.shutdown.set()
        if self._driver_task is not None:
            self._driver_task.cancel()
            try:
                await self._driver_task
            except asyncio.CancelledError:
                pass  # expected — we just cancelled it
            except Exception:
                # The task died with a real error instead of cancelling. Log it;
                # stop_driver still completes so shutdown is not blocked.
                logger.warning("driver task raised during shutdown", exc_info=True)
            self._driver_task = None
            self._driver = None

    async def close(self) -> None:
        await self.stop_driver()
        self._closed = True
        if self._owns_db:
            await self._db.dispose()

    def _refuse_if_closed(self) -> None:
        """Guard the entry points that touch the pool. Disposing the pool does not invalidate the handle, so a call
        after ``close()`` either raises from deep inside the driver or quietly opens a fresh connection — which
        leaks a pool nothing will dispose.
        """
        if self._closed:
            raise FlowClosed("this Flow is closed; construct a new one")

    # ── Declaration ──────────────────────────────────────────────────────

    def scheduled(
        self,
        name: str,
        *,
        version: int,
        cron: str,
        state: type | None = None,
        enabled: bool = True,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:

        def _decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
            self._registry.register(
                compile_scheduled(
                    fn,
                    name=name,
                    version=version,
                    cron=cron,
                    state_cls=state,
                    enabled=enabled,
                    defaults=self._config,
                )
            )
            return fn

        return _decorator

    def pipeline(
        self,
        name: str,
        *,
        version: int,
        state: type | None = None,
        cron: str | None = None,
    ) -> Callable[[type], type]:

        def _decorator(cls: type) -> type:
            self._registry.register(
                compile_pipeline(cls, name=name, version=version, cron=cron, state_cls=state, defaults=self._config)
            )
            return cls

        return _decorator

    def registered(self) -> list[str]:
        return self._registry.names()

    # ── Dispatch and read ────────────────────────────────────────────────

    async def run(
        self,
        name: str,
        *,
        state: JsonObject | None = None,
        metadata: JsonObject | None = None,
        version: int | None = None,
    ) -> WorkflowInstance:
        self._refuse_if_closed()
        defn = self._registry.get(name, version=version)
        if defn is None:
            raise NotFoundError(f"workflow {name!r} is not registered in this process")

        run_input = dict(state or {})
        run_metadata = dict(metadata or {})

        for extension in self.extensions:
            hook = getattr(extension, "before_start", None)
            if hook is None:
                continue
            extra = await hook(name, defn.version, run_input, run_metadata)
            if extra:
                run_metadata.update(extra)

        run_id = await _runs.insert_run(
            self._store,
            workflow_name=name,
            workflow_version=defn.version,
            input=run_input,
            metadata=run_metadata,
        )
        return await self.get(run_id)

    async def get(self, run_id: UUID) -> WorkflowInstance:
        """The run with this id. Raises ``NotFoundError`` when it does not exist."""
        self._refuse_if_closed()
        info = await _runs.fetch_run(self._store, run_id)
        if info is None:
            raise NotFoundError(f"run {run_id} not found")
        return bind_instance(info, self._store)

    def query(self) -> InstanceQuery:
        return InstanceQuery(self._store)

    def _execution_context(self) -> FlowRuntime:
        return self._runtime


def _with_lock_key(config: RuntimeConfig, schema: str) -> RuntimeConfig:
    """Give the driver a lock key derived from *this* instance's schema. Leader election is one advisory lock, so
    two ``Flow``s sharing a key share a leader: whichever wins drives and the other idles, with its schedules
    never firing.
    """
    if config.leader_lock_key != _DERIVE_LEADER_LOCK_KEY:
        return config
    return config.model_copy(update={"leader_lock_key": advisory_key(schema, "driver")})
