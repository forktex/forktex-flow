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

"""``Flow`` — the public facade. Construct one per process.

Declare workflows with the two decorators, dispatch with :meth:`run`, find runs
with :meth:`query`::

    flow = Flow(database_url="postgresql+asyncpg://…")

    @flow.scheduled("cloud.backup.create", version=1, cron="0 2 * * *", state=BackupState)
    async def backup_create(ctx: Ctx, state: BackupState) -> dict: ...

    @flow.pipeline("cloud.deploy.apply", version=4, state=DeployState)
    class DeployApply:
        steps = [load_manifest, provision, step(dns_setup, when=lambda s: s["dns"])]

    await flow.init()            # bring the schema up
    await flow.start_driver()    # compete for leadership and execute

    instance = await flow.run("cloud.deploy.apply", state={...}, metadata={...})
    page = await flow.query().workflow("cloud.deploy.apply").status("running").fetch()

This class assembles a :class:`FlowStore`, a :class:`FlowRegistry` and a
:class:`RuntimeConfig` and hands them down. It is imported by ``__init__`` and
by nothing else in the package — every layer below takes what it needs rather
than taking this.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from typing import TYPE_CHECKING, Any

from forktex.database import Database, advisory_key
from forktex.error import BadRequestError, NotFoundError
from forktex.log import get_logger

from forktex_flow.domain.config import RuntimeConfig
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

#: Default advisory-lock key for leader election. Derived through
#: `advisory_key`, the one place that folds a digest into Postgres's signed
#: bigint range — a second derivation of the same thing is what let three
#: incompatible lock keys into the ecosystem before.
_DEFAULT_LEADER_LOCK_KEY = advisory_key(_MODEL_SCHEMA, "driver")


class Flow:
    """Durable workflow runtime.

    Args:
        database_url: SQLAlchemy async URL. Builds a pool dedicated to this
            Flow. Mutually exclusive with ``database``.
        database: An existing :class:`forktex.database.Database` to share a pool
            with the rest of the application. Its ``schema_translate_map`` must
            remap ``forktex_flow`` to ``schema``.
        schema: The Postgres schema to create and use. Defaults to
            ``forktex_flow``.
        extensions: Consumer hooks — metadata injection and terminal
            notifications. See :class:`FlowExtension`.
        config: Retry and polling behaviour. See :class:`RuntimeConfig`.
        echo: Echo SQL. Only meaningful with ``database_url``.
        engine_kwargs: Passed verbatim to ``create_async_engine``.
    """

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
            # Connection management belongs to `Database`, not here — this used
            # to duplicate `init_engine` almost line for line. Construction is
            # lazy either way: nothing connects until something executes.
            self._db = Database(
                database_url,  # type: ignore[arg-type]  # guarded above
                echo=echo,
                schema_translate_map={_MODEL_SCHEMA: schema},
                **engine_kwargs,
            )
            self._owns_db = True

        self.extensions: tuple[FlowExtension, ...] = tuple(extensions or ())
        self._store = FlowStore(database=self._db, schema=schema)
        self._registry = FlowRegistry()
        self._config = config or RuntimeConfig(leader_lock_key=_DEFAULT_LEADER_LOCK_KEY)
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
        """The Postgres schema this Flow reads and writes."""
        return self._store.schema

    @property
    def engine(self) -> AsyncEngine:
        """The engine backing this Flow — for DDL, advisory locks, migrations."""
        return self._store.engine

    def session(self) -> AbstractAsyncContextManager[AsyncSession]:
        """A transactional session on this Flow's pool.

        Public because a consumer sometimes needs to read the engine's own
        tables — tailing ``run_event`` for a live progress stream, say — inside
        the same pool rather than opening a second one.
        """
        return self._store.session()

    async def init(self) -> None:
        """Bring the schema up to date. Idempotent, and safe to call from every
        worker: concurrent callers serialise on an advisory lock."""
        await apply_migrations(self.engine, self.schema)

    async def start_driver(self) -> None:
        """Start competing for leadership and executing runs. Returns at once.

        Safe on every worker — only the lock holder actually drives.
        """
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
        """Stop the driver and release resources. Call from your shutdown handler.

        Disposes the pool only if this Flow created it: a pool passed in via
        ``database=`` belongs to the caller, and disposing it here would tear
        down the rest of the application's database access.
        """
        await self.stop_driver()
        if self._owns_db:
            await self._db.dispose()

    # ── Declaration ──────────────────────────────────────────────────────

    def scheduled(
        self,
        name: str,
        *,
        version: int,
        cron: str,
        state: type | None = None,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Register one async function to run on a cron schedule."""

        def _decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
            self._registry.register(
                compile_scheduled(fn, name=name, version=version, cron=cron, state_cls=state, defaults=self._config)
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
        """Register a class with a ``steps`` list as an ordered workflow.

        ``cron`` makes it scheduled as well as callable — the two decorators
        differ only in whether the definition carries a schedule.
        """

        def _decorator(cls: type) -> type:
            self._registry.register(
                compile_pipeline(cls, name=name, version=version, cron=cron, state_cls=state, defaults=self._config)
            )
            return cls

        return _decorator

    def registered(self) -> list[str]:
        """Every workflow name declared in this process."""
        return self._registry.names()

    # ── Dispatch and read ────────────────────────────────────────────────

    async def run(
        self,
        name: str,
        *,
        state: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
        version: int | None = None,
    ) -> WorkflowInstance:
        """Submit a run and return immediately with a handle on it.

        The run is inserted ``pending``; the driver picks it up on its next
        tick. Await :meth:`WorkflowInstance.wait` for the outcome.

        A run started this way is labelled ``triggered_by="manual"``, which is
        what ``.triggered_by()`` filters on. The label is not a parameter: the
        only other value is ``"schedule"``, the driver writes it, and
        ``ck_run_triggered_by`` rejects anything else — so exposing it would let
        a caller either mislabel a run or take an IntegrityError from four
        frames down.
        """
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
        """The run with this id.

        Raises ``NotFoundError`` when it does not exist.
        """
        info = await _runs.fetch_run(self._store, run_id)
        if info is None:
            raise NotFoundError(f"run {run_id} not found")
        return bind_instance(info, self._store)

    def query(self) -> InstanceQuery:
        """Start a fluent query over runs."""
        return InstanceQuery(self._store)

    def _execution_context(self) -> FlowRuntime:
        """What the driver hands the executor.

        Private, but named: :mod:`forktex_flow.testing` needs it to run a
        workflow inline instead of through a driver, and reaching into four
        attributes from another module is worse than one accessor that says
        what it is for.
        """
        return self._runtime
