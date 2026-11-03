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
import json
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from typing import TYPE_CHECKING, Any, Self

import sqlalchemy as sa
from forktex import iso
from forktex.database import Database, advisory_key
from forktex.error import BadRequestError, ConflictError, NotFoundError
from forktex.log import get_logger
from forktex.types import JsonObject

from forktex_flow.domain.bpmn import parse_bpmn
from forktex_flow.domain.config import RuntimeConfig
from forktex_flow.domain.process import ProcessModel
from forktex_flow.domain.state import check_variables
from forktex_flow.domain.types import DEFAULT_NAMESPACE, DefinitionInfo
from forktex_flow.errors import FlowClosed
from forktex_flow.persist import definitions as _definitions
from forktex_flow.persist import instances as _instances
from forktex_flow.persist.migrations import apply_migrations
from forktex_flow.persist.models import ElementRun
from forktex_flow.persist.store import FlowStore
from forktex_flow.read.instance import InstanceQuery, WorkflowInstance, bind_instance
from forktex_flow.runtime.compiler import compile_pipeline, compile_scheduled
from forktex_flow.runtime.context import FlowRuntime
from forktex_flow.runtime.driver import Driver
from forktex_flow.runtime.engine import Advance, notify
from forktex_flow.runtime.registry import FlowRegistry, Handler

if TYPE_CHECKING:
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

    from forktex_flow.domain.node import HandlerFn
    from forktex_flow.extension import FlowExtension

__all__ = ["Flow"]

logger = get_logger(__name__)

#: The schema literal the ORM models are declared against; `schema_translate_map` rewrites it per instance.
_MODEL_SCHEMA = "forktex_flow"

#: ``leader_lock_key`` left at this value means "derive it from the schema"; `advisory_key` never returns 0.
_DERIVE_LEADER_LOCK_KEY = 0


class Flow:
    """Durable workflow runtime. A supplied ``database``'s ``schema_translate_map`` must remap ``forktex_flow`` to
    ``schema``. With ``drive=True``, ``async with`` also starts the driver.
    """

    def __init__(
        self,
        database_url: str | None = None,
        *,
        database: Database | None = None,
        schema: str = _MODEL_SCHEMA,
        extensions: list[FlowExtension] | None = None,
        config: RuntimeConfig | None = None,
        drive: bool = False,
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
                )
            self._db = database
            self._owns_db = False
        else:
            self._db = Database(
                database_url,  # type: ignore[arg-type]  # guarded above
                echo=echo,
                schema_translate_map={_MODEL_SCHEMA: schema},
                **engine_kwargs,
            )
            self._owns_db = True

        self.extensions: tuple[FlowExtension, ...] = tuple(extensions or ())
        self.drive = drive
        self._closed = False
        self._driver_lock = asyncio.Lock()
        self._deploy_lock = asyncio.Lock()
        self._config = _with_lock_key(config or RuntimeConfig(), schema)
        self._store = FlowStore(database=self._db, schema=schema, leader_lock_key=self._config.leader_lock_key)
        self._registry = FlowRegistry()
        self._runtime = FlowRuntime(
            store=self._store, registry=self._registry, config=self._config, extensions=self.extensions
        )
        #: key → the version this process deployed for each code-declared workflow.
        self._deployed: dict[str, DefinitionInfo] = {}
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
        """One transaction on this Flow's pool. The block owns the boundary: ``commit()`` raises
        ``TransactionBoundaryError``.
        """
        return self._store.transaction()

    async def init(self) -> None:
        """Bring the schema up to date and deploy every code-declared workflow. Idempotent and safe from every
        worker at once: migrations and deploys serialise on advisory locks.
        """
        self._refuse_if_closed()
        await apply_migrations(self.engine, self.schema)
        await self._deploy_declared()

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
            self._driver_task = asyncio.create_task(self._driver.run(), name=f"forktex_flow.driver[{self.schema}]")

    async def stop_driver(self) -> None:
        """Stop claiming, wait up to ``drain_timeout`` for running jobs, and hand the rest back. Idempotent."""
        if self._driver is not None:
            self._driver.shutdown.set()
        if self._driver_task is not None:
            try:
                await self._driver_task
            except Exception:
                logger.warning("driver task raised during shutdown", exc_info=True)
            self._driver_task = None
            self._driver = None

    async def close(self) -> None:
        await self.stop_driver()
        self._closed = True
        if self._owns_db:
            await self._db.dispose()

    async def __aenter__(self) -> Self:
        await self.init()
        if self.drive:
            await self.start_driver()
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.close()

    # ── Declaration ──────────────────────────────────────────────────────

    def handler(self, task_type: str) -> Callable[[HandlerFn], HandlerFn]:
        """Run ``fn`` for every service task of ``task_type`` this Flow claims. Raises ``AlreadyExistsError`` for a
        type it already handles.
        """

        def _decorator(fn: HandlerFn) -> HandlerFn:
            self._registry.handle(Handler(task_type=task_type, fn=fn))
            return fn

        return _decorator

    def scheduled(
        self, key: str, *, cron: str, state: type | None = None, enabled: bool = True
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def _decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
            self._registry.declare(*compile_scheduled(fn, key=key, cron=cron, state_cls=state, enabled=enabled))
            return fn

        return _decorator

    def pipeline(
        self, key: str, *, state: type | None = None, cron: str | None = None, enabled: bool = True
    ) -> Callable[[type], type]:
        def _decorator(cls: type) -> type:
            self._registry.declare(*compile_pipeline(cls, key=key, cron=cron, state_cls=state, enabled=enabled))
            return cls

        return _decorator

    def registered(self) -> list[str]:
        return self._registry.keys()

    # ── Definitions ──────────────────────────────────────────────────────

    async def deploy(
        self, model: ProcessModel | JsonObject | str, *, namespace: str = DEFAULT_NAMESPACE
    ) -> DefinitionInfo:
        """The version holding this model — a ``ProcessModel``, its JSON, or BPMN 2.0 XML: a new one when it
        changed, the existing one when it did not. Raises ``BadRequestError`` naming everything that stops it
        from running. XML is kept verbatim as the version's source.
        """
        self._refuse_if_closed()
        if isinstance(model, str):
            parsed, source = parse_bpmn(model), model
        else:
            parsed = model if isinstance(model, ProcessModel) else ProcessModel.model_validate(model)
            source = json.dumps(model if isinstance(model, dict) else parsed.model_dump(mode="json", exclude_none=True))
        async with self._store.transaction() as session:
            definition, _created = await _definitions.deploy(
                session, parsed, schema=self.schema, namespace=namespace, source=source
            )
        return definition

    async def definition(
        self, key: str, *, namespace: str = DEFAULT_NAMESPACE, version: int | None = None
    ) -> DefinitionInfo:
        """That version, or the latest. Raises ``NotFoundError``."""
        self._refuse_if_closed()
        async with self._store.transaction() as session:
            found = await _definitions.find(session, namespace, key, version)
        if found is None:
            raise NotFoundError(f"workflow {key!r} is not deployed in namespace {namespace!r}")
        return found

    # ── Instances ────────────────────────────────────────────────────────

    async def start(
        self,
        key: str,
        *,
        variables: JsonObject | None = None,
        metadata: JsonObject | None = None,
        namespace: str = DEFAULT_NAMESPACE,
        version: int | None = None,
    ) -> WorkflowInstance:
        """Start an instance and advance it to its first wait. A code-declared workflow starts the version this
        process deployed; any other the latest, unless ``version`` pins one. Raises ``NotFoundError``.
        """
        self._refuse_if_closed()
        await self._deploy_declared()
        definition = self._deployed.get(key) if namespace == DEFAULT_NAMESPACE and version is None else None
        if definition is None:
            definition = await self.definition(key, namespace=namespace, version=version)

        start_variables = dict(variables or {})
        check_variables(start_variables)
        start_metadata = dict(metadata or {})
        for extension in self.extensions:
            hook = getattr(extension, "before_start", None)
            if hook is not None:
                start_metadata.update(await hook(key, definition.version, start_variables, start_metadata) or {})

        async with self._store.transaction() as session:
            advance = Advance(self._runtime, session)
            instance_id = await advance.start(definition, variables=start_variables, metadata=start_metadata)
        await notify(self._runtime, advance.finished)
        return await self.get(instance_id)

    async def complete_task(
        self, instance_id: UUID, element_id: str, payload: JsonObject | None = None
    ) -> WorkflowInstance:
        """Complete a waiting user task with ``payload`` as its output, and advance the instance. Raises
        ``NotFoundError`` for no such task, ``ConflictError`` when it is not waiting.
        """
        self._refuse_if_closed()
        output = dict(payload or {})
        check_variables(output)
        async with self._store.transaction() as session:
            run = await session.scalar(
                sa.select(ElementRun).where(
                    ElementRun.instance_id == instance_id,
                    ElementRun.element_id == element_id,
                    ElementRun.element_type == "userTask",
                )
            )
            if run is None:
                raise NotFoundError(f"instance {instance_id} has no user task {element_id!r}")
            advance = Advance(self._runtime, session)
            if not await advance.complete(run.id, output, expected="waiting"):
                raise ConflictError(f"user task {element_id!r} of instance {instance_id} is not waiting")
        await notify(self._runtime, advance.finished)
        return await self.get(instance_id)

    async def get(self, instance_id: UUID) -> WorkflowInstance:
        """Raises ``NotFoundError`` when it does not exist."""
        self._refuse_if_closed()
        info = await _instances.fetch(self._store, instance_id)
        if info is None:
            raise NotFoundError(f"instance {instance_id} not found")
        return bind_instance(info, self._store)

    def query(self) -> InstanceQuery:
        return InstanceQuery(self._store)

    # ── Internals ────────────────────────────────────────────────────────

    async def _deploy_declared(self) -> None:
        pending = [key for key in self._registry.definitions if key not in self._deployed]
        if not pending:
            return
        async with self._deploy_lock, self._store.transaction() as session:
            now = iso.now()
            for key in pending:
                declared = self._registry.definitions[key]
                source = json.dumps(declared.model.model_dump(mode="json", exclude_none=True))
                definition, _created = await _definitions.deploy(
                    session, declared.model, schema=self.schema, namespace=DEFAULT_NAMESPACE, source=source
                )
                if declared.cron is not None:
                    await _definitions.upsert_schedule(
                        session, DEFAULT_NAMESPACE, key, cron=declared.cron, enabled=declared.enabled, now=now
                    )
                self._deployed[key] = definition

    def _refuse_if_closed(self) -> None:
        """Disposing the pool does not invalidate the handle, so a call after ``close()`` would otherwise open a
        fresh pool nothing disposes.
        """
        if self._closed:
            raise FlowClosed("this Flow is closed; construct a new one")


def _with_lock_key(config: RuntimeConfig, schema: str) -> RuntimeConfig:
    if config.leader_lock_key != _DERIVE_LEADER_LOCK_KEY:
        return config
    return config.model_copy(update={"leader_lock_key": advisory_key(schema, "driver")})
