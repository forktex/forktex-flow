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

"""Moving an instance's token. Every change happens inside the caller's transaction while it holds the instance's
row lock, and runs through events, gateways and end events until the token reaches something that waits: a job, a
user task or a child instance. A transaction that fails changes nothing, so the token is never half-moved.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import sqlalchemy as sa
from forktex import iso
from forktex.log import get_logger
from forktex.types import JsonObject
from pydantic import ValidationError

from forktex_flow.domain.jsonlogic import evaluate, truthy
from forktex_flow.domain.process import (
    ACTIVITY_SEPARATOR,
    AdHocDecision,
    AdHocSubProcess,
    CallActivity,
    EndEvent,
    ExclusiveGateway,
    ProcessModel,
    ServiceTask,
    UserTask,
)
from forktex_flow.domain.state import apply_state_update
from forktex_flow.domain.types import OPEN_ELEMENT_STATUSES, DefinitionInfo, TriggeredBy
from forktex_flow.errors import WorkflowFailed
from forktex_flow.persist import definitions as _definitions
from forktex_flow.persist import instances as _instances
from forktex_flow.persist.mappers import to_instance_info
from forktex_flow.persist.models import ElementRun, Instance

if TYPE_CHECKING:
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncSession

    from forktex_flow.domain.process import Element
    from forktex_flow.runtime.context import FlowRuntime

__all__ = ["Advance", "notify"]

logger = get_logger(__name__)


class Advance:
    def __init__(self, runtime: FlowRuntime, session: AsyncSession) -> None:
        self.runtime = runtime
        self.session = session
        self.finished: list[UUID] = []

    async def start(
        self,
        definition: DefinitionInfo,
        *,
        variables: JsonObject,
        metadata: JsonObject,
        triggered_by: TriggeredBy = "manual",
        parent: tuple[UUID, str] | None = None,
    ) -> UUID:
        instance = Instance(
            namespace=definition.namespace,
            key=definition.key,
            version=definition.version,
            definition_id=definition.definition_id,
            status="active",
            input=variables,
            variables=variables,
            metadata_=metadata,
            triggered_by=triggered_by,
            parent_instance_id=None if parent is None else parent[0],
            parent_element_id=None if parent is None else parent[1],
        )
        self.session.add(instance)
        await self.session.flush()
        _instances.emit(self.session, instance.id, "instance_started", {"version": definition.version})
        await self._enter(instance, definition.model, definition.model.start.id)
        return instance.id

    async def complete(self, element_run_id: UUID, output: JsonObject, *, expected: str = "running") -> bool:
        run = await self.session.get(ElementRun, element_run_id)
        if run is None:
            return False
        instance = await _instances.lock(self.session, run.instance_id)
        await self.session.refresh(run)
        if instance is None or instance.status != "active" or run.status != expected:
            return False
        model = await self._model(instance)
        found = model.activity(run.element_id)
        element = found[2] if found is not None else model.element(run.element_id)
        if isinstance(element, AdHocSubProcess):
            try:
                decision = AdHocDecision.model_validate(output)
            except ValidationError as exc:
                await self.fail(run.id, f"the decider returned no valid AdHocDecision: {exc}", expected=expected)
                return True
            if decision.activate and not decision.done:
                await self._round(instance, element, run, decision)
                return True
            output = decision.output
        run.status = "completed"
        run.output = output
        run.finished_at = iso.now()
        run.heartbeat_at = None
        instance.variables = self._merge(instance, element, output)
        _instances.emit(self.session, instance.id, "element_completed", {"element": run.element_id})
        if found is not None:
            await self._close_round(instance, found[0], found[1])
            return True
        await self._leave(instance, model, run.element_id)
        return True

    async def fail(self, element_run_id: UUID, error: str, *, expected: str = "running") -> bool:
        run = await self.session.get(ElementRun, element_run_id)
        if run is None:
            return False
        instance = await _instances.lock(self.session, run.instance_id)
        await self.session.refresh(run)
        if instance is None or instance.status != "active" or run.status != expected:
            return False
        run.status = "failed"
        run.error = error
        run.finished_at = iso.now()
        run.heartbeat_at = None
        _instances.emit(self.session, instance.id, "element_failed", {"element": run.element_id})
        await self._fail(instance, f"element {run.element_id!r} failed: {_summary(error)}")
        return True

    async def _enter(self, instance: Instance, model: ProcessModel, element_id: str) -> None:
        while True:
            element = model.element(element_id)
            if isinstance(element, ServiceTask):
                self._record(instance, element, "pending", task_type=element.task_type, retries=element.retries)
                return
            if isinstance(element, UserTask):
                self._record(instance, element, "waiting")
                _instances.emit(self.session, instance.id, "task_waiting", {"element": element.id})
                return
            if isinstance(element, CallActivity):
                await self._call(instance, element)
                return
            if isinstance(element, AdHocSubProcess):
                self._record(instance, element, "pending", task_type=element.task_type, retries=element.retries)
                return
            self._record(instance, element, "completed")
            if isinstance(element, EndEvent):
                await self._finish(instance)
                return
            if isinstance(element, ExclusiveGateway):
                target = _choose(model, element, instance.variables)
                if target is None:
                    await self._fail(instance, f"no flow out of gateway {element.id!r} matched, and it has no default")
                    return
                element_id = target
                continue
            element_id = model.outgoing(element.id)[0].target

    async def _leave(self, instance: Instance, model: ProcessModel, element_id: str) -> None:
        await self._enter(instance, model, model.outgoing(element_id)[0].target)

    async def _call(self, instance: Instance, element: CallActivity) -> None:
        run = self._record(instance, element, "waiting")
        child = await _definitions.find(self.session, instance.namespace, element.called_element, element.version)
        if child is None:
            pinned = "" if element.version is None else f" version {element.version}"
            error = f"{element.called_element!r}{pinned} is not deployed in namespace {instance.namespace!r}"
            run.status = "failed"
            run.error = error
            await self._fail(instance, error)
            return
        await self.session.flush()
        run.child_instance_id = await self.start(
            child,
            variables=dict(instance.variables),
            metadata=dict(instance.metadata_),
            triggered_by="call",
            parent=(instance.id, element.id),
        )

    async def _round(
        self, instance: Instance, element: AdHocSubProcess, decider: ElementRun, decision: AdHocDecision
    ) -> None:
        prefix = f"{element.id}{ACTIVITY_SEPARATOR}"
        names = await self.session.scalars(
            sa.select(ElementRun.element_id).where(
                ElementRun.instance_id == instance.id, ElementRun.element_id.startswith(prefix, autoescape=True)
            )
        )
        rounds = [int(name.split(ACTIVITY_SEPARATOR)[1].split(".")[0]) for name in names]
        number = max(rounds, default=0) + 1
        unknown = [a.element_id for a in decision.activate if element.activity(a.element_id) is None]
        if number > element.max_iterations or unknown:
            error = (
                f"adHocSubProcess {element.id!r} asked for activities it does not have: {unknown}"
                if unknown
                else f"adHocSubProcess {element.id!r} did not finish within {element.max_iterations} rounds"
            )
            decider.status = "failed"
            decider.error = error
            decider.finished_at = iso.now()
            await self._fail(instance, error)
            return
        decider.status = "waiting"
        decider.heartbeat_at = None
        for index, activation in enumerate(decision.activate):
            activity = element.activity(activation.element_id)
            if activity is None:
                continue
            run = self._record(
                instance,
                activity,
                "pending",
                task_type=activity.task_type,
                retries=activity.retries,
                element_id=ACTIVITY_SEPARATOR.join((element.id, f"{number}.{index}", activity.id)),
            )
            run.input = activation.variables
        _instances.emit(
            self.session,
            instance.id,
            "adhoc_round",
            {"element": element.id, "round": number, "activate": [a.element_id for a in decision.activate]},
        )

    async def _close_round(self, instance: Instance, element: AdHocSubProcess, number: int) -> None:
        # Once every activity of the round has finished, the decider is asked again, with fresh attempts.
        await self.session.flush()
        prefix = f"{element.id}{ACTIVITY_SEPARATOR}{number}."
        still_open = await self.session.scalar(
            sa.select(sa.func.count(ElementRun.id)).where(
                ElementRun.instance_id == instance.id,
                ElementRun.element_id.startswith(prefix, autoescape=True),
                ElementRun.status.in_(OPEN_ELEMENT_STATUSES),
            )
        )
        if still_open:
            return
        decider = await self.session.scalar(
            sa.select(ElementRun).where(ElementRun.instance_id == instance.id, ElementRun.element_id == element.id)
        )
        if decider is not None and decider.status == "waiting":
            decider.status = "pending"
            decider.attempts = 0
            decider.next_attempt_at = iso.now()

    async def _finish(self, instance: Instance) -> None:
        instance.status = "completed"
        instance.finished_at = iso.now()
        _instances.emit(self.session, instance.id, "instance_completed")
        self.finished.append(instance.id)
        waiting = await self._waiting_parent(instance)
        if waiting is not None:
            await self.session.flush()
            await self.complete(waiting.id, dict(instance.variables), expected="waiting")

    async def _fail(self, instance: Instance, error: str) -> None:
        instance.status = "failed"
        instance.error = error
        instance.finished_at = iso.now()
        await self.session.flush()
        await _instances.close_open_elements(self.session, instance.id, "cancelled")
        children = await self.session.scalars(
            sa.select(Instance.id).where(Instance.parent_instance_id == instance.id, Instance.status == "active")
        )
        for child in list(children):
            await _instances.cancel(self.session, child, "its parent failed")
        _instances.emit(self.session, instance.id, "instance_failed", {"error": error})
        self.finished.append(instance.id)
        waiting = await self._waiting_parent(instance)
        if waiting is not None:
            await self.fail(waiting.id, f"child instance {instance.id} failed: {error}", expected="waiting")

    async def _waiting_parent(self, instance: Instance) -> ElementRun | None:
        if instance.parent_instance_id is None:
            return None
        return await self.session.scalar(
            sa.select(ElementRun).where(
                ElementRun.instance_id == instance.parent_instance_id,
                ElementRun.element_id == instance.parent_element_id,
            )
        )

    def _record(
        self,
        instance: Instance,
        element: Element,
        status: str,
        *,
        task_type: str | None = None,
        retries: int | None = None,
        element_id: str | None = None,
    ) -> ElementRun:
        done = status == "completed"
        run = ElementRun(
            instance_id=instance.id,
            element_id=element_id or element.id,
            element_type=element.type,
            task_type=task_type,
            status=status,
            max_attempts=retries or (self.runtime.config.max_attempts if task_type else 1),
            finished_at=iso.now() if done else None,
        )
        self.session.add(run)
        return run

    def _merge(self, instance: Instance, element: Element, output: JsonObject) -> JsonObject:
        result_variable = getattr(element, "result_variable", None)
        if result_variable is not None:
            return {**instance.variables, result_variable: output}
        return apply_state_update(dict(instance.variables), output, self.runtime.registry.reducers(instance.key))

    async def _model(self, instance: Instance) -> ProcessModel:
        return (await self.runtime.definition(self.session, instance.definition_id)).model


async def notify(runtime: FlowRuntime, instance_ids: list[UUID]) -> None:
    """Wake this process's driver, then call each extension's terminal hook for these instances. A hook that raises
    is logged, never propagated.
    """
    runtime.wake.set()
    if not runtime.extensions or not instance_ids:
        return
    async with runtime.store.transaction() as session:
        rows = [await session.get(Instance, instance_id) for instance_id in instance_ids]
        infos = [to_instance_info(row) for row in rows if row is not None]
    for info in infos:
        for extension in runtime.extensions:
            hook = getattr(extension, "after_complete" if info.status == "completed" else "after_fail", None)
            if hook is None:
                continue
            try:
                await hook(info, info.variables if info.status == "completed" else WorkflowFailed(info.error or ""))
            except Exception:
                logger.warning(
                    "extension %s raised; instance outcome unaffected", type(extension).__name__, exc_info=True
                )


def _choose(model: ProcessModel, gateway: ExclusiveGateway, variables: JsonObject) -> str | None:
    default: str | None = None
    for flow in model.outgoing(gateway.id):
        if flow.id == gateway.default:
            default = flow.target
        elif flow.condition is not None and truthy(evaluate(flow.condition, variables)):
            return flow.target
    return default


def _summary(error: str) -> str:
    # A traceback's last line names the exception; any other message leads with its point.
    lines = [line.strip() for line in error.strip().splitlines() if line.strip()]
    if not lines:
        return error
    return lines[-1] if lines[0].startswith("Traceback") else lines[0]
