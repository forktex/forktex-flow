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

from collections.abc import Callable, Sequence
from typing import Any

from croniter import croniter
from forktex.error import AlreadyExistsError, BadRequestError
from pydantic import BaseModel

from forktex_flow.domain.node import StepSpec, step
from forktex_flow.domain.process import (
    PRIVATE_SEPARATOR,
    Element,
    EndEvent,
    ExclusiveGateway,
    ProcessModel,
    SequenceFlow,
    ServiceTask,
    StartEvent,
)
from forktex_flow.domain.state import extract_reducers, validate_state
from forktex_flow.runtime.registry import CodeDefinition, Handler

__all__ = ["compile_pipeline"]

_START, _END = "start", "end"


def compile_pipeline(
    key: str,
    steps: Sequence[StepSpec | Callable[..., Any] | str],
    *,
    state: type | None = None,
    cron: str | None = None,
    enabled: bool = True,
) -> tuple[CodeDefinition, list[Handler]]:
    """start → each step in order → end. A step with ``when`` sits behind a gateway whose default skips it."""
    specs = [entry if isinstance(entry, StepSpec) else step(entry) for entry in steps]
    if not specs:
        raise BadRequestError(f"pipeline {key!r} has no steps")
    if PRIVATE_SEPARATOR in key:
        raise BadRequestError(f"pipeline key {key!r} cannot contain {PRIVATE_SEPARATOR!r}")
    if cron is not None and not croniter.is_valid(cron):
        raise BadRequestError(f"pipeline {key!r}: {cron!r} is not a valid cron expression")
    model_state = state if isinstance(state, type) and issubclass(state, BaseModel) else None
    if cron is not None and model_state is not None:
        try:
            validate_state(model_state, {})
        except BadRequestError as exc:
            raise BadRequestError(
                f"pipeline {key!r} is scheduled, so it starts with no variables, which its state refuses: {exc.message}"
            ) from exc

    elements: list[Element] = [StartEvent(id=_START)]
    flows: list[SequenceFlow] = []
    handlers: list[Handler] = []
    used = {_START, _END}
    #: Where the token can arrive from at the next element, and the flow id that must carry it, if fixed.
    sources: list[tuple[str, str | None]] = [(_START, None)]

    def claim(element_id: str) -> None:
        if element_id in (_START, _END):
            raise BadRequestError(f"pipeline {key!r}: {element_id!r} is reserved; give the step another name=")
        if element_id in used:
            raise AlreadyExistsError(f"pipeline {key!r} has two steps named {element_id!r}; give one a name=")
        used.add(element_id)

    def enter(target: str) -> None:
        for source, flow_id in sources:
            flows.append(SequenceFlow(id=flow_id or f"{source}->{target}", source=source, target=target))

    for spec in specs:
        claim(spec.name)
        task_type = spec.target if isinstance(spec.target, str) else f"{key}{PRIVATE_SEPARATOR}{spec.name}"
        if spec.when is not None:
            gateway = f"{spec.name}.when"
            claim(gateway)
            enter(gateway)
            elements.append(ExclusiveGateway(id=gateway, default=f"{gateway}.default"))
            flows.append(
                SequenceFlow(id=f"{gateway}->{spec.name}", source=gateway, target=spec.name, condition=spec.when)
            )
            sources = [(spec.name, None), (gateway, f"{gateway}.default")]
        else:
            enter(spec.name)
            sources = [(spec.name, None)]
        elements.append(ServiceTask(id=spec.name, task_type=task_type, retries=spec.max_attempts, backoff=spec.backoff))
        if not isinstance(spec.target, str):
            handlers.append(Handler(task_type=task_type, fn=spec.target, state=model_state))
    enter(_END)
    elements.append(EndEvent(id=_END))

    model = ProcessModel(key=key, elements=tuple(elements), flows=tuple(flows))
    definition = CodeDefinition(
        model=model, reducers=extract_reducers(state), state=model_state, cron=cron, enabled=enabled
    )
    return definition, handlers
