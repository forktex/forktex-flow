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

from itertools import pairwise
from typing import Any

from croniter import croniter
from forktex.error import BadRequestError

from forktex_flow.domain.node import HandlerFn, StepMeta, StepSpec, WhenFn, step_meta
from forktex_flow.domain.process import EndEvent, ProcessModel, SequenceFlow, ServiceTask, StartEvent
from forktex_flow.domain.state import extract_reducers
from forktex_flow.runtime.registry import CodeDefinition, Handler

__all__ = ["compile_pipeline", "compile_scheduled"]

_START, _END = "start", "end"


def compile_scheduled(
    fn: HandlerFn, *, key: str, cron: str, state_cls: type | None = None, enabled: bool = True
) -> tuple[CodeDefinition, list[Handler]]:
    return _linear(key, [fn], cron=cron, state_cls=state_cls, enabled=enabled)


def compile_pipeline(
    cls: type, *, key: str, cron: str | None = None, state_cls: type | None = None, enabled: bool = True
) -> tuple[CodeDefinition, list[Handler]]:
    steps: list[Any] = list(getattr(cls, "steps", []))
    if not steps:
        raise BadRequestError(f"pipeline {key!r}: class {cls.__name__} declares no steps")
    return _linear(
        key,
        steps,
        cron=cron,
        state_cls=state_cls if state_cls is not None else getattr(cls, "state_cls", None),
        enabled=enabled,
    )


def _linear(
    key: str, steps: list[Any], *, cron: str | None, state_cls: type | None, enabled: bool
) -> tuple[CodeDefinition, list[Handler]]:
    if cron is not None and not croniter.is_valid(cron):
        raise BadRequestError(f"workflow {key!r}: {cron!r} is not a valid cron expression")

    tasks: list[ServiceTask] = []
    handlers: list[Handler] = []
    used = {_START, _END}
    for index, item in enumerate(steps):
        fn, meta, when = _resolve(item)
        # Suffixed by position, which stays stable when the list is edited elsewhere.
        name = getattr(fn, "__name__", f"step_{index}")
        if name in used:
            name = f"{name}_{index}"
        used.add(name)
        task_type = f"{key}:{name}"
        tasks.append(
            ServiceTask(
                id=name,
                task_type=task_type,
                retries=meta.max_attempts,
                backoff=meta.backoff,
            )
        )
        handlers.append(Handler(task_type=task_type, fn=fn, when=when))

    ids = [_START, *(task.id for task in tasks), _END]
    model = ProcessModel(
        key=key,
        elements=(StartEvent(id=_START), *tasks, EndEvent(id=_END)),
        flows=tuple(
            SequenceFlow(id=f"{source}->{target}", source=source, target=target) for source, target in pairwise(ids)
        ),
    )
    return CodeDefinition(model=model, reducers=extract_reducers(state_cls), cron=cron, enabled=enabled), handlers


def _resolve(item: HandlerFn | StepSpec) -> tuple[HandlerFn, StepMeta, WhenFn | None]:
    if not isinstance(item, StepSpec):
        return item, step_meta(item) or StepMeta(), None
    decorated = step_meta(item.fn) or StepMeta()
    meta = StepMeta(
        max_attempts=item.max_attempts if item.max_attempts is not None else decorated.max_attempts,
        backoff=item.backoff if item.backoff is not None else decorated.backoff,
    )
    return item.fn, meta, item.when
