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

from forktex.error import AlreadyExistsError
from forktex.types import BaseValueObject
from pydantic import Field

from forktex_flow.domain.node import HandlerFn, WhenFn
from forktex_flow.domain.process import ProcessModel
from forktex_flow.domain.state import ReducerFn

__all__ = ["CodeDefinition", "FlowRegistry", "Handler"]


class Handler(BaseValueObject):
    task_type: str
    fn: HandlerFn
    when: WhenFn | None = None


class CodeDefinition(BaseValueObject):
    model: ProcessModel
    reducers: dict[str, ReducerFn] = Field(default_factory=dict)
    cron: str | None = None
    enabled: bool = True


class FlowRegistry(BaseValueObject):
    handlers: dict[str, Handler] = Field(default_factory=dict)
    definitions: dict[str, CodeDefinition] = Field(default_factory=dict)

    def handle(self, handler: Handler) -> None:
        """Raises ``AlreadyExistsError`` for a task type this Flow already handles."""
        if handler.task_type in self.handlers:
            raise AlreadyExistsError(f"task type {handler.task_type!r} already has a handler")
        self.handlers[handler.task_type] = handler

    def declare(self, definition: CodeDefinition, handlers: list[Handler]) -> None:
        """Raises ``AlreadyExistsError`` for a key or task type already declared, registering nothing."""
        key = definition.model.key
        if key in self.definitions:
            raise AlreadyExistsError(f"workflow {key!r} is already declared")
        taken = [handler.task_type for handler in handlers if handler.task_type in self.handlers]
        if taken:
            raise AlreadyExistsError(f"task types {taken} already have handlers")
        self.definitions[key] = definition
        for handler in handlers:
            self.handlers[handler.task_type] = handler

    def handler(self, task_type: str) -> Handler | None:
        return self.handlers.get(task_type)

    def task_types(self) -> list[str]:
        return sorted(self.handlers)

    def reducers(self, key: str) -> dict[str, ReducerFn]:
        definition = self.definitions.get(key)
        return {} if definition is None else definition.reducers

    def keys(self) -> list[str]:
        return sorted(self.definitions)
