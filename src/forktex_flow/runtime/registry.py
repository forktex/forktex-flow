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

from forktex_flow.domain.definition import WorkflowDefinition

__all__ = ["FlowRegistry"]


class FlowRegistry(BaseValueObject):
    definitions: dict[tuple[str, int], WorkflowDefinition] = Field(default_factory=dict)

    def register(self, defn: WorkflowDefinition) -> None:
        """Add a definition. Raises if that exact ``(name, version)`` is taken."""
        key = (defn.name, defn.version)
        if key in self.definitions:
            raise AlreadyExistsError(f"workflow {defn.name!r} version {defn.version} is already registered")
        self.definitions[key] = defn

    def get(self, name: str, *, version: int | None = None) -> WorkflowDefinition | None:
        if version is not None:
            return self.definitions.get((name, version))
        matching = [defn for (candidate, _), defn in self.definitions.items() if candidate == name]
        return max(matching, key=lambda d: d.version) if matching else None

    def scheduled(self) -> list[WorkflowDefinition]:
        return [defn for defn in self.definitions.values() if defn.schedule is not None]

    def names(self) -> list[str]:
        return sorted({name for name, _ in self.definitions})
