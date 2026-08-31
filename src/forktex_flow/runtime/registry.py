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

"""In-process registry of workflow definitions.

Keyed by ``(name, version)``. Registration is at import time — a decorator runs
when its module is imported — so a duplicate key is a programming error caught
at startup rather than a race.
"""

from __future__ import annotations

from forktex.error import AlreadyExistsError
from forktex.types import BaseValueObject
from pydantic import Field

from forktex_flow.domain.definition import WorkflowDefinition

__all__ = ["FlowRegistry"]


class FlowRegistry(BaseValueObject):
    """Every workflow declared in this process, by ``(name, version)``."""

    definitions: dict[tuple[str, int], WorkflowDefinition] = Field(default_factory=dict)

    def register(self, defn: WorkflowDefinition) -> None:
        """Add a definition. Raises if that exact ``(name, version)`` is taken.

        Re-registering under the same version is always a mistake: the two
        declarations disagree about what the workflow is, and a run picked up
        after a restart would execute whichever one happened to be imported
        second. Bump the version instead.
        """
        key = (defn.name, defn.version)
        if key in self.definitions:
            raise AlreadyExistsError(f"workflow {defn.name!r} version {defn.version} is already registered")
        self.definitions[key] = defn

    def get(self, name: str, *, version: int | None = None) -> WorkflowDefinition | None:
        """Look one up. With ``version=None``, the highest registered version."""
        if version is not None:
            return self.definitions.get((name, version))
        matching = [defn for (candidate, _), defn in self.definitions.items() if candidate == name]
        return max(matching, key=lambda d: d.version) if matching else None

    def scheduled(self) -> list[WorkflowDefinition]:
        """Every definition carrying a cron expression."""
        return [defn for defn in self.definitions.values() if defn.schedule is not None]

    def names(self) -> list[str]:
        """Every registered workflow name, deduplicated."""
        return sorted({name for name, _ in self.definitions})
