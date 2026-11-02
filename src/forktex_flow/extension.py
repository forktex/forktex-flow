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

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    # Typing-only, so `extension` stays importable without pulling the engine's
    # module graph in behind it.
    from forktex.types import JsonObject, JsonValue

    from forktex_flow.domain.types import RunInfo

__all__ = ["FlowExtension"]


@runtime_checkable
class FlowExtension(Protocol):
    """Hooks the library calls at well-defined points in a run's life. A hook that raises is logged and swallowed —
    an extension observes a run, it does not get to change its outcome.
    """

    async def before_start(
        self,
        name: str,
        version: int,
        input: JsonObject,
        metadata: JsonObject,
    ) -> JsonObject:
        return {}

    async def after_complete(self, run: RunInfo, output: JsonValue) -> None:
        """Called once a run has reached ``completed``."""

    async def after_fail(self, run: RunInfo, error: BaseException) -> None:
        """Called once a run has reached ``failed`` or ``cancelled``."""
