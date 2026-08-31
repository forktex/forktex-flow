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

"""``FlowExtension`` — the hook protocol a consumer implements to observe runs.

Three moments: before a run is inserted, and after it reaches each of its two
terminal states. That is the whole contract, and it is a `Protocol` rather than a
base class, so a consumer writes a plain object with the one or two methods it
cares about and passes it in.

The library ships this file and no concrete extension. Every consumer's tenant
model is its own — a ContextVar, a request-scoped dependency, something else —
and a resolver here would be one product's preferences leaking into all of them.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    # Typing-only, so `extension` stays importable without pulling the engine's
    # module graph in behind it.
    from forktex.types import JsonValue

    from forktex_flow.domain.types import RunInfo

__all__ = ["FlowExtension"]


@runtime_checkable
class FlowExtension(Protocol):
    """Hooks the library calls at well-defined points in a run's life.

    Every method is optional: implement the ones you need and omit the rest.
    Hooks are awaited, so work inside them completes before the run is
    considered finished. A hook that raises is logged and swallowed — an
    extension observes a run, it does not get to change its outcome.
    """

    async def before_start(
        self,
        name: str,
        version: int,
        input: dict[str, Any],
        metadata: dict[str, Any],
    ) -> dict[str, Any]:
        """Augment a run's metadata before its row is inserted.

        Return a dict merged into ``metadata``; return ``{}`` to add nothing.
        This is where a consumer stamps request context onto a run —
        ``{"org_id": current_org.get(), "trace_id": current_trace.get()}`` —
        which then reaches every step through ``Ctx.metadata`` and is queryable
        through ``flow.query().metadata(org_id=…)``.
        """
        return {}

    async def after_complete(self, run: RunInfo, output: JsonValue) -> None:
        """Called once a run has reached ``completed``."""

    async def after_fail(self, run: RunInfo, error: BaseException) -> None:
        """Called once a run has reached ``failed`` or ``cancelled``."""
