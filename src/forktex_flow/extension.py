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
    from forktex.types import JsonObject

    from forktex_flow.domain.types import InstanceInfo
    from forktex_flow.errors import WorkflowFailed

__all__ = ["FlowExtension"]


@runtime_checkable
class FlowExtension(Protocol):
    """Hooks called at fixed points of an instance's life. A terminal hook that raises is logged and swallowed: an
    extension observes an instance, it does not change its outcome.
    """

    async def before_start(self, key: str, version: int, variables: JsonObject, metadata: JsonObject) -> JsonObject:
        return {}

    async def after_complete(self, instance: InstanceInfo, variables: JsonObject) -> None:
        """Called once an instance has reached ``completed``."""

    async def after_fail(self, instance: InstanceInfo, error: WorkflowFailed) -> None:
        """Called once an instance has reached ``failed``."""
