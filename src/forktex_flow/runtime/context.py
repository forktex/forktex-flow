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

"""``FlowRuntime`` — everything the executor and the driver need to do their job.

The counterpart to :class:`FlowStore` one layer up. ``persist`` and ``read`` need
a pool and a schema; ``runtime`` additionally needs to know what is registered,
how hard to retry, and who is listening. Bundling the four means the executor
takes one parameter instead of four, without taking the facade.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from forktex_flow.domain.config import RuntimeConfig
    from forktex_flow.extension import FlowExtension
    from forktex_flow.persist.store import FlowStore
    from forktex_flow.runtime.registry import FlowRegistry

__all__ = ["FlowRuntime"]


@dataclass(frozen=True, slots=True)
class FlowRuntime:
    """The execution context: where to write, what is declared, how to behave."""

    store: FlowStore
    registry: FlowRegistry
    config: RuntimeConfig
    extensions: tuple[FlowExtension, ...] = field(default_factory=tuple)
