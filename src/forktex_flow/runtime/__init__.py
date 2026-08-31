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

"""Flow's execution machinery — the part that actually runs a workflow.

`compiler` turns a decorated function or class into a definition, `registry`
holds what has been declared, `execution` is the durable step boundary that
makes a re-run idempotent, `ctx` is the handle a step body is given, `driver` is
the leader-elected loop that claims and advances runs, and `context` bundles
what all of them need.

Depends on `persist`, `read` and `domain`. Nothing below imports back up into
it, and nothing here imports the facade.
"""

from forktex_flow.runtime.compiler import compile_pipeline, compile_scheduled
from forktex_flow.runtime.context import FlowRuntime
from forktex_flow.runtime.ctx import Ctx
from forktex_flow.runtime.driver import Driver
from forktex_flow.runtime.execution import execute_run
from forktex_flow.runtime.registry import FlowRegistry

__all__ = [
    "Ctx",
    "Driver",
    "FlowRegistry",
    "FlowRuntime",
    "compile_pipeline",
    "compile_scheduled",
    "execute_run",
]
