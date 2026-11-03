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

"""Flow's execution machinery. `compiler` turns the decorators into definitions, `registry` holds a Flow's handlers,
`engine` moves an instance's token, `worker` runs one job, `driver` claims jobs and leads, and `ctx` is the handle a
handler is given.
"""

from forktex_flow.runtime.compiler import compile_pipeline
from forktex_flow.runtime.context import FlowRuntime
from forktex_flow.runtime.ctx import Ctx
from forktex_flow.runtime.driver import Driver
from forktex_flow.runtime.engine import Advance
from forktex_flow.runtime.registry import CodeDefinition, FlowRegistry, Handler
from forktex_flow.runtime.worker import run_job

__all__ = [
    "Advance",
    "CodeDefinition",
    "Ctx",
    "Driver",
    "FlowRegistry",
    "FlowRuntime",
    "Handler",
    "compile_pipeline",
    "run_job",
]
