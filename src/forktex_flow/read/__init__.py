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

"""Flow's read layer: the fluent finder and the SQL it compiles to.

`instance` is the user-facing surface (`InstanceQuery`, `WorkflowInstance`);
`query` compiles a `RunFilter` into SQL and maps rows onto a page. Split because
one is a vocabulary a consumer holds and the other is machinery it never sees.
"""

from forktex_flow.read.instance import (
    InstancePage,
    InstanceQuery,
    NodeInstance,
    WorkflowInstance,
    bind_instance,
)
from forktex_flow.read.query import count_runs, fetch_page, summarise_runs

__all__ = [
    "InstancePage",
    "InstanceQuery",
    "NodeInstance",
    "WorkflowInstance",
    "bind_instance",
    "count_runs",
    "fetch_page",
    "summarise_runs",
]
