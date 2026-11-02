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

"""Exception hierarchy for ``forktex_flow``. Two are internal control flow rather than reportable failures, and are
kept out of ``__all__``: a reader looking for "what can this package raise" still finds all of it in one file.
"""

from __future__ import annotations

from forktex.error import AppError, AppErrorCode


class FlowError(AppError):
    """Base for any error the library itself raises."""

    code = AppErrorCode.INTERNAL


class StepFailed(FlowError):
    """A step's body raised an exception that exhausted its retries."""

    code = AppErrorCode.FAILED


class WorkflowFailed(FlowError):
    """A run reached terminal ``failed``. Surfaced from ``WorkflowInstance.wait()``."""

    code = AppErrorCode.FAILED


class WorkflowCancelled(FlowError):
    code = AppErrorCode.CANCELLED


class StepClaimLost(FlowError):
    """A worker tried to advance a step row it no longer owns. Internal."""

    code = AppErrorCode.CONFLICT


class FlowClosed(FlowError):
    code = AppErrorCode.FAILED


class StateNotSerialisable(FlowError):
    """A state value cannot be written to JSONB."""

    code = AppErrorCode.VALIDATION


class StepRetryRequested(FlowError):
    """A step body failed with retries remaining. Control flow, internal."""

    code = AppErrorCode.FAILED


__all__ = [
    "FlowError",
    "StepFailed",
    "WorkflowCancelled",
    "WorkflowFailed",
]
