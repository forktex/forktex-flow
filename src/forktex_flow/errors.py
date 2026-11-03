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

"""Everything ``forktex_flow`` raises."""

from __future__ import annotations

from forktex.error import AppError, AppErrorCode


class FlowError(AppError):
    """Base for any error the library itself raises."""

    code = AppErrorCode.INTERNAL


class WorkflowFailed(FlowError):
    """An instance reached ``failed``; ``after_fail`` hooks receive it, carrying the instance's error."""

    code = AppErrorCode.FAILED


class WorkflowCancelled(FlowError):
    """Raised by a handler to cancel its instance, with the message as the cancel reason."""

    code = AppErrorCode.CANCELLED


class FlowClosed(FlowError):
    """A call reached a ``Flow`` after ``close()``."""

    code = AppErrorCode.FAILED


class StateNotSerialisable(FlowError):
    """Variables that JSONB cannot hold."""

    code = AppErrorCode.VALIDATION


__all__ = [
    "FlowClosed",
    "FlowError",
    "StateNotSerialisable",
    "WorkflowCancelled",
    "WorkflowFailed",
]
