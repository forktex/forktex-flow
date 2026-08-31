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

"""Exception hierarchy for ``forktex_flow``.

Every public class here is an ``AppError`` subclass, so a transport that already
knows how to render an ``AppError`` — notably ``forktex.api``'s error-envelope
middleware — renders these too, with the right code instead of a masked 500.
Each leaf sets the ``code`` that carries its meaning; ``AppError.__init__(message,
*, details=None)`` is inherited unchanged.

Two of these are internal control flow rather than reportable failures
(:class:`StepClaimLost`, :class:`StepRetryRequested`). They live here anyway,
because a reader looking for "what can this package raise" must find all of it in
one file, and they are kept out of ``__all__`` so they are not part of the
published surface.
"""

from __future__ import annotations

from forktex.error import AppError, AppErrorCode


class FlowError(AppError):
    """Base for any error originating from the flow library itself —
    registration, schema, driver, execution. Distinct from errors raised by user
    step code, which the library captures as :class:`StepFailed`."""

    code = AppErrorCode.INTERNAL


class StepFailed(FlowError):
    """A step's body raised an exception that exhausted its retries.

    The original cause is chained via ``__cause__``. The step's row in
    ``forktex_flow.step_run`` carries the truncated traceback and the final
    attempt count.

    ``FAILED`` rather than ``INTERNAL``: the workflow failing is a real outcome
    the caller may need to distinguish from the server breaking.
    """

    code = AppErrorCode.FAILED


class WorkflowFailed(FlowError):
    """A run reached terminal ``failed`` status, typically because a step inside
    it raised :class:`StepFailed`. Surfaced from ``WorkflowInstance.wait()`` for
    callers that want a synchronous result."""

    code = AppErrorCode.FAILED


class WorkflowCancelled(FlowError):
    """A run was cancelled through ``WorkflowInstance.cancel()``."""

    code = AppErrorCode.CANCELLED


class StepClaimLost(FlowError):
    """A worker tried to advance a step row it no longer owns.

    Raised when the conditional claim update matches no row — another worker
    reclaimed the step after this one's heartbeat went stale. The correct
    response is to abandon the attempt silently, not to fail the run: the step
    is already running somewhere else. Internal; not exported.
    """

    code = AppErrorCode.CONFLICT


class StepRetryRequested(FlowError):
    """A step body failed with retries remaining.

    Control flow, not a report: the executor catches it, marks the run
    ``pending``, and the driver re-picks it after the step's backoff window.
    Internal; not exported.
    """

    code = AppErrorCode.FAILED


__all__ = [
    "FlowError",
    "StepFailed",
    "WorkflowCancelled",
    "WorkflowFailed",
]
