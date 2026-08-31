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

"""Execution tuning — the knobs the driver and executor read.

A frozen value object rather than attributes on ``Flow``, because it is the
whole reason the lower layers used to take the facade: they needed a handful of
numbers and took the entire engine to get them. Pure data, no I/O, so it sits at the
bottom of the DAG where every layer can read it.
"""

from __future__ import annotations

from forktex.error import BadRequestError
from forktex.types import BaseValueObject
from pydantic import model_validator


class RuntimeConfig(BaseValueObject):
    """How hard the driver polls, and how patient it is with a stalled step.

    Attributes:
        poll_interval:      Seconds between driver ticks when it holds the lock.
        election_interval:  Seconds a worker waits before standing for election
                            again after losing one. Bounds how long a takeover
                            takes after the leader dies: too long and work sits
                            idle, too short and every worker polls the lock.
        heartbeat_interval: Seconds between heartbeat writes on a running step.
        stale_threshold:    Seconds without a heartbeat after which a step is
                            considered abandoned and may be reclaimed. Must
                            exceed ``heartbeat_interval``, or a healthy step
                            gets reclaimed out from under itself.
        max_attempts:       Default attempts per step, when ``@step`` does not
                            override it.
        backoff:            Default per-attempt delay in seconds; the last entry
                            is reused once attempts run past its length.
        leader_lock_key:    The advisory-lock key the driver competes on.
    """

    poll_interval: float = 1.0
    election_interval: float = 2.0
    heartbeat_interval: float = 10.0
    stale_threshold: float = 60.0
    max_attempts: int = 3
    backoff: tuple[float, ...] = (30.0, 120.0, 300.0)
    leader_lock_key: int = 0

    @model_validator(mode="after")
    def _check(self) -> RuntimeConfig:
        if self.max_attempts < 1:
            raise BadRequestError("max_attempts must be >= 1")
        if not self.backoff:
            raise BadRequestError("backoff must be a non-empty tuple")
        if self.poll_interval <= 0:
            raise BadRequestError("poll_interval must be > 0")
        if self.election_interval <= 0:
            raise BadRequestError("election_interval must be > 0")
        if self.stale_threshold <= self.heartbeat_interval:
            # Otherwise the reclaim scan finds a step whose heartbeat is merely
            # *due* and hands it to a second worker, which is how one step body
            # ends up running twice.
            raise BadRequestError(
                f"stale_threshold ({self.stale_threshold}) must exceed heartbeat_interval ({self.heartbeat_interval})"
            )
        return self


__all__ = ["RuntimeConfig"]
