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

from forktex.error import BadRequestError
from forktex.types import BaseValueObject
from pydantic import model_validator


class RuntimeConfig(BaseValueObject):
    """How hard a driver polls and how patient it is with a stalled job. ``stale_threshold`` must exceed
    ``heartbeat_interval``, or a healthy job is reclaimed out from under itself. ``max_attempts`` and ``backoff``
    apply to a service task that declares no ``retries`` or ``backoff`` of its own; ``max_concurrent_jobs`` caps
    the jobs one ``Flow`` runs at once.
    """

    poll_interval: float = 1.0
    election_interval: float = 2.0
    heartbeat_interval: float = 10.0
    stale_threshold: float = 60.0
    max_attempts: int = 3
    max_concurrent_jobs: int = 10
    backoff: tuple[float, ...] = (30.0, 120.0, 300.0)
    leader_lock_key: int = 0
    #: How long stopping the driver waits for running jobs before handing them back. Keep the host's stop grace
    #: period above it.
    drain_timeout: float = 25.0

    @model_validator(mode="after")
    def _check(self) -> RuntimeConfig:
        if self.max_attempts < 1:
            raise BadRequestError("max_attempts must be >= 1")
        if not self.backoff:
            raise BadRequestError("backoff must be a non-empty tuple")
        if self.poll_interval <= 0:
            raise BadRequestError("poll_interval must be > 0")
        if self.max_concurrent_jobs < 1:
            raise BadRequestError("max_concurrent_jobs must be >= 1")
        if self.election_interval <= 0:
            raise BadRequestError("election_interval must be > 0")
        if self.drain_timeout <= 0:
            raise BadRequestError("drain_timeout must be > 0")
        if self.stale_threshold <= self.heartbeat_interval:
            raise BadRequestError(
                f"stale_threshold ({self.stale_threshold}) must exceed heartbeat_interval ({self.heartbeat_interval})"
            )
        return self


__all__ = ["RuntimeConfig"]
