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

import pytest
from forktex.error import BadRequestError

from forktex_flow import RuntimeConfig


def test_the_defaults_are_valid():
    config = RuntimeConfig()
    assert (config.max_attempts, config.backoff) == (3, (30.0, 120.0, 300.0))


@pytest.mark.parametrize(
    ("settings", "named"),
    [
        ({"heartbeat_interval": 10.0, "stale_threshold": 10.0}, "stale_threshold"),
        ({"max_attempts": 0}, "max_attempts"),
        ({"backoff": ()}, "backoff"),
        ({"election_interval": 0}, "election_interval"),
        ({"poll_interval": 0}, "poll_interval"),
        ({"max_concurrent_jobs": 0}, "max_concurrent_jobs"),
        ({"drain_timeout": 0}, "drain_timeout"),
    ],
)
def test_a_setting_that_cannot_work_is_refused_by_name(settings: dict, named: str):
    with pytest.raises(BadRequestError, match=named):
        RuntimeConfig(**settings)


def test_the_config_is_frozen():
    config = RuntimeConfig()
    with pytest.raises(Exception):
        config.max_attempts = 99
