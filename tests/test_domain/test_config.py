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

"""`RuntimeConfig`'s guards.

Each of these rejects a configuration that would fail later, at a time and place
that would not point back at the setting responsible.
"""

from __future__ import annotations

import pytest
from forktex.error import BadRequestError

from forktex_flow import RuntimeConfig


def test_the_defaults_are_valid():
    """Constructing with nothing must work — every consumer that does not tune
    anything relies on it."""
    config = RuntimeConfig()
    assert config.max_attempts == 3
    assert config.backoff == (30.0, 120.0, 300.0)
    assert config.stale_threshold > config.heartbeat_interval


def test_stale_threshold_must_exceed_heartbeat_interval():
    """The one guard that prevents a correctness bug rather than a silly value.

    A step heartbeats every `heartbeat_interval`. If a step is considered stale
    at or before that mark, the reclaim scan finds a step whose heartbeat is
    merely *due* and hands it to a second worker — one step body running twice,
    concurrently, with no error anywhere.
    """
    with pytest.raises(BadRequestError, match="stale_threshold"):
        RuntimeConfig(heartbeat_interval=10.0, stale_threshold=10.0)
    with pytest.raises(BadRequestError, match="stale_threshold"):
        RuntimeConfig(heartbeat_interval=30.0, stale_threshold=5.0)


def test_max_attempts_must_allow_at_least_one_try():
    with pytest.raises(BadRequestError, match="max_attempts"):
        RuntimeConfig(max_attempts=0)


def test_backoff_must_not_be_empty():
    """An empty tuple has no last entry to repeat, so the retry path would index
    into nothing on the first failure."""
    with pytest.raises(BadRequestError, match="backoff"):
        RuntimeConfig(backoff=())


def test_election_interval_must_be_positive():
    """Zero would make a losing worker re-poll the advisory lock without pause —
    every non-leader hammering Postgres for a lock it cannot have."""
    with pytest.raises(BadRequestError, match="election_interval"):
        RuntimeConfig(election_interval=0)


def test_poll_interval_must_be_positive():
    """Zero or negative turns the driver's wait into a busy loop against Postgres."""
    with pytest.raises(BadRequestError, match="poll_interval"):
        RuntimeConfig(poll_interval=0)


def test_a_valid_tuned_config_is_accepted():
    config = RuntimeConfig(
        poll_interval=0.25,
        election_interval=0.1,
        heartbeat_interval=2.0,
        stale_threshold=15.0,
        max_attempts=5,
        backoff=(1.0, 2.0),
        leader_lock_key=42,
    )
    assert config.max_attempts == 5
    assert config.leader_lock_key == 42
    assert config.election_interval == 0.1


def test_the_config_is_frozen():
    """A value object: the driver reads these on every tick, and a mutation
    mid-run would change behaviour without any record of it."""
    config = RuntimeConfig()
    with pytest.raises(Exception):
        config.max_attempts = 99


def test_the_retry_defaults_have_exactly_one_home():
    """`RuntimeConfig` is where the default retry contract lives.

    It was written in three places — here, `domain/node.py` and
    `runtime/compiler.py` — and the two copies were the ones actually read, so a
    consumer setting `RuntimeConfig(max_attempts=7)` got steps with 3. This
    asserts the copies are gone rather than merely currently agreeing.
    """
    import pathlib

    src = pathlib.Path(__file__).resolve().parents[2] / "src" / "forktex_flow"
    offenders = []
    for path in sorted(src.rglob("*.py")):
        if path.name == "config.py":
            continue
        for lineno, line in enumerate(path.read_text().splitlines(), 1):
            if "_DEFAULT_MAX_ATTEMPTS" in line or "_DEFAULT_BACKOFF" in line:
                offenders.append(f"{path.relative_to(src)}:{lineno}")
    assert offenders == [], "the retry defaults have a second home:\n  " + "\n  ".join(offenders)


def test_a_bare_step_takes_its_retry_contract_from_the_config_defaults():
    """The decorator's fallback and the engine's configured default are the same
    numbers, so tuning one does not leave the other behind."""
    from forktex_flow import step
    from forktex_flow.domain.node import step_meta

    @step
    async def undecorated_defaults(ctx: object, state: dict) -> dict:
        return {}

    meta = step_meta(undecorated_defaults)
    config = RuntimeConfig()
    assert meta is not None
    assert meta.max_attempts == config.max_attempts
    assert meta.backoff == config.backoff
