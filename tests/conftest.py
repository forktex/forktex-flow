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

"""One Postgres container per session; each test gets a migrated schema of its own."""

from __future__ import annotations

import uuid
from collections.abc import Callable, AsyncIterator, Iterator

import asyncio
import operator
from typing import Annotated, TypedDict

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.engine import URL

from forktex_flow import Flow, RuntimeConfig
from tests._containers import start_postgres


@pytest.fixture(scope="session")
def postgres_url() -> Iterator[URL]:
    container, url = start_postgres()
    yield url
    container.stop()


@pytest.fixture(scope="session")
def postgres_url_str(postgres_url: URL) -> str:
    return postgres_url.render_as_string(hide_password=False)


@pytest_asyncio.fixture
async def flow(postgres_url_str: str) -> AsyncIterator[Flow]:
    """A migrated `Flow` on a schema of its own, not driving, dropped after the test."""
    schema = "forktex_flow_" + uuid.uuid7().hex
    instance = Flow(postgres_url_str, schema=schema)
    await instance.init()
    try:
        yield instance
    finally:
        async with instance.engine.begin() as conn:
            await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await instance.close()


async def until(predicate: Callable[[], bool], *, timeout: float = 10.0, interval: float = 0.02) -> None:
    """Wait for a condition instead of sleeping a guessed interval."""
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError(f"condition still false after {timeout}s")
        await asyncio.sleep(interval)


class LoggingState(TypedDict, total=False):
    """The state most tests use: one reducer-merged list."""

    logs: Annotated[list[str], operator.add]


#: Short enough that a test watching the driver finishes in well under a second.
FAST = RuntimeConfig(
    poll_interval=0.02,
    election_interval=0.02,
    heartbeat_interval=0.05,
    stale_threshold=0.3,
    backoff=(0.05,),
    drain_timeout=2.0,
)


@pytest_asyncio.fixture
async def schema(postgres_url_str: str) -> AsyncIterator[str]:
    """A schema name for tests that build several `Flow`s over one database, dropped afterwards."""
    name = "forktex_flow_" + uuid.uuid7().hex
    yield name
    probe = Flow(postgres_url_str, schema=name)
    async with probe.engine.begin() as conn:
        await conn.execute(text(f'DROP SCHEMA IF EXISTS "{name}" CASCADE'))
    await probe.close()
