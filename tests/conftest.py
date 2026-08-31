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

"""Session-scoped Postgres, per-test schema.

One container for the whole run — pulling and booting Postgres per test file
would dominate the suite's wall clock. Isolation is at the schema level instead:
each test gets its own `forktex_flow_<uuid7>` schema, brought up by the real
migration runner, so nothing is mocked and nothing is shared.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator, Iterator

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.engine import URL

from forktex_flow import Flow
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
    """A migrated `Flow` on a schema of its own, torn down after the test.

    The driver is NOT started: a test that wants one starts it explicitly, so
    nothing races a background loop it did not ask for.
    """
    schema = "forktex_flow_" + uuid.uuid7().hex
    instance = Flow(postgres_url_str, schema=schema)
    await instance.init()
    try:
        yield instance
    finally:
        async with instance.engine.begin() as conn:
            await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await instance.close()
