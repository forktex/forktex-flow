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

"""Shared testcontainer factories.

Postgres only: this library talks to exactly one piece of infrastructure. The
image tag is pinned (not ``:latest``) so CI is reproducible and a breaking
upstream push cannot fail the suite without a deliberate bump here.

``start_postgres`` returns ``(container, url)``. The caller is responsible for
``container.stop()`` on teardown.

The default is the version the suite develops against; ``image`` exists so the
floor the README advertises can be exercised too, rather than asserted.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import urlparse

from sqlalchemy.engine import URL


DEFAULT_IMAGE = "postgres:17-alpine"


def start_postgres(image: str = DEFAULT_IMAGE) -> tuple[Any, URL]:
    """Start Postgres. Returns ``(container, asyncpg URL)``."""
    from testcontainers.postgres import PostgresContainer

    container = PostgresContainer(image)
    container.start()
    raw = container.get_connection_url()
    parsed = urlparse(raw)
    url = URL.create(
        drivername="postgresql+asyncpg",
        username=parsed.username,
        password=parsed.password,
        host=parsed.hostname,
        port=parsed.port,
        database=parsed.path.lstrip("/"),
        query={"ssl": "disable"},
    )
    return container, url
