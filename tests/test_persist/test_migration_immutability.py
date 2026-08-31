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

"""An applied migration is never edited.

The failure this prevents is quiet, which is why it needs a test rather than a
convention. Editing a migration that has already run leaves every database that
ran the old version describing a schema this code no longer expects — the
version table says v0001 is applied, so the runner never revisits it, and the
divergence only surfaces later as a column that does not exist. A sibling
project hit exactly that and had to hand-write a bridging migration in a
consumer repo to route around it.

So: to change the schema, add `v0002__…`. To change *this* file, you must have
decided that no database anywhere has run the old content.
"""

from __future__ import annotations

import hashlib
import pathlib

import pytest

MIGRATIONS = pathlib.Path(__file__).resolve().parents[2] / "src" / "forktex_flow" / "persist" / "migrations"

#: sha256 of every migration that has shipped. A new migration adds a line here;
#: an existing line changing means an applied migration was edited.
SHIPPED: dict[str, str] = {
    "v0001__schema.sql": "0f6ea1df4f2047f682c2a02130d4c490ab99aac29ff218d974397d8d8b096a67",
}


def _digest(path: pathlib.Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.mark.parametrize("name", sorted(SHIPPED))
def test_a_shipped_migration_is_byte_for_byte_unchanged(name: str):
    path = MIGRATIONS / name
    assert path.exists(), f"{name} was deleted; a shipped migration cannot be withdrawn"
    assert _digest(path) == SHIPPED[name], (
        f"{name} was edited after shipping.\n"
        "Databases that already applied it will not re-run it, so they now describe a\n"
        "different schema from this file. Add the next vNNNN migration instead.\n"
        "If you are certain no database has run the old content, update the hash here\n"
        f"to: {_digest(path)}"
    )


def test_every_migration_on_disk_is_accounted_for():
    """A new migration must be registered above, so the next edit to it is caught."""
    on_disk = {p.name for p in MIGRATIONS.glob("v*.sql")}
    unregistered = sorted(on_disk - set(SHIPPED))
    assert unregistered == [], f"add these to SHIPPED with their hashes: {unregistered}"


def test_migrations_are_numbered_without_gaps_or_duplicates():
    """The runner applies in numeric order and records each version. A duplicate
    number means one silently shadows the other; a gap usually means a file was
    deleted rather than superseded."""
    versions = sorted(int(name[1:5]) for name in SHIPPED)
    assert versions == list(range(1, len(versions) + 1)), f"migration numbering is not contiguous: {versions}"
