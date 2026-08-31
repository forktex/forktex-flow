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

"""The layering rules, checked rather than trusted.

`concept.package-layout` marks `dag-one-way` a blocker and `concept.forktex-substrate`
records that no repo enforces it — "the highest-value gap in the corpus", because
the level-DAG guard went to backlog with the four-level model it encoded.

These are that guard, rebuilt against the layout this package actually has.
"""

from __future__ import annotations

import ast
import pathlib

import pytest

SRC = pathlib.Path(__file__).resolve().parents[2] / "src" / "forktex_flow"

#: The import direction. A module in a layer may import from its own layer and
#: from any layer to its left, never to its right.
LAYERS = ["domain", "persist", "read", "runtime"]

#: Modules at the package root, by role. `flow` is the facade: it may import
#: everything, and only the layer *above* it may import it.
FACADE = "flow"

#: The layer above the facade — modules that compose it rather than being
#: composed by it. `testing` belongs here for the same reason `__init__` does:
#: it is a consumer of the public engine, not a part of it, and it must not be
#: importable *by* anything the facade depends on.
ABOVE_FACADE = {"__init__", "testing"}


def _layer_of(path: pathlib.Path) -> str:
    """Which layer a file belongs to: a directory name, or its root module name."""
    rel = path.relative_to(SRC)
    return rel.parts[0] if rel.parts[0] in LAYERS else rel.stem


def _imported_layer(module: str) -> str | None:
    """Which layer a `forktex_flow...` module name refers to, if any."""
    parts = module.split(".")
    if parts[0] != "forktex_flow" or len(parts) < 2:
        return None
    return parts[1] if parts[1] in LAYERS else parts[1]


def _internal_imports(path: pathlib.Path) -> list[tuple[int, str]]:
    """Every `from forktex_flow... import` in a file, as (lineno, module)."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    found: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module and node.module.startswith("forktex_flow"):
            found.append((node.lineno, node.module))
        elif isinstance(node, ast.Import):
            found.extend((node.lineno, alias.name) for alias in node.names if alias.name.startswith("forktex_flow"))
    return found


def _source_files() -> list[pathlib.Path]:
    return sorted(p for p in SRC.rglob("*.py") if "__pycache__" not in p.parts)


@pytest.mark.parametrize("path", _source_files(), ids=lambda p: str(p.relative_to(SRC)))
def test_imports_never_run_up_the_layers(path: pathlib.Path):
    """`domain -> persist -> read -> runtime`, one way only.

    An upward import is not a style problem: it means the lower layer cannot be
    understood, tested or reused without the higher one, and it is how a package
    ends up needing deferred imports to compile at all.
    """
    source_layer = _layer_of(path)
    if source_layer not in LAYERS:
        return  # root modules are covered by the facade tests below
    source_rank = LAYERS.index(source_layer)

    for lineno, module in _internal_imports(path):
        target = _imported_layer(module)
        if target not in LAYERS:
            continue
        assert LAYERS.index(target) <= source_rank, (
            f"{path.relative_to(SRC)}:{lineno} imports {module!r}: {source_layer} must not import {target}"
        )


@pytest.mark.parametrize(
    "path",
    [p for p in _source_files() if _layer_of(p) == "domain"],
    ids=lambda p: str(p.relative_to(SRC)),
)
def test_domain_performs_no_io(path: pathlib.Path):
    """`domain/` holds vocabulary. It imports nothing that touches a database.

    This is what makes the state reducer and the definition model testable with
    no fixture at all — and it is the property that decays first, because
    reaching for a session inside a value object is always locally convenient.
    """
    forbidden = {"sqlalchemy", "asyncpg", "forktex.database", "forktex_flow.persist"}
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        names = (
            [alias.name for alias in node.names]
            if isinstance(node, ast.Import)
            else [node.module or ""]
            if isinstance(node, ast.ImportFrom)
            else []
        )
        for name in names:
            assert not any(name == f or name.startswith(f + ".") for f in forbidden), (
                f"{path.relative_to(SRC)}:{node.lineno} imports {name!r}; domain/ must not perform I/O"
            )


def test_only_dunder_init_imports_the_facade():
    """The facade imports everything and is imported by nothing but `__init__`.

    Nine modules used to import `Flow` under TYPE_CHECKING, because functions in
    every lower layer took the whole engine to reach two of its attributes. That
    is the shape this asserts against.

    `testing` is exempt by role, not by exception: it composes the facade for a
    consumer's test suite, the same relationship `__init__` has. Nothing the
    facade depends on may import either.
    """
    offenders: list[str] = []
    for path in _source_files():
        if path.parent == SRC and (path.stem == FACADE or path.stem in ABOVE_FACADE):
            continue
        for lineno, module in _internal_imports(path):
            if module in {f"forktex_flow.{FACADE}", "forktex_flow"}:
                offenders.append(f"{path.relative_to(SRC)}:{lineno} imports {module}")
    assert offenders == [], "only __init__.py may import the facade:\n  " + "\n  ".join(offenders)


def test_cycle_debt_is_zero():
    """No import is deferred inside a function to break a cycle.

    `package-layout:flat-until-cycles` makes this number the trigger for a
    layered layout, so a layered package that still carries debt has bought the
    directories without buying the property they exist for. A ratchet at zero,
    not a budget: the first one to appear is the one to fix.
    """
    debt: list[str] = []
    for path in _source_files():
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
                continue
            for inner in ast.walk(node):
                if isinstance(inner, ast.ImportFrom) and (inner.module or "").startswith("forktex_flow"):
                    debt.append(f"{path.relative_to(SRC)}:{inner.lineno} in {node.name}()")
                elif isinstance(inner, ast.Import):
                    debt.extend(
                        f"{path.relative_to(SRC)}:{inner.lineno} in {node.name}()"
                        for alias in inner.names
                        if alias.name.startswith("forktex_flow")
                    )
    assert debt == [], "function-local self-imports (cycle debt) must stay at 0:\n  " + "\n  ".join(debt)
