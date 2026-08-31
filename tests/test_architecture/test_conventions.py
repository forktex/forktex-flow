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

"""Conventions the corpus names, checked here so they cannot quietly lapse."""

from __future__ import annotations

import ast
import pathlib

import pytest

from forktex.error import AppError

SRC = pathlib.Path(__file__).resolve().parents[2] / "src" / "forktex_flow"


def _files() -> list[pathlib.Path]:
    return sorted(p for p in SRC.rglob("*.py") if "__pycache__" not in p.parts)


def test_every_error_class_lives_in_errors_py():
    """`package-layout:same-concern-same-name`. A reader who has learned one
    package has learned where to look in the next."""
    strays: list[str] = []
    for path in _files():
        if path.name == "errors.py":
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in tree.body:
            if isinstance(node, ast.ClassDef) and node.name.endswith(("Error", "Failed", "Cancelled", "Lost")):
                strays.append(f"{path.relative_to(SRC)}:{node.lineno} {node.name}")
    assert strays == [], "error classes must live in errors.py:\n  " + "\n  ".join(strays)


def test_every_error_derives_from_app_error():
    """`errors:code-not-status`. An exception that is not an `AppError` carries
    no code, so a transport can only render it as a 500."""
    import forktex_flow.errors as errors_module

    classes = [
        getattr(errors_module, name)
        for name in dir(errors_module)
        if isinstance(getattr(errors_module, name), type) and issubclass(getattr(errors_module, name), BaseException)
    ]
    wrong = [c.__name__ for c in classes if c is not AppError and not issubclass(c, AppError)]
    assert wrong == [], f"errors must derive from forktex.error.AppError: {wrong}"


def test_no_public_function_raises_a_bare_builtin_exception():
    """`errors:raise-the-leaf`. `raise ValueError(...)` from a library reaches a
    caller as an unclassifiable 500; the leaf that names the condition does not."""
    bare = {"RuntimeError", "ValueError", "TypeError", "Exception", "NotImplementedError", "KeyError"}
    offenders: list[str] = []
    for path in _files():
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Raise) or node.exc is None:
                continue
            call = node.exc
            name = (
                call.func.id
                if isinstance(call, ast.Call) and isinstance(call.func, ast.Name)
                else call.id
                if isinstance(call, ast.Name)
                else None
            )
            if name in bare:
                offenders.append(f"{path.relative_to(SRC)}:{node.lineno} raises {name}")
    assert offenders == [], "raise the leaf that names the condition:\n  " + "\n  ".join(offenders)


def test_no_non_v7_uuid_constructor():
    """`python:uuid7-never-sliced`. A v4 primary key writes into random leaves of
    the index instead of appending to it."""
    offenders: list[str] = []
    for path in _files():
        for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            for bad in ("uuid1(", "uuid3(", "uuid4(", "uuid5(", "gen_random_uuid"):
                if bad in line:
                    offenders.append(f"{path.relative_to(SRC)}:{lineno} {bad}")
    assert offenders == [], "every id is a full uuid7:\n  " + "\n  ".join(offenders)


def test_no_uuid_is_sliced():
    """v7 front-loads the timestamp, so `hex[:8]` yields one distinct value per
    millisecond rather than four billion."""
    offenders: list[str] = []
    for path in _files():
        for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if ".hex[" in line or "uuid7())[" in line:
                offenders.append(f"{path.relative_to(SRC)}:{lineno}")
    assert offenders == [], "a uuid is never sliced:\n  " + "\n  ".join(offenders)


def test_src_carries_no_lint_exemptions_except_the_documented_one():
    """`substrate:no-lint-exemptions-in-src`. One `type: ignore` survives, on the
    `database_url`/`database` either-or that the type system cannot express but
    the constructor guards; anything new is a prompt to name the type."""
    found: list[str] = []
    for path in _files():
        for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            # An actual trailing pragma, not the phrase appearing in prose:
            # a docstring explaining why a helper exists "without a per-site
            # type: ignore" is the opposite of an exemption.
            stripped = line.split("#", 1)[-1].strip() if "#" in line else ""
            if stripped.startswith(("noqa", "type: ignore")):
                found.append(f"{path.relative_to(SRC)}:{lineno}")
    assert found == ["flow.py:143"], f"unexpected lint exemptions in src/: {found}"


@pytest.mark.parametrize("path", _files(), ids=lambda p: str(p.relative_to(SRC)))
def test_no_todo_comments(path: pathlib.Path):
    """`clean:no-what-comments`. A TODO is a decision deferred into a place
    nobody reads; it belongs in the ledger or in the code."""
    for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        for marker in ("TODO", "FIXME", "XXX"):
            assert marker not in line, f"{path.relative_to(SRC)}:{lineno} carries a {marker}"


def test_raw_sql_appears_only_as_server_default():
    """`database:no-sql-strings`. `sa.text` is allowed for a server_default
    literal and an index predicate — nowhere else."""
    offenders: list[str] = []
    for path in _files():
        for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if "exec_driver_sql" in line:
                offenders.append(f"{path.relative_to(SRC)}:{lineno} exec_driver_sql")
            elif "sa.text(" in line and "server_default" not in line:
                offenders.append(f"{path.relative_to(SRC)}:{lineno} sa.text outside server_default")
    assert offenders == [], "statements are constructs, not strings:\n  " + "\n  ".join(offenders)


def test_no_docstring_contains_a_declaration():
    """A docstring holding `__all__ = [...]` or an import is a scripted edit that
    landed inside the wrong triple-quote.

    One did: an `__all__` block was written into the body of
    `update_run_output`'s docstring, where every gate ignored it — ruff sees a
    string, pyright sees a string, the tests never read it. The only thing that
    catches it is looking, so this looks.
    """
    offenders: list[str] = []
    for path in _files():
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Module | ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef):
                continue
            doc = ast.get_docstring(node)
            if not doc:
                continue
            for lineno, line in enumerate(doc.splitlines(), 1):
                stripped = line.strip()
                if stripped.startswith(("__all__", "import ", "from ")) and " import " not in stripped[:5]:
                    name = getattr(node, "name", "<module>")
                    offenders.append(f"{path.relative_to(SRC)} in {name}() docstring line {lineno}: {stripped[:50]}")
    assert offenders == [], "a declaration is buried in a docstring:\n  " + "\n  ".join(offenders)


def test_the_status_vocabulary_agrees_across_its_three_homes():
    """`RunStatus`, the ORM's CHECK constraint, and the migration SQL.

    Three restatements of one list, and they must agree. They cannot be reduced
    to one: the SQL is frozen byte-for-byte by
    `test_migration_immutability.py`, correctly — an applied migration is never
    edited — so the Python side cannot generate it. What can be done is refuse
    to let them drift, which is what this asserts. The same shape of duplication
    (retry defaults in three places) already shipped a real defect.
    """
    import re
    from typing import get_args

    from forktex_flow.domain.types import RunStatus
    from forktex_flow.persist.models import _STATUSES

    declared = set(get_args(RunStatus))
    assert set(_STATUSES) == declared, f"persist/models._STATUSES disagrees with RunStatus: {_STATUSES}"

    sql = (SRC / "persist" / "migrations" / "v0001__schema.sql").read_text(encoding="utf-8")
    checks = re.findall(r"CHECK \(status IN \(([^)]*)\)\)", sql)
    assert checks, "no status CHECK constraint found in the migration"
    for clause in checks:
        in_sql = {value.strip().strip("'") for value in clause.split(",")}
        assert in_sql == declared, f"a migration CHECK disagrees with RunStatus: {sorted(in_sql)}"
