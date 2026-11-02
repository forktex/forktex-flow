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
import re
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
            stripped = line.split("#", 1)[-1].strip() if "#" in line else ""
            if stripped.startswith(("noqa", "type: ignore")):
                found.append(str(path.relative_to(SRC)))
    assert found == ["flow.py"], f"unexpected lint exemptions in src/: {found}"


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
                if _is_declaration(stripped):
                    name = getattr(node, "name", "<module>")
                    offenders.append(f"{path.relative_to(SRC)} in {name}() docstring line {lineno}: {stripped[:50]}")
    assert offenders == [], "a declaration is buried in a docstring:\n  " + "\n  ".join(offenders)


def _is_declaration(line: str) -> bool:
    """Whether a docstring line parses as an import or an `__all__` assignment.

    Matching on a prefix cannot tell a declaration from prose that happens to
    begin with one of these words — "import direction a layered package
    forbids" is a sentence. Parsing it answers exactly.
    """
    try:
        parsed = ast.parse(line)
    except SyntaxError:
        return False
    return any(
        isinstance(node, ast.Import | ast.ImportFrom)
        or (isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == "__all__" for t in node.targets))
        for node in parsed.body
    )


def test_the_status_vocabulary_agrees_with_the_migration():
    """`RunStatus` and the CHECK constraint in the migration.

    The ORM's constraint is rendered from `RunStatus`, so those two cannot
    drift. The SQL cannot be generated from it — an applied migration is frozen
    byte-for-byte by `test_migration_immutability.py`, correctly — so adding a
    status to the type needs an incremental migration, and this is what says so
    before the database rejects a row.
    """
    import re
    from typing import get_args

    from forktex_flow.domain.types import RunStatus

    declared = set(get_args(RunStatus))

    # Every migration, not a named one: the baseline is a file per table, so
    # the status constraints live in whichever files declare a status column.
    sql = "\n".join(
        path.read_text(encoding="utf-8") for path in sorted((SRC / "persist" / "migrations").glob("v*.sql"))
    )
    checks = re.findall(r"CHECK \(status IN \(([^)]*)\)\)", sql)
    assert checks, "no status CHECK constraint found in any migration"
    for clause in checks:
        in_sql = {value.strip().strip("'") for value in clause.split(",")}
        assert in_sql == declared, f"a migration CHECK disagrees with RunStatus: {sorted(in_sql)}"


def test_a_module_declares_its_public_surface_before_its_helpers():
    """`package-layout:public-first`. A reader opening a module meets what it
    offers before what it needs to offer it. Private helpers interleaved with
    the public surface also hide whether a name is API or scaffolding.
    """
    offenders: list[str] = []
    for path in _files():
        tree = ast.parse(path.read_text(encoding="utf-8"))
        first_private: str | None = None
        for node in tree.body:
            if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef):
                continue
            if node.name.startswith("_") and not node.name.startswith("__"):
                first_private = first_private or node.name
            elif first_private and not node.name.startswith("_"):
                offenders.append(
                    f"{path.relative_to(SRC)}: public `{node.name}` is defined after private `{first_private}`"
                )
                break
    assert offenders == [], "public surface must come first:\n  " + "\n  ".join(offenders)


_DOC_STOPWORDS = frozenset(
    "the a an of for to this it its and or on in from with that is are was one by as into at be "
    "not no their which".split()
)


def test_no_docstring_merely_restates_the_name_it_sits_on():
    """A docstring earns its place by saying something the name and signature do
    not. One that paraphrases the name is noise a reader has to read past, and
    it is worse than no docstring: it looks like documentation.

    A declaration whose name and signature already say everything is left bare
    on purpose — that is why this asserts the absence of restatement rather than
    the presence of a docstring.
    """
    offenders: list[str] = []
    for path in _files():
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef):
                continue
            doc = ast.get_docstring(node)
            if not doc:
                continue
            opening = doc.strip().split("\n\n")[0].replace("\n", " ")
            content = [w for w in re.findall(r"[a-z]+", opening.lower()) if w not in _DOC_STOPWORDS]
            if not content or len(content) > 9:
                continue
            from_name = set(re.findall(r"[a-z]+", node.name.lower()))
            echoes = sum(
                1
                for w in content
                if w in from_name or any(w.startswith(part[:4]) for part in from_name if len(part) > 3)
            )
            if echoes / len(content) >= 0.6:
                offenders.append(f"{path.relative_to(SRC)}::{node.name}: {opening!r}")
    assert offenders == [], "these docstrings restate their own name:\n  " + "\n  ".join(offenders)


def test_an_overload_stub_carries_no_docstring():
    """`@overload` stubs exist for the type checker. The implementation holds the
    documentation; a copy on each stub is the same text three times, and they
    drift apart one edit later.
    """
    offenders: list[str] = []
    for path in _files():
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
                continue
            decorators = {ast.unparse(d) for d in node.decorator_list}
            if "overload" in decorators and ast.get_docstring(node):
                offenders.append(f"{path.relative_to(SRC)}::{node.name}")
    assert offenders == [], "an @overload stub carries a docstring:\n  " + "\n  ".join(offenders)


_TESTS = pathlib.Path(__file__).resolve().parents[1]


def test_a_test_whose_body_outgrows_its_name_says_what_it_pins():
    """Most tests here are named as a sentence, and a docstring on top of that
    would restate it — so the convention is a descriptive name and no docstring.
    That holds only while the name still covers the body. Two shapes break it:
    a name too terse to say anything (`test_metadata_containment_filters` over
    six statements), and a body large enough that its setup establishes
    something the name never mentions (`test_since_and_until_bound_the_window`
    over eight). Both are calibrated against those two, and the threshold has to
    stay above the many 6-statement tests whose sentence names do cover them —
    a docstring there would restate the name, which the gate above forbids.
    """
    offenders: list[str] = []
    for path in sorted(_TESTS.rglob("test_*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
                continue
            if not node.name.startswith("test_") or ast.get_docstring(node):
                continue
            statements = [n for n in node.body if not (isinstance(n, ast.Expr) and isinstance(n.value, ast.Constant))]
            words = len(node.name.removeprefix("test_").split("_"))
            terse_name = words <= 4 and len(statements) >= 5
            large_body = len(statements) >= 8 and words <= 6
            if terse_name or large_body:
                offenders.append(
                    f"{path.relative_to(_TESTS)}::{node.name} ({len(statements)} statements, {words}-word name)"
                )
    assert offenders == [], "these tests need a docstring or a longer name:\n  " + "\n  ".join(offenders)


#: A docstring in `src/` survives only by saying one of these — what the call
#: raises or guarantees, when a hook fires, or that something is internal.
#: Anything else belongs in `docs/`, which the pages already carry.
_STATES_A_CONTRACT = re.compile(
    r"\b(rais(e|es|ed)|idempotent|returns?\s+(``?)?(False|True|None)|not durable|not an error|"
    r"never|must |no-op|does not|cannot|safe (to|before)|refus(e|es)|declin(e|es|ed)|append-only|"
    r"unguarded|mutates|internal|positionally|called once)\b",
    re.I,
)


def test_every_docstring_states_a_contract_rather_than_describing_the_code():
    """`docs:essential-only`. The reference package for this codebase carries
    almost no docstrings: semantics live in `docs/`, and the code is read as
    code. A docstring here earns its place by stating what a caller cannot see
    from the name and signature — what it raises, what it guarantees, when a
    hook fires.

    A package `__init__` is exempt: `test_public_surface.py` requires one. An
    error class is exempt: its docstring *is* the raised-when condition.
    """
    offenders: list[str] = []
    for path in _files():
        tree = ast.parse(path.read_text(encoding="utf-8"))
        is_package = path.name == "__init__.py"
        errors = {
            node.name
            for node in ast.walk(tree)
            if isinstance(node, ast.ClassDef)
            and any("Error" in ast.unparse(b) or "Flow" in ast.unparse(b) for b in node.bases)
        }
        for node in ast.walk(tree):
            if not isinstance(node, ast.Module | ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef):
                continue
            doc = ast.get_docstring(node)
            if not doc:
                continue
            if is_package and isinstance(node, ast.Module):
                continue
            if isinstance(node, ast.ClassDef) and node.name in errors and path.name == "errors.py":
                continue
            if not _STATES_A_CONTRACT.search(doc):
                offenders.append(f"{path.relative_to(SRC)}::{getattr(node, 'name', '<module>')}")
    assert offenders == [], (
        "these docstrings describe the code instead of stating a contract — "
        "delete them, or move what they say to docs/:\n  " + "\n  ".join(offenders)
    )


#: Raising the base class says "our invariant broke", and `FlowError` carries
#: `INTERNAL` for that reason. Every other condition gets the leaf that names
#: it. Each entry here is a deliberate invariant check, not a shortcut.
_BASE_ERROR_RAISES = {"runtime/driver.py"}


def test_the_base_error_is_raised_only_where_an_invariant_broke():
    """`errors:name-the-condition`. A caller distinguishes outcomes by error
    class, so a bare `FlowError` where a leaf exists reports "the server is
    broken" for something the caller could have handled.
    """
    found: set[str] = set()
    for path in _files():
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Raise) or node.exc is None:
                continue
            raised = node.exc.func if isinstance(node.exc, ast.Call) else node.exc
            if ast.unparse(raised) == "FlowError":
                found.add(str(path.relative_to(SRC)))
    unexpected = sorted(found - _BASE_ERROR_RAISES)
    assert unexpected == [], "the base error is raised where a leaf should name the condition: " + ", ".join(unexpected)
    stale = sorted(_BASE_ERROR_RAISES - found)
    assert stale == [], f"this allowlist names files that no longer raise FlowError: {stale}"
