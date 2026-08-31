#!/usr/bin/env python3

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

"""Every committed Python file must parse **on the interpreter the project targets**.

`ruff` and `mypy` skip files they cannot parse, and a module nothing imports is
never executed by the test suite — so a hard `SyntaxError` can sit in the tree
under a green gate.

**Run this with the project interpreter (3.14), never the host `python3`.** The
Makefile invokes it through poetry for exactly this reason: PEP 758
(Python 3.14) allows `except A, B:` without parentheses, and ruff's formatter
rewrites to that form under `target-version = "py314"`. A 3.12 interpreter
reports every one of those lines as a syntax error — a false positive that once
cost a whole debugging session and 21 "fixes" that ruff promptly (and correctly)
reverted.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SKIP = {".venv", "node_modules", "__pycache__", ".git", "build", "dist", ".forktex"}
MIN_VERSION = (3, 14)


def main() -> int:
    if sys.version_info < MIN_VERSION:
        want = ".".join(str(p) for p in MIN_VERSION)
        have = ".".join(str(p) for p in sys.version_info[:3])
        print(
            f"parse-check must run on Python >= {want}; got {have}.\n"
            f"  Run it as: poetry run python scripts/check_parse.py",
            file=sys.stderr,
        )
        return 2

    failures: list[tuple[Path, int | None, str]] = []
    checked = 0
    for path in ROOT.rglob("*.py"):
        if any(part in SKIP for part in path.parts):
            continue
        checked += 1
        try:
            ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except SyntaxError as exc:
            failures.append((path.relative_to(ROOT), exc.lineno, exc.msg))
        except UnicodeDecodeError as exc:
            failures.append((path.relative_to(ROOT), None, f"not valid UTF-8: {exc}"))

    if failures:
        print(f"parse-check FAILED: {len(failures)} of {checked} file(s) do not parse", file=sys.stderr)
        for rel, lineno, msg in failures:
            print(f"  {rel}:{lineno or '?'}: {msg}", file=sys.stderr)
        return 1

    print(f"parse-check OK: {checked} Python files parse on {'.'.join(str(p) for p in sys.version_info[:3])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
