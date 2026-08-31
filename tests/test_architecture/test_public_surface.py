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

"""What the package publishes, and what it promises about it."""

from __future__ import annotations

import ast
import importlib
import pathlib

import pytest

SRC = pathlib.Path(__file__).resolve().parents[2] / "src" / "forktex_flow"

#: Every package a consumer may import by name. Written out rather than derived:
#: this list IS the shipped contract, so adding a package should be a deliberate
#: edit here, not something a directory listing quietly grants.
PACKAGES = ["", "domain", "persist", "read", "runtime"]

#: The root's exports, frozen. A change here is an API change — that is the
#: point of asserting the exact set rather than merely that one exists.
ROOT_SURFACE = {
    "TERMINAL_STATUSES",
    "Ctx",
    "Flow",
    "FlowError",
    "FlowExtension",
    "InstancePage",
    "InstanceQuery",
    "NodeInstance",
    "RunInfo",
    "RunStatus",
    "RunSummary",
    "RuntimeConfig",
    "StepFailed",
    "StepRunInfo",
    "StepSpec",
    "StepStatus",
    "WorkflowCancelled",
    "WorkflowFailed",
    "WorkflowInstance",
    "apply_migrations",
    "apply_state_update",
    "extract_reducers",
    "isolated_flow",
    "make_ctx",
    "run_to_completion",
    "step",
}


def _module(package: str):
    return importlib.import_module(f"forktex_flow.{package}" if package else "forktex_flow")


@pytest.mark.parametrize("package", PACKAGES)
def test_every_package_declares_a_non_empty_all(package: str):
    module = _module(package)
    assert hasattr(module, "__all__"), f"forktex_flow.{package} has no __all__"
    assert module.__all__, f"forktex_flow.{package}.__all__ is empty"


@pytest.mark.parametrize("package", PACKAGES)
def test_no_package_exports_a_private_name(package: str):
    """A name in `__all__` is public by definition, so spelling it with a
    leading underscore tells the reader the opposite of the truth."""
    private = sorted(n for n in _module(package).__all__ if n.startswith("_"))
    assert private == [], f"forktex_flow.{package} exports private-looking names: {private}"


@pytest.mark.parametrize("package", PACKAGES)
def test_every_exported_name_resolves(package: str):
    module = _module(package)
    missing = sorted(n for n in module.__all__ if not hasattr(module, n))
    assert missing == [], f"forktex_flow.{package}.__all__ names unimportable attributes: {missing}"


@pytest.mark.parametrize("package", PACKAGES)
def test_every_package_has_a_docstring(package: str):
    assert (_module(package).__doc__ or "").strip(), f"forktex_flow.{package} has no module docstring"


def test_root_surface_is_exactly_what_is_documented():
    """The frozen set. A deliberate addition edits this test; an accidental one
    fails it, which is the only difference that matters."""
    import forktex_flow

    assert set(forktex_flow.__all__) == ROOT_SURFACE


def test_every_public_module_declares_all():
    """Not just the packages — every non-private module too, since a consumer
    can import `forktex_flow.read.query` as readily as `forktex_flow.read`."""
    missing: list[str] = []
    for path in sorted(SRC.rglob("*.py")):
        if "__pycache__" in path.parts or path.stem.startswith("_") and path.stem != "__init__":
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"))
        declared = any(
            isinstance(node, ast.Assign) and any(getattr(t, "id", None) == "__all__" for t in node.targets)
            for node in tree.body
        )
        if not declared:
            missing.append(str(path.relative_to(SRC)))
    assert missing == [], f"modules without __all__: {missing}"


def test_the_migrations_are_packaged_as_data():
    """The .sql files are not importable code, so nothing fails at import time
    if they are missing from the wheel — `Flow.init()` just brings up a schema
    with no tables. Assert they are where the runner looks."""
    migrations = sorted((SRC / "persist" / "migrations").glob("v*.sql"))
    assert migrations, "no v*.sql migrations found"
    assert migrations[0].name == "v0001__schema.sql"
