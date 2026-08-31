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

"""The documentation names things that exist.

Docs drift silently: nothing fails when a reference page describes a method that
was renamed two releases ago, and the reader who tries it is the one who finds
out. These are the checks that can be made mechanically — that every public name
is documented, and that every name the docs claim is public actually is.
"""

from __future__ import annotations

import pathlib
import re

import pytest

import forktex_flow

ROOT = pathlib.Path(__file__).resolve().parents[2]
README = ROOT / "README.md"
REFERENCE = ROOT / "docs" / "flow.md"
DEVELOPMENT = ROOT / "docs" / "development.md"


@pytest.mark.parametrize("page", [README, REFERENCE, DEVELOPMENT], ids=lambda p: p.name)
def test_the_page_exists_and_is_not_a_stub(page: pathlib.Path):
    assert page.exists(), f"{page.name} is missing"
    assert len(page.read_text(encoding="utf-8").splitlines()) > 40, f"{page.name} looks like a stub"


def test_every_public_name_appears_in_the_reference():
    """`docs/flow.md` is the reference, so a name a consumer can import and
    cannot look up is a hole in it."""
    text = REFERENCE.read_text(encoding="utf-8")
    missing = sorted(name for name in forktex_flow.__all__ if name not in text)
    assert missing == [], f"public names absent from docs/flow.md: {missing}"


def test_the_reference_claims_the_right_surface_size():
    """The reference states a count. A stale number is a small lie that makes the
    reader distrust the rest of the page."""
    text = REFERENCE.read_text(encoding="utf-8")
    stated = re.search(r"(\d+) names, frozen by test", text)
    assert stated is not None, "docs/flow.md no longer states the surface size"
    assert int(stated.group(1)) == len(forktex_flow.__all__)


def test_the_readme_states_the_same_surface_size():
    text = README.read_text(encoding="utf-8")
    stated = re.search(r"(\d+) names, asserted as an exact\s+set", text)
    assert stated is not None, "README no longer states the surface size"
    assert int(stated.group(1)) == len(forktex_flow.__all__)


def test_no_page_documents_a_name_that_was_cut():
    """The removed surface must be described as removed, never demonstrated.
    A code fence showing `flow.graph(...)` would send a reader to a method that
    does not exist."""
    cut = ["flow.graph(", "flow.define(", "step_template(", "ctx.spawn(", "ctx.wait_signal("]
    for page in (README, REFERENCE):
        text = page.read_text(encoding="utf-8")
        for name in cut:
            assert name not in text, f"{page.name} demonstrates the removed {name!r}"


def test_the_readme_python_blocks_reference_only_importable_names():
    """Every `from forktex_flow import …` in the README must actually work."""
    text = README.read_text(encoding="utf-8")
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped.startswith("from forktex_flow import "):
            continue
        for name in stripped.removeprefix("from forktex_flow import ").split(","):
            symbol = name.strip()
            assert hasattr(forktex_flow, symbol), f"README imports {symbol!r}, which does not exist"


def test_the_reference_and_the_readme_do_not_both_own_the_stability_rules():
    """`knowledge:one-source-per-concern`: the stability contract is authored in
    the README. The reference points at it rather than restating it, so the two
    cannot drift into disagreeing about what is promised."""
    reference = REFERENCE.read_text(encoding="utf-8")
    assert "## Stability" not in reference, "the stability contract has a second author in docs/flow.md"


def test_the_readme_does_not_claim_the_substrate_is_unavailable():
    """A retracted claim, gated so it cannot come back.

    `forktex` 0.11.0 is on PyPI and `forktex>=0.11,<1` resolves. It looks
    unavailable to any interpreter below 3.14, because pip silently hides a
    release whose `requires-python` floor the interpreter does not meet — so
    `pip index versions forktex` under 3.12 reports 0.2.4, an older lineage, and
    the dependency appears unsatisfiable.

    That is the same class of mistake as reading PEP 758 syntax with a 3.12
    parser: the tool is right, the interpreter is wrong, and the conclusion is
    confidently false. It reached the README once. Not twice.
    """
    text = README.read_text(encoding="utf-8")
    forbidden = [
        "not yet resolvable",
        "unrelated project",
        "unrelated lineage on PyPI",
        "extra-index-url",
        "private index",
        "cannot resolve",
    ]
    found = [phrase for phrase in forbidden if phrase in text]
    assert found == [], (
        f"the README claims the substrate is unavailable: {found}. "
        "It is on PyPI; check with a 3.14 interpreter before believing otherwise."
    )


def test_the_declared_python_floor_is_stated_where_a_reader_installs():
    """The floor is the whole reason an install can appear to fail, so the
    install section has to name it."""
    text = README.read_text(encoding="utf-8")
    install = text[text.index("## Installing") :]
    assert "3.14" in install.split("## Wiring")[0], "the install section does not name the Python floor"
