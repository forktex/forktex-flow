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

"""Verify the repo is coherent about which version it is about to publish.

    python scripts/release_check.py            # version vs CHANGELOG
    python scripts/release_check.py v0.1.0     # ...and vs the git tag

Three ways a publish goes out wrong that nothing else here catches: the tag
disagrees with `pyproject.toml`, the version being shipped has no CHANGELOG
section, or real changes are still filed under `[Unreleased]` and therefore
absent from the notes for the release that contains them. A PyPI version is
immutable, so each of these is unfixable after the upload.

This lives in a script rather than inline in the publish workflow so it can be
run before pushing a tag (`gate:ci-is-one-command`).
"""

from __future__ import annotations

import re
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _declared_version() -> str:
    with (ROOT / "pyproject.toml").open("rb") as handle:
        return str(tomllib.load(handle)["project"]["version"])


def _changelog_sections(text: str) -> dict[str, str]:
    """Map each ``## [name]`` heading to the body under it."""
    sections: dict[str, str] = {}
    current: str | None = None
    for line in text.split("\n"):
        heading = re.match(r"^##\s+\[([^\]]+)\]", line)
        if heading:
            current = heading.group(1)
            sections[current] = ""
        elif current is not None:
            sections[current] += line + "\n"
    return sections


def _dist_problems(version: str) -> list[str]:
    """Confirm dist/ holds exactly this version's artefacts, and nothing else.

    `make publish` uploads `dist/*` verbatim. `make build` clears the directory
    first, so the two agree — until someone bumps the version and publishes
    without rebuilding, which uploads the previous version's wheel under a
    release everyone believes is the new one. PyPI will not let it be replaced.
    """
    dist = ROOT / "dist"
    if not dist.is_dir():
        return ["dist/ does not exist — run `make build`"]

    artefacts = sorted(p.name for p in dist.iterdir() if p.suffix in {".whl", ".gz"})
    if not artefacts:
        return ["dist/ is empty — run `make build`"]

    # Wheels normalise `-` to `_`; sdists follow the same rule for the name.
    stem = f"{version}"
    stale = [name for name in artefacts if f"-{stem}-" not in name and f"-{stem}." not in name]
    if stale:
        return [
            f"dist/ holds artefacts that are not version {version}: {', '.join(stale)}. "
            "Run `make build` — publishing uploads dist/ as it stands."
        ]
    return []


def check(tag: str | None) -> list[str]:
    version = _declared_version()
    problems: list[str] = []

    if tag is not None:
        expected = tag.removeprefix("refs/tags/").removeprefix("v")
        if expected != version:
            problems.append(f"tag {expected!r} does not match pyproject version {version!r}")

    sections = _changelog_sections((ROOT / "CHANGELOG.md").read_text(encoding="utf-8"))

    if version not in sections:
        problems.append(f"CHANGELOG.md has no '## [{version}]' section")

    unreleased = sections.get("Unreleased", "").strip()
    if unreleased:
        problems.append(
            "CHANGELOG.md still has entries under '## [Unreleased]'. Move them under "
            f"'## [{version}]' — a published version whose changes are filed as "
            "unreleased cannot be corrected afterwards."
        )

    problems.extend(_dist_problems(version))

    return problems


def main() -> int:
    tag = sys.argv[1] if len(sys.argv) > 1 else None
    problems = check(tag)
    if problems:
        print("release-check FAILED:")
        for problem in problems:
            print(f"  - {problem}")
        return 1
    print(f"release-check OK: {_declared_version()} is declared, dated, described and built.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
