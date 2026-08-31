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

"""The reducer contract — the part consumers depend on most and notice least."""

from __future__ import annotations

import operator
from typing import Annotated, TypedDict

from forktex_flow import apply_state_update, extract_reducers


class DeployState(TypedDict):
    logs: Annotated[list[str], operator.add]
    summary: Annotated[list[dict], operator.add]
    server_id: str
    attempts: int


def test_extracts_only_callable_annotated_metadata():
    reducers = extract_reducers(DeployState)
    assert reducers == {"logs": operator.add, "summary": operator.add}


def test_a_field_with_no_reducer_is_last_write_wins():
    merged = apply_state_update({"server_id": "old"}, {"server_id": "new"}, extract_reducers(DeployState))
    assert merged["server_id"] == "new"


def test_a_reducer_field_accumulates():
    reducers = extract_reducers(DeployState)
    state = apply_state_update({"logs": ["a"]}, {"logs": ["b"]}, reducers)
    state = apply_state_update(state, {"logs": ["c"]}, reducers)
    assert state["logs"] == ["a", "b", "c"]


def test_a_reducer_is_not_called_on_the_first_write():
    """`operator.add` against a missing key would be `None + [x]`. The first
    write installs the value instead, so a step need not seed every accumulator."""
    assert apply_state_update({}, {"logs": ["a"]}, extract_reducers(DeployState)) == {"logs": ["a"]}


def test_the_input_state_is_not_mutated():
    original = {"logs": ["a"]}
    apply_state_update(original, {"logs": ["b"]}, extract_reducers(DeployState))
    assert original == {"logs": ["a"]}


def test_non_callable_annotated_metadata_is_ignored():
    class Documented(TypedDict):
        note: Annotated[str, "a description, not a reducer"]

    assert extract_reducers(Documented) == {}


def test_an_untyped_state_has_no_reducers():
    assert extract_reducers(None) == {}


def test_unresolvable_annotations_fall_back_to_last_write_wins():
    """A state class whose hints cannot be resolved must degrade to overwrite,
    not raise — failing closed here would take down every run using it."""

    class Broken(TypedDict):
        field: "SomeTypeThatDoesNotExist"  # noqa: F821

    assert extract_reducers(Broken) == {}
