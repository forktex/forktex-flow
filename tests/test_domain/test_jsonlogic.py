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

"""Gateway conditions are JSON Logic: data a tenant's editor can build and flow can store, evaluated the same way
on every worker.
"""

from __future__ import annotations

import pytest
from forktex.error import BadRequestError

from forktex_flow.domain.jsonlogic import check_rule, evaluate, truthy

VARIABLES = {
    "amount": 1500,
    "currency": "EUR",
    "review": {"decision": "escalate", "notes": []},
    "tags": ["vip", "overdue"],
}


@pytest.mark.parametrize(
    ("rule", "expected"),
    [
        ({"var": "amount"}, 1500),
        ({"var": "review.decision"}, "escalate"),
        ({"var": "tags.1"}, "overdue"),
        ({"var": ["missing.path", "fallback"]}, "fallback"),
        ({"==": [{"var": "currency"}, "EUR"]}, True),
        ({"==": [{"var": "amount"}, 1500.0]}, True),
        ({"!=": [{"var": "currency"}, "RON"]}, True),
        ({">": [{"var": "amount"}, 1000]}, True),
        ({"<=": [{"var": "amount"}, 1000]}, False),
        ({"<": [1000, {"var": "amount"}, 2000]}, True),
        ({"and": [{">": [{"var": "amount"}, 1000]}, {"in": ["vip", {"var": "tags"}]}]}, True),
        ({"or": [{"var": "nope"}, {"var": "currency"}]}, "EUR"),
        ({"!": {"var": "review.notes"}}, True),
        ({"!!": {"var": "tags"}}, True),
        ({"in": ["UR", {"var": "currency"}]}, True),
        (
            {"if": [{">": [{"var": "amount"}, 5000]}, "board", {">": [{"var": "amount"}, 1000]}, "manager", "auto"]},
            "manager",
        ),
        ({"missing": ["amount", "approver", "review.decision"]}, ["approver"]),
    ],
)
def test_a_rule_evaluates_against_the_variables(rule, expected):
    assert evaluate(rule, VARIABLES) == expected


def test_equality_is_strict_about_type():
    """JavaScript's loose `==` would route an instance holding the string "1500" down the same branch as 1500."""
    assert evaluate({"==": ["1500", 1500]}, {}) is False
    assert evaluate({"==": [True, 1]}, {}) is False


def test_comparing_incomparable_values_is_false_rather_than_an_error():
    """A variable of the wrong type sends the gateway to its default instead of failing the instance."""
    assert evaluate({">": [{"var": "currency"}, 10]}, VARIABLES) is False


def test_an_empty_list_is_false():
    assert truthy([]) is False
    assert truthy([0]) is True


def test_an_unsupported_operator_is_named():
    with pytest.raises(BadRequestError, match="'cat' is not supported"):
        check_rule({"and": [{"var": "a"}, {"cat": ["a", "b"]}]})


def test_a_rule_with_two_operators_is_rejected():
    with pytest.raises(BadRequestError, match="exactly one operator"):
        check_rule({"var": "a", "==": [1, 1]})
