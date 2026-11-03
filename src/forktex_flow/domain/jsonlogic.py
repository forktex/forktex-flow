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

"""The JSON Logic subset a gateway condition may use: ``var``, ``missing``, comparisons, ``and``/``or``/``!``/``!!``,
``if`` and ``in``. Equality is strict — ``==`` behaves as ``===`` — so ``"1"`` never equals ``1``. Ordering compares
numbers with numbers and strings with strings; any other pair is false. Truthiness follows JSON Logic: ``0``,
``""``, ``[]``, ``null`` and ``false`` are false.
"""

from __future__ import annotations

from collections.abc import Callable
from itertools import pairwise

from forktex.error import BadRequestError
from forktex.types import JsonValue

__all__ = ["OPERATORS", "check_rule", "evaluate", "truthy"]

#: An operator, given its arguments — already evaluated, unless it is one of `_LAZY` — and the data.
Operator = Callable[[list[JsonValue], JsonValue], JsonValue]


def check_rule(rule: JsonValue) -> None:
    """Raise ``BadRequestError`` naming the first operator this evaluator does not implement."""
    if isinstance(rule, list):
        for item in rule:
            check_rule(item)
        return
    if not isinstance(rule, dict):
        return
    if len(rule) != 1:
        raise BadRequestError(f"a JSON Logic rule has exactly one operator, got {sorted(rule)}")
    ((operator, args),) = rule.items()
    if operator not in OPERATORS:
        raise BadRequestError(f"JSON Logic operator {operator!r} is not supported; use one of {sorted(OPERATORS)}")
    check_rule(args)


def evaluate(rule: JsonValue, data: JsonValue) -> JsonValue:
    if isinstance(rule, list):
        return [evaluate(item, data) for item in rule]
    if not isinstance(rule, dict):
        return rule
    ((operator, raw),) = rule.items()
    args: list[JsonValue] = raw if isinstance(raw, list) else [raw]
    if operator in _LAZY:
        return _LAZY[operator](args, data)
    return OPERATORS[operator]([evaluate(arg, data) for arg in args], data)


def truthy(value: JsonValue) -> bool:
    if isinstance(value, list):
        return len(value) > 0
    return bool(value)


def _var(args: list[JsonValue], data: JsonValue) -> JsonValue:
    path = args[0] if args else None
    default = args[1] if len(args) > 1 else None
    if path is None or path == "":
        return data
    current = data
    for part in str(path).split("."):
        if isinstance(current, dict) and part in current:
            current = current[part]
        elif isinstance(current, list) and part.isdigit() and int(part) < len(current):
            current = current[int(part)]
        else:
            return default
    return current


def _missing(args: list[JsonValue], data: JsonValue) -> JsonValue:
    keys = args[0] if len(args) == 1 and isinstance(args[0], list) else args
    return [key for key in keys if not _present(key, data)]


def _present(path: JsonValue, data: JsonValue) -> bool:
    current = data
    for part in str(path).split("."):
        if isinstance(current, dict) and part in current:
            current = current[part]
        else:
            return False
    return True


def _chain(test: Callable[[JsonValue, JsonValue], bool]) -> Operator:
    # Three arguments is JSON Logic's "between": `{"<": [1, x, 10]}`.
    return lambda args, _data: all(test(left, right) for left, right in pairwise(args))


def _same(left: JsonValue, right: JsonValue) -> bool:
    if _is_number(left) and _is_number(right):
        return left == right
    return type(left) is type(right) and left == right


def _order(left: JsonValue, right: JsonValue) -> int | None:
    if isinstance(left, int | float) and isinstance(right, int | float) and _is_number(left) and _is_number(right):
        return (left > right) - (left < right)
    if isinstance(left, str) and isinstance(right, str):
        return (left > right) - (left < right)
    return None


def _ordered(accept: Callable[[int], bool]) -> Operator:
    def _test(left: JsonValue, right: JsonValue) -> bool:
        order = _order(left, right)
        return order is not None and accept(order)

    return _chain(_test)


def _is_number(value: JsonValue) -> bool:
    return isinstance(value, int | float) and not isinstance(value, bool)


def _and(args: list[JsonValue], data: JsonValue) -> JsonValue:
    value: JsonValue = True
    for arg in args:
        value = evaluate(arg, data)
        if not truthy(value):
            return value
    return value


def _or(args: list[JsonValue], data: JsonValue) -> JsonValue:
    value: JsonValue = False
    for arg in args:
        value = evaluate(arg, data)
        if truthy(value):
            return value
    return value


def _if(args: list[JsonValue], data: JsonValue) -> JsonValue:
    # `[cond, then, cond, then, ..., else]`.
    for index in range(0, len(args) - 1, 2):
        if truthy(evaluate(args[index], data)):
            return evaluate(args[index + 1], data)
    return evaluate(args[-1], data) if len(args) % 2 else None


def _in(args: list[JsonValue], _data: JsonValue) -> JsonValue:
    needle = args[0] if args else None
    haystack = args[1] if len(args) > 1 else None
    if isinstance(haystack, str):
        return isinstance(needle, str) and needle in haystack
    return isinstance(haystack, list) and needle in haystack


#: Operators whose arguments are evaluated only as far as the result needs.
_LAZY: dict[str, Operator] = {"and": _and, "or": _or, "if": _if}

#: Every operator a rule may use.
OPERATORS: dict[str, Operator] = {
    "var": _var,
    "missing": _missing,
    "==": _chain(_same),
    "===": _chain(_same),
    "!=": _chain(lambda left, right: not _same(left, right)),
    "!==": _chain(lambda left, right: not _same(left, right)),
    "<": _ordered(lambda order: order < 0),
    "<=": _ordered(lambda order: order <= 0),
    ">": _ordered(lambda order: order > 0),
    ">=": _ordered(lambda order: order >= 0),
    "!": lambda args, _data: not truthy(args[0] if args else None),
    "!!": lambda args, _data: truthy(args[0] if args else None),
    "in": _in,
    **_LAZY,
}
