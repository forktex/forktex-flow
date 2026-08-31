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

"""State reduction: extract ``Annotated`` reducers and merge partial updates.

A step returns the *delta* it wants applied, not the whole state. How a delta is
combined with what is already there is declared on the state class itself::

    class DeployState(TypedDict):
        logs: Annotated[list[str], operator.add]   # accumulates
        server_id: str                             # last write wins

This is the single merge point for the whole engine. Every path that advances a
run's state goes through :func:`apply_state_update`; nothing calls ``dict.update``
on a run state, because that silently turns an accumulator into an overwrite and
the only symptom is a log that keeps just its final line.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Annotated, Any, get_args, get_origin, get_type_hints

#: ``reducer(existing, incoming) -> merged``.
ReducerFn = Callable[[Any, Any], Any]


def extract_reducers(state_cls: type | None) -> dict[str, ReducerFn]:
    """Build ``{field: reducer}`` from a state class's ``Annotated`` hints.

    ``Annotated[list[str], operator.add]`` yields ``operator.add`` for that
    field. Everything else is absent from the result, which the merge reads as
    last-write-wins. Only callable metadata is treated as a reducer, so
    ``Annotated`` may also carry documentation strings or validators without
    breaking extraction.

    ``None`` (an untyped ``dict`` state) yields no reducers.
    """
    if state_cls is None:
        return {}

    reducers: dict[str, ReducerFn] = {}
    try:
        hints = get_type_hints(state_cls, include_extras=True)
    except NameError, TypeError:
        # A state class defined with an unresolvable forward reference — in a
        # REPL, or under `from __future__ import annotations` where a name is
        # only imported under TYPE_CHECKING. Fall back to the raw annotations:
        # they are strings there, so no reducer is found and every field
        # becomes last-write-wins, which is the safe direction to fail.
        hints = getattr(state_cls, "__annotations__", {})

    for field_name, hint in hints.items():
        if get_origin(hint) is not Annotated:
            continue
        for meta in get_args(hint)[1:]:
            if callable(meta):
                reducers[field_name] = meta
                break

    return reducers


def apply_state_update(
    current: dict[str, Any],
    update: dict[str, Any],
    reducers: dict[str, ReducerFn],
) -> dict[str, Any]:
    """Merge a step's partial ``update`` into ``current`` using ``reducers``.

    A key with a reducer *and* an existing value is combined:
    ``reducer(current[key], new)``. Otherwise the new value is used as-is — a
    reducer is never called against a missing key, so ``operator.add`` on the
    first write does not have to cope with ``None``.

    Returns a new dict; ``current`` is not mutated.
    """
    result = dict(current)
    for key, new_value in update.items():
        if key in reducers and key in current:
            result[key] = reducers[key](current[key], new_value)
        else:
            result[key] = new_value
    return result


__all__ = ["ReducerFn", "apply_state_update", "extract_reducers"]
