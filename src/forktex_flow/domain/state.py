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


from __future__ import annotations

import annotationlib
from collections.abc import Callable
from typing import Annotated, Any, get_args, get_origin

from forktex.log import get_logger
from forktex.types import JsonObject

#: ``reducer(existing, incoming) -> merged``.
logger = get_logger(__name__)

ReducerFn = Callable[[Any, Any], Any]


def extract_reducers(state_cls: type | None) -> dict[str, ReducerFn]:
    """Build ``{field: reducer}`` from a state class's ``Annotated`` hints. A state class under ``from __future__
    import annotations`` holding a name that exists only for the type checker cannot be resolved as a whole, and
    resolving it as a whole would drop every reducer beside the unresolvable field.
    """
    if state_cls is None:
        return {}

    reducers: dict[str, ReducerFn] = {}
    for field_name, hint in _resolved_hints(state_cls).items():
        if get_origin(hint) is not Annotated:
            continue
        for meta in get_args(hint)[1:]:
            if callable(meta):
                reducers[field_name] = meta
                break

    return reducers


def apply_state_update(
    current: JsonObject,
    update: JsonObject,
    reducers: dict[str, ReducerFn],
) -> JsonObject:
    """Merge a step's partial ``update`` into ``current`` using ``reducers``. A key with no reducer, or no existing
    value, takes the new value as-is; a reducer is never called against a missing key, so ``operator.add`` on
    the first write does not have to cope with ``None``.
    """
    result = dict(current)
    for key, new_value in update.items():
        if key in reducers and key in current:
            result[key] = reducers[key](current[key], new_value)
        else:
            result[key] = new_value
    return result


__all__ = ["ReducerFn", "apply_state_update", "extract_reducers"]


def _resolved_hints(state_cls: type) -> dict[str, object]:
    try:
        raw = annotationlib.get_annotations(state_cls, format=annotationlib.Format.VALUE, eval_str=True)
    except Exception:
        logger.warning("could not read annotations of %s; its reducers are lost", state_cls.__qualname__)
        return {}

    resolved: dict[str, object] = {}
    for field_name, hint in raw.items():
        if not isinstance(hint, annotationlib.ForwardRef):
            resolved[field_name] = hint
            continue
        try:
            resolved[field_name] = hint.evaluate()
        except Exception:
            logger.debug(
                "%s.%s has an unresolvable annotation; it carries no reducer",
                state_cls.__qualname__,
                field_name,
            )
    return resolved
