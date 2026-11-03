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
import json
from collections.abc import Callable
from typing import Annotated, Any, get_args, get_origin

from forktex.error import BadRequestError
from forktex.log import get_logger
from forktex.types import JsonObject
from pydantic import BaseModel, ValidationError

from forktex_flow.errors import StateNotSerialisable

#: ``reducer(existing, incoming) -> merged``.
logger = get_logger(__name__)

ReducerFn = Callable[[Any, Any], Any]


def extract_reducers(state_cls: type | None) -> dict[str, ReducerFn]:
    """Build ``{field: reducer}`` from a state class's ``Annotated`` hints, one field at a time, so a field whose
    annotation cannot be resolved loses only its own reducer.
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
    """Merge a handler's partial ``update`` into ``current`` using ``reducers``. A key with no reducer, or no existing
    value, takes the new value as-is.
    """
    result = dict(current)
    for key, new_value in update.items():
        if key in reducers and key in current:
            result[key] = reducers[key](current[key], new_value)
        else:
            result[key] = new_value
    return result


def check_variables(output: object) -> None:
    """Raise ``StateNotSerialisable`` unless ``output`` is ``None`` or a dict JSONB can hold."""
    if output is None:
        return
    if not isinstance(output, dict):
        raise StateNotSerialisable(f"variables are a dict, got {type(output).__name__}")
    try:
        json.dumps(output)
    except TypeError as exc:
        raise StateNotSerialisable(
            f"variables cannot be written to JSONB ({exc}); carry an id and build the object inside the handler"
        ) from exc


def validate_state[M: BaseModel](state: type[M], variables: JsonObject) -> M:
    """Raises ``BadRequestError`` listing every field that does not match."""
    try:
        return state.model_validate(variables)
    except ValidationError as exc:
        fields = "; ".join(
            f"{'.'.join(str(p) for p in error['loc']) or '(root)'}: {error['msg']}" for error in exc.errors()
        )
        raise BadRequestError(f"variables do not match {state.__name__}: {fields}") from exc


def to_variables(output: object) -> JsonObject:
    """A handler's return value as the variables it changes: a pydantic model counts only the fields that were set,
    and ``None`` changes nothing. Raises ``StateNotSerialisable`` for anything else JSONB cannot hold.
    """
    if isinstance(output, BaseModel):
        output = output.model_dump(mode="json", exclude_unset=True)
    check_variables(output)
    return output if isinstance(output, dict) else {}


__all__ = ["ReducerFn", "apply_state_update", "check_variables", "extract_reducers", "to_variables", "validate_state"]


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
