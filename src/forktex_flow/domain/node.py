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

"""What a pipeline is made of: ``step`` entries, and the one shape every step and handler function has."""

from __future__ import annotations

import inspect
import re
from collections.abc import Callable, Coroutine
from typing import Any

from forktex.error import BadRequestError
from forktex.types import BaseValueObject, JsonObject
from pydantic import Field

from forktex_flow.domain.process import PRIVATE_SEPARATOR

__all__ = ["HandlerFn", "StepSpec", "check_handler", "step"]

#: ``async def name(ctx: Ctx, state) -> dict | BaseModel | None``.
HandlerFn = Callable[[Any, Any], Coroutine[Any, Any, object]]

_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_.-]*")


class StepSpec(BaseValueObject):
    """One pipeline entry: a function run as a private step, or the task type of a shared handler."""

    target: Callable[..., Any] | str
    name: str
    when: JsonObject | None = None
    max_attempts: int | None = Field(default=None, ge=1)
    backoff: tuple[float, ...] | None = None


def step(
    target: Callable[..., Any] | str,
    /,
    *,
    name: str | None = None,
    when: JsonObject | None = None,
    max_attempts: int | None = None,
    backoff: tuple[float, ...] | None = None,
) -> StepSpec:
    """A pipeline entry. ``name`` defaults to the function's name or the task type, and is the step's identity
    across deploys. ``when`` is a JSON Logic rule over the variables; when it is false the step is not run.
    Raises ``BadRequestError`` for anything that cannot be a step.
    """
    if isinstance(target, str):
        if not target or PRIVATE_SEPARATOR in target:
            raise BadRequestError(f"{target!r} is not a shared task type; it must be non-empty and contain no ':'")
        default_name = target
    else:
        check_handler(target)
        default_name = getattr(target, "__name__", "")
    chosen = default_name if name is None else name
    if not _NAME.fullmatch(chosen):
        raise BadRequestError(
            f"step name {chosen!r} must start with a letter or '_' and contain only letters, digits, '_', '.' or '-'; "
            "pass name="
        )
    if when is not None and not isinstance(when, dict):
        raise BadRequestError(f"step {chosen!r}: when= is a JSON Logic rule such as {{'var': 'express'}}")
    return StepSpec(target=target, name=chosen, when=when, max_attempts=max_attempts, backoff=backoff)


def check_handler(fn: object) -> None:
    """Raises ``BadRequestError`` unless ``fn`` is ``async def name(ctx, state)``: exactly two positional
    parameters.
    """
    label = getattr(fn, "__qualname__", repr(fn))
    if not inspect.iscoroutinefunction(fn):
        raise BadRequestError(f"{label} must be an async function: async def name(ctx, state)")
    parameters = list(inspect.signature(fn).parameters.values())
    positional = (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
    if len(parameters) != 2 or any(parameter.kind not in positional for parameter in parameters):
        found = ", ".join(str(parameter) for parameter in parameters)
        raise BadRequestError(f"{label} must take exactly (ctx, state); it takes ({found})")
