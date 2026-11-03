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

"""The ``@step`` decorator and the spec it produces. The two forms cannot be confused: the second requires ``fn``
positionally.
"""

from __future__ import annotations

from collections.abc import Callable, Coroutine
from typing import Any, overload

from forktex.error import BadRequestError
from forktex.types import BaseValueObject

_STEP_META_ATTR = "__forktex_step_meta__"


class StepMeta(BaseValueObject):
    """A step's own retry contract; what it leaves unset comes from the ``Flow``'s ``RuntimeConfig``."""

    max_attempts: int | None = None
    backoff: tuple[float, ...] | None = None


#: ``async def fn(ctx: Ctx, variables: dict[str, Any]) -> dict[str, Any] | None`` — a handler returns the output
#: merged into the instance's variables.
HandlerFn = Callable[[Any, dict[str, Any]], Coroutine[Any, Any, dict[str, Any] | None]]

#: A pipeline step's guard, evaluated against the variables before the step runs.
WhenFn = Callable[[dict[str, Any]], bool]


class StepSpec(BaseValueObject):
    fn: Callable[..., Any]
    when: WhenFn | None = None
    max_attempts: int | None = None
    backoff: tuple[float, ...] | None = None


def step_meta(fn: object) -> StepMeta | None:
    meta = getattr(fn, _STEP_META_ATTR, None)
    return meta if isinstance(meta, StepMeta) else None


# The three call shapes, declared separately so a caller gets the type it
# actually receives. A single union return is technically true and practically
# useless: `@step` hands back the function unchanged, but a union makes every
# later `step(decorated, when=...)` and `scheduled(...)(decorated)` a type error.
@overload
def step(fn: Callable[..., Any], /) -> Callable[..., Any]: ...


@overload
def step(
    fn: Callable[..., Any],
    /,
    *,
    max_attempts: int | None = ...,
    backoff: tuple[float, ...] | None = ...,
    when: Callable[[dict[str, Any]], bool] | None = ...,
) -> StepSpec: ...


@overload
def step(
    fn: None = ...,
    /,
    *,
    max_attempts: int | None = ...,
    backoff: tuple[float, ...] | None = ...,
    when: Callable[[dict[str, Any]], bool] | None = ...,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]: ...


def step(
    fn: Callable[..., Any] | None = None,
    /,
    *,
    max_attempts: int | None = None,
    backoff: tuple[float, ...] | None = None,
    when: Callable[[dict[str, Any]], bool] | None = None,
) -> Callable[..., Any] | StepSpec:
    """Attach a retry contract to a step, or build a guarded pipeline entry. ``fn`` must be positional; a bare
    ``@step`` changes nothing.
    """
    if fn is None:
        if when is not None:
            raise BadRequestError("step(when=...) requires fn as a positional argument")

        def _attach(target: Callable[..., Any]) -> Callable[..., Any]:
            setattr(target, _STEP_META_ATTR, StepMeta(max_attempts=max_attempts, backoff=backoff))
            return target

        return _attach
    if when is not None or max_attempts is not None or backoff is not None:
        return StepSpec(fn=fn, when=when, max_attempts=max_attempts, backoff=backoff)
    return fn
