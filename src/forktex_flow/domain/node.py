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

from collections.abc import Callable
from typing import Any, overload

from forktex.error import BadRequestError
from forktex.types import BaseValueObject

from forktex_flow.domain.config import RuntimeConfig

# Attribute the @step decorator attaches to a function to carry its retry
# contract. Named here once so call sites read it via ``step_meta`` rather
# than repeating the dunder string.
_STEP_META_ATTR = "__forktex_step_meta__"

#: The retry contract a bare `@step` gets. Read from `RuntimeConfig`'s own
#: defaults rather than restated here: the same two numbers written in three
#: places is how a consumer sets `RuntimeConfig(max_attempts=7)` and every step
#: still gets 3.
_DEFAULTS = RuntimeConfig()


class StepMeta(BaseValueObject):
    max_attempts: int
    backoff: tuple[float, ...]


class StepSpec(BaseValueObject):
    fn: Callable[..., Any]
    when: Callable[[dict[str, Any]], bool] | None = None
    max_attempts: int | None = None
    backoff: tuple[float, ...] | None = None


def step_meta(fn: object) -> StepMeta | None:
    meta = getattr(fn, _STEP_META_ATTR, None)
    return meta if isinstance(meta, StepMeta) else None


def has_step_meta(fn: object) -> bool:
    return isinstance(getattr(fn, _STEP_META_ATTR, None), StepMeta)


# The three call shapes, declared separately so a caller gets the type it
# actually receives. A single union return is technically true and practically
# useless: `@step` hands back the function unchanged, but a union makes every
# later `step(decorated, when=...)` and `scheduled(...)(decorated)` an error —
# 13 of them in ForkTex Cloud, against code written exactly as the README shows.
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
    """Attach a retry contract to a step, or build a guarded pipeline entry. ``fn`` must be positional."""
    resolved_max_attempts = max_attempts if max_attempts is not None else _DEFAULTS.max_attempts
    resolved_backoff = backoff if backoff is not None else _DEFAULTS.backoff

    def _attach_meta(target: Callable[..., Any]) -> Callable[..., Any]:
        meta = StepMeta(max_attempts=resolved_max_attempts, backoff=resolved_backoff)
        setattr(target, _STEP_META_ATTR, meta)
        return target

    if fn is None:
        if when is not None:
            # `when` selects a step *within a pipeline*, so it is meaningless
            # without the function it guards. Raising here turns a silently
            # ignored guard into an error at import time.
            raise BadRequestError("step(when=...) requires fn as a positional argument")
        return _attach_meta

    if when is not None or max_attempts is not None or backoff is not None:
        return StepSpec(fn=fn, when=when, max_attempts=max_attempts, backoff=backoff)

    return _attach_meta(fn)


__all__ = [
    "StepMeta",
    "StepSpec",
    "has_step_meta",
    "step",
    "step_meta",
]
