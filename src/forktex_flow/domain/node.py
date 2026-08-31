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

"""The ``@step`` decorator and the spec it produces.

``step`` is deliberately dual-mode. Bare ``@step`` attaches a retry contract to
the function and returns it unchanged, so a step is still an ordinary callable a
test can await directly. ``step(fn, when=...)`` instead returns a
:class:`StepSpec` for use inside a pipeline's ``steps`` list. The two forms
cannot be confused because the second requires ``fn`` positionally.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

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
    """Retry + backoff configuration stored on the function itself.

    The executor reads it via :func:`step_meta` at dispatch time. Storing it on
    the function rather than in a registry means the function carries its own
    durability contract and works with any ``Flow`` without prior registration.
    """

    max_attempts: int
    backoff: tuple[float, ...]


class StepSpec(BaseValueObject):
    """Returned by ``step(fn, when=...)`` for use in a pipeline's ``steps`` list."""

    fn: Callable[..., Any]
    when: Callable[[dict[str, Any]], bool] | None = None
    max_attempts: int | None = None
    backoff: tuple[float, ...] | None = None


def step_meta(fn: object) -> StepMeta | None:
    """Return the :class:`StepMeta` attached by :func:`step`, or ``None``.

    ``None`` is a legitimate result, not a broken invariant: the compilers
    accept bare, undecorated callables and fall back to the default retry
    config. The typed accessor centralises the attribute name and gives call
    sites a precise return type without a per-site ``type: ignore``.
    """
    meta = getattr(fn, _STEP_META_ATTR, None)
    return meta if isinstance(meta, StepMeta) else None


def has_step_meta(fn: object) -> bool:
    """True if ``fn`` was decorated with :func:`step`."""
    return isinstance(getattr(fn, _STEP_META_ATTR, None), StepMeta)


def step(
    fn: Callable[..., Any] | None = None,
    /,
    *,
    max_attempts: int | None = None,
    backoff: tuple[float, ...] | None = None,
    when: Callable[[dict[str, Any]], bool] | None = None,
) -> Callable[..., Any] | StepSpec:
    """Attach a retry contract to a step, or build a guarded pipeline entry.

    As a decorator — ``@step`` or ``@step(max_attempts=5)`` — attaches
    :class:`StepMeta` and returns ``fn`` unchanged.

    As a pipeline entry — ``step(fn, when=predicate)`` — returns a
    :class:`StepSpec`. ``fn`` must be positional.
    """
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
