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

"""`step` is dual-mode, which is the sort of API that goes wrong quietly."""

from __future__ import annotations

import pytest
from forktex.error import BadRequestError

from forktex_flow import StepSpec, step
from forktex_flow.domain.node import StepMeta, has_step_meta, step_meta


async def _body(ctx: object, state: dict) -> dict:
    return {}


def test_bare_decorator_returns_the_function_unchanged():
    """A step stays an ordinary callable, so a unit test can await it directly
    without a Flow, a database, or a run."""

    @step
    async def decorated(ctx: object, state: dict) -> dict:
        return {"ok": True}

    assert has_step_meta(decorated)
    assert step_meta(decorated) == StepMeta(max_attempts=3, backoff=(30.0, 120.0, 300.0))


def test_decorator_with_options_attaches_them():
    @step(max_attempts=5, backoff=(1.0, 2.0))
    async def decorated(ctx: object, state: dict) -> dict:
        return {}

    meta = step_meta(decorated)
    assert meta is not None
    assert (meta.max_attempts, meta.backoff) == (5, (1.0, 2.0))


def test_a_positional_call_with_a_guard_returns_a_spec_not_a_function():
    """The two modes are told apart by whether `fn` was passed positionally.
    Getting this wrong silently would put a bare function where a guard was
    intended, and the guard would never run."""
    spec = step(_body, when=lambda s: True)
    assert isinstance(spec, StepSpec)
    assert spec.fn is _body
    assert spec.when is not None


def test_a_positional_call_with_retry_options_returns_a_spec():
    spec = step(_body, max_attempts=9)
    assert isinstance(spec, StepSpec)
    assert spec.max_attempts == 9
    assert spec.when is None


def test_a_guard_without_a_function_is_rejected():
    """`when` selects a step *within a pipeline*, so it is meaningless without
    the function it guards. Silently ignoring it would ship a pipeline whose
    conditional steps all run unconditionally."""
    with pytest.raises(BadRequestError, match="requires fn as a positional argument"):
        step(when=lambda s: True)


def test_an_undecorated_callable_carries_no_meta():
    """`None` here is a legitimate answer, not a broken invariant — the compilers
    accept bare callables and fall back to the defaults."""
    assert step_meta(_body) is None
    assert has_step_meta(_body) is False


def test_step_meta_ignores_a_foreign_attribute():
    """Defensive: the marker attribute is read back with a type check, so an
    unrelated object parked on the same name is not mistaken for retry config."""

    async def impostor(ctx: object, state: dict) -> dict:
        return {}

    impostor.__forktex_step_meta__ = "not a StepMeta"  # type: ignore[attr-defined]
    assert step_meta(impostor) is None
