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

"""Turning a decorated function or class into a :class:`WorkflowDefinition`.

Two entry points, because there are two ways to declare a workflow: a single
async function on a cron (:func:`compile_scheduled`) and a class with a ``steps``
list (:func:`compile_pipeline`). Both produce the same ordered node list, so the
executor has one shape to run rather than one per declaration style.
"""

from __future__ import annotations

from typing import Any

from forktex.error import BadRequestError

from forktex_flow.domain.config import RuntimeConfig
from forktex_flow.domain.definition import NodeDef, NodeFn, WhenFn, WorkflowDefinition
from forktex_flow.domain.node import StepMeta, StepSpec, step_meta
from forktex_flow.domain.state import ReducerFn, extract_reducers

__all__ = ["compile_pipeline", "compile_scheduled"]


def _resolve_step(item: NodeFn | StepSpec, defaults: RuntimeConfig) -> tuple[NodeFn, StepMeta | None, WhenFn | None]:
    """Extract ``(fn, meta, when_fn)`` from one entry in a ``steps`` list.

    An entry is either a plain callable — ``@step``-decorated or bare — or a
    :class:`StepSpec` from ``step(fn, when=...)``. ``meta`` is ``None`` when the
    function carries no ``@step`` config, which the caller reads as "use the
    defaults".
    """
    if not isinstance(item, StepSpec):
        return item, step_meta(item), None

    meta = step_meta(item.fn)
    if item.max_attempts is not None or item.backoff is not None:
        # Values given to `step(...)` at the call site beat the decorator's:
        # the pipeline is the more specific context.
        meta = StepMeta(
            max_attempts=(
                item.max_attempts
                if item.max_attempts is not None
                else (meta.max_attempts if meta else defaults.max_attempts)
            ),
            backoff=item.backoff if item.backoff is not None else (meta.backoff if meta else defaults.backoff),
        )
    return item.fn, meta, item.when


def _retry(meta: StepMeta | None, defaults: RuntimeConfig) -> tuple[int, tuple[float, ...]]:
    """A step's retry contract: its own if it declared one, else the engine's.

    An undecorated step used to fall back to a pair of module constants, which
    meant `RuntimeConfig(max_attempts=7)` configured nothing — the value was read
    nowhere and every step still got 3.
    """
    if meta is None:
        return defaults.max_attempts, defaults.backoff
    return meta.max_attempts, meta.backoff


def compile_scheduled(
    fn: NodeFn,
    *,
    name: str,
    version: int,
    cron: str,
    state_cls: type | None = None,
    enabled: bool = True,
    defaults: RuntimeConfig | None = None,
) -> WorkflowDefinition:
    """Compile one async function into a single-step workflow on a cron."""
    resolved = defaults or RuntimeConfig()
    max_attempts, backoff = _retry(step_meta(fn), resolved)
    return WorkflowDefinition(
        name=name,
        version=version,
        state_cls=state_cls,
        nodes=(
            NodeDef(
                name=getattr(fn, "__name__", "run"),
                fn=fn,
                max_attempts=max_attempts,
                backoff=backoff,
            ),
        ),
        reducers=extract_reducers(state_cls),
        schedule=cron,
        enabled=enabled,
    )


def compile_pipeline(
    cls: type,
    *,
    name: str,
    version: int,
    cron: str | None = None,
    state_cls: type | None = None,
    defaults: RuntimeConfig | None = None,
) -> WorkflowDefinition:
    """Compile a class with a ``steps`` list into an ordered workflow.

    Each entry is a callable or a :class:`StepSpec`. A spec's ``when`` guard
    becomes :attr:`NodeDef.when_fn`; the executor evaluates it against the
    current state and skips the step without breaking the chain.
    """
    steps: list[Any] = list(getattr(cls, "steps", []))
    if not steps:
        raise BadRequestError(f"pipeline {name!r} v{version}: class {cls.__name__} declares no steps")

    resolved = defaults or RuntimeConfig()
    nodes: list[NodeDef] = []
    used: dict[str, int] = {}
    for index, item in enumerate(steps):
        fn, meta, when_fn = _resolve_step(item, resolved)
        max_attempts, backoff = _retry(meta, resolved)

        # The step name identifies its row in `step_run`, so the same function
        # used twice in one pipeline needs two names. Suffix by position rather
        # than by an occurrence counter: the position is stable when the list is
        # edited elsewhere, so a resumed run still matches its recorded rows.
        node_name = getattr(fn, "__name__", f"step_{index}")
        if node_name in used:
            node_name = f"{node_name}_{index}"
        used[node_name] = index

        nodes.append(
            NodeDef(
                name=node_name,
                fn=fn,
                max_attempts=max_attempts,
                backoff=backoff,
                when_fn=when_fn,
            )
        )

    effective_state_cls = state_cls if state_cls is not None else getattr(cls, "state_cls", None)
    reducers: dict[str, ReducerFn] = extract_reducers(effective_state_cls)

    return WorkflowDefinition(
        name=name,
        version=version,
        state_cls=effective_state_cls,
        nodes=tuple(nodes),
        reducers=reducers,
        schedule=cron,
    )
