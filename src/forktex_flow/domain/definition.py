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

"""WorkflowDefinition — the internal model every decorator compiles to.

A workflow is an **ordered list of steps**. That is the whole topology: there is
no edge set, no router, no START/END sentinel and no reachability analysis,
because a pipeline's order *is* its graph and a scheduled workflow is a pipeline
of one. Branching is expressed by a step's ``when_fn`` — a pure predicate over
the state dict, evaluated immediately before dispatch — which is strictly weaker
than a router and strictly easier to reason about: a run visits every node once,
in order, executing the ones whose guard passes.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from forktex.error import BadRequestError
from forktex.types import BaseValueObject
from pydantic import BaseModel, Field, model_validator

from forktex_flow.domain.state import ReducerFn

#: ``async def fn(ctx: Ctx, state: dict[str, Any]) -> dict[str, Any]`` — a step
#: returns the *delta* it wants merged into state, not the whole state.
NodeFn = Callable[[Any, dict[str, Any]], Awaitable[dict[str, Any]]]

#: A guard evaluated against the current state before a node is dispatched.
WhenFn = Callable[[dict[str, Any]], bool]


class NodeDef(BaseValueObject):
    """One step in the workflow.

    ``when_fn`` is evaluated against the current state dict before the node is
    dispatched; ``None`` means "always execute".
    """

    name: str
    fn: NodeFn
    max_attempts: int
    backoff: tuple[float, ...]
    when_fn: WhenFn | None = None


class WorkflowDefinition(BaseModel):
    """Complete, validated description of a workflow.

    Every decorator compiles to this, and the executor reads only from here —
    so ``@pipeline`` and ``@scheduled`` differ by exactly one field
    (``schedule``) rather than by an execution path.

    Attributes:
        name:      Workflow identifier, unique per version.
        version:   Bumped when the step list changes shape.
        state_cls: The TypedDict defining the state schema. ``None`` for
                   workflows that carry an untyped ``dict`` state.
        nodes:     The steps, **in execution order**.
        reducers:  ``field_name -> merge function``, extracted from the
                   ``Annotated`` hints on ``state_cls``. A field with no
                   reducer is last-write-wins.
        schedule:  Cron expression, or ``None`` for on-demand workflows.
    """

    name: str
    version: int
    state_cls: type | None
    nodes: tuple[NodeDef, ...] = ()
    reducers: dict[str, ReducerFn] = Field(default_factory=dict)
    schedule: str | None = None

    @model_validator(mode="after")
    def _check_topology(self) -> WorkflowDefinition:
        """Reject a definition that cannot run, at declaration time.

        A model validator rather than a method a caller must remember to call:
        the previous shape had a ``validate()`` each compiler had to invoke, and
        one forgetting it produced a workflow that failed on its first dispatch
        in production rather than at import.
        """
        if not self.nodes:
            raise BadRequestError(f"workflow {self.name!r} v{self.version} has no steps")
        seen: set[str] = set()
        for node in self.nodes:
            if node.name in seen:
                raise BadRequestError(
                    f"workflow {self.name!r} v{self.version}: duplicate step name {node.name!r} — "
                    "step names identify rows in step_run and must be unique within a workflow"
                )
            seen.add(node.name)
        return self

    def node_names(self) -> tuple[str, ...]:
        """Step names in execution order."""
        return tuple(node.name for node in self.nodes)


__all__ = [
    "NodeDef",
    "NodeFn",
    "WhenFn",
    "WorkflowDefinition",
]
