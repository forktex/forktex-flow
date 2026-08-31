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

"""`WorkflowDefinition` rejects a workflow that cannot run, at declaration time.

Declaration time is import time: a decorator runs when its module is imported.
So a definition that would fail is caught when the process starts rather than
when the first run is dispatched, which in a deploy pipeline could be days later
and in production.
"""

from __future__ import annotations

import operator
from typing import Annotated, TypedDict

import pytest
from forktex.error import BadRequestError

from forktex_flow import step
from forktex_flow.domain.definition import NodeDef, WorkflowDefinition


class State(TypedDict, total=False):
    logs: Annotated[list[str], operator.add]


async def _noop(ctx: object, state: dict) -> dict:
    return {}


def _node(name: str) -> NodeDef:
    return NodeDef(name=name, fn=_noop, max_attempts=3, backoff=(1.0,))


def test_a_workflow_with_no_steps_is_rejected():
    """An empty pipeline would be dispatched, complete instantly and record
    nothing — a run that looks successful and did no work."""
    with pytest.raises(BadRequestError, match="no steps"):
        WorkflowDefinition(name="empty", version=1, state_cls=None, nodes=())


def test_a_duplicate_step_name_is_rejected():
    """The step name identifies its row in `step_run`, and the memoisation key is
    built from it. Two steps sharing a name would share a cache row: the second
    would be served the first's output and never execute."""
    with pytest.raises(BadRequestError, match="duplicate step name"):
        WorkflowDefinition(
            name="dupes",
            version=1,
            state_cls=None,
            nodes=(_node("same"), _node("same")),
        )


def test_a_valid_definition_exposes_its_steps_in_order():
    defn = WorkflowDefinition(
        name="ordered",
        version=2,
        state_cls=None,
        nodes=(_node("first"), _node("second"), _node("third")),
    )
    assert defn.node_names() == ("first", "second", "third")


def test_the_compiler_disambiguates_a_repeated_function():
    """A pipeline may legitimately repeat a step. The compiler suffixes by
    position — stable when the list is edited elsewhere, so a resumed run still
    matches the rows it already wrote."""
    from forktex_flow.runtime.compiler import compile_pipeline

    async def work(ctx: object, state: State) -> dict:
        return {}

    defn = compile_pipeline(
        type("Repeated", (), {"steps": [work, work, work]}),
        name="t.repeat",
        version=1,
        state_cls=State,
    )
    assert defn.node_names() == ("work", "work_1", "work_2")


def test_a_pipeline_class_with_no_steps_is_rejected_by_the_compiler():
    from forktex_flow.runtime.compiler import compile_pipeline

    with pytest.raises(BadRequestError, match="declares no steps"):
        compile_pipeline(type("Bare", (), {}), name="t.bare", version=1, state_cls=State)


def test_a_scheduled_workflow_carries_its_cron_and_one_step():
    from forktex_flow.runtime.compiler import compile_scheduled

    async def nightly(ctx: object, state: State) -> dict:
        return {}

    defn = compile_scheduled(nightly, name="t.nightly", version=1, cron="0 2 * * *", state_cls=State)
    assert defn.schedule == "0 2 * * *"
    assert defn.node_names() == ("nightly",)
    assert defn.reducers == {"logs": operator.add}


def test_a_guard_from_step_reaches_the_compiled_node():
    from forktex_flow.runtime.compiler import compile_pipeline

    async def guarded(ctx: object, state: State) -> dict:
        return {}

    def predicate(state: dict) -> bool:
        return True

    defn = compile_pipeline(
        type("Guarded", (), {"steps": [step(guarded, when=predicate)]}),
        name="t.guarded",
        version=1,
        state_cls=State,
    )
    assert defn.nodes[0].when_fn is predicate


def test_step_retry_overrides_beat_the_decorator():
    """`step(fn, max_attempts=…)` is the more specific context — the pipeline
    knows how this call site should behave, the decorator only how the function
    usually should."""
    from forktex_flow.runtime.compiler import compile_pipeline

    @step(max_attempts=2, backoff=(5.0,))
    async def decorated(ctx: object, state: State) -> dict:
        return {}

    defn = compile_pipeline(
        type("Overridden", (), {"steps": [step(decorated, max_attempts=7)]}),
        name="t.override",
        version=1,
        state_cls=State,
    )
    assert defn.nodes[0].max_attempts == 7
    assert defn.nodes[0].backoff == (5.0,), "an unspecified override falls back to the decorator's value"
