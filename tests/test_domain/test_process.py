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

"""A process definition deploys only if it can run, and says everything wrong with it at once.

Definitions arrive as data — from a tenant's editor, an API call, a compiled decorator — so the model is the
only place a broken one can be stopped before an instance is stranded halfway through it.
"""

from __future__ import annotations

import pytest
from forktex.error import BadRequestError

from forktex_flow.domain.process import ProcessModel, ServiceTask


def _process(elements: list[dict], flows: list[tuple[str, str]], **extra: object) -> ProcessModel:
    return ProcessModel.model_validate(
        {
            "key": "billing.dunning",
            "elements": elements,
            "flows": [{"id": f"{s}->{t}", "source": s, "target": t} for s, t in flows],
            **extra,
        }
    )


START = {"id": "start", "type": "startEvent"}
END = {"id": "end", "type": "endEvent"}


def _task(element_id: str) -> dict:
    return {"id": element_id, "type": "serviceTask", "taskType": f"billing.{element_id}"}


def test_a_bpmn_shaped_json_definition_is_accepted():
    model = ProcessModel.model_validate(
        {
            "key": "billing.dunning",
            "elements": [
                START,
                {"id": "remind", "type": "serviceTask", "taskType": "billing.remind", "retries": 5},
                {"id": "review", "type": "userTask", "resultVariable": "review"},
                {"id": "decide", "type": "exclusiveGateway", "default": "close"},
                {"id": "escalate", "type": "callActivity", "calledElement": "billing.escalation"},
                END,
            ],
            "flows": [
                {"id": "f1", "source": "start", "target": "remind"},
                {"id": "f2", "source": "remind", "target": "review"},
                {"id": "f3", "source": "review", "target": "decide"},
                {
                    "id": "f4",
                    "source": "decide",
                    "target": "escalate",
                    "condition": {"==": [{"var": "review.decision"}, "escalate"]},
                },
                {"id": "close", "source": "decide", "target": "end"},
                {"id": "f6", "source": "escalate", "target": "end"},
            ],
        }
    )

    assert model.start.id == "start"
    remind = model.element("remind")
    assert isinstance(remind, ServiceTask)
    assert (remind.task_type, remind.retries) == ("billing.remind", 5)
    assert [flow.target for flow in model.outgoing("decide")] == ["escalate", "end"]


def test_the_definition_round_trips_through_its_bpmn_spelling():
    """What a consumer stores and reads back is the camelCase form a BPMN tool would recognise."""
    model = _process([START, _task("remind"), END], [("start", "remind"), ("remind", "end")])

    dumped = model.model_dump(mode="json", exclude_none=True)

    assert dumped["elements"][1] == {"id": "remind", "type": "serviceTask", "taskType": "billing.remind"}
    assert ProcessModel.model_validate(dumped) == model


def test_the_checksum_ignores_key_order_but_not_behaviour():
    one = _process([START, _task("remind"), END], [("start", "remind"), ("remind", "end")])
    reordered = ProcessModel.model_validate(
        {
            "flows": [
                {"target": "remind", "source": "start", "id": "start->remind"},
                {"target": "end", "source": "remind", "id": "remind->end"},
            ],
            "elements": [START, {"taskType": "billing.remind", "type": "serviceTask", "id": "remind"}, END],
            "key": "billing.dunning",
        }
    )
    retried = _process([START, {**_task("remind"), "retries": 9}, END], [("start", "remind"), ("remind", "end")])

    assert one.checksum() == reordered.checksum()
    assert one.checksum() != retried.checksum()


def test_an_unknown_element_type_is_rejected():
    with pytest.raises(ValueError, match="parallelGateway"):
        _process([START, {"id": "split", "type": "parallelGateway"}, END], [("start", "split"), ("split", "end")])


def test_every_problem_is_reported_in_one_error():
    """An editor shows the whole list; fixing one problem per deploy attempt is no way to author a process."""
    with pytest.raises(BadRequestError) as caught:
        _process(
            [START, {"id": "start", "type": "endEvent"}, _task("remind")],
            [("start", "ghost")],
        )

    message = caught.value.message
    assert "element id 'start' is used twice" in message
    assert "unknown element 'ghost'" in message


@pytest.mark.parametrize(
    ("elements", "flows", "problem"),
    [
        pytest.param([_task("a"), END], [("a", "end")], "exactly one startEvent", id="no-start"),
        pytest.param(
            [START, {"id": "start2", "type": "startEvent"}, END],
            [("start", "end"), ("start2", "end")],
            "exactly one startEvent",
            id="two-starts",
        ),
        pytest.param([START, _task("a")], [("start", "a")], "at least one endEvent", id="no-end"),
        pytest.param(
            [START, _task("a"), _task("b"), END],
            [("start", "a"), ("a", "b"), ("a", "end"), ("b", "end")],
            "exactly one outgoing flow",
            id="implicit-parallel-split",
        ),
        pytest.param(
            [START, _task("a"), _task("orphan"), END],
            [("start", "a"), ("a", "end"), ("orphan", "end")],
            "'orphan' has no incoming flow",
            id="orphan",
        ),
        pytest.param(
            [START, _task("a"), _task("b"), END, {"id": "j", "type": "exclusiveGateway", "default": "j->end"}],
            [("start", "a"), ("a", "j"), ("j", "end"), ("b", "a")],
            "not reachable",
            id="unreachable",
        ),
        pytest.param(
            [START, _task("a"), END, {"id": "end2", "type": "endEvent"}],
            [("start", "a"), ("a", "end"), ("end", "end2")],
            "endEvent 'end' has outgoing flows",
            id="end-continues",
        ),
    ],
)
def test_a_process_that_cannot_run_is_rejected(elements, flows, problem):
    with pytest.raises(BadRequestError, match=problem):
        _process(elements, flows)


def test_a_cycle_is_rejected():
    """Each element runs at most once per instance, so a loop back would have nothing to resume from."""
    gateway = {"id": "check", "type": "exclusiveGateway", "default": "check->end"}
    with pytest.raises(BadRequestError, match="cycle"):
        ProcessModel.model_validate(
            {
                "key": "retry",
                "elements": [START, _task("work"), gateway, END],
                "flows": [
                    {"id": "s", "source": "start", "target": "work"},
                    {"id": "w", "source": "work", "target": "check"},
                    {"id": "check->end", "source": "check", "target": "end"},
                    {"id": "again", "source": "check", "target": "work", "condition": {"!": {"var": "done"}}},
                ],
            }
        )


def _gateway(default: str | None, flows: list[dict]) -> ProcessModel:
    return ProcessModel.model_validate(
        {
            "key": "route",
            "elements": [START, {"id": "g", "type": "exclusiveGateway", "default": default}, _task("a"), END],
            "flows": [
                {"id": "in", "source": "start", "target": "g"},
                *flows,
                {"id": "a-out", "source": "a", "target": "end"},
            ],
        }
    )


def test_a_gateway_flow_needs_a_condition_unless_it_is_the_default():
    with pytest.raises(BadRequestError, match="without a condition"):
        _gateway(None, [{"id": "x", "source": "g", "target": "a"}, {"id": "y", "source": "g", "target": "end"}])


def test_a_gateway_default_must_be_one_of_its_own_flows():
    with pytest.raises(BadRequestError, match="'elsewhere' is not one of its flows"):
        _gateway("elsewhere", [{"id": "x", "source": "g", "target": "a", "condition": {"var": "go"}}])


def test_a_default_flow_cannot_also_carry_a_condition():
    with pytest.raises(BadRequestError, match="default and cannot carry a condition"):
        _gateway("x", [{"id": "x", "source": "g", "target": "a", "condition": {"var": "go"}}])


def test_a_condition_with_an_unsupported_operator_is_rejected_at_deploy():
    """An operator the evaluator lacks would otherwise surface as a failed instance, long after deploy."""
    with pytest.raises(BadRequestError, match="'regex' is not supported"):
        _gateway(
            "y",
            [
                {"id": "x", "source": "g", "target": "a", "condition": {"regex": [{"var": "name"}, "^a"]}},
                {"id": "y", "source": "g", "target": "end"},
            ],
        )


def test_a_condition_outside_a_gateway_is_rejected():
    """Only a gateway reads conditions, so one anywhere else would be silently ignored."""
    with pytest.raises(BadRequestError, match="leaves no gateway"):
        ProcessModel.model_validate(
            {
                "key": "k",
                "elements": [START, END],
                "flows": [{"id": "f", "source": "start", "target": "end", "condition": {"var": "x"}}],
            }
        )
