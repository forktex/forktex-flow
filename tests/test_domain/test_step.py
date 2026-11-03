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

import functools

import pytest
from forktex.error import AlreadyExistsError, BadRequestError

from forktex_flow import Ctx, Flow, ServiceTask, step


async def ship(ctx: Ctx, state: dict) -> dict:
    return {}


async def _task(flow: Flow, entry: object, element_id: str = "ship") -> ServiceTask:
    flow.pipeline("p", steps=[entry])
    await flow.init()
    task = (await flow.definition("p")).model.element(element_id)
    assert isinstance(task, ServiceTask)
    return task


@pytest.mark.asyncio
async def test_a_plain_step_leaves_retries_to_the_config(flow: Flow):
    task = await _task(flow, ship)
    assert (task.task_type, task.retries, task.backoff) == ("p:ship", None, None)


@pytest.mark.asyncio
async def test_a_steps_retry_options_are_part_of_its_definition(flow: Flow):
    task = await _task(flow, step(ship, max_attempts=5, backoff=(1.0, 9.0)))
    assert (task.retries, task.backoff) == (5, (1.0, 9.0))


@pytest.mark.asyncio
async def test_a_shared_handler_is_a_step_by_its_task_type(flow: Flow):
    task = await _task(flow, "payments.charge", element_id="payments.charge")
    assert task.task_type == "payments.charge"


@pytest.mark.asyncio
async def test_a_name_gives_the_same_function_a_second_step(flow: Flow):
    flow.pipeline("p", steps=[ship, step(ship, name="ship_again")])
    await flow.init()
    model = (await flow.definition("p")).model
    assert [e.id for e in model.elements] == ["start", "ship", "ship_again", "end"]


@pytest.mark.parametrize(
    "fn",
    [
        lambda ctx, state: {},
        functools.partial(lambda: {}),
    ],
    ids=["sync", "no-params"],
)
def test_a_step_that_is_not_async_def_ctx_state_is_refused(fn: object):
    with pytest.raises(BadRequestError):
        step(fn)  # type: ignore[arg-type]


@pytest.mark.parametrize("signature", ["(state)", "(ctx, state, extra)", "(ctx, *, state)"])
def test_a_handler_with_another_shape_is_refused(flow: Flow, signature: str):
    namespace: dict = {}
    exec(f"async def handle{signature}: return {{}}", namespace)
    with pytest.raises(BadRequestError, match=r"must take exactly \(ctx, state\)"):
        flow.handler("payments.charge")(namespace["handle"])


def test_two_steps_with_one_name_are_refused(flow: Flow):
    with pytest.raises(AlreadyExistsError, match="give one a name="):
        flow.pipeline("p", steps=[ship, ship])


def test_start_and_end_are_reserved_step_names(flow: Flow):
    with pytest.raises(BadRequestError, match="reserved"):
        flow.pipeline("p", steps=[step(ship, name="end")])


def test_a_function_without_a_usable_name_needs_one(flow: Flow):
    nameless = functools.partial(ship)
    with pytest.raises(BadRequestError, match="pass name="):
        step(nameless)
    flow.pipeline("p", steps=[step(nameless, name="ship")])


def test_a_condition_is_json_logic():
    with pytest.raises(BadRequestError, match="JSON Logic"):
        step(ship, when=lambda state: True)  # type: ignore[arg-type]


def test_a_condition_the_evaluator_lacks_is_refused(flow: Flow):
    with pytest.raises(BadRequestError, match="cannot run"):
        flow.pipeline("p", steps=[step(ship, when={"regex": ["x", "y"]})])


@pytest.mark.parametrize("task_type", ["orders:ship", ""])
def test_a_private_or_empty_task_type_is_refused(flow: Flow, task_type: str):
    with pytest.raises(BadRequestError):
        flow.handler(task_type)
    with pytest.raises(BadRequestError):
        step(task_type)


def test_a_pipeline_needs_steps(flow: Flow):
    with pytest.raises(BadRequestError, match="no steps"):
        flow.pipeline("p", steps=[])
