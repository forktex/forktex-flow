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
"""``step`` sets a pipeline step's retry contract; what it leaves unset comes from the ``Flow``'s config."""

from __future__ import annotations

import pytest
from forktex.error import BadRequestError

from forktex_flow import Flow, ServiceTask, step


def _work():
    async def _work(ctx: object, state: dict) -> dict:
        return {}

    return _work


async def _task(flow: Flow, item: object) -> ServiceTask:
    flow.pipeline("p")(type("P", (), {"steps": [item]}))
    await flow.init()
    task = (await flow.definition("p")).model.element("_work")
    assert isinstance(task, ServiceTask)
    return task


@pytest.mark.asyncio
async def test_a_bare_step_leaves_the_retry_contract_to_the_config(flow: Flow):
    task = await _task(flow, step(_work()))
    assert (task.retries, task.backoff) == (None, None)


@pytest.mark.asyncio
async def test_a_decorated_step_carries_its_retry_contract(flow: Flow):
    task = await _task(flow, step(max_attempts=5)(_work()))
    assert (task.retries, task.backoff) == (5, None)


@pytest.mark.asyncio
async def test_options_at_the_pipeline_override_the_decorator_one_by_one(flow: Flow):
    task = await _task(flow, step(step(max_attempts=5, backoff=(9.0,))(_work()), backoff=(1.0,)))
    assert (task.retries, task.backoff) == (5, (1.0,))


def test_a_guard_without_a_function_is_refused():
    with pytest.raises(BadRequestError, match="requires fn as a positional argument"):
        step(when=lambda s: True)
