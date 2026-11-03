# Guide

Complete, runnable files for the common ways to use `forktex-flow`. Each block
starts with the path it belongs at. The exact behaviour of every name used here
is in the [reference](reference.md).

- [Before you start](#before-you-start)
- [Your first workflow](#your-first-workflow)
- [Declare your workflows](#declare-your-workflows)
- [Run inside a web app](#run-inside-a-web-app)
- [Run a separate worker](#run-a-separate-worker)
- [Start work from a script](#start-work-from-a-script)
- [Run on a schedule](#run-on-a-schedule)
- [Deploy definitions at runtime](#deploy-definitions-at-runtime)
- [Wait for a person](#wait-for-a-person)
- [Import BPMN from a modeler](#import-bpmn-from-a-modeler)
- [Let an agent choose the steps](#let-an-agent-choose-the-steps)
- [Follow, find and cancel instances](#follow-find-and-cancel-instances)
- [Test your workflows](#test-your-workflows)

## Before you start

`forktex-flow` runs only on PostgreSQL (14 or newer). Every example needs:

1. **A database.** Any Postgres you can reach. For a local one:

   ```bash
   docker run -d --name flow-db -e POSTGRES_PASSWORD=flow -p 5432:5432 postgres:17
   ```

   The database user must be allowed to create the `forktex_flow` schema, which
   is created on first start.

2. **`DATABASE_URL`** pointing at it, with the `postgresql+asyncpg://` scheme:

   ```bash
   export DATABASE_URL=postgresql+asyncpg://postgres:flow@localhost:5432/postgres
   ```

3. **The package**, plus whatever a scenario adds (FastAPI, pytest):

   ```bash
   pip install forktex-flow
   ```

Every process that uses the same database and schema shares the same
workflows: an app that starts an instance, a worker that runs it and a script
that queries it see the same state.

## Your first workflow

One file: a two-step pipeline, started and waited on.

```python
# quickstart.py
import asyncio
import os

from pydantic import BaseModel

from forktex_flow import Ctx, Flow

flow = Flow(os.environ["DATABASE_URL"], drive=True)


class Signup(BaseModel):
    email: str
    user_id: str | None = None
    welcomed: bool = False


async def create_account(ctx: Ctx, signup: Signup) -> dict:
    return {"user_id": "u_" + signup.email.split("@")[0]}


async def send_welcome(ctx: Ctx, signup: Signup) -> dict:
    return {"welcomed": True}


flow.pipeline("users.signup", state=Signup, steps=[create_account, send_welcome])


async def main() -> None:
    async with flow:  # create the tables, deploy the pipeline, start working
        instance = await flow.start("users.signup", variables={"email": "ada@example.com"})
        done = await instance.wait(timeout=30)
        print(done.status, done.variables)
        print([(element.element_id, element.status) for element in done.elements])


asyncio.run(main())
```

```bash
python quickstart.py
```

```
completed {'email': 'ada@example.com', 'user_id': 'u_ada', 'welcomed': True}
[('start', 'completed'), ('create_account', 'completed'), ('send_welcome', 'completed'), ('end', 'completed')]
```

If the process dies after `create_account`, the next process to start finishes
the workflow with `send_welcome`. `create_account` does not run again.

## Declare your workflows

A `Flow` holds its own declarations: they are registered on that one object, and
nothing is global. A one-file program can build its `Flow` at module level. An
application should keep its declarations in a `register` function, so the web
app, the worker and the tests can each apply them to their own `Flow`.

```python
# myapp/workflows.py
from pydantic import BaseModel

from forktex_flow import Ctx, Flow, step


class Order(BaseModel):
    order_id: str
    total: float
    express: bool = False
    charge_id: str | None = None
    tracking: str | None = None


async def charge(ctx: Ctx, order: Order) -> dict:
    # Call your payment provider here, with ctx.instance_id as the idempotency
    # key: a job interrupted by a crash runs again.
    return {"charge_id": f"ch_{order.order_id}"}


async def upgrade_shipping(ctx: Ctx, order: Order) -> dict:
    return {"express": True}


async def ship(ctx: Ctx, order: Order) -> dict:
    return {"tracking": f"trk_{order.order_id}"}


def register(flow: Flow) -> Flow:
    flow.pipeline(
        "orders.fulfil",
        state=Order,
        steps=[
            step(charge, max_attempts=5, backoff=(1.0, 10.0)),
            step(upgrade_shipping, when={"var": "express"}),
            ship,
        ],
    )
    return flow
```

- Every step is `async def name(ctx, state)`, and returns only what it changed.
  With a pydantic `state=`, `flow.start()` refuses variables that do not match,
  and each step receives the validated model.
- `step(...)` sets a step's options: `max_attempts` and `backoff` (otherwise
  `RuntimeConfig`'s), `when` (a [JSON Logic](reference.md#conditions) condition;
  when it is false the step is not run), and `name`.
- A step's name, its function's name by default, is its identity. Renaming it is
  a breaking change for instances still running.
- Once a step has completed it never runs again, even if the process dies before
  the next one starts.

## Run inside a web app

Every replica of the app also runs the work. Any number of replicas can run;
they never take the same job.

```python
# myapp/main.py
import os
from contextlib import asynccontextmanager
from uuid import UUID

from fastapi import FastAPI
from pydantic import BaseModel

from forktex_flow import Flow
from myapp.workflows import register

flow = register(Flow(os.environ["DATABASE_URL"], drive=True))


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with flow:  # migrate, deploy the declarations, start working; on exit, drain and close
        yield


app = FastAPI(lifespan=lifespan)


class NewOrder(BaseModel):
    order_id: str
    total: float
    express: bool = False


@app.post("/orders")
async def create_order(order: NewOrder) -> dict:
    instance = await flow.start("orders.fulfil", variables=order.model_dump())
    return {"instance_id": str(instance.instance_id)}


@app.get("/orders/{instance_id}")
async def get_order(instance_id: UUID) -> dict:
    instance = await flow.get(instance_id)
    return {
        "status": instance.status,
        "steps": {element.element_id: element.status for element in instance.elements},
        "tracking": instance.variables.get("tracking"),
    }


@app.get("/ready")
async def ready() -> dict:
    await flow.verify()  # raises when the database is unreachable
    return {"ready": True}
```

```bash
pip install fastapi uvicorn
uvicorn myapp.main:app
curl -X POST localhost:8000/orders -H 'content-type: application/json' -d '{"order_id": "A-1", "total": 40, "express": true}'
curl localhost:8000/orders/<instance_id>
```

On shutdown, running jobs get 25 seconds to finish (`RuntimeConfig.drain_timeout`).
Give the process a longer stop grace period than that.

## Run a separate worker

To keep the work out of the web process, build the app's `Flow` with
`drive=False`, so it only starts and reads instances:

```python
# myapp/main.py
flow = register(Flow(os.environ["DATABASE_URL"], drive=False))
```

and run the work in its own process:

```python
# myapp/worker.py
import asyncio
import os
import signal

from forktex_flow import Flow
from myapp.workflows import register


async def main() -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    async with register(Flow(os.environ["DATABASE_URL"], drive=True)):
        await stop.wait()


asyncio.run(main())
```

```bash
python -m myapp.worker
```

Run as many workers as you like. A worker only takes jobs it has handlers for,
so a worker for heavy or privileged work is one whose `register` declares only
those workflows. `RuntimeConfig(max_concurrent_jobs=...)` caps how many jobs one
process runs at once.

## Start work from a script

A script or a deploy job starts instances and exits; a running worker or app
does the work.

```python
# scripts/reprocess.py
import asyncio
import os

from forktex_flow import Flow
from myapp.workflows import register


async def main() -> None:
    async with register(Flow(os.environ["DATABASE_URL"])) as flow:
        for order_id in ("A-1", "A-2", "A-3"):
            await flow.start("orders.fulfil", variables={"order_id": order_id, "total": 10.0, "express": False})


asyncio.run(main())
```

A release step that should only bring the schema up to date and deploy the
declarations runs `await flow.init()` the same way.

## Run on a schedule

A pipeline with `cron=` is scheduled. Add one to `register`:

```python
# myapp/workflows.py
async def daily_report(ctx: Ctx, state: dict) -> dict:
    # Build and send the report here.
    return {"sent": True}


def register(flow: Flow) -> Flow:
    flow.pipeline(
        "orders.fulfil",
        state=Order,
        steps=[
            step(charge, max_attempts=5, backoff=(1.0, 10.0)),
            step(upgrade_shipping, when={"var": "express"}),
            ship,
        ],
    )
    flow.pipeline("reports.daily", cron="0 6 * * *", steps=[daily_report])
    return flow
```

Cron is evaluated in UTC. However many processes run, each due time starts
exactly one instance, with no variables; a scheduled pipeline's `state=` must
accept that, and is refused when it is declared otherwise.

To turn a schedule off without a deploy, see
[Arm or disarm a schedule](operating.md#arm-or-disarm-a-schedule).

## Deploy definitions at runtime

When the process is data (configured per customer, drawn in an editor, stored
in your database), deploy it as a definition. The steps your code performs are
handlers, registered by task type: the catalogue those definitions can use.
Namespaces, typically one per customer, keep each one's definitions apart.

```python
# refunds.py
import asyncio
import os

from pydantic import BaseModel

from forktex_flow import Ctx, Flow

flow = Flow(os.environ["DATABASE_URL"], drive=True)


class Refund(BaseModel):
    amount: float


@flow.handler("refunds.pay", state=Refund)
async def pay(ctx: Ctx, refund: Refund) -> dict:
    return {"paid": refund.amount}


@flow.handler("refunds.notify")
async def notify(ctx: Ctx, variables: dict) -> dict:
    return {"notified": True}


REFUND = {
    "key": "refunds.standard",
    "elements": [
        {"id": "start", "type": "startEvent"},
        {"id": "pay", "type": "serviceTask", "taskType": "refunds.pay"},
        {"id": "notify", "type": "serviceTask", "taskType": "refunds.notify"},
        {"id": "end", "type": "endEvent"},
    ],
    "flows": [
        {"id": "f1", "source": "start", "target": "pay"},
        {"id": "f2", "source": "pay", "target": "notify"},
        {"id": "f3", "source": "notify", "target": "end"},
    ],
}


async def main() -> None:
    customer_id = 42
    namespace = f"customer-{customer_id}"
    async with flow:
        definition = await flow.deploy(REFUND, namespace=namespace)
        instance = await flow.start("refunds.standard", namespace=namespace, variables={"amount": 30})
        done = await instance.wait(timeout=30)
        print(definition.version, done.status, done.variables)


asyncio.run(main())
```

- Deploying an unchanged definition returns the existing version; a changed one
  becomes the next version. An instance finishes on the version it started on.
- A definition that cannot run is refused with `BadRequestError`, which lists
  every problem.
- A handler serves its task type in every namespace. When it must behave
  differently per customer, or refuse one, it reads `ctx.namespace`.
- A pipeline's own steps (task type `<pipeline key>:<step name>`) are private:
  a definition with another key that names one is refused. Put shared work in a
  handler, and list it in a pipeline by its task type:
  `steps=["refunds.pay", ...]`.
- A `callActivity` finds only definitions in its own namespace. To call one of
  your code pipelines from a customer's definition, deploy it into that namespace
  too: `await flow.deploy((await flow.definition("orders.fulfil")).model, namespace=namespace)`.

The [definition format](reference.md#the-definition-format) lists every element.

## Wait for a person

A `userTask` waits until your code completes it, and a gateway then routes on
what was submitted. Deciding who may complete it is up to your code.

```python
# approvals.py
import asyncio
import os

from forktex_flow import Ctx, Flow

flow = Flow(os.environ["DATABASE_URL"], drive=True)


@flow.handler("refunds.pay")
async def pay(ctx: Ctx, variables: dict) -> dict:
    return {"paid": variables["amount"]}


REFUND_WITH_REVIEW = {
    "key": "refunds.reviewed",
    "elements": [
        {"id": "start", "type": "startEvent"},
        {"id": "large", "type": "exclusiveGateway", "default": "small"},
        {"id": "review", "type": "userTask", "resultVariable": "review"},
        {"id": "approved", "type": "exclusiveGateway", "default": "rejected"},
        {"id": "pay", "type": "serviceTask", "taskType": "refunds.pay"},
        {"id": "paid", "type": "endEvent"},
        {"id": "declined", "type": "endEvent"},
    ],
    "flows": [
        {"id": "f1", "source": "start", "target": "large"},
        {"id": "needs_review", "source": "large", "target": "review", "condition": {">": [{"var": "amount"}, 100]}},
        {"id": "small", "source": "large", "target": "pay"},
        {"id": "f2", "source": "review", "target": "approved"},
        {"id": "yes", "source": "approved", "target": "pay",
         "condition": {"==": [{"var": "review.decision"}, "approve"]}},
        {"id": "rejected", "source": "approved", "target": "declined"},
        {"id": "f3", "source": "pay", "target": "paid"},
    ],
}


async def main() -> None:
    async with flow:
        await flow.deploy(REFUND_WITH_REVIEW)
        instance = await flow.start("refunds.reviewed", variables={"amount": 250})

        # What a review screen lists: every instance waiting at "review".
        waiting = await flow.query().key("refunds.reviewed").current_element("review").fetch()
        print([i.instance_id for i in waiting.items])

        # What the reviewer's "approve" button does.
        await flow.complete_task(instance.instance_id, "review", {"decision": "approve", "by": "dana"})

        done = await instance.wait(timeout=30)
        print(done.status, done.variables)


asyncio.run(main())
```

Completing a task that is no longer waiting raises `ConflictError`, so when two
reviewers act at once, exactly one succeeds.

## Import BPMN from a modeler

Processes drawn in Camunda Modeler (or any BPMN 2.0 editor) deploy as they are.
A service task's type comes from `zeebe:taskDefinition`.

```python
# bpmn_import.py
import asyncio
import os

from forktex_flow import Ctx, Flow

flow = Flow(os.environ["DATABASE_URL"], drive=True)

ORDER_SHIPPED = """<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0"
                  id="defs" targetNamespace="https://example.com/bpmn">
  <bpmn:process id="orders.shipped" isExecutable="true">
    <bpmn:startEvent id="start" />
    <bpmn:serviceTask id="email" name="Email the customer">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="email.send" retries="3" />
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:endEvent id="end" />
    <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="email" />
    <bpmn:sequenceFlow id="f2" sourceRef="email" targetRef="end" />
  </bpmn:process>
</bpmn:definitions>
"""


@flow.handler("email.send")
async def send_email(ctx: Ctx, variables: dict) -> dict:
    return {"emailed": variables["customer"]}


async def main() -> None:
    async with flow:
        definition = await flow.deploy(ORDER_SHIPPED)  # or Path("order-shipped.bpmn").read_text()
        done = await (await flow.start("orders.shipped", variables={"customer": "a@example.com"})).wait(timeout=30)
        print(done.status, done.variables)
        print(definition.source == ORDER_SHIPPED)  # the document is kept as written


asyncio.run(main())
```

Gateway conditions are written as JSON Logic in `conditionExpression`. The
[BPMN section](reference.md#bpmn-xml) of the reference lists what is read and
what is refused.

## Let an agent choose the steps

An `adHocSubProcess` gives control to a decider each round. The decider picks
which activities to run; they run in parallel as ordinary jobs, and the decider
runs again with their results.

```python
# agent.py
import asyncio
import os

from forktex_flow import Activation, AdHocDecision, Ctx, Flow

flow = Flow(os.environ["DATABASE_URL"], drive=True)


@flow.handler("research.decide")
async def decide(ctx: Ctx, variables: dict) -> AdHocDecision:
    # Ask your model what to do next, offering it ctx.activities as the tools.
    if "found" in variables:
        return AdHocDecision(done=True, output={"answer": variables["found"]["hits"][0]})
    return AdHocDecision(activate=(Activation(element_id="search", variables={"query": variables["question"]}),))


@flow.handler("tools.search")
async def search(ctx: Ctx, variables: dict) -> dict:
    return {"hits": [f"result for {variables['query']}"]}


RESEARCH = {
    "key": "research.question",
    "elements": [
        {"id": "start", "type": "startEvent"},
        {"id": "agent", "type": "adHocSubProcess", "taskType": "research.decide", "maxIterations": 5,
         "resultVariable": "result",
         "elements": [{"id": "search", "type": "serviceTask", "taskType": "tools.search", "resultVariable": "found"}]},
        {"id": "end", "type": "endEvent"},
    ],
    "flows": [
        {"id": "f1", "source": "start", "target": "agent"},
        {"id": "f2", "source": "agent", "target": "end"},
    ],
}


async def main() -> None:
    async with flow:
        await flow.deploy(RESEARCH)
        done = await (await flow.start("research.question", variables={"question": "durable workflows"})).wait(timeout=30)
        print(done.status, done.variables["result"])


asyncio.run(main())
```

`maxIterations` fails the instance if the decider never finishes. An activity
can be a `callActivity`, which runs another definition (another agent, say) as a
child instance.

## Follow, find and cancel instances

```python
# follow.py
import asyncio
import os
from datetime import UTC, datetime, timedelta

from forktex_flow import Ctx, Flow

flow = Flow(os.environ["DATABASE_URL"], drive=True)


async def nightly_import(ctx: Ctx, state: dict) -> dict:
    for batch in range(3):
        await ctx.emit("progress", {"batch": batch})
    return {"batches": 3}


flow.pipeline("imports.nightly", cron="0 3 * * *", steps=[nightly_import])


async def main() -> None:
    async with flow:
        instance = await flow.start("imports.nightly")  # a scheduled pipeline can be started by hand too

        async for snapshot in instance.stream():  # one snapshot per change, ending when finished
            print(snapshot.status, [element.element_id for element in snapshot.current_elements])

        done = await instance.refresh()
        for element in done.elements:  # every step visited: status, attempts, output, error, timing
            print(element.element_id, element.status, element.attempts, element.output)

        events = await done.events()  # the engine's history, and what handlers emitted
        print([(event.event_type, event.payload) for event in events])
        newer = await done.events(after=events[-1].id)  # read on from the last id seen
        print(newer)

        # Find instances.
        yesterday = datetime.now(UTC) - timedelta(days=1)
        print(await flow.query().key("imports.nightly").since(yesterday).summary())
        page = await flow.query().status("active").limit(20).fetch()
        print(len(page.items), page.next_cursor)

        # Cancel one: running handlers are stopped and child instances cancelled.
        other = await flow.start("imports.nightly")
        print(await other.cancel("started by mistake"))  # False if it had already finished


asyncio.run(main())
```

## Test your workflows

A step is a plain async function, so most tests call it directly with
`make_ctx()` and need no database. For the
whole workflow, `isolated_flow` gives a `Flow` on a throwaway schema and
`run_to_completion` runs its jobs inline, without a worker and without waiting
for retry backoff.

```bash
pip install pytest pytest-asyncio "testcontainers[postgres]>=4.15"
```

```toml
# pyproject.toml
[tool.pytest.ini_options]
asyncio_mode = "auto"
asyncio_default_fixture_loop_scope = "function"
```

```python
# tests/conftest.py
import pytest
from testcontainers.community.postgres import PostgresContainer


@pytest.fixture(scope="session")
def postgres_url():
    with PostgresContainer("postgres:17") as postgres:
        yield postgres.get_connection_url(driver="asyncpg")
```

```python
# tests/test_orders.py
from forktex_flow import isolated_flow, make_ctx, run_to_completion
from myapp.workflows import Order, charge, register


async def test_charge_records_the_charge():
    order = Order(order_id="A-1", total=40.0)
    assert await charge(make_ctx(), order) == {"charge_id": "ch_A-1"}


async def test_an_express_order_is_upgraded_and_shipped(postgres_url):
    async with isolated_flow(postgres_url) as flow:
        register(flow)
        done = await run_to_completion(flow, "orders.fulfil", variables={"order_id": "A-1", "total": 40.0, "express": True})
    assert done.status == "completed"
    assert done.element("upgrade_shipping").status == "completed"
    assert done.variables["tracking"] == "trk_A-1"


async def test_a_standard_order_is_not_upgraded(postgres_url):
    async with isolated_flow(postgres_url) as flow:
        register(flow)
        done = await run_to_completion(flow, "orders.fulfil", variables={"order_id": "A-2", "total": 10.0})
    assert done.element("upgrade_shipping") is None
```

```bash
pytest
```

`run_to_completion` returns early when the instance waits on a person; complete
the task with `flow.complete_task(...)` and continue with
`run_pending(flow, instance.instance_id)`.
