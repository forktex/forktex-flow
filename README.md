# forktex-flow

Durable workflows in the Postgres you already run. No broker, no scheduler
service, no control plane: your own processes claim the work, and the database
holds every instance's state.

```bash
pip install forktex-flow
```

Python 3.14, PostgreSQL 14+. Built on [`forktex`](https://github.com/forktex/forktex-py).

## Five-minute tour

Declare the work in code, start it, and wait:

```python
import operator
from typing import Annotated, TypedDict

from forktex_flow import Ctx, Flow, step

flow = Flow("postgresql+asyncpg://user:pass@host/db", drive=True)


class Deploy(TypedDict):
    logs: Annotated[list[str], operator.add]   # accumulates across steps
    server_id: str                             # last write wins
    dns: bool


async def provision(ctx: Ctx, state: Deploy) -> dict:
    server = await hosting.create_server()
    return {"server_id": server.id, "logs": [f"provisioned {server.id}"]}


async def point_dns(ctx: Ctx, state: Deploy) -> dict:
    await dns.point(state["server_id"])
    return {"logs": ["dns pointed"]}


@flow.pipeline("deploy.apply", state=Deploy)
class DeployApply:
    steps = [provision, step(point_dns, when=lambda s: s["dns"])]


async def main() -> None:
    async with flow:   # migrate, deploy the declarations, start driving
        instance = await flow.start("deploy.apply", variables={"dns": True, "logs": []})
        done = await instance.wait(timeout=600)
        print(done.status, done.variables["logs"])
```

If the process dies between `provision` and `point_dns`, another process takes
the job over: `provision` is not repeated, and `point_dns` runs.

Underneath, everything is a **process definition**: BPMN elements joined by
sequence flows, versioned in the database. A pipeline is a ready-made one
(start → one service task per step → end). Definitions can also be written as
JSON or BPMN XML and deployed at runtime. Both kinds run on the same engine.

## Wiring it in

Pick the shape that matches the process you already have. Every shape uses the
same tables, so they mix freely: a web app can start work that a worker
finishes.

### A web app that also does the work

Drive inside the app's lifespan and run it on every replica. Each replica claims
the jobs it has handlers for; one elected replica also fires schedules and
reclaims jobs whose process died.

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI

from myapp.workflows import flow   # importing the module runs the decorators


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with flow:   # on exit: drain running jobs, hand the rest back, close the pool
        yield


app = FastAPI(lifespan=lifespan)


@app.post("/deploys")
async def create_deploy(body: DeployRequest) -> dict:
    instance = await flow.start("deploy.apply", variables=body.model_dump(), metadata={"user": body.user})
    return {"id": str(instance.instance_id)}
```

Use `await flow.verify()` as the readiness probe. Keep the host's stop grace
period above `RuntimeConfig.drain_timeout` (25 s by default).

### A dedicated worker

The app builds its `Flow` with `drive=False` (the default) and only starts
instances. A separate process does the work:

```python
import asyncio
import signal

from myapp.workflows import flow   # built with drive=True here


async def main() -> None:
    stop = asyncio.Event()
    asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, stop.set)
    async with flow:
        await stop.wait()


asyncio.run(main())
```

A worker runs only the task types it registers handlers for. Split CPU-heavy
work, or anything needing special credentials, into its own worker by
registering only those handlers there. Cap one process's concurrency with
`RuntimeConfig(max_concurrent_jobs=...)`.

### A script or a deploy job

A one-off script enters the `Flow`, starts an instance and exits. A worker picks
the instance up later:

```python
async with Flow(url) as flow:
    await flow.start("reports.monthly", variables={"month": "2026-10"})
```

A release step that should only migrate the schema and deploy the declarations
calls `await flow.init()` (or `apply_migrations(engine, schema)` without
building a `Flow`) and exits.

### Scheduled work

```python
@flow.scheduled("billing.close_day", cron="0 2 * * *", enabled=False)
async def close_day(ctx: Ctx, variables: dict) -> dict:
    return {"closed": await ledger.close_yesterday()}
```

Whichever replica leads starts an instance per due time, and only one does.
`enabled=False` declares a schedule disarmed. The flag is written only with the
row, so arming it is a deliberate act on the database (see
[`docs/operating.md`](docs/operating.md#operator-actions)), never a side effect
of a deploy.

## Definitions as data

Code pipelines cover work the product itself defines. When the process belongs
to a tenant or comes from an editor, deploy it as data. The handlers stay in
your code and are registered by **task type**:

```python
@flow.handler("invoice.issue")
async def issue(ctx: Ctx, variables: dict) -> dict:
    return {"invoice": await billing.issue(variables["order"])}


await flow.deploy(definition_json, namespace="org-42")   # a ProcessModel, its JSON, or BPMN 2.0 XML
await flow.start("billing.dunning", namespace="org-42", variables={"order": 7})
```

- **Namespaces keep tenants apart.** The same key in two namespaces is two
  definitions, versioned on its own, and a call activity never leaves its
  namespace. To let a tenant's process call one of your code pipelines, deploy
  that pipeline into the tenant's namespace:
  `await flow.deploy((await flow.definition("deploy.apply")).model, namespace="org-42")`.
- **Deploy refuses a definition that cannot run**, listing every reason at once,
  so an editor can show them all.
- **Redeploying an unchanged definition is free**; a changed one becomes the
  next version. Running instances finish on the version they started.
- **BPMN XML from Camunda Modeler** works as written: task types come from
  `zeebe:taskDefinition`, and the XML is kept verbatim in
  `DefinitionInfo.source`.

### Approvals and other human steps

A `userTask` waits until your code completes it. Who may do that is your
decision:

```python
waiting = await flow.query().namespace("org-42").current_element("review").fetch()
await flow.complete_task(waiting.items[0].instance_id, "review", {"decision": "approve"})
```

A gateway after it routes on the payload with a JSON Logic condition such as
`{"==": [{"var": "review.decision"}, "approve"]}`. Completing a task that is no
longer waiting raises `ConflictError`, so of two reviewers clicking at once,
exactly one wins.

### Agents

An `adHocSubProcess` hands control to a decider each round. The decider picks
which tools to run, the tools run in parallel as ordinary jobs, and the decider
runs again with their results:

```python
from forktex_flow import Activation, AdHocDecision


@flow.handler("agent.decide")
async def decide(ctx: Ctx, variables: dict) -> AdHocDecision:
    plan = await llm.next_step(variables, tools=ctx.activities)
    if plan.done:
        return AdHocDecision(done=True, output={"answer": plan.answer})
    return AdHocDecision(activate=tuple(Activation(element_id=c.tool, variables=c.args) for c in plan.calls))
```

`maxIterations` bounds a decider that never finishes. A tool may itself be a
`callActivity` that runs a sub-agent.

## Following an instance

```python
instance = await flow.get(instance_id)
async for snapshot in instance.stream():      # a snapshot per change, ending when terminal
    print(snapshot.status, [e.element_id for e in snapshot.current_elements])

await instance.cancel("customer withdrew")    # reaches running handlers and child instances

failed_today = await flow.query().status("failed").since(midnight).summary()
```

A handler records progress with `await ctx.emit("progress", {...})`, which
appends to `instance_event`. A UI tails that table by `id`.

## Testing your workflows

```python
from forktex_flow import isolated_flow, make_ctx, run_to_completion


async def test_provision_returns_the_server():
    assert (await provision(make_ctx(), {"logs": []}))["server_id"]


async def test_the_pipeline_completes(postgres_url):
    async with isolated_flow(postgres_url) as flow:   # a throwaway schema
        register_workflows(flow)
        done = await run_to_completion(flow, "deploy.apply", variables={"dns": False, "logs": []})
    assert done.status == "completed"
```

`isolated_flow` takes any Postgres URL, such as one from testcontainers.
`run_to_completion` and `run_pending` run jobs inline, so they need no driver
and wait for no backoff.

## Things to know

- **At-least-once.** A job interrupted by a crash runs again. Key side effects
  on `ctx.instance_id` and `ctx.element_id`.
- **No idempotency key.** Two `start()` calls start two instances.
- **Definitions are acyclic.** An element runs at most once per instance;
  repetition is a new instance, a call activity, or an ad-hoc subprocess.
- **Variables are JSON.** Return a dict, or a pydantic model, that JSONB can
  hold.
- **A rolling deploy that removes a step** leaves old instances waiting at it
  until a process with its handler runs again; the leader logs a warning naming
  the task type.

## Documentation

- [`docs/flow.md`](docs/flow.md): the reference. Every public name, option,
  element and error.
- [`docs/operating.md`](docs/operating.md): failure behaviour, logging,
  operator SQL and the schema.
- [`docs/development.md`](docs/development.md): working on the library itself.

## Stability

The public surface is `forktex_flow.__all__`; anything else may change in any
release. Semantic versioning applies to it, to `FlowExtension`'s hook
signatures, to the `forktex_flow` schema name, and to
`instance_event(id, instance_id, ts, event_type, payload)`. Migrations are
forward-only.

## Licence

AGPL-3.0-or-later, or a commercial licence from FORKTEX S.R.L.
(info@forktex.com). See `LICENSE` and `NOTICE`.
