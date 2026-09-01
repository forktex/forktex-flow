# forktex-flow

Durable workflows in Postgres. No broker, no scheduler process, no separate
control plane — the database you already run, plus your own worker processes, is
the whole infrastructure.

```bash
pip install forktex-flow
```

Python 3.14, PostgreSQL 14+. Built on the
[`forktex`](https://github.com/forktex/forktex-py) substrate, which pip resolves
for you.

## What it does

You write ordinary async functions. The library makes them survive a crash.

```python
import operator
from typing import Annotated, TypedDict

from forktex_flow import Ctx, Flow, step

flow = Flow(database_url="postgresql+asyncpg://user:pass@host/db")


class DeployState(TypedDict):
    logs: Annotated[list[str], operator.add]   # accumulates across steps
    server_id: str                             # last write wins
    dns_enabled: bool


async def provision(ctx: Ctx, state: DeployState) -> dict:
    server = await hetzner.create_server()
    return {"server_id": server.id, "logs": [f"provisioned {server.id}"]}


async def configure_dns(ctx: Ctx, state: DeployState) -> dict:
    await dns.point(state["server_id"])
    return {"logs": ["dns configured"]}


@flow.pipeline("deploy.apply", version=1, state=DeployState)
class DeployApply:
    steps = [provision, step(configure_dns, when=lambda s: s["dns_enabled"])]
```

Start a worker and dispatch — Postgres must be running and reachable at that DSN:

```python
await flow.start_driver()   # migrates the schema, then competes for leadership

instance = await flow.run("deploy.apply", state={"dns_enabled": True, "logs": []})
final = await instance.wait(timeout=600)
print(final.status, final.state["logs"])
```

If the worker dies after `provision` and before `configure_dns`, another worker
picks the run up, `provision` is **not** re-run — its recorded output is
returned — and `configure_dns` executes for the first time. No second server
gets created.

## The model, in four sentences

A workflow is an **ordered list of steps**. A step returns the *delta* it wants
merged into the run's state, not the whole state. Every step is memoised on
`(run_id, step, input state)`, so a run picked up after a crash re-executes from
the top and skips straight past work that already happened. There is no graph, no
router and no signal inbox: a run visits every step once, in order, executing the
ones whose guard passes.

That is deliberately less than a general workflow engine offers. It is what a
deploy pipeline, a nightly reconcile or an ingest job actually needs, and it fits
in one file you can read.

## Installing

```bash
pip install forktex-flow
```

To test your own workflows, take the extra — it brings in `testcontainers`:

```bash
pip install "forktex-flow[testing]"
```

**Install with Python 3.14.** `forktex` and `forktex-flow` both declare
`requires-python = ">=3.14"`, and pip hides a release whose floor your
interpreter does not meet rather than saying why — so an older interpreter
reports the newest `forktex` as `0.2.4`, an unrelated lineage, and the
dependency looks unsatisfiable. It is not; the interpreter is.

Developing against a local checkout of the substrate:

```bash
pip install -e ../forktex-py
pip install -e .
```

## Wiring it into your app

The library runs inside a process you already have. In FastAPI that is the
lifespan.

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI
from forktex_flow import Flow

flow = Flow(database_url=settings.database_url)
import myapp.workflows  # noqa: F401 — importing runs the @flow.pipeline decorators


@asynccontextmanager
async def lifespan(app: FastAPI):
    await flow.start_driver()   # migrates, then competes for the leader lock
    yield
    await flow.close()          # stops driving, releases the pool


app = FastAPI(lifespan=lifespan)
```

Run this on **every** replica. Exactly one wins the advisory lock and executes;
the rest stand by and take over if it dies. Workflows must be imported before
`start_driver()` — a decorator only runs when its module does.

`start_driver()` calls `init()` for you. Call `init()` alone when you want the
schema migrated by something that does not execute anything, such as a deploy
hook.

## How consumers use it

One example and one sentence each; **[`docs/flow.md`](docs/flow.md) has the exact
semantics.**

### Conditional steps

`when=` is a predicate over state, evaluated immediately before dispatch.

```python
@flow.pipeline("deploy.apply", version=4, state=DeployState)
class DeployApply:
    steps = [
        load_manifest,
        step(provision_vps, when=lambda s: s["is_new_server"]),
        deploy_services,
        step(ssl_provision, when=lambda s: s["ssl_enabled"]),
        health_check,
    ]
```

### Accumulating state

Any callable in the `Annotated` metadata is the reducer; a field without one is
last-write-wins.

```python
class State(TypedDict):
    logs:      Annotated[list[str], operator.add]     # every step appends
    server_id: str                                    # each step overwrites
```

### Cron workflows

```python
@flow.scheduled("lifecycle.reconcile", version=1, cron="*/30 * * * *", state=State)
async def reconcile(ctx: Ctx, state: State) -> dict:
    return {"logs": ["reconciled"]}
```

Registered on every worker; fired once, by whichever holds the lock.

`enabled=False` writes the schedule's row without starting its timer — for a
cron whose first tick is destructive and wants a deliberate hand. The flag is
read once, when the row is created; after that the database owns the switch, so
`UPDATE scheduled_run SET enabled = true` arms it and no later deploy re-arms or
disarms it behind you.

### Per-step retries

```python
@step(max_attempts=5, backoff=(30.0, 120.0, 300.0))
async def flaky(ctx: Ctx, state: State) -> dict: ...
```

### Progress from inside a long step

```python
await ctx.emit("progress", {"done": i, "total": len(chunks)})
```

### Stopping a run

```python
from forktex_flow import WorkflowCancelled

async def check_target(ctx: Ctx, state: State) -> dict:
    if not await target_exists(state["server_id"]):
        raise WorkflowCancelled("the target was deleted while we were deploying")
    return {}
```

From outside, `await instance.cancel()` stops the run before its **next** step
and returns whether it was still stoppable. Work already done stays done.

### Finding runs

```python
page = await (
    flow.query()
    .workflow("deploy.apply", version=4)
    .status("running", "pending")
    .metadata(org_id=org)
    .limit(25)
    .fetch(cursor)
)
stuck = await flow.query().current_node("provision_vps").count()
```

### Testing your workflows

```python
from forktex_flow import isolated_flow, make_ctx, run_to_completion

# A step is an ordinary callable — no database needed.
assert await provision(make_ctx(), {"logs": []}) == {"server_id": "srv-1", ...}

# Or run the whole pipeline against a throwaway schema.
async with isolated_flow(postgres_url) as flow:
    register_my_workflows(flow)
    final = await run_to_completion(flow, "deploy.apply", state={"logs": []})
    assert final.state["logs"] == ["provisioned", "dns configured"]
```

## Where it flexes

- **Share your pool** — pass a `Database` instead of a URL and `close()` leaves
  it alone, because it is yours.
- **Many schemas in one process** — `Flow(url, schema="flow_tenant_a")`. Two
  instances see nothing of each other.
- **Stamp context onto every run** — a `FlowExtension` with `before_start`
  returns a dict merged into metadata, which reaches every step via
  `Ctx.metadata` and is queryable through `.metadata(**kv)` against a GIN index.
- **React to outcomes** — `after_complete` / `after_fail`, awaited, with a
  failure in one isolated from the run.
- **Tune retries and timings** — `RuntimeConfig(poll_interval=…,
  election_interval=…, heartbeat_interval=…, stale_threshold=…, max_attempts=…)`.
- **Read the engine's own tables** — `flow.session()` is public, because tailing
  `run_event` for a progress stream should not need a second pool.

## What you get for free

- **Steps are memoised.** Side effects happen once, across any number of resumes.
- **A stalled step is reclaimed** and re-dispatched; the run holding it is swept
  back so it can be claimed again.
- **One leader executes.** The holder's death drops its connection, releases the
  lock, and the next election has a new leader — the entire failover mechanism.
- **Work is partitioned, not duplicated.** Two leaders overlapping at handover
  divide the queue rather than racing over it.

Each is asserted by a test against a real Postgres, with two independent
connection pools where contention is the point.

## Errors

| Class | From | Raised when |
|---|---|---|
| `FlowError` | `forktex_flow` | The library itself failed |
| `StepFailed` | `forktex_flow` | A step body raised past its retries |
| `WorkflowFailed` | `forktex_flow` | A run reached terminal `failed` |
| `WorkflowCancelled` | `forktex_flow` | A run was cancelled |
| `NotFoundError` | **`forktex.error`** | Unregistered workflow, or unknown run id |
| `AlreadyExistsError` | **`forktex.error`** | A `(name, version)` registered twice |
| `BadRequestError` | **`forktex.error`** | A bad argument — each method's docs say which |

The last three come from the substrate, not this package:

```python
from forktex.error import AlreadyExistsError, BadRequestError, NotFoundError
```

All of them derive from `forktex.error.AppError`, so a transport that renders an
`AppError` renders these with the right code rather than a masked 500.

## Stability

**The public surface is `forktex_flow.__all__`** — 26 names, asserted as an exact
set by `tests/test_architecture/test_public_surface.py`. Adding or removing one
is a deliberate edit to that test. Anything reachable but not in `__all__` is
internal and may change in any release.

**This is 0.x, but not a free-for-all.** A public name is removed only after one
minor release in which it still works and its docstring says it is deprecated.
Behaviour changes a correct consumer would notice are called out under *Changed*
in `CHANGELOG.md`.

**`FlowExtension`'s hook signatures are frozen.** New lifecycle points arrive as
new optional methods, never as new parameters on existing ones — so an extension
written today keeps working.

**Migrations are forward-only, and an applied one is never edited.** A schema
change is a new `v000N`; `tests/test_persist/test_migration_immutability.py`
pins the content hash of every shipped migration, and
`test_schema_drift.py` proves the ORM and the SQL describe the same columns.

**Two things outside the Python API are also contracts:** the `forktex_flow`
schema name, and `run_event(id, ts, event_type, payload, run_id)`, which
consumers read directly by SQL without importing this package.

**Deliberately absent, and not returning by oversight:** graph workflows,
`step_template`, config-defined workflows, signals, `parallel`, and child
workflows. Each was measured to have zero call sites before it was cut; see
*Removed* in `CHANGELOG.md`.

## Documentation

[`docs/flow.md`](docs/flow.md) — the reference: every public name, exact
durability and cancellation semantics, operating notes, and the gotchas.
[`docs/development.md`](docs/development.md) — contributing.

## Licence

AGPL-3.0-or-later, or a commercial licence from FORKTEX S.R.L.
(info@forktex.com). See `LICENSE` and `NOTICE`.
