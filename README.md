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
the top and skips past work that already happened. There is no graph, no router
and no signal inbox: a run visits every step once, in order, executing the ones
whose guard passes.

Less than a general workflow engine offers, and what a deploy pipeline, a nightly
reconcile or an ingest job actually needs.

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
reports an unrelated, much older lineage and the dependency looks
unsatisfiable. It is not; the interpreter is.

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
    +
    await flow.close()          # stops driving, releases the pool


app = FastAPI(lifespan=lifespan)
```

Run this on **every** replica. Exactly one wins the advisory lock and executes;
the rest stand by and take over if it dies. Workflows must be imported before
`start_driver()` — a decorator only runs when its module does.

`start_driver()` calls `init()` for you. Call `init()` alone when you want the
schema migrated by something that does not execute anything, such as a deploy
hook.

## Every option you have

The sections below are the whole consumer surface, in the order you meet it:
declare a workflow, build a `Flow`, run it, watch it, tune it. Exact semantics
for each are in [`docs/flow.md`](docs/flow.md); failure behaviour is in
[`docs/operating.md`](docs/operating.md).

### 1. Declaring a workflow

Two forms, and nothing else:

| Form | Declares |
|---|---|
| `@flow.pipeline(name, *, version, state=None, cron=None)` | A class with a `steps` list. `cron=` also schedules it. |
| `@flow.scheduled(name, *, version, cron, state=None, enabled=True)` | One async function on a cron. |

`enabled=False` registers the schedule without arming its timer. The flag is
read only when the row is first written; the database owns it afterwards, so no
deploy re-arms or disarms a schedule behind an operator.

### 2. Declaring a step

A step is `async def (ctx: Ctx, state: YourState) -> dict`. Bare, it takes the
engine's retry defaults. Wrapped in `step(...)`, it takes its own:

| Option | Effect |
|---|---|
| `step(fn, when=predicate)` | Run only if `predicate(state)` is true. Evaluated immediately before dispatch. |
| `step(fn, max_attempts=n)` | Attempts for this step, overriding `RuntimeConfig.max_attempts`. |
| `step(fn, backoff=(a, b, c))` | Seconds between attempts. The last entry repeats once attempts run past the tuple. |
| `@step(max_attempts=…, backoff=…)` | The same as a decorator, when the step needs no `when=`. |

### 3. Declaring state

A `TypedDict`. A step returns the **delta** it wants merged, not the whole
state. A field annotated with a reducer accumulates; everything else is
last-write-wins:

```python
class DeployState(TypedDict):
    logs: Annotated[list[str], operator.add]   # merged with operator.add
    server_id: str                             # overwritten
```

State must be JSON-serialisable — it is the memoisation key and it round-trips
through JSONB. A value JSONB cannot take raises `StateNotSerialisable`.

### 4. Building a `Flow`

```python
Flow(database_url=None, *, database=None, schema="forktex_flow",
     extensions=None, config=None, echo=False, **engine_kwargs)
```

| Option | Default | What it does |
|---|---|---|
| `database_url` | — | A DSN. The pool is this `Flow`'s and `close()` disposes it. |
| `database` | — | A `forktex` `Database` you own. `close()` leaves it alone. Exactly one of these two. |
| `schema` | `forktex_flow` | The Postgres schema. Two `Flow`s on different schemas share nothing. |
| `extensions` | `None` | `FlowExtension` objects, called on lifecycle transitions. |
| `config` | `RuntimeConfig()` | Retry, polling and concurrency behaviour. |
| `echo` | `False` | SQLAlchemy statement logging. |
| `**engine_kwargs` | — | Passed verbatim to `create_async_engine` (`pool_size`, `pool_pre_ping`, …). |

Construction is lazy; nothing connects until something executes.

### 5. Lifecycle

| Call | When |
|---|---|
| `await flow.init()` | Create the schema and apply migrations. Idempotent, and safe from every worker at once. |
| `await flow.verify()` | Probe the pool, creating nothing. For a readiness check. |
| `await flow.start_driver()` | Begin competing for leadership. Returns at once. |
| `await flow.stop_driver()` | Stop cleanly: waits up to 30s for in-flight runs, then cancels what is left mid-step. |
| `await flow.close()` | `stop_driver()`, then dispose the pool if this `Flow` made it. Every entry point raises `FlowClosed` after. |

`apply_migrations(engine, schema)` brings a schema up without building a `Flow`
— for a migration job that does not run workflows.

### 6. Starting a run

```python
instance = await flow.run("deploy.apply", state={"logs": []}, metadata={"actor": "ci"})
```

| Argument | Effect |
|---|---|
| `name` | The registered workflow name. |
| `state` | The initial state. Becomes `run.input`, which a resume replays from. |
| `metadata` | Arbitrary JSON, queryable with `.metadata(**kv)` against a GIN index. |
| `version` | A specific version. Omitted, the highest registered one. |

There is no idempotency key: calling `run()` twice queues two runs.
`flow.registered()` lists every name declared in this process.

### 7. Watching one run

`run()` and `flow.get(run_id)` both return a `WorkflowInstance` — a snapshot
plus four calls:

| Call | Returns |
|---|---|
| `await instance.wait(timeout=None)` | The snapshot once terminal, or at timeout. A timeout is not an error. |
| `await instance.refresh()` | A fresh snapshot now. |
| `async for snap in instance.stream()` | A snapshot per change, over `LISTEN`/`NOTIFY`. |
| `await instance.cancel()` | `True` if it took effect; `False` if the run had already finished. |

Its fields: `instance_id`, `workflow_name`, `workflow_version`, `status`,
`state`, `metadata`, `current_node`, `started_at`, `finished_at`, and `nodes` —
a `NodeInstance` per step with `name`, `status`, `attempt`, `started_at`,
`finished_at`, `duration`, `state_delta`, `error`.

### 8. Finding runs

`flow.query()` returns an `InstanceQuery`. Filters chain and **replace** on
repeat, except `.metadata()` and `.state()`, which accumulate:

| Filter | Matches |
|---|---|
| `.workflow(name, version=None)` | One workflow, optionally one version. |
| `.status(*statuses)` | Runs in any of these statuses. |
| `.triggered_by(*triggers)` | `manual`, `schedule`, or both. |
| `.metadata(**kv)` | `metadata @> {...}` containment. |
| `.state(**kv)` | `input @> {...}` containment. |
| `.current_node(*names)` | Runs whose running step is one of these. |
| `.since(dt)` / `.until(dt)` | Started at/after, or before. |
| `.limit(n)` | Page size. Default 50. |

And four terminals:

| Terminal | Returns |
|---|---|
| `await q.fetch(cursor=None)` | An `InstancePage`: `items`, `has_more`, `next_cursor`, `total`. Keyset-paged. |
| `await q.first()` | The newest match, or `None`. |
| `await q.count()` | How many match, without fetching them. |
| `await q.summary()` | A `RunSummary`: `total`, `by_status`, `avg_duration_seconds`, `p95_duration_seconds`, `oldest_started_at`, `newest_started_at`. |

### 9. Inside a step

The `Ctx` a step is handed carries `run_id`, `workflow_name`,
`workflow_version`, `node_name`, `attempt` and `metadata`, plus two calls:

| Call | Effect |
|---|---|
| `await ctx.emit(event_type, payload=None)` | Append a row to `run_event` — progress from inside a long step. |
| `await ctx.sleep(seconds)` | Sleep. **Not durable**: a worker dying mid-sleep restarts the run and the sleep begins again. |

### 10. Tuning the engine

```python
Flow(url, config=RuntimeConfig(poll_interval=0.5, max_concurrent_runs=25))
```

| Field | Default | Controls |
|---|---|---|
| `poll_interval` | `1.0` | Seconds between driver ticks while it leads. |
| `election_interval` | `2.0` | Seconds a loser waits before standing again — the bound on takeover time. |
| `heartbeat_interval` | `10.0` | Seconds between heartbeat writes on a running step and run. |
| `stale_threshold` | `60.0` | Silence after which a step or run is reclaimed. Must exceed `heartbeat_interval`. |
| `max_attempts` | `3` | Default attempts per step. |
| `max_concurrent_runs` | `10` | Runs one worker executes at once. |
| `backoff` | `(30.0, 120.0, 300.0)` | Default seconds between attempts. |
| `leader_lock_key` | `0` | Advisory lock key. Derived from the schema when left at `0`. |

### 11. Reacting to runs

A `FlowExtension` is any object with one or more of these. A failure in a hook
is logged and never changes the run's outcome:

| Hook | Called | Can |
|---|---|---|
| `before_start(name, version, input, metadata)` | Before a run is inserted | Return a dict merged into its metadata. |
| `after_complete(run, output)` | Once terminal `completed` | Observe. `run` is the run as it ended. |
| `after_fail(run, error)` | Once terminal `failed` or `cancelled` | Observe. |

### 12. Testing your workflows

| Helper | Gives you |
|---|---|
| `make_ctx(**overrides)` | A `Ctx` with no database, to await a step directly. `ctx.emit()` raises rather than dropping an event. |
| `isolated_flow(url)` | An async context manager: a migrated `Flow` on a throwaway schema, dropped after. |
| `run_to_completion(flow, name, state=…)` | Dispatch and execute a run inline, with no driver. |
| `apply_state_update(current, update, reducers)` · `extract_reducers(cls)` | The reducer machinery, to test your own state classes. |

```python
async with isolated_flow(postgres_url) as flow:
    register_my_workflows(flow)
    final = await run_to_completion(flow, "deploy.apply", state={"logs": []})
    assert final.state["logs"] == ["provisioned", "dns configured"]
```

### 13. Reading the engine's own tables

`flow.transaction()` is public — tailing `run_event` for a progress stream needs no
second pool. `flow.schema` and `flow.engine` are there for DDL, advisory locks
and migrations. The engine owns the transaction, so `commit()` raises.

## Errors

Everything this library raises derives from `forktex.error.AppError`, so a
transport that renders an `AppError` renders these with the right code rather
than a masked 500. The table of what is raised when is in
[`docs/flow.md`](docs/flow.md#errors).

## Stability

**The public surface is `forktex_flow.__all__`** — 28 names, asserted as an exact
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
change is a new `v{NNNN}`; `tests/test_persist/test_migration_immutability.py`
pins the content hash of every shipped migration, and
`test_schema_drift.py` proves the ORM and the SQL describe the same columns.

**Two things outside the Python API are also contracts:** the `forktex_flow`
schema name, and `run_event(id, ts, event_type, payload, run_id)`, which
consumers read directly by SQL without importing this package.

**Absent by design:** graph workflows,
`step_template`, config-defined workflows, signals, `parallel`, and child
workflows. Each was measured to have zero call sites before it was cut; see
*Removed* in `CHANGELOG.md`.

## Documentation

[`docs/flow.md`](docs/flow.md) — the reference: every public name, the API and
the gotchas.
[`docs/operating.md`](docs/operating.md) — durability and cancellation
semantics, leadership, what to watch, and the schema.
[`docs/development.md`](docs/development.md) — contributing.

## Licence

AGPL-3.0-or-later, or a commercial licence from FORKTEX S.R.L.
(info@forktex.com). See `LICENSE` and `NOTICE`.
