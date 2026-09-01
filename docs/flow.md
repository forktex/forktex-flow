# `forktex_flow` — reference

The complete public surface, the semantics behind it, and the things that will
surprise you. For orientation and usage patterns, start with the
[README](../README.md); this page is what you read when you need the exact
behaviour of something.

## Public surface

`forktex_flow.__all__` is the contract — 26 names, frozen by test. Everything
else is internal.

### Entry point

| Name | What it is |
|---|---|
| `Flow` | The facade. One per process. |
| `Ctx` | The per-step handle passed to every step body. |
| `step` | Decorator, and pipeline-entry builder. |
| `StepSpec` | What `step(fn, when=…)` returns. |

### Results

| Name | What it is |
|---|---|
| `WorkflowInstance` | A run: a snapshot plus `wait` / `refresh` / `stream` / `cancel`. |
| `NodeInstance` | One step's execution record within a run. |
| `InstanceQuery` | The fluent finder behind `Flow.query()`. |
| `InstancePage` | One keyset page of `WorkflowInstance`. |
| `RunInfo`, `StepRunInfo` | Row shapes the persistence layer returns. |
| `RunSummary` | Counts and duration percentiles over a matching set. |
| `RunStatus`, `StepStatus` | `pending` · `running` · `completed` · `failed` · `cancelled`. |
| `TERMINAL_STATUSES` | The three a run does not leave. |

### Configuration

| Name | What it is |
|---|---|
| `RuntimeConfig` | Retry and polling behaviour. |
| `FlowExtension` | The consumer hook protocol. |

`WorkflowDefinition`, `NodeDef`, `StepMeta` and `RunFilter` are what the
decorators and the query builder compile *to*. They are importable from their
modules but deliberately not in the root surface: no consumer constructs one, and
freezing them would make an internal representation an API.

### Errors and helpers

| Name | What it is |
|---|---|
| `FlowError`, `StepFailed`, `WorkflowFailed`, `WorkflowCancelled` | The error leaves. |
| `apply_migrations` | Bring a schema up without constructing a `Flow`. |
| `apply_state_update`, `extract_reducers` | The reducer machinery, exposed for testing your own state classes. |

### Testing

| Name | What it is |
|---|---|
| `make_ctx` | A `Ctx` for awaiting a step directly, with no database. |
| `isolated_flow` | An async context manager: a migrated `Flow` on a throwaway schema. |
| `run_to_completion` | Dispatch and execute a run inline, without a driver. |

## `Flow`

```python
Flow(
    database_url: str | None = None,
    *,
    database: Database | None = None,
    schema: str = "forktex_flow",
    extensions: list[FlowExtension] | None = None,
    config: RuntimeConfig | None = None,
    echo: bool = False,
    **engine_kwargs,
)
```

Exactly one of `database_url` or `database`. A URL builds a pool this `Flow`
owns and disposes on `close()`; a `Database` is yours and is left alone. When you
pass one, its `schema_translate_map` must remap `forktex_flow` to `schema` — a
mismatch logs a warning rather than failing, because the tables will resolve to
whatever the map says and that may be intentional.

`engine_kwargs` go verbatim to `create_async_engine` (`pool_size`,
`max_overflow`, `pool_pre_ping`, …). Construction is lazy; nothing connects until
something executes.

| Method | Behaviour |
|---|---|
| `await init()` | Create the schema and apply migrations. Idempotent; concurrent callers serialise on an advisory lock, so calling it from every worker is safe. |
| `await start_driver()` | Begin competing for leadership. Returns at once. Calling twice does not start a second loop. |
| `await stop_driver()` | Stop cleanly. Idempotent, and safe before any start. |
| `await close()` | `stop_driver()`, then dispose the pool **only if this `Flow` created it**. |
| `pipeline(name, *, version, state=None, cron=None)` | Class decorator over a `steps` list. |
| `scheduled(name, *, version, cron, state=None, enabled=True)` | Function decorator. `enabled=False` registers the schedule without arming its timer; read only when the row is first created, so the database stays authoritative afterwards. |
| `registered() -> list[str]` | Every workflow name declared in this process. |
| `await run(name, *, state=None, metadata=None, version=None) -> WorkflowInstance` | Insert a run `pending` and return a handle. The driver picks it up on its next tick. Labelled `triggered_by="manual"`. |
| `await get(run_id) -> WorkflowInstance` | Raises `NotFoundError` if absent. |
| `query() -> InstanceQuery` | Start a finder. |
| `session()` | An async transactional session on this `Flow`'s pool. |
| `schema`, `engine` | The schema name and the underlying engine. |

`version=None` on `run()` takes the highest registered version. Registering the
same `(name, version)` twice raises `AlreadyExistsError`.

## Steps

A step is `async def fn(ctx: Ctx, state: dict) -> dict`. It returns the **delta**
to merge, or `None` (treated as `{}`).

```python
@step                                          # defaults: 3 attempts, (30, 120, 300)
@step(max_attempts=5, backoff=(10.0, 60.0))    # a decorator with a retry contract
step(fn, when=predicate)                       # a guarded pipeline entry
step(fn, max_attempts=7)                       # a per-call-site override
```

Bare `@step` returns the function unchanged, so a step stays an ordinary callable
a unit test can await directly with a hand-built `Ctx` and no database.
`step(when=…)` without a positional `fn` raises `BadRequestError` — a guard with
nothing to guard is always a mistake.

The same function may appear twice in one pipeline; the compiler suffixes by
position (`work`, `work_1`, `work_2`). Position, not an occurrence counter, so a
resumed run still matches the rows it already wrote.

## `Ctx`

| Field | |
|---|---|
| `run_id`, `workflow_name`, `workflow_version` | Which run. |
| `node_name`, `attempt` | Rewritten by the executor before each step call. |
| `metadata` | What the run was started with, after `before_start` hooks. |

| Method | |
|---|---|
| `await emit(event_type, payload=None)` | Append a row to this run's event log. |
| `await sleep(seconds)` | Wait, in process. |

`Ctx` is constructible without a database — `emit` is the only method that needs
one, and it says so if it does not have one.

## State and reducers

```python
class State(TypedDict):
    logs:      Annotated[list[str], operator.add]
    server_id: str
```

`extract_reducers` reads the first **callable** in each field's `Annotated`
metadata. Other metadata (a description, a validator) is ignored. A field with no
reducer is last-write-wins.

`apply_state_update(current, update, reducers)` returns a new dict; `current` is
not mutated. A reducer is never called against a missing key — the first write
installs the value — so `operator.add` does not have to cope with `None`.

Every path that advances a run's state goes through it. Nothing calls
`dict.update` on a run state: that silently turns an accumulator into an
overwrite, and the only symptom is a log that keeps just its final line.

## Querying

`InstanceQuery` filters mutate and return `self`. Nothing executes until a
terminal method is awaited, so the builder is I/O-free and testable without a
database.

| Filter | Matches |
|---|---|
| `.workflow(name, version=None)` | Name, and optionally one version. |
| `.status(*statuses)` | Any-of. |
| `.metadata(**kv)` | JSONB containment on `run.metadata`. Repeated calls accumulate. |
| `.state(**kv)` | JSONB containment on `run.input` — the state a run *started* with. |
| `.current_node(*names)` | Runs whose currently-running step is one of these. |
| `.since(dt)` / `.until(dt)` | `started_at >= dt` / `started_at < dt`. |
| `.triggered_by(*labels)` | `manual` or `schedule`. |
| `.limit(n)` | Page size, default 50. Must be positive. |

| Terminal | Returns |
|---|---|
| `await fetch(cursor=None)` | `InstancePage`, newest first. |
| `await count()` | `int`, without fetching rows. |
| `await first()` | The newest match, or `None`. |
| `await summary()` | `RunSummary`. |

Filters compose; a later one never replaces an earlier one.

**Only `started_at` orders results, and that is deliberate.** Paging is keyset
over `(started_at, id)`: both NOT NULL, covered by `ix_run_started_at_id` in that
order, so the boundary compiles to a row comparison Postgres pushes into the
index as an `Index Cond`. Offering `finished_at` as a sort key would reintroduce
a nullable column, and a row comparison against NULL evaluates to NULL — dropping
the row from the page without failing anything.

`RunSummary.p95_duration_seconds` is the worst per-status p95, not a true global
one; computing that needs an ungrouped pass. Named here because the field name
suggests otherwise.

## `WorkflowInstance`

| Method | Behaviour |
|---|---|
| `await refresh()` | A fresh snapshot. Does not mutate the one you hold. |
| `await wait(timeout=None)` | Blocks until terminal, polling every 0.5s. On timeout returns the snapshot it has — a timeout is not an error, and raising would discard the progress the run made. |
| `async for s in stream()` | Yields a snapshot whenever the run or any step moves, ending on the terminal one. The terminal snapshot is always emitted. |
| `await cancel()` | Ask the run to stop; returns whether it was still stoppable. See below. |

An instance rebuilt from a wire payload has no database behind it; live methods
raise `NotFoundError` saying so rather than failing three frames deeper.

## Durability, precisely

**The memoisation key** is `(run_id, step_qualname, args_hash)` with a unique
constraint, where `args_hash` covers the step's **input state and its position**.
A completed row is returned without re-running the body.

**Resume replays from `run.input`.** `output` is written after every step for
observability but is never the resume point: resuming from a merged state would
feed a different state into the hash and miss every cached step, re-running side
effects that already happened.

**The hash normalises what it can.** Datetimes go through `to_iso`, which
converts to UTC first — so the same instant written with two different offsets
hashes identically, and a resume cannot miss a cached step because the caller's
tzinfo differed. Dates, UUIDs and unknown objects have fallbacks; state must
otherwise be JSON-serialisable, since it round-trips through JSONB.

**Retries are per step.** A retryable failure returns the step to `pending` with
`next_attempt_at` set. The last backoff entry repeats once attempts run past the
tuple, rather than collapsing back to the first.

**A skipped step records a completed row** with `{"__skipped__": true}`. It has
to: otherwise a resume re-evaluates the guard against a state that has since
moved on, and a step correctly skipped the first time runs anyway.

**Reclaim.** Running steps heartbeat every `heartbeat_interval`. A step silent
for longer than `stale_threshold` returns to `pending`; the run holding it is
swept from `running` back to `pending` so the next tick re-claims it. The cutoff
is computed on the **database** clock — comparing a database `heartbeat_at`
against an application-side `now()` puts two clocks on either side of the
comparison, and a live step gets reclaimed whenever the app clock runs ahead.

**Cancellation is cooperative, and between steps.** `cancel()` writes
`status='cancelled'`, and the executor re-reads the status before dispatching
each step — so a run that has not started stops before its first step, and a run
mid-flight stops before its *next* one. **The step already running completes**:
the library cannot interrupt arbitrary consumer code, and pretending otherwise
would leave half-finished side effects with nothing recording them.

`cancel()` returns `False` when the run had already finished, and changes
nothing. A terminal status is final — `update_run_status` carries a
`WHERE status NOT IN (terminal)` guard, without which a late writer overwrote a
finished run and left `status='completed'` beside `cancel_reason='cancelled by
operator'`.

**Claiming is a compare-and-swap.** `mark_step_running` updates
`WHERE status = 'pending'` and raises `StepClaimLost` when it matches nothing.
The loser abandons silently: whichever attempt won owns recording the outcome,
and a write from the loser would race it.

## Leadership

Every process competes for one session-scoped Postgres advisory lock, keyed on
the schema. Only the holder ticks; a worker that loses waits
`config.election_interval` before standing again, which is what bounds how long a
takeover takes after the leader dies. Its death drops the connection, releases the
lock, and the next election has a new leader — the entire failover mechanism, and
the reason there is no liveness protocol of our own to get wrong.

A tick, in order: fire due schedules → reclaim stale steps → revive runs holding
a ready step → claim up to ten pending runs and dispatch each. Each phase is
isolated; one raising does not cost the tick the other two, because they recover
independent kinds of stuck work.

Runs execute concurrently inside the one leader — they are coroutines, bounded by
the event loop, so a few hundred in-flight I/O-bound runs. Scaling past that needs
a sharded lock, deliberately not built because nothing is near it.

## Testing your workflows

Three helpers, because otherwise every consumer writes them.

**A step is an ordinary callable.** `@step` returns the function unchanged, so
most of what a step does is testable with no database at all:

```python
from forktex_flow import make_ctx

async def test_provision_records_the_server():
    result = await provision(make_ctx(metadata={"org_id": "acme"}), {"logs": []})
    assert result["server_id"] == "srv-1"
```

`make_ctx` is not bound to a database, so `Ctx.emit()` raises rather than
silently dropping an event a test believed it recorded.

**A whole pipeline needs Postgres.** `isolated_flow` gives one a schema of its
own and drops it afterwards, which is what makes a suite safe to run against a
single database:

```python
import pytest_asyncio
from forktex_flow import isolated_flow, run_to_completion

@pytest_asyncio.fixture
async def flow(postgres_url):
    async with isolated_flow(postgres_url) as f:
        register_my_workflows(f)
        yield f

async def test_the_deploy_pipeline(flow):
    final = await run_to_completion(flow, "deploy.apply", state={"logs": [], "dns_enabled": True})
    assert final.status == "completed"
    assert final.state["logs"] == ["provisioned", "dns configured"]
```

`run_to_completion` executes inline rather than starting a driver, so a test
asserts on a result instead of polling a background loop and hoping the tick
landed. It re-executes a run that scheduled a retry until the run is terminal —
give such steps `backoff=(0.0,)` or the wait is real — and gives up after
`max_attempts` passes so a workflow that never settles fails the test instead of
hanging it.

Needs `pip install "forktex-flow[testing]"` for `testcontainers`, or point
`isolated_flow` at any Postgres you already have.

## Operating it

**Logging.** Every module logs through `forktex.log.get_logger(__name__)`, so
the logger names are `forktex_flow.runtime.driver`, `forktex_flow.persist.steps`
and so on — configure them like any other. The driver logs at `info` when it
wins leadership and when it reclaims stale steps, at `warning` when it declines
a duplicate dispatch or an extension hook raises, and at `exception` when a tick
phase fails. Nothing logs per step; that is what `run_event` is for.

**What to watch.** The engine has no metrics endpoint — it has queries:

```python
# Runs that have been executing a given step for a while.
stuck = await flow.query().status("running").current_node("provision_vps").count()

# The backlog: anything pending is waiting for a leader to pick it up.
backlog = await flow.query().status("pending").count()

# Failures in the last hour, by workflow.
recent = await flow.query().status("failed").since(hour_ago).summary()
```

A `pending` count that grows without bound means no worker holds the lock — the
single most useful alert to have.

**Capacity.** Runs execute concurrently inside the one leader as coroutines,
bounded by its event loop: a few hundred in-flight I/O-bound runs. Only the
leader executes, so adding replicas buys availability, not throughput. Past that
ceiling you need a sharded lock, which is deliberately not built.

**Growth.** `run_event` is append-only and `step_run` keeps a row per step
invocation, so both grow with usage and neither is pruned for you. A retention
job deleting old terminal runs cascades to both — decide the policy before the
tables decide it for you.

**Deploying a new version.** A run records the version it started under and keeps
executing that definition, so a rolling deploy where both versions are briefly
registered is safe. Bump the version whenever the step list changes shape;
re-registering an existing `(name, version)` raises `AlreadyExistsError`.

## Extensions

```python
class TenantScope:
    async def before_start(self, name, version, input, metadata) -> dict: ...
    async def after_complete(self, run: RunInfo, output) -> None: ...
    async def after_fail(self, run: RunInfo, error: BaseException) -> None: ...
```

A `Protocol`, and every method is optional — implement what you need on a plain
object. `before_start` returns a dict merged into the run's metadata before the
row is inserted; that metadata reaches every step via `Ctx.metadata` and is
queryable via `.metadata(**kv)` against a GIN index. Terminal hooks are
**awaited**; one that raises is logged and swallowed.

Per-run scoping goes in metadata, not in columns. An earlier design let an
extension declare extra columns on `run`/`step_run`, added by `ALTER TABLE` at
`init()` — but nothing ever wrote to them and nothing ever read them, so it was
143 lines of DDL machinery producing columns that stayed NULL. JSONB containment
on `metadata` does the job with an index behind it and no schema to migrate.

## Schema

| Table | Holds |
|---|---|
| `workflow` | `(name, version)` registry — what `scheduled_run` foreign-keys to. |
| `run` | One row per `flow.run()`. |
| `step_run` | One row per step invocation; the memoisation table. |
| `run_event` | Append-only progress log. |
| `scheduled_run` | Cron state: expression, `enabled`, `next_fire_at`. |
| `flow_schema_version` | Applied migrations. Not the runner's default name — deployed databases carry this one. |

## Gotchas

- **A version is immutable.** Re-registering `(name, version)` raises. Two
  declarations disagreeing about what a workflow is means a run resumed after a
  restart executes whichever module was imported second. Bump the version.
- **State must be JSON-serialisable.** It is the memoisation key and it
  round-trips through JSONB. A step needing a session or a client builds one
  inside its own body from ids carried in state.
- **`Ctx.sleep` is not durable.** A worker dying mid-sleep re-executes the run
  from the top and the sleep starts over. For a delay that must survive a
  restart, end the step and let the retry backoff carry the wait.
- **`stale_threshold` must exceed `heartbeat_interval`.** `RuntimeConfig` rejects
  the reverse at construction, because it means a healthy step gets reclaimed
  while it is still running.
- **Cancellation is not a rollback.** Completed steps stay completed and stay
  visible.
- **A step that is not registered in the dispatching process fails its run**
  rather than hanging. A `running` row nobody will pick up is how a queue
  silently accumulates work forever.
