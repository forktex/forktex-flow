# `forktex_flow` — reference

The complete public surface, the semantics behind it, and the things that will
surprise you. For orientation and usage patterns, start with the
[README](../README.md); this page is what you read when you need the exact
behaviour of something.

## Public surface

`forktex_flow.__all__` is the contract — 28 names, frozen by test. Everything
else is internal. Each name has its own section below unless the gloss says it
all.

| Name | What it is |
|---|---|
| `Flow` | The facade. One per process. |
| `Ctx` | The per-step handle passed to every step body. |
| `step` | Decorator, and pipeline-entry builder. |
| `StepSpec` | What `step(fn, when=…)` returns. |
| `WorkflowInstance` | A run: a snapshot plus `wait` / `refresh` / `stream` / `cancel`. |
| `NodeInstance` | One step's execution record within a run. |
| `InstanceQuery` | The fluent finder behind `Flow.query()`. |
| `InstancePage` | One keyset page of `WorkflowInstance`. |
| `RunInfo`, `StepRunInfo` | Row shapes persistence returns. |
| `RunSummary` | Counts and duration percentiles over a match set. |
| `RunStatus`, `StepStatus` | `pending` · `running` · `completed` · `failed` · `cancelled`. |
| `TERMINAL_STATUSES` | The three a run does not leave. |
| `RuntimeConfig` | Retry, polling and concurrency behaviour. |
| `FlowExtension` | The consumer hook protocol. |
| `FlowError`, `StepFailed`, `WorkflowFailed`, `WorkflowCancelled` | The error leaves. |
| `StateNotSerialisable` | A state value JSONB cannot take. |
| `FlowClosed` | An entry point called after `close()`. |
| `apply_migrations` | Bring a schema up without constructing a `Flow`. |
| `apply_state_update`, `extract_reducers` | Reducer machinery, for testing your own state classes. |
| `make_ctx` | A `Ctx` for awaiting a step directly, with no database. |
| `isolated_flow` | Context manager: a migrated `Flow` on a throwaway schema. |
| `run_to_completion` | Dispatch and execute a run inline, without a driver. |

`WorkflowDefinition`, `NodeDef`, `StepMeta` and `RunFilter` are what the
decorators and the query builder compile *to* — importable from their modules,
but not in the root surface: no consumer constructs one, and freezing them would
make an internal representation an API.

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
mismatch logs a warning rather than failing: the tables resolve to whatever the
map says, which may be intentional.

`engine_kwargs` go verbatim to `create_async_engine` (`pool_size`,
`max_overflow`, `pool_pre_ping`, …).

Every method and argument is listed in the
[README](../README.md#every-option-you-have). What it does not say:

- `init()` is safe from every worker at once — concurrent callers serialise on
  an advisory lock.
- `verify()` raises `DatabaseUnreachableError`, and is shaped for
  `create_app(readiness_probes=...)`.
- `start_driver()` called twice does not start a second loop, and
  `stop_driver()` is safe before any start.
- `run(version=None)` takes the highest registered version. Registering the same
  `(name, version)` twice raises `AlreadyExistsError`.
- `run()` labels the run `triggered_by="manual"`; the driver labels a cron
  `"schedule"`. The `ck_run_triggered_by` constraint permits nothing else.
- `get(run_id)` raises `NotFoundError` when the run does not exist.
- `session()` hands out a read session on this `Flow`'s pool. The engine owns the
  transaction, so `commit()` raises `TransactionBoundaryError`.

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
position (`work` and `work_2`). Position, not an occurrence counter, so a
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

Every filter and terminal is listed in the
[README](../README.md#8-finding-runs). What it does not say:

`InstanceQuery` filters mutate and return `self`. Nothing executes until a
terminal is awaited, so the builder is I/O-free and testable without a database.

`metadata()` and `state()` accumulate across calls. Every other filter replaces
its previous value, so `.status("a").status("b")` filters on `"b"` alone.
`.state(**kv)` matches `run.input` — the state a run *started* with, not its
current state. Both compile to JSONB containment against a GIN index.

**Only `started_at` orders results, and that is deliberate.** Paging is keyset
over `(started_at, id)`: both NOT NULL, covered by `ix_run_started_at_id` in that
order, so the boundary compiles to a row comparison Postgres pushes into the
index as an `Index Cond`. Offering `finished_at` as a sort key would reintroduce
a nullable column, and a row comparison against NULL evaluates to NULL — dropping
the row from the page without failing anything.

`limit(n)` must be positive. `RunSummary.p95_duration_seconds` is the worst
per-status p95, not a true global one; computing that needs an ungrouped pass,
which the field name does not suggest.

## `WorkflowInstance`

| Method | Behaviour |
|---|---|
| `await refresh()` | A fresh snapshot. Does not mutate the one you hold. |
| `await wait(timeout=None)` | Blocks until terminal, polling every 0.5s. On timeout returns the snapshot it has — a timeout is not an error, and raising would discard the progress the run made. |
| `async for s in stream()` | Yields a snapshot whenever the run or any step moves, ending on the terminal one. The terminal snapshot is always emitted. |
| `await cancel()` | Ask the run to stop; returns whether it was still stoppable. See below. |

An instance rebuilt from a wire payload has no database behind it; live methods
raise `NotFoundError` saying so rather than failing three frames deeper.

## Running it in production

How the engine behaves under failure — memoisation and resume, leader election,
reclaim, the tables it owns, what to watch — is
[`docs/operating.md`](operating.md). Read it before putting a driver in front of
real work.

## Testing your workflows

Three helpers.

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
own and drops it afterwards, so a suite is safe against a single database:

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

## Errors

| Class | From | Raised when |
|---|---|---|
| `FlowError` | `forktex_flow` | The library itself failed |
| `StepFailed` | `forktex_flow` | A step body raised past its retries |
| `WorkflowFailed` | `forktex_flow` | A run reached terminal `failed` |
| `WorkflowCancelled` | `forktex_flow` | A run was cancelled |
| `StateNotSerialisable` | `forktex_flow` | State carries a value JSONB cannot take |
| `FlowClosed` | `forktex_flow` | An entry point was called after `close()` |
| `NotFoundError` | **`forktex.error`** | Unregistered workflow, or unknown run id |
| `AlreadyExistsError` | **`forktex.error`** | A `(name, version)` registered twice |
| `BadRequestError` | **`forktex.error`** | A bad argument — each method's docs say which |

The last three come from the substrate — `from forktex.error import
AlreadyExistsError, BadRequestError, NotFoundError`.

## Gotchas

- **A version is immutable.** Re-registering `(name, version)` raises. Two
  declarations disagreeing about what a workflow is means a run resumed after a
  restart executes whichever module was imported second. Bump the version.
- **State must be JSON-serialisable.** It is the memoisation key and it
  round-trips through JSONB. A step needing a session or a client builds one
  inside its own body from ids carried in state. A value JSONB cannot take
  raises `StateNotSerialisable` when the key is computed, which is the same run
  failure the state write would cause a moment later, named.
- **`Ctx.sleep` is not durable.** A worker dying mid-sleep re-executes the run
  from the top and the sleep starts over. For a delay that must survive a
  restart, end the step and let the retry backoff carry the wait.
- **`stale_threshold` must exceed `heartbeat_interval`.** `RuntimeConfig` rejects
  the reverse at construction: it reclaims a step that is still running.
- **Cancellation is not a rollback.** Completed steps stay completed and stay
  visible.
- **A step that is not registered in the dispatching process fails its run**
  rather than hanging, so an unclaimable `running` row cannot accumulate.
- **Delivery is at-least-once, not exactly-once.** A step body can run twice —
  a heartbeat that lagged past `stale_threshold` is enough. The memoisation in
  `step_run` makes a *completed* step skip on replay; it cannot un-send an email
  a step sent before it crashed. Side effects need their own idempotency key.
- **`flow.run()` takes no idempotency key.** Calling it twice queues two runs.
  Deduplicate before dispatch if a caller may retry.
- **There is no process host.** `start_driver()` runs inside your event loop;
  nothing daemonises, supervises or restarts it. `stop_driver()` waits up to 30
  seconds for in-flight runs and then **cancels** whatever is left, mid-step.
  Those runs stay `running` until a heartbeat goes stale and the next leader
  reclaims them, so a step whose side effects are not idempotent should finish
  well inside that window.
- **A CPU-bound step stalls the engine, not just itself.** Runs execute on the
  leader's loop, so a step that blocks without awaiting delays every other run
  on that worker, and the delay is the blocking duration. Push blocking work to
  a thread or a subprocess.
