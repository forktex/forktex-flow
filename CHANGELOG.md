# Changelog

All notable changes to `forktex-flow` are documented here. This project follows
[Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.4.0] — 2026-11-01

### Fixed

- **A run whose worker died between steps stayed `running` forever.** Recovery could only see
  a run through its step rows, and a run has none before its first step, between any two steps,
  or after its last — so a worker lost in any of those windows left a run nothing would pick up.
  Runs now carry `heartbeat_at`, stamped for the whole of `execute_run`, and the revive sweep
  re-pends a `running` run with no live step whose heartbeat has gone stale.
- **Two `Flow`s on different schemas shared a leader, and one of them never drove.** The
  advisory-lock key was folded from the literal `forktex_flow` rather than the instance's schema,
  and the derived default applied only when no `RuntimeConfig` was passed — so any consumer
  supplying a config got key `0`, which nothing special-cased. Unrelated tenants competed for one
  lock: whichever won drove, the other idled with its schedules never firing.
  `RuntimeConfig.leader_lock_key` left at 0 now derives from the instance's own schema.
- **A leader that lost its advisory lock kept driving.** The lock was taken once and never
  rechecked, so a terminated backend, a Postgres restart or a pooler recycle left two workers
  both believing they led. The driver now revalidates each tick and stands down.
- **Two concurrent `start_driver()` calls leaked a driver that could not be stopped.** The
  second overwrote the first's handle while it was still holding the leader lock; the call is
  now serialised.
- **A step's completion, failure or skip could be recorded by a worker that no longer owned
  it.** Those writes now require the row to still be `running` and raise `StepClaimLost`
  if it is not; `heartbeat_step` reports the loss instead of stamping a row it does not own.
- **A cancelled run emitted `run_completed`** and fired `after_complete`.
- **`after_complete` and `after_fail` were handed the snapshot taken before execution**, whose
  status is still `running`, so an extension branching on it saw a run that never finished.
- **A cron occurrence could be consumed without its run being created.** The schedule advance
  committed in its own transaction and the insert followed in another, with its failure logged
  and swallowed — so one blip skipped the job while `last_fired_at` claimed it had run. Both
  now share a transaction.
- **A reducer was silently dropped** when the state `TypedDict` used
  `from __future__ import annotations` together with a name imported only under `TYPE_CHECKING`:
  one unresolvable annotation discarded *every* reducer on the class, and conflicting updates
  then overwrote each other instead of merging. Annotations are now resolved per field.
- **`update_run_output` could overwrite a terminal run's output.**
- **The memoisation key could depend on a heap address.** A state value JSONB could not take
  fell back to `str(value)`, which for an object without its own `__str__` renders as
  `<Class object at 0x...>` — so the same state hashed differently in the next process and a
  completed step re-executed. Such a value now raises `StateNotSerialisable`, which is the run
  failure the state write would cause anyway.
- **A step whose output was exactly `{"__skipped__": true}` lost it on replay.** The skip was
  carried in the output payload, which is a value a step body may return; it is now a column.

### Added

- `Flow.verify()` — probes the pool without migrating, for a readiness check.
- `make lock-check` — `poetry check --lock`, first in `ci`.
- The baseline is one migration per table — `v0001__workflow`, `v0002__run`, `v0003__step_run`,
  `v0004__run_event`, `v0005__scheduled_run` — each complete and final. They apply in FK
  dependency order, which the runner's numeric ordering is what guarantees.
- `RunClaim` and `StepSlot` replace the ad-hoc tuples returned by `claim_pending_runs` and
  `upsert_pending_step`; `StepSlot.status` is typed, so a status comparison is checked.
- `RuntimeConfig.max_concurrent_runs` (default 10) bounds the runs one worker executes at once.
  The claim batch bounded a single claim, not the total, so runs slower than the poll interval
  accumulated in flight at the batch size every tick.
- `FlowClosed` and `StateNotSerialisable` join the root surface, which is now 28 names. Both are
  reachable from consumer code: one from calling an entry point after `close()`, the other from
  returning a value JSONB cannot take.

### Changed

- **Docstrings carry contracts, not rationale.** `src/` went from 26% docstring lines to 8%: a
  docstring survives only where it states something the name and signature cannot — what a call
  raises, an ownership guard, that `Ctx.sleep` is not durable. Design rationale and worked examples
  were removed; `docs/flow.md`, `docs/operating.md` and this file already carried them. Gates
  forbid a docstring that restates its own name, and one on an `@overload` stub.
- **`docs/operating.md` carries the operator actions no route covers** — arming or disarming a
  schedule, pruning history through the cascade, and resetting the schema. Its SQL is executed
  against a migrated schema by `tests/test_persist/test_operator_sql.py`, so a stale column name
  is found here rather than by whoever is mid-incident.
- **`README.md` is a complete map of consumer options** — thirteen sections from declaring a
  workflow to reading the engine's own tables, with every `RuntimeConfig` field, `InstanceQuery`
  filter and terminal, `WorkflowInstance` call and `Ctx` call in a table. Gates assert that every
  public call and config default appears there, so a new option cannot ship undocumented.
- Every JSONB-carried mapping is typed `forktex.types.JsonObject` rather than `dict[str, Any]`.
  A SQLAlchemy column→value mapping stays `dict[str, Any]`: it carries `sa.func.now()`, not JSON.
- A module declares its public surface before its helpers, asserted by `test_conventions.py`.
- `make test` reports exact coverage (`--cov-precision=2`) instead of a rounded figure the floor
  did not compare against.
- Gates assert the import surface: `import forktex_flow` pulls in no web framework or S3 client,
  and `src/` reaches for no `forktex` module beyond `database`, `error`, `iso`, `log` and `types`.
- `docs/operating.md` is new: durability, leadership, what to watch and the schema move there from
  `docs/flow.md`, which is now the API reference alone.
- The schema-drift gate compares the models to a migrated database with alembic's own
  comparator, so a divergence in a type, nullability, server default, constraint or index fails
  it — not only a missing column. The models now declare every index the SQL creates, by name
  and shape; four they had declared did not exist, two of them not in any shape.
- The status vocabulary is derived from `RunStatus` rather than restated in `persist/models.py`.
- `close()` marks the handle closed, and `init`, `verify`, `start_driver`, `run` and `get` raise
  `FlowClosed` afterwards. Disposing the pool left the handle working, so a later call quietly
  built a fresh pool that nothing would dispose.
- Schedule rows are seeded once per leadership term rather than twice per schedule per tick.
- `step_run` constrains `skipped` to completed rows. The column admitted a `running` step marked
  skipped — a state the executor cannot interpret — with the invariant held by its one caller
  rather than by the table.
- The terminal extension hooks are dispatched through a `TerminalHook` literal, so a misspelled
  hook name is a type error rather than a dispatch that silently notifies nobody.

### Breaking

- **The migration baseline was rebuilt and `v0001__schema.sql` withdrawn.** 0.2.0 and 0.2.1
  shipped it, so a database created by either records version 1 and skips the new
  `v0001__workflow.sql` as already applied. `Flow.init()` then stops at `v0002__run.sql` with

  ```
  MigrationError: migration 'v0002__run.sql' failed and was rolled back
  ```

  — `CREATE TABLE IF NOT EXISTS run` is a no-op on the existing table, and
  `ix_run_status_heartbeat` then indexes a `heartbeat_at` column that database has never had. It fails
  loudly and rolls back rather than half-upgrading. There is no in-place upgrade from 0.2.x: drop
  the schema and let `Flow.init()` rebuild it.

  ```sql
  DROP SCHEMA forktex_flow CASCADE;
  ```

  This discards run history and `scheduled_run`, whose `enabled` flags the database owns — every
  schedule comes back **disarmed**. Done while 0.4.0 is unpublished and pre-1.0; the baseline is
  fixed from this release on.
- Requires `forktex>=0.14,<0.15`, whose migration runner verifies an applied migration's content
  and refuses an out-of-order version. `Database.session()` was removed in forktex 0.13.
- **`Flow.session()` and `FlowStore.session()` no longer let the caller commit.** They are
  `forktex.database.transactional_session()`: one transaction per block, committed on clean exit,
  rolled back on error, and `commit()`/`rollback()`/`begin()` raise `TransactionBoundaryError`.
  Delete the commit from any block you open through them; to write to your own tables, use your
  own session.

## [0.2.1] — 2026-10-13

### Fixed

- **A step's `backoff` is now actually waited.** `claim_pending_runs` selected on
  `Run.status == "pending"` and nothing else. A step asking to be retried sets
  `next_attempt_at` and the run goes straight back to `pending`
  (`execution.py`, on `StepRetryRequested`), so the driver re-dispatched it on
  the very next tick — a second later — and the declared delay was never
  observed.

  The consequence is not "slightly early". `@step(max_attempts=720,
  backoff=(15, 30, 60))` reads as roughly half a day of patience and delivered
  about twelve minutes of busy-looping: every wait built on retries — polling a
  DNS record a human still has to create, holding a deploy until an operator
  approves a plan — was an unadvertised twelve-minute timeout plus 720
  pointless round-trips.

  `_revive_runs_with_pending_steps` already gated on the due time, which is why
  this survived a reading: that sweep is for runs stuck at `running`, and a
  retrying run never passes through it.

  A run is now skipped only while it is genuinely blocked — it has a pending
  step scheduled in the future and no pending step that is due. A fresh run
  with no step rows, and a run resumed between steps, both still claim; turning
  a backoff into a stall would be the worse bug.

## [0.2.0] — 2026-08-31

### Added

- **`flow.scheduled(..., enabled=False)`** registers a cron without arming its
  timer. The workflow is registered and still dispatchable through `flow.run()`;
  only the schedule is off. It exists for a cron whose first tick is destructive
  — ForkTex Cloud's `reap_expired_servers` dispatches a destroy against expired
  infrastructure every five minutes — where arming on boot and disarming
  afterwards loses the race: the first tick can land before anyone runs the
  UPDATE.

  The flag is read when the row is **first written**. From then on the database
  owns it: the upsert refreshes `cron` and deliberately leaves `enabled` alone,
  so `UPDATE scheduled_run SET enabled = true` is how a schedule is armed and no
  later deploy re-arms or disarms one behind an operator. Both halves are
  tested, and each was verified to fail with its own half reverted.

  No new public name; `__all__` is unchanged at 26.

### Fixed

- **`step` was typed as a union its callers could not use.** A bare `@step`
  returns the function unchanged, but the single `Callable | StepSpec` return
  meant every later `step(decorated, when=…)` and `scheduled(...)(decorated)`
  failed type checking — 13 errors in ForkTex Cloud, against code written
  exactly as this README shows it. Replaced with three `@overload` signatures,
  one per call shape, so a caller gets the type it actually receives. No
  runtime change.

## [0.1.0] — 2026-08-31

First release under this distribution name. The engine previously shipped as
`forktex-core[flow]` through 2.x and then sat in `forktex-py/backlog/core/flow/`
while the `forktex` distribution was repurposed as the primitives library. This
is not a port of that code: the surface was cut to what its consumers actually
use, the layering was inverted, and four latent bugs were fixed.

### Added

- **The advertised PostgreSQL floor is exercised, not asserted**
  (`tests/test_persist/test_postgres_floor.py`). Every other test runs the
  version we develop against, so "PostgreSQL 14+" in the README was the one
  compatibility claim with nothing behind it, and it fails silently — a
  consumer on the floor version gets an error inside `Flow.init()`, not a
  resolver message. The test reads the floor out of the README and boots that
  server, so raising the claim without checking it fails.

- **`make release-check`** — the version, its dated `CHANGELOG.md` section, an
  empty `[Unreleased]`, the git tag (with `TAG=`) and the contents of `dist/`
  all have to agree. Runs last in `make ci` and first in both publish targets.

- **Concurrency guarantees asserted against two independent connection pools**
  (`tests/test_runtime/test_concurrency.py`). One leader at a time; leadership
  transfers when the holder lets go; two workers executing one run run the step
  body **once**; a concurrent claim raises for the loser; concurrent upserts
  converge on a single row; two drivers partition the pending queue rather than
  duplicating it; and two schemas in one process stay isolated. A single shared
  pool would let SQLAlchemy serialise these races and the tests would pass while
  the guarantee did not hold — hence the second `Flow`.

- **End-to-end failover** (`test_driver.py`): two live drivers, the leader stops
  with work outstanding, and the standby takes over and drains it.

- **The README's headline claim is executable** (`tests/test_read/test_readme_examples.py`).
  Documentation that has never run is a hypothesis; each test there mirrors a
  promise the README makes to a prospective consumer.

- **`tests/test_architecture/test_docs_accuracy.py`** — every public name appears
  in the reference, the surface counts the pages state match `__all__`, no page
  demonstrates a removed API, every `from forktex_flow import …` in the README
  resolves, and the stability contract has exactly one author.

- **A stated stability contract** (`README.md`, *Stability*). What 0.x promises, how a
  public name is removed (one deprecating release first), that migrations are forward-only
  and never edited once applied, and that the `forktex_flow` schema name plus
  `run_event(id, ts, event_type, payload, run_id)` are contracts with consumers that never
  import this package.

- **`tests/test_persist/test_migration_immutability.py`** — a content hash per shipped
  migration. Editing one fails CI instead of quietly leaving already-migrated databases
  describing a schema this code no longer expects; the runner records v0001 as applied and
  never revisits it, so the divergence would surface much later as a missing column.

- **`scripts/wheel_smoke.py`, wired into `make smoke`** — installs the built wheel into a
  clean venv and runs a workflow end to end from site-packages, refusing to run against a
  source checkout. The source-tree suite cannot see a packaging fault; this is what proves
  the `.sql` migrations ship and that `Flow.init()` can find them through importlib's view
  of the installed package.

- **`forktex_flow`, a distribution of its own,** built on `forktex>=0.11`. Not a
  module inside `forktex`: the substrate is four primitives, eight infrastructure
  facades and two bootstraps, and a workflow engine with its own domain models,
  Postgres schema and migrations is none of those. Its consumer set is also
  nearly disjoint from `grid`'s, so folding it in would make every consumer of
  `forktex.error` carry a workflow engine it never imports.

- **A layering guard that actually runs.** `tests/test_architecture/test_layering.py`
  asserts the `domain → persist → read → runtime → facade` direction, that
  `domain/` performs no I/O, that only `__init__` imports the facade, and a
  cycle-debt ratchet at zero. `concept.forktex-substrate` records the missing
  level-DAG guard as "the highest-value gap in the corpus"; this is it.

- **`forktex_flow.testing`** — `make_ctx`, `isolated_flow`, `run_to_completion`.
  A consumer could not test a workflow without hand-rolling the schema-per-test
  fixture this repo already had internally; the docs offered the reducer
  machinery and nothing else. `run_to_completion` executes inline rather than
  starting a driver, so a test asserts on a result instead of polling a
  background loop. `testcontainers` is an optional `[testing]` extra.

- **`tests/test_persist/test_schema_drift.py`** — migrates a real database,
  reflects every table, and diffs the columns against the ORM in both directions,
  plus the three indexes the query and reclaim paths depend on. The existing test
  checked table *names*; columns are where drift hides, and a squashed migration
  leaving a database a column short is what broke a sibling project.

- **A status-vocabulary agreement test.** `RunStatus`, `persist/models._STATUSES`
  and every `CHECK (status IN (…))` in the migration must name the same set. The
  three cannot be reduced to one — the SQL is frozen byte-for-byte by the
  immutability hash, correctly — but they can be stopped from drifting.

### Changed

- **The facade is no longer passed down the stack.** Thirty-five functions across
  `persist`, `read` and `runtime` took `flow: Flow` in order to reach
  `flow.session` and `flow.schema`, which made nine lower-layer modules import
  the facade for its type — the one direction a layered package forbids. They
  now take a `FlowStore` (a pool and a schema) or a `FlowRuntime` (that plus the
  registry, config and extensions).

- **A workflow is an ordered list of steps.** The graph track — edges, routers,
  `START`/`END`, reachability analysis — is gone, along with `step_template`, the
  namespace/config track (`flow.define`, `compile_config`, the
  `workflow_definition` table), signals (`persist/signals.py`, the `signal` table,
  `SignalTimeout`, `GraphStuckError`), `parallel`/`ParallelGroup`, `Ctx.spawn`/
  `wait`/`map`, and `audit.py`. Measured across all three consumers, none of it
  had a single call site outside a docstring. 6,279 LOC to 3,090.

- **`replay.py` is `execution.py`.** The name read as an optional feature; it is
  the executor.

- **`read/query.py` is three functions, not one with a `mode` string.** The single
  entry point returned `Page | int | dict` and forced callers to `cast()` and
  re-check the type at runtime.

- **The schema is re-authored as one `v0001`.** Five tables. `run.status` and
  `step_run.status` gained CHECK constraints, `step_run` gained bounds on
  `attempts` and `max_attempts`, and the keyset index is now `(started_at DESC,
  id DESC)` — both columns, in paging order, so the row comparison lands as an
  Index Cond rather than a Filter.

- **Migration bookkeeping is `forktex.database.SchemaMigrationRunner`'s.** What
  stayed is the part it cannot own: applying a `FlowExtension`'s declared
  columns, now in `persist/extension_columns.py`.

- **The leader lock key is `advisory_key("forktex_flow", "driver")`,** not a local
  `zlib.crc32`. `advisory_key` is the one place that folds a digest into
  Postgres's signed bigint range.

- **Run ids come from the column's `default=uuid.uuid7`,** read back after flush,
  instead of being minted at two call sites.

- **`InstanceQuery`'s mutation semantics are stated.** Filters mutate the builder
  and return it, so a query cannot be branched — `base.status(...)` narrows
  `base` itself. True before; now documented and pinned by a test, because a
  consumer expecting a copy gets one result set where they wanted two.

- **`StepStatus` is `RunStatus`.** Two identical `Literal`s that had to stay
  identical are one, aliased so a signature can still say which it means.

- **`upsert_pending_step` is one statement.** It was a SELECT, an INSERT, and —
  when two workers raced — an `IntegrityError`, a rollback and a second SELECT:
  three round trips and the read-then-write window `database:single-write-statement`
  exists to close. Now `ON CONFLICT DO UPDATE … RETURNING` against
  `uq_step_run_identity`, so the losing writer is handed the winner's row instead
  of an exception to recover from. Verified by removing the conflict clause: the
  loser gets a `UniqueViolationError`; with it, both converge on one row.

- **`election_interval` is configurable.** It was a hardcoded 2.0s module
  constant while every other timing lived on `RuntimeConfig`, so a consumer could
  not tune how fast a takeover happens after the leader dies. Validated as
  positive, documented, and the failover test sets it low and is deterministic.

- **`ReducerFn` had two definitions** — one in `domain/definition.py`, one in
  `domain/state.py`, with the compiler importing the wrong one. `state.py` owns
  reducers and is now the only home (`knowledge:one-source-per-concern`).

### Fixed

- **The licence-header gate passed files with the wrong product name.**
  `has_current_header` tested for two substrings — `SPDX-License-Identifier` and
  `along with this program` — and the product line sits between them, so any
  header with correct ends and a stale middle counted as current. 18 of 29
  source files declared themselves part of `forktex-core`, a distribution that
  no longer exists, and the gate reported "all files have current license
  headers" while they went into the wheel. The check now compares every
  non-blank line of the canonical header; 20 files were corrected.

- **`make publish` could upload a stale wheel.** It sent `dist/*` as it stood;
  `make build` clears the directory, but a version bump published without
  rebuilding would ship the previous wheel under the new release — and a PyPI
  version cannot be replaced, only superseded. `release-check` now refuses when
  `dist/` holds anything that is not the declared version.

- **The release workflow published without running the gate**, on a
  `python-version: "3.x"` interpreter. A release can be cut from any ref, so
  "CI passed on the merge" was a different claim from "this artefact passes";
  and an interpreter below the `>=3.14` floor does not fail loudly, because pip
  hides releases it cannot use and the substrate silently resolves to an
  unrelated 0.2.x lineage. Now pinned to 3.14 and runs `make ci` plus
  `release-check TAG=…` before uploading, with no build logic inlined
  (`gate:ci-is-one-command`).

- **A due schedule fired twice at leader handover.** `_fire_due_scheduled_runs`
  SELECTed due rows, mutated the ORM objects and committed, while its docstring
  claimed `UPDATE … RETURNING` atomicity it did not have. Two drivers overlapping
  for one tick — which is what handover is — both read the same row and both
  fired it. The claim is now a compare-and-swap on `next_fire_at`.

- **`.metadata(k=v).summary()` ignored its filter.** The summary branch built its
  own `where` list and had drifted from the page and count branches, so it
  aggregated over every run in the table. `_filter_clauses` is now the only place
  a filter becomes SQL.

- **Extension hooks could be lost.** `after_complete`/`after_fail` were scheduled
  on the event loop without a reference held, so a hook could be garbage-collected
  before running and was guaranteed lost if the process exited right after a run
  finished — exactly when `after_complete` fires. They are awaited now, with
  failures logged and swallowed.

- **`avg_duration_seconds` averaged per-status means unweighted,** so one failed
  run counted as much as a thousand completed ones.

- **`Ctx.spawn` wrote `triggered_by="spawn"`,** which `ck_run_triggered_by` did
  not allow — every spawned run failed its INSERT. Removed with the rest of the
  child-workflow surface rather than fixed; `flow.run()` from inside a step
  starts another run.

- **`compile_config` hardcoded `reducers={}`,** so config-defined workflows
  silently lost every accumulator. Gone with the namespace track;
  `extract_reducers` is now called unconditionally by both compilers.

- **`ParallelGroup` merged state with `dict.update`,** bypassing the reducers.
  Gone with `parallel`; `apply_state_update` is the only merge point left.

- **`WorkflowInstance.cancel()` did not cancel a running run.** `update_run_status`
  had no status guard and the executor never re-read status between steps, so an
  operator cancelling a run mid-flight had it silently overwritten by the
  executor's own success write — leaving `status='completed'` beside
  `cancel_reason='cancelled by operator'`, a self-contradictory row with nothing
  raised anywhere. Reproduced against a container before and after.

  Two halves: `update_run_status` now carries `WHERE status NOT IN (terminal)`
  and returns whether it matched, so a terminal status is final; and the executor
  re-reads status before each step, raising `WorkflowCancelled` through the
  handler that already existed but was reachable only from user code.
  Cancellation is cooperative and between steps — the step already running
  completes — and `cancel()` now returns `False` when the run had already
  finished, instead of rewriting its outcome.

- **`current_node()` spliced the caller's string into a `LIKE` pattern.**
  `step_qualname LIKE '%:' || name`, unescaped, so `current_node("%")` matched
  every running step and a name that was the tail of another workflow's step
  matched across workflows. Now an exact match on `step_name`, a column that was
  already there.


- **`RuntimeConfig(max_attempts=…)` and `.backoff` configured nothing.** Both are
  documented as the default for steps that do not override them; both were read
  nowhere. The retry defaults were written in *three* places — `RuntimeConfig`,
  `domain/node.py` and `runtime/compiler.py` — and the two module-constant copies
  were the ones the compiler actually used, so a consumer setting 7 got steps
  with 3 and no error anywhere. Verified against a container before and after.
  `RuntimeConfig` is now the single home, threaded into both compilers, with a
  test asserting the copies stay gone rather than merely currently agreeing.

- **`reclaim_stale_steps` appended its note unboundedly.** Every other write of
  `error` truncates at 5,000 characters; this one concatenated
  `"[reclaimed: heartbeat stale]"` onto whatever was there. A step reclaimed in a
  loop grew the column forever, and `TEXT` meant nothing ever complained. Now
  left-truncated to the same cap, which is named once.

- **An `__all__` block was buried inside a docstring.** A scripted edit landed it
  in the body of `update_run_output`'s docstring, where ruff, pyright and the
  tests all see a harmless string. The same docstring still described
  `execute_graph_run`, deleted in the rewrite. Both fixed, and
  `test_conventions.py` now fails on a declaration inside any docstring.

### Removed

- Sorting by any field but `started_at`. `finished_at` is nullable, and a keyset
  row comparison against NULL evaluates to NULL and drops the row from the page
  without failing anything.
- `Flow.list` and `persist.runs.list_runs` — an unpaginated second implementation
  of the filters `flow.query()` already compiles.
- `namespace` throughout. It only ever came from the config track's
  `metadata["__namespace__"]`, and no run row carried one.

- **`triggered_by` left `Flow.run()`'s signature.** It was a free `str` against a
  CHECK constraint permitting exactly `manual` and `schedule`, so the only thing
  a caller could do with it was mislabel a run or take an `IntegrityError` four
  frames down. The driver still writes `"schedule"`; a caller's run is `"manual"`.

- **`WorkflowDefinition.node()` and `has_step_meta()`** — no production caller;
  the only tests were testing them for their own sake.

- **Extension-declared columns** — `ColumnDef`, `extra_run_columns`,
  `extra_step_run_columns`, `persist/extension_columns.py` (143 lines) and the
  `flow_schema_extension` tracker. The machinery ran `ALTER TABLE` at `init()` to
  add columns that **nothing ever wrote to and nothing ever read**: `before_start`
  returns a dict merged into `metadata` (JSONB), never into the columns, so a
  declared `org_id` column stayed NULL for the life of the database while the
  actual `org_id` lived in `metadata` and was queried from there. Documentation
  showed both and implied they connected.

  Per-run scoping goes through `before_start` → `metadata` → `.metadata(**kv)`,
  which is GIN-indexed and needs no schema change. `agent:smallest-thing-that-works`:
  working code that nobody asked for is still a cost, because somebody has to read it.

- **Five names left the root surface** — `WorkflowDefinition`, `NodeDef`,
  `StepMeta`, `RunFilter` (what the decorators and query builder compile *to*; no
  consumer constructs one) and `RunUpdate` (a shape nothing produced or accepted).
  Still importable from their modules; freezing them made an internal
  representation an API. **29 → 23 names.**
