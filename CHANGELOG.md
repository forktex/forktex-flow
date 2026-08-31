# Changelog

All notable changes to `forktex-flow` are documented here. This project follows
[Semantic Versioning](https://semver.org/).

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
