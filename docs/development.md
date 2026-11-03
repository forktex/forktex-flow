# Developing `forktex-flow`

For working on the library itself. Everything a consumer needs is in the
[README](../README.md), the [guide](guide.md), the [reference](reference.md)
and [operating](operating.md); nothing here is part of the public contract.

## Architecture

**Definitions are data.** Every workflow is a `ProcessModel`: BPMN elements
joined by sequence flows. `flow.pipeline(...)` compiles code to the same model
(`runtime/compiler.py`): start → one service task per step → end, with an
exclusive gateway in front of each step that has a `when` condition. Everything
that changes behaviour (steps, retry policy, conditions) is in the model, so it
is versioned. A definition row is immutable per `(namespace, key, version)`, and
its checksum over the model decides whether a deploy is new.

**Task types are the contract between definitions and code**, as in BPMN.
A handler's task type is public: any definition in any namespace may use it.
A pipeline step's is `<key>:<name>`, and the model refuses that form under any
other key (`domain/process.py`), so code-private steps stay private however
definitions arrive. Every step and handler has one shape,
`async def name(ctx, state)`, checked when it is declared (`domain/node.py`).

**What code supplies is held per `Flow`.** A `Flow` owns a `FlowRegistry` with
its handlers (by task type, each with the pydantic state it receives, if any),
its declared definitions, and its pipelines' state models and reducers (by
key). There is no module-level state; two `Flow` objects in
one process share nothing but the database.

**An instance is a token.** `runtime/engine.py` moves it inside the caller's
transaction, under the instance's row lock, through events and gateways until
it reaches a wait: a job, a user task or a child instance. Every element visited
is one `element_run` row, unique on `(instance_id, element_id)`. That uniqueness
is why definitions are acyclic and why a completed element never repeats.

**A job is an `element_run` with a `task_type`.** `runtime/driver.py` claims
pending jobs for the task types its registry handles (`FOR UPDATE SKIP LOCKED`,
counting the attempt at claim), and `runtime/worker.py` runs one: the handler,
its heartbeat, and the outcome, which is the engine moving the token on. Backoff
is `next_attempt_at`. When an instance advances, the process wakes its driver at
once instead of waiting for the next poll.

**One process leads.** The holder of a session advisory lock, keyed on the
schema, fires due schedules, reclaims jobs whose heartbeat went silent, and
warns about task types nothing is claiming.

**Namespaces isolate.** A start or a call activity resolves its definition in
one namespace and never falls back to another. Queries span every namespace
unless filtered.

**Ad-hoc subprocesses run in rounds.** The decider is a job; each activation is
a job with element id `<subprocess>/<round>.<position>/<activity>`, so the
per-instance uniqueness holds across rounds.

## Layout

```
src/forktex_flow/
  __init__.py   the public surface (__all__)
  flow.py       the facade
  errors.py     the library's errors
  extension.py  the hook protocol
  testing.py    helpers for consumers' tests
  domain/       no I/O: process model, bpmn, jsonlogic, step, state, types, config
  persist/      ORM models, SQL migrations, definitions / instances / jobs
  read/         WorkflowInstance and InstanceQuery
  runtime/      compiler, registry, engine, worker, driver, ctx
tests/          domain (no database), persist and engine (real Postgres)
```

Imports run one way: `domain → persist → read → runtime → flow`.

## Commands

```bash
make install   # poetry install --with dev
make test      # pytest with the coverage floor; needs Docker
make ci        # the full gate: lock, format, lint, licence headers, types, audit, tests, build, wheel smoke
```

Run `make ci` on its own, since a concurrent run races over `dist/`.

## Conventions

- Python 3.14 only. `except A, B:` (PEP 758) is valid there.
- Errors come from `errors.py` or `forktex.error`, never a bare builtin.
- Ids are UUIDv7 and never sliced.
- SQL is written as SQLAlchemy constructs.
- Docstrings state contracts.
- Tests go through the public API against a real Postgres, and would pass
  against the installed wheel.
- The consumer docs describe the library as it is, for someone meeting it for
  the first time. Every snippet in them runs as written. How the library came to
  be lives in git history, not in the docs.

## Adding a migration

Add `src/forktex_flow/persist/migrations/v{NNNN}__{name}.sql`, using `{schema}`
for the schema name. Change `persist/models.py` in the same commit;
`tests/test_persist/test_schema_drift.py` compares the two. Never edit a
migration that has shipped: the runner refuses a changed file, so fix one with
the next.

## Release

Publishing is manual.

1. Bump `version` in `pyproject.toml` and add a dated section to
   `CHANGELOG.md`, one line per change.
2. Run `make ci`. It builds `dist/`, installs the wheel into a clean venv, runs a
   workflow from it, and checks that the version, CHANGELOG and `dist/` agree.
3. Run `make publish-test` and try the TestPyPI install, then `make publish`. A
   version can never be uploaded twice.
4. Tag it with `git tag vX.Y.Z`.
