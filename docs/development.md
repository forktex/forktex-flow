# Developing `forktex-flow`

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

- `runtime/engine.py` moves an instance's token, inside the caller's
  transaction and under the instance's row lock.
- `runtime/worker.py` runs one job: handler, heartbeat, outcome.
- `runtime/driver.py` claims jobs and, when elected, fires schedules, reclaims
  stalled jobs and reports unclaimed ones.
- `runtime/compiler.py` turns the decorators into definitions.

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
- Docstrings state contracts. Behaviour is documented in `docs/`.
- Tests go through the public API against a real Postgres, and would pass
  against the installed wheel.

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
