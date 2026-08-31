# AGENTS.md — working in this repo

`forktex-flow` is a **library**: Postgres-native durable workflow execution,
built on the [`forktex`](https://github.com/forktex/forktex-py) substrate. No
CLI, no console script, no filesystem authority.

## Layout

```
src/forktex_flow/
  __init__.py       public surface; __all__ IS the contract
  errors.py         every error this package raises
  extension.py      the consumer hook protocol
  flow.py           the facade
  domain/           vocabulary — zero I/O
  persist/          models, migrations, aggregates, store
  read/             instance (user-facing), query (SQL)
  runtime/          compiler, registry, execution, driver, ctx, context
tests/              mirrors src/, plus test_architecture/
docs/               flow.md (consumer reference), development.md
```

One rule holds the shape: **imports run `domain → persist → read → runtime →
facade`, never back**. `domain/` imports nothing from its own package and
nothing that performs I/O. The facade imports everything and is imported only by
`__init__.py`.

Unlike most repos, that rule is **enforced by a test** —
`tests/test_architecture/test_layering.py`, plus a cycle-debt ratchet at zero.
`test_public_surface.py` freezes the exact root `__all__`;
`test_conventions.py` checks errors, uuid7, raw SQL and lint exemptions. If you
are changing structure, run `pytest tests/test_architecture` first: it is fast
and it will tell you what you broke.

## Commands

```bash
make install     # poetry install --with dev
make test        # pytest + coverage floor
make lint        # ruff check
make typecheck   # pyright, src/ only
make ci          # the full gate; run this before proposing a change is done
```

Integration tests use a real Postgres via testcontainers — **Docker must be
running**. There are no mocks for it.

## Conventions

- **Python 3.14.** PEP 695 generics and PEP 758 (`except A, B:`) are in use.
  ruff targets `py314`; do not lower it.
- **`src/` carries no lint exemptions** beyond the single documented
  `type: ignore`; `test_conventions.py` asserts that exact list.
- **Errors live in `errors.py`** and derive from `forktex.error.AppError`.
- **Identifiers are UUIDv7, never sliced,** declared as a column `default=`.
- **Statements are SQLAlchemy constructs.** `sa.text` only for `server_default`.
- **Check the substrate before writing a mechanic.** `forktex.database` already
  owns sessions, pagination, cursors, DDL, reflection, advisory locks and the
  migration runner. A second copy is the bug, not a style choice.
- **Comments explain *why*.**

## Things that look wrong but are not

- **`forktex_flow` as a string literal** in `persist/models.py`, the SQL migration
  and `flow.py`'s `schema_translate_map` is a physical Postgres schema name in
  deployed consumer databases. Renaming it orphans data.
- **The version table is `flow_schema_version`,** not `SchemaMigrationRunner`'s
  default of `schema_version`. Deployed databases carry the former.
- **`forktex` looks stuck at 0.2.4 on PyPI.** It is not — 0.11.0 is published.
  pip silently hides a release whose `requires-python` floor your interpreter
  does not meet, so any check run under 3.12 sees an older lineage and concludes
  the dependency is unsatisfiable. Check with a 3.14 interpreter. This reached
  the README once as a confident, false claim; `test_docs_accuracy.py` now
  refuses it.
- **`except A, B:` without parentheses** is PEP 758, valid on 3.14. Checking
  syntax with a system `python3` (3.12) reports every one as an error — a false
  positive that has cost this ecosystem three separate debugging sessions.
  `make parse-check` runs first in `make ci` and refuses to run below 3.14.
- **`read/instance.py` holds behaviour, not just shapes.** `wait`/`stream`/`cancel`
  live on `WorkflowInstance` rather than on `Flow` precisely so the read layer
  does not have to import the facade.

## Documentation

[`docs/flow.md`](docs/flow.md) is the consumer reference — install, wiring,
public surface, durability, errors, gotchas.
[`docs/development.md`](docs/development.md) is the contributor guide. Keep both
in step with the code.
