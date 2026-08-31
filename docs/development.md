# Developing `forktex-flow`

## Layout

```
src/forktex_flow/
  __init__.py       the public surface; __all__ IS the contract
  errors.py         every error this package raises
  extension.py      the consumer hook protocol
  flow.py           the facade — imports everything, imported only by __init__
  domain/           vocabulary: definition, node, state, filter, types, config
  persist/          models, migrations, one module per aggregate, store
  read/             instance (user-facing), query (SQL)
  runtime/          compiler, registry, execution, driver, ctx, context
tests/              mirrors src/, plus test_architecture/
docs/               flow.md (consumer reference) + this page
```

Imports run **one way**: `domain → persist → read → runtime → facade`. `domain/`
imports nothing from its own package and nothing that performs I/O. The facade
imports everything and is imported only by `__init__.py`.

**Why layered at 3,350 LOC**, when `package-layout:flat-until-cycles` says flat
until cycle debt appears and this package's debt is zero. The debt is zero
*because* of the layers. This is the package the rule's own evidence is drawn
from: flat at ~6,000 LOC it carried 59 deferred imports, and adopting the layered
DAG is what took it to zero. Flattening 28 modules back into one directory would
re-run that experiment, and it would delete `test_layering.py` — the level-DAG
guard `concept.forktex-substrate` calls the highest-value gap in the corpus, and
the only place it exists. Recorded as a deliberate deviation rather than left to
look like an oversight.

`tests/test_architecture/test_layering.py` enforces all of that, plus a
cycle-debt ratchet at zero. It is the level-DAG guard `concept.package-layout`
records as missing everywhere — this package is where it exists.

## Commands

```bash
make install     # poetry install --with dev
make test        # pytest + coverage floor
make lint        # ruff check
make typecheck   # pyright, src/ only
make release-check  # version, CHANGELOG and (with TAG=) the git tag agree
make ci          # the full gate; run this before proposing a change is done
```

Integration tests use a real Postgres via testcontainers — **Docker must be
running**. There are no mocks for it.

## Conventions

- **Python 3.14.** PEP 695 generics and PEP 758 (`except A, B:`) are in use.
  ruff targets `py314`; do not lower it, and do not check syntax with a system
  `python3` — a 3.12 interpreter reports every PEP 758 line as a syntax error.
  `make parse-check` runs first in `make ci` and refuses to run below 3.14.
- **`src/` carries no lint exemptions.** One `type: ignore` survives, on the
  `database_url`/`database` either-or that the type system cannot express;
  `test_conventions.py` asserts that exact list.
- **Errors live in `errors.py`** and derive from `forktex.error.AppError`. No
  public function raises a bare `RuntimeError`.
- **Identifiers are UUIDv7, never sliced,** and declared as a column `default=`
  rather than minted at the call site.
- **Statements are SQLAlchemy constructs.** `sa.text` appears only for
  `server_default` literals.
- **Comments explain *why*.** A comment restating *what* a line does is a missing
  name; extract or rename instead.

## Check the substrate before writing a mechanic

`forktex.database` already provides engine/session lifecycle, transaction
boundaries, page and cursor shapes, keyset predicates, generic CRUD, DDL
constructs, reflection, identifier validation, integrity translation, advisory
locks, model mixins and the SQL migration runner. Writing any of them again is
the bug, not a style choice. This package consumes `Database`, `substrate_base`,
`SchemaMigrationRunner`, `advisory_key`, `try_advisory_lock`, `Page`,
`encode_cursor`/`decode_cursor`, `ddl`, `reflect` and `validate_identifier`.

## Adding a migration

1. Write `src/forktex_flow/persist/migrations/v000N__description.sql`. The runner
   globs `v*.sql` and applies them in numeric order, so nothing registers it.
2. Use `{schema}` where the schema name goes — a plain string replace, not
   `str.format`, so literal `{`/`}` in JSONB defaults are safe.
3. Change `persist/models.py` in the same commit. `test_schema_drift.py` migrates
   a real database and diffs its columns against the ORM, so the two cannot part.
4. Add the new file's sha256 to `SHIPPED` in
   `tests/test_persist/test_migration_immutability.py`. That test tells you the
   hash when it fails.

**Never edit a migration that has shipped.** Databases that applied it will not
re-run it, so they would describe a schema this code no longer expects. Fix a bad
migration by writing the next one.

## Running a subset of tests

The full suite boots a Postgres container and takes about a minute. While
iterating:

```bash
poetry run pytest tests/test_architecture -q         # layering, surface, conventions — no database, instant
poetry run pytest tests/test_domain -q               # pure logic — no database
poetry run pytest tests/test_runtime/test_driver.py -q
poetry run pytest tests/ -q -k reducer               # by name
```

`make test` adds the coverage floor, and `make ci` is what has to pass before a
change is done.

## Adding a module

1. Put it in the layer it belongs to; do not create a new top-level directory.
2. Declare `__all__` — `test_public_surface.py` asserts every module has one.
3. If it belongs on the public surface, add it to `src/forktex_flow/__init__.py`
   **and** to `ROOT_SURFACE` in `test_public_surface.py`. That list is the frozen
   contract: a deliberate addition edits the test, an accidental one fails it.
4. Document it in `docs/flow.md`.

## Known constraints

- **`Flow.__init__` takes `**engine_kwargs`.** Anything unrecognised is forwarded
  to `create_async_engine`, which means a future first-class constructor option
  could collide with a keyword a consumer is already passing through. Narrowing
  it to an `engine_kwargs: dict` parameter would be a breaking change to a
  constructor consumers already call, so it stays — but add new options with the
  collision in mind.
- **`RunStatus` is a bare `Literal`.** A sixth status breaks every consumer who
  wrote an exhaustive `match` over the current five. That is a major-version
  event, not a minor one.
- **`InstanceQuery` mutates and returns `self`.** A builder cannot be branched.
  Documented, tested, and now frozen by that fact — making it copy-on-write later
  would silently change behaviour for anyone relying on the current semantics.

## Things that look wrong but are not

- **`forktex_flow` as a string literal** in `persist/models.py`, the SQL
  migration and `flow.py`'s `schema_translate_map` is a physical Postgres schema
  name in deployed consumer databases. Renaming it orphans data.
- **The version table is `flow_schema_version`,** not `SchemaMigrationRunner`'s
  default of `schema_version`. Deployed databases carry the former; a runner
  looking for the latter finds no applied versions and re-runs v0001.
- **`forktex` looks stuck at 0.2.4 on PyPI.** It is not — 0.11.0 is published.
  pip silently hides a release whose `requires-python` floor your interpreter
  does not meet, so any check run under 3.12 sees an older lineage and concludes
  the dependency is unsatisfiable. Check with a 3.14 interpreter. This reached
  the README once as a confident, false claim; `test_docs_accuracy.py` now
  refuses it.
- **`except A, B:` without parentheses** is PEP 758 and valid on 3.14. ruff's
  formatter emits it under `target-version = "py314"`.

## Consumers

### ForkTex Cloud — the swap, when it happens

Cloud runs against a stand-in (`api/src/shared/flow.py`, ~260 lines) that keeps this
package's shape while `Flow.run()` raises and the HTTP layer renders 503. Replacing it
is four edits, and they are written down here so the next cycle does not have to
rediscover them:

1. **`api/src/shared/flow.py` collapses to a re-export** —
   `from forktex_flow import Ctx, Flow, FlowExtension, StepSpec, WorkflowInstance, step`.
   `FlowUnavailableError` disappears with it: dispatch works, so nothing renders 503.
2. **`api/src/deploy/flow_extension.py` — delete the `before_dispatch` override.** It is
   dead code. The stand-in invented that hook; the real `FlowExtension` protocol has
   `before_start`, `after_complete` and `after_fail`, and nothing else. Cloud's override
   is a no-op that nothing would call.
3. **`namespace` is gone from the surface.** `api/src/flow_api/schemas.py`'s
   `FlowInstanceRead.namespace` and the `namespace=` line in `routes.py`'s
   `_instance_model()` come out, then `make codegen` regenerates the SDK and client.
   It only ever carried `metadata["__namespace__"]` from the config track, which this
   package does not have.
4. **`api/pyproject.toml` gains `forktex-flow`** — a poetry path dependency during
   development, a version pin before publish.

Unaffected, and worth stating because they look fragile: cloud's `Deployment.flow_run_id`
(a plain UUID column, deliberately no FK across the schema boundary) and the raw-SQL
`run_event` tail behind its SSE endpoint. Both are covered by this package's own tests.

### Anything else

`intelligence` and `network` both pin `forktex-core` 2.x, which no longer exists on PyPI.
Neither can consume this package without first migrating off that lineage; treat them as
migration projects, not as constraints on this one.

## Release

`make ci` ends in `release-check`, which fails if `pyproject.toml`'s version has no
dated `CHANGELOG.md` section, or if entries are still filed under `[Unreleased]`. A
PyPI version is immutable, so a release whose own changes are documented as unreleased
cannot be corrected afterwards — only superseded.

**Pick one path.** Publishing twice is not an error you recover from; the second upload
is rejected because the version already exists, and by then the first is public.

**Credentials.** Nothing is configured in this checkout — no `~/.pypirc`, no `TWINE_*`
in the environment — so the first upload will otherwise stop at an interactive prompt.
twine knows the `pypi` and `testpypi` repositories by name already; only the token is
missing. Both indexes are separate accounts with separate tokens:

```bash
export TWINE_USERNAME=__token__
export TWINE_PASSWORD=pypi-…      # the token for the index you are targeting
```

Use a project-scoped token where possible. A token is never needed in the repo, and a
`.pypirc` written into this directory would be ignored by `.gitignore` only if you name
it as such — prefer the environment.

*Manual, from a laptop:*

1. Bump `version` in `pyproject.toml`; move `[Unreleased]` into a dated
   `## [x.y.z]` section in `CHANGELOG.md`.
2. `make ci` — the whole gate. `smoke` installs the built wheel into a throwaway
   venv and checks the `.sql` migrations shipped inside it, which is a packaging
   fault no source-tree test can see.
3. `make publish-test`, then install from TestPyPI into a clean venv and run
   `scripts/wheel_smoke.py` against *that* install — the wheel your machine built
   and the wheel an index serves are not the same artefact until you have checked.
4. `make publish` — irreversible per version.
5. `git tag vX.Y.Z && git push --tags`. Create the GitHub release **from the existing
   tag without publishing assets**, or skip the release entirely: creating a release
   fires `publish.yml`, which uploads again and fails.

*Automated, from a tag:* do steps 1–3, push the tag, then create a GitHub release.
`publish.yml` runs `make ci` and `make release-check TAG=…` before uploading, so a tag
that disagrees with `pyproject.toml` stops the publish rather than shipping a
mislabelled artefact. Do not also run `make publish`.

`publish.yml` authenticates with a long-lived `PYPI_API_TOKEN` secret. PyPI trusted
publishing (OIDC, no stored token) is the better shape and is a one-time setup on the
project's PyPI page; it is not done, and switching before that setup exists would break
the only working path.
