# AGENTS.md — working in this repo

`forktex-flow` is a **library**: Postgres-native durable workflow execution on
the [`forktex`](https://github.com/forktex/forktex-py) substrate. No CLI, no
console script, no filesystem authority.

Two documents carry the detail, and this file does not restate them:

- [`docs/flow.md`](docs/flow.md) — the consumer reference: wiring, public
  surface, durability, errors, gotchas.
- [`docs/operating.md`](docs/operating.md) — durability, leadership, what to
  watch, and the tables the engine owns.
- [`docs/development.md`](docs/development.md) — layout, commands, conventions,
  adding a migration or a module, and the things that look wrong but are not.
  **Read it before changing anything.**

Three facts worth having before you start:

- **Imports run `domain → persist → read → runtime → facade`, never back**, and
  a test enforces it. If you are changing structure, run
  `pytest tests/test_architecture` first — it is fast, needs no database, and
  names what you broke.
- **Integration tests need Docker.** They use a real Postgres through
  testcontainers; there are no mocks for it.
- **`make ci` is the gate.** Run it before saying a change is done, and alone —
  a concurrent run races `dist/`.
