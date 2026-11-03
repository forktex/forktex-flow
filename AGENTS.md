# AGENTS.md

`forktex-flow` is a library: durable workflows on Postgres, built on
[`forktex`](https://github.com/forktex/forktex-py). It has no CLI and no
service of its own.

For consumers: [`README.md`](README.md), [`docs/guide.md`](docs/guide.md),
[`docs/reference.md`](docs/reference.md), [`docs/operating.md`](docs/operating.md).

For working on the library: [`docs/development.md`](docs/development.md), the
architecture, layout, commands and conventions. Read it before changing
anything.

`make ci` is the gate. It needs Docker, and should run alone. Publishing is
manual: a person runs `make publish`.
