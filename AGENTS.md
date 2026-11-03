# AGENTS.md

`forktex-flow` is a library: durable workflows on Postgres, built on
[`forktex`](https://github.com/forktex/forktex-py). It has no CLI and no
service of its own.

- [`README.md`](README.md): how a consumer wires it in.
- [`docs/flow.md`](docs/flow.md): the reference.
- [`docs/operating.md`](docs/operating.md): failure behaviour and operations.
- [`docs/development.md`](docs/development.md): layout, commands, conventions.
  Read it before changing anything.

`make ci` is the gate. It needs Docker, and should run alone. Publishing is
manual: a person runs `make publish`.
