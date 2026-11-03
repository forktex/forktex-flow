# Security Policy

## Reporting a vulnerability

Email **info@forktex.com** with details and, where possible, a reproduction. Please do
not open a public issue for an unfixed vulnerability.

## Supported versions

| Version | Supported |
| --- | --- |
| 1.0.x | ✅ |

## Scope

`forktex-flow` is a library. It has no CLI, no daemon and no network listener; it runs
inside the consuming service's process, under that service's privileges, and talks only
to the Postgres database you give it. The security-relevant surface:

- **SQL.** Every statement is a SQLAlchemy construct; nothing builds SQL by
  interpolating a value. The schema name is the only identifier that reaches DDL.
- **Definitions as data.** `flow.deploy()` accepts JSON and BPMN XML that may come
  from a tenant. Gateway conditions are JSON Logic, evaluated by a closed set of
  operators — no code execution. BPMN XML with a DOCTYPE is refused, which closes
  entity expansion, and the parser never fetches external resources.
- **Handlers are yours.** A definition can only name task types; it runs code only
  through the handlers your process registered. A `@flow.handler` task type is
  usable from every namespace, so a handler that must not serve a tenant checks
  `ctx.namespace`. A pipeline's own steps cannot be named by any other definition.

## What this library does not do

- **No authorization.** Who may deploy a definition, start an instance or complete a
  user task is the consuming service's decision.
- **No secret storage.** Variables and metadata are stored as plain JSONB. Do not put
  credentials in them; carry an id and resolve the secret inside the handler.
- **No phone-home.** The only network traffic is to the database you configure.

## Operator responsibilities

- **Namespaces are not a security boundary on their own.** They keep definitions and
  instances apart, but every query a consumer exposes must filter by the caller's
  namespace.
- **Transport security.** Use a TLS-enabled database URL.
- **Dependency currency.** `make audit` runs `pip-audit` against the resolved
  environment and is part of `make ci`.

## Licensing

Dual-licensed: AGPL-3.0-or-later, or a commercial license from FORKTEX S.R.L. See
[LICENSE](LICENSE) and [NOTICE](NOTICE).
