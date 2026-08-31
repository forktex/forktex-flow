# Security Policy

## Reporting a vulnerability

Email **info@forktex.com** with details and, where possible, a reproduction. Please do
not open a public issue for an unfixed vulnerability.

## Supported versions

| Version | Supported |
| --- | --- |
| 1.0.x | ✅ |
| 0.8.x and earlier | ❌ — a different package (the agentic CLI); see [CHANGELOG](CHANGELOG.md) |

## Scope

`forktex` is a library. It has no CLI, no daemon, no network listener, and no ambient
filesystem access — it runs entirely inside the consuming service's process, under that
service's privileges. The security-relevant surface is therefore narrow:

- **`vault`** — Fernet symmetric encryption at rest and the `EncryptedJSON` column type.
- **`database`** — SQL construction. Everything is SQLAlchemy Core/ORM; no module in
  this library builds a SQL string by interpolation. Identifiers that reach DDL are
  validated (`database.identifiers`) and quoted by the dialect's preparer.
- **`storage` / `store` / `vector` / `cache` / `queue`** — clients for external
  infrastructure. Credentials are supplied by the caller and never persisted by the
  library.
- **`log`** — structured output. It does not redact; see below.

## What this library does not do

- **No phone-home.** Nothing here contacts a ForkTex service. The only network traffic
  is to the infrastructure *you* configure (your Postgres, your Redis, your S3).
- **No credential storage.** Connection strings and keys are passed in by the consumer
  and held in memory for the process lifetime only.
- **No authentication or authorization.** These are the consuming service's concern.

## Operator responsibilities

- **`vault` key management.** The KEK is derived with a single SHA-256 over the value
  you supply, which is *not* a password-stretching KDF. Supply a high-entropy key
  (e.g. 32 random bytes), not a passphrase. Length alone is not sufficient.
- **Log redaction.** `log` emits the fields you give it. It does not scan for secrets;
  do not pass credentials, tokens, or personal data into log context.
- **Transport security.** Use TLS-enabled connection URLs for every backing service.
  The library does not downgrade or override your transport settings.
- **Dependency currency.** Optional extras pull real clients (`cryptography`,
  `aioboto3`, `qdrant-client`, `pymongo`, `arq`). `make audit` runs `pip-audit` against
  the resolved environment and is part of `make ci`.

## Licensing

Dual-licensed: AGPL-3.0-or-later, or a commercial license from FORKTEX S.R.L. See
[LICENSE](LICENSE) and [NOTICE](NOTICE).
