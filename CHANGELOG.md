# Changelog

All notable changes to `forktex-flow` are documented here. This project follows
[Semantic Versioning](https://semver.org/).

## [Unreleased]

## [1.0.0] — 2026-11-03

First release. Requires Python 3.14, PostgreSQL 14+, `forktex>=1.0,<2`, SQLAlchemy 2.1, asyncpg 0.32, pydantic 2.14 and croniter 6.2 or newer.

- One engine for process definitions: `startEvent`, `serviceTask`, `userTask`, `exclusiveGateway`, `callActivity`, `adHocSubProcess` and `endEvent`, joined by sequence flows, stored versioned per namespace.
- `flow.pipeline(key, steps=[...], state=, cron=)` compiles to a definition; `step(...)` sets a step's name, retry policy and JSON Logic condition; `flow.deploy()` takes a `ProcessModel`, its JSON, or BPMN 2.0 XML.
- Every step and handler is `async def name(ctx, state)`, checked at declaration; a pydantic `state=` is validated at `start()` and handed to each step; steps return only what changed.
- A pipeline's steps have private task types (`<key>:<name>`) that no other definition may use; `@flow.handler` task types serve every namespace.
- Handlers registered per `Flow` by task type; every driving `Flow` claims the jobs it can run, and one elected leader fires schedules and reclaims stalled jobs.
- Gateway conditions in JSON Logic, checked at deploy.
- User tasks completed with `flow.complete_task()`; call activities with parent/child lineage; ad-hoc subprocesses driven by a decider, for agents.
- A process claims its next job as soon as it advances an instance; the leader warns about job types no process claims.
- At-least-once execution: retries with backoff, heartbeats, reclaim, cancellation that reaches running handlers and children, and a drain that hands running jobs back on shutdown.
- `DefinitionInfo.source` keeps what was deployed, BPMN XML verbatim.
- `WorkflowInstance` with `wait`, `refresh`, `stream`, `events`, `cancel`; `InstanceQuery` with keyset paging and summaries; `FlowExtension` lifecycle hooks.
- Testing helpers: `make_ctx`, `isolated_flow`, `run_to_completion`, `run_pending`.
