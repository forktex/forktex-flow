# Changelog

All notable changes to `forktex-flow` are documented here. This project follows
[Semantic Versioning](https://semver.org/).

## [Unreleased]

## [1.0.0] — 2026-11-03

First release. Requires Python 3.14, PostgreSQL 14+ and `forktex>=0.15,<0.16`.

- One engine for process definitions: `startEvent`, `serviceTask`, `userTask`, `exclusiveGateway`, `callActivity`, `adHocSubProcess` and `endEvent`, joined by sequence flows, stored versioned per namespace.
- `@flow.pipeline`, `@flow.scheduled` and `step(...)` compile to definitions; `flow.deploy()` takes a `ProcessModel`, its JSON, or BPMN 2.0 XML.
- Handlers registered per `Flow` by task type; every driving `Flow` claims the jobs it can run, and one elected leader fires schedules and reclaims stalled jobs.
- Gateway conditions in JSON Logic, checked at deploy.
- User tasks completed with `flow.complete_task()`; call activities with parent/child lineage; ad-hoc subprocesses driven by a decider, for agents.
- A process claims its next job as soon as it advances an instance; the leader warns about job types no process claims.
- At-least-once execution: retries with backoff, heartbeats, reclaim, cancellation that reaches running handlers and children, and a drain that hands running jobs back on shutdown.
- `DefinitionInfo.source` keeps what was deployed, BPMN XML verbatim.
- `WorkflowInstance` with `wait`, `refresh`, `stream`, `cancel`; `InstanceQuery` with keyset paging and summaries; `FlowExtension` lifecycle hooks.
- Testing helpers: `make_ctx`, `isolated_flow`, `run_to_completion`, `run_pending`.
