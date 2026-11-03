<p align="center">
  <a href="https://pypi.org/project/forktex-flow/"><img src="https://img.shields.io/pypi/v/forktex-flow.svg" alt="PyPI"></a>
  <a href="https://pypi.org/project/forktex-flow/"><img src="https://img.shields.io/pypi/pyversions/forktex-flow.svg" alt="Python"></a>
  <a href="https://github.com/forktex/forktex-flow/blob/master/LICENSE"><img src="https://img.shields.io/pypi/l/forktex-flow.svg" alt="License"></a>
</p>

<p align="center"><em>Durable workflows that run entirely on PostgreSQL.</em></p>

Your own processes do the work, and Postgres holds every workflow's state, so a
crash or a redeploy picks up where it left off. There is no broker, no queue
and no extra service: the only infrastructure is a Postgres database.

```bash
pip install forktex-flow
```

## Requirements

| | |
|---|---|
| **Python** | 3.14 |
| **PostgreSQL** | 14 or newer, the only database it runs on. Local, in a container or managed. |
| **Connection** | Direct, or through a pooler in session mode. A transaction-mode pooler breaks the lock that elects the leader. |
| **Permission** | Creating the `forktex_flow` schema on first start, or owning one created for it. Nothing else in the database is touched. |
| **Driver** | `asyncpg`, installed with the package; URLs use `postgresql+asyncpg://`. |

## What you can build

| You want to | Read |
|---|---|
| Run a first workflow in one file | [Your first workflow](docs/guide.md#your-first-workflow) |
| Keep workflows in one module shared by an app, workers and tests | [Declare your workflows](docs/guide.md#declare-your-workflows) |
| Run the work inside a FastAPI (or any ASGI) app | [Run inside a web app](docs/guide.md#run-inside-a-web-app) |
| Run the work in separate worker processes | [Run a separate worker](docs/guide.md#run-a-separate-worker) |
| Start workflows from a script or a deploy job | [Start work from a script](docs/guide.md#start-work-from-a-script) |
| Run something on a cron | [Run on a schedule](docs/guide.md#run-on-a-schedule) |
| Let each customer have their own process, stored as data | [Deploy definitions at runtime](docs/guide.md#deploy-definitions-at-runtime) |
| Pause for an approval, then branch on it | [Wait for a person](docs/guide.md#wait-for-a-person) |
| Run processes drawn in Camunda Modeler | [Import BPMN from a modeler](docs/guide.md#import-bpmn-from-a-modeler) |
| Let an LLM decide which tools to call, durably | [Let an agent choose the steps](docs/guide.md#let-an-agent-choose-the-steps) |
| Show progress, list failures, cancel | [Follow, find and cancel instances](docs/guide.md#follow-find-and-cancel-instances) |
| Test it all with pytest | [Test your workflows](docs/guide.md#test-your-workflows) |

Every section of the guide is a complete file you can copy and run.

## How it works

| Concept | What it is |
|---|---|
| **Definition** | A process: steps joined by arrows, using BPMN's element names. A code pipeline is one; JSON and BPMN XML deployed at runtime are too. Each change is a new version. |
| **Instance** | One run of a definition, with its own variables. It finishes on the version it started on. |
| **Step** | `async def name(ctx, state)`, returning only what it changed. The one shape every step and handler has: [the template](docs/reference.md#the-function-shape). |
| **Job** | One step of one instance. Any driving process with a handler for it can claim it; two processes never claim the same job. |
| **Leader** | One process at a time fires schedules and hands back jobs whose process died. Any process can take over. |
| **Namespace** | Keeps one customer's definitions and instances apart from another's. |

## Things to know

| | |
|---|---|
| **A job can run more than once** | If a process dies mid-job, the job runs again. Make side effects idempotent, for example by keying them on `ctx.instance_id`. |
| **Starting twice starts two instances** | Deduplicate before calling `start()` if you need to. |
| **A definition has no loops** | Each step runs at most once per instance. Repeat work with a new instance, a call to another definition, or an agent. |
| **Variables are JSON** | Steps return dicts, or pydantic models, of JSON-compatible values. |
| **A step's name is its identity** | Renaming or removing a step affects instances still running on the old version; see [deploying](docs/operating.md#sharing-the-work). |

## Documentation

| Page | For |
|---|---|
| [Guide](docs/guide.md) | Runnable examples for each way to use it |
| [Reference](docs/reference.md) | Every public name, option, element and error |
| [Operating](docs/operating.md) | Failure behaviour, deploys, logging, monitoring and the tables |

## Stability

Semantic versioning covers the names in `forktex_flow.__all__`, the
`FlowExtension` hook signatures, the `forktex_flow` schema name and the
`instance_event(id, instance_id, ts, event_type, payload)` table. Migrations are
forward-only. Changes are listed in the [CHANGELOG](CHANGELOG.md).

## Contributing

Working on `forktex-flow` itself? Start with
[`docs/development.md`](docs/development.md).

## Licence

AGPL-3.0-or-later, or a commercial licence from FORKTEX S.R.L.
(info@forktex.com). See [LICENSE](LICENSE) and [NOTICE](NOTICE).
