# `forktex_flow` reference

Every public name and its exact behaviour. The [guide](guide.md) shows them in
use; [operating](operating.md) covers failure and deploys.

## The function shape

Every pipeline step and every handler is written exactly like this:

```python
async def name(ctx: Ctx, state: S) -> dict | BaseModel | None:
    ...
```

| Part | Contract |
|---|---|
| `async def` with two positional parameters | Anything else (a sync function, one parameter, keyword-only parameters, extra parameters) raises `BadRequestError` at `flow.pipeline(...)`, `step(...)` or `@flow.handler(...)`. |
| `ctx` | The [`Ctx`](#ctx) of this attempt. |
| `state` | With a pydantic `state=` model: the variables validated as that model. Variables that no longer match fail the element at once, without retries. Otherwise: the variables as a `dict`. |
| Returns | Only what changed. A `dict`: every key in it. A pydantic model: only the fields that were set, so an explicit `None` clears a field. `None`: no change. Anything JSONB cannot hold fails the attempt with `StateNotSerialisable`. |
| Raises | Any exception fails the attempt; it is retried until `max_attempts`. `WorkflowCancelled(reason)` cancels the instance instead. |

Each returned field replaces the stored one, unless the state declares a
reducer for it (`Annotated[list[str], operator.add]`), which merges the two.
An element with `resultVariable` stores its output under that name instead.

## Public surface

`forktex_flow.__all__` is the contract. Everything else is internal.

| Name | What it is |
|---|---|
| `Flow` | The facade: one per process and schema. |
| `Ctx` | The first argument of every step and handler. |
| `step`, `StepSpec` | A pipeline entry with options, and its type. |
| `ProcessModel` | A process definition: elements and sequence flows. |
| `StartEvent`, `EndEvent`, `ServiceTask`, `UserTask`, `ExclusiveGateway`, `CallActivity`, `AdHocSubProcess`, `SequenceFlow` | Its parts. |
| `AdHocDecision`, `Activation` | What an ad-hoc decider returns. |
| `parse_bpmn` | BPMN 2.0 XML compiled to a `ProcessModel`. |
| `DefinitionInfo` | One deployed version: `namespace`, `key`, `version`, `checksum`, `model`, `source`. |
| `WorkflowInstance` | An instance snapshot, plus `wait` / `refresh` / `stream` / `events` / `cancel`. |
| `InstanceInfo`, `ElementRunInfo`, `InstanceEventInfo` | The snapshot and history shapes. |
| `InstanceStatus`, `ElementStatus`, `TERMINAL_STATUSES` | Status vocabularies; an instance never leaves a terminal status. |
| `DEFAULT_NAMESPACE` | `"default"`: where code-declared workflows live, and every call's default. |
| `InstanceQuery`, `InstancePage`, `InstanceSummary` | Finding instances. |
| `RuntimeConfig` | Retry, polling and concurrency settings. |
| `FlowExtension` | Lifecycle hooks. |
| `FlowError`, `WorkflowFailed`, `WorkflowCancelled`, `StateNotSerialisable`, `FlowClosed` | The library's errors. |
| `apply_migrations` | Bring a schema up without building a `Flow`. |
| `apply_state_update`, `extract_reducers` | The reducer machinery, for testing state classes. |
| `make_ctx`, `isolated_flow`, `run_to_completion`, `run_pending` | Testing helpers. |

## `Flow`

```python
Flow(database_url=None, *, database=None, schema="forktex_flow",
     extensions=None, config=None, drive=False, echo=False, **engine_kwargs)
```

| Argument | Default | Meaning |
|---|---|---|
| `database_url` | — | A DSN. The pool belongs to this `Flow`, and `close()` disposes it. |
| `database` | — | A `forktex` `Database` you own, whose `schema_translate_map` remaps `forktex_flow` to `schema`. `close()` leaves it open. Pass exactly one of the two. |
| `schema` | `forktex_flow` | The Postgres schema. Flows on different schemas share nothing. |
| `extensions` | `None` | `FlowExtension` objects. |
| `config` | `RuntimeConfig()` | See [`RuntimeConfig`](#runtimeconfig). |
| `drive` | `False` | Whether `async with` also starts the driver. |
| `echo`, `**engine_kwargs` | — | Passed to SQLAlchemy (`pool_size`, `pool_pre_ping`, …). |

Nothing connects until something executes.

### Lifecycle

| Call | Does |
|---|---|
| `async with flow:` | `init()`, plus `start_driver()` when `drive=True`; `close()` on exit. |
| `await flow.init()` | Migrate the schema and deploy every declared workflow. Idempotent and safe from every process at once. |
| `await flow.verify()` | Probe the pool and create nothing. Raises `DatabaseUnreachableError`. |
| `await flow.start_driver()` | Start claiming jobs and standing for leadership. A second call is a no-op. |
| `await flow.stop_driver()` | Stop claiming, give running jobs `drain_timeout`, and hand the rest back as pending without spending an attempt. |
| `await flow.close()` | `stop_driver()`, then dispose an owned pool. Every later call raises `FlowClosed`. |

`flow.schema`, `flow.engine` and `flow.transaction()` expose the database for
your own SQL against the engine's tables. A `transaction()` block commits on
exit, so calling `commit()` inside it raises.

### Declaring code workflows

| Call | Declares |
|---|---|
| `flow.pipeline(key, *, steps, state=None, cron=None, enabled=True)` | start → each step in order → end, deployed into `DEFAULT_NAMESPACE`. `cron=` also schedules it. |
| `@flow.handler(task_type, *, state=None)` | The function run for every service task (or ad-hoc decider) of that type, in any namespace. |

- `steps` holds functions, task types of `@flow.handler`s (shared steps), or
  `step(...)` of either.
- `state=` is a pydantic model or a `TypedDict`. A pydantic model is enforced:
  `start()` raises `BadRequestError` naming every field that does not match, and
  stores the variables with the model's defaults filled in. A `TypedDict` only
  declares reducers.
- A scheduled pipeline starts with no variables, so its `state=` must accept
  `{}`; otherwise declaring it raises `BadRequestError`.
- `enabled=False` declares a schedule disarmed. The flag is read only when the
  schedule row is first written.
- Declarations deploy into `DEFAULT_NAMESPACE` on `init()` or the first
  `start()`. An unchanged one keeps its version.
- `flow.registered()` lists the declared keys.
- Declaring a key or task type twice raises `AlreadyExistsError`. A bad cron or
  condition, a pipeline with no steps, or a task type containing `:` raises
  `BadRequestError` at the call.

### `step`

```python
step(target, /, *, name=None, when=None, max_attempts=None, backoff=None)
```

| Option | Effect |
|---|---|
| `target` | A function (a private step) or a handler's task type (a shared step). |
| `name` | The step's element id. Defaults to the function's name, or the task type. Letters, digits, `_`, `.` and `-`; `start` and `end` are reserved. |
| `when` | A [JSON Logic](#conditions) condition over the variables. It compiles to an exclusive gateway in front of the step whose default path goes around it, so a false condition never visits the step. |
| `max_attempts`, `backoff` | The step's retry policy, stored in its definition. Unset, they come from `RuntimeConfig`. |

**Identity.** A private step's task type is `<key>:<name>`. The name is what
connects a running instance to your code: renaming or removing a step leaves
instances on the old version waiting at it (see
[deploying](operating.md#sharing-the-work)). Two steps with one name raise
`AlreadyExistsError`; give one a `name=`.

**Visibility.** A handler's task type can be used by any definition in any
namespace. A private step's task type can only be used by a definition with
the same key, which is the pipeline itself or a copy of it deployed into
another namespace; deploying any other definition that names it raises
`BadRequestError`.

### Definitions

| Call | Does |
|---|---|
| `await flow.deploy(model, *, namespace="default")` | Deploy a `ProcessModel`, its JSON, or BPMN XML. Returns the existing version if the checksum is unchanged, or else the next one. Raises `BadRequestError` listing every reason it cannot run. |
| `await flow.definition(key, *, namespace="default", version=None)` | That version, or the latest. Raises `NotFoundError`. |

### Instances

| Call | Does |
|---|---|
| `await flow.start(key, *, variables=None, metadata=None, namespace="default", version=None)` | Start an instance and advance it to its first wait within the same transaction. A declared key starts the version this process deployed; any other key starts the latest version, unless `version` pins one. |
| `await flow.complete_task(instance_id, element_id, payload=None)` | Complete a waiting user task with `payload` as its output. Raises `NotFoundError` if there is no such user task, `ConflictError` if it is not waiting. |
| `await flow.get(instance_id)` | A `WorkflowInstance`. Raises `NotFoundError`. |
| `flow.query()` | An `InstanceQuery`. |

## The definition format

A `ProcessModel` is JSON using BPMN's names:

```json
{
  "key": "billing.dunning",
  "elements": [
    {"id": "start",    "type": "startEvent"},
    {"id": "remind",   "type": "serviceTask", "taskType": "billing.remind", "retries": 3},
    {"id": "review",   "type": "userTask", "resultVariable": "review"},
    {"id": "decide",   "type": "exclusiveGateway", "default": "close"},
    {"id": "escalate", "type": "callActivity", "calledElement": "billing.escalation"},
    {"id": "end",      "type": "endEvent"}
  ],
  "flows": [
    {"id": "f1", "source": "start", "target": "remind"},
    {"id": "f2", "source": "remind", "target": "review"},
    {"id": "f3", "source": "review", "target": "decide"},
    {"id": "f4", "source": "decide", "target": "escalate",
     "condition": {"==": [{"var": "review.decision"}, "escalate"]}},
    {"id": "close", "source": "decide", "target": "end"},
    {"id": "f6", "source": "escalate", "target": "end"}
  ]
}
```

| Element | Does |
|---|---|
| `startEvent` | Where an instance begins. Exactly one. |
| `serviceTask` | A job: the handler for `taskType` runs it on whichever driving `Flow` claims it. `retries` and `backoff` fall back to `RuntimeConfig`. |
| `userTask` | Waits for `complete_task()`. |
| `exclusiveGateway` | Takes the first outgoing flow, in declared order, whose condition holds; otherwise its `default`. With neither, the instance fails. |
| `callActivity` | Starts a child instance of `calledElement` in the same namespace and waits for it. Uses the child's latest version unless `version` pins one. The child's final variables are the element's output, and a failed child fails the parent. |
| `adHocSubProcess` | A decider picks activities from `elements` each round; see below. |
| `endEvent` | Completes the instance. At least one. |

Any element may carry `resultVariable`, which nests its output under that name
instead of merging it. An instance's variables start as its `input`, which never
changes. A child instance starts with a copy of its parent's variables.

**Deploy refuses** a model with:
- an id used twice, or an id containing `/`;
- a flow to an unknown element;
- an unreachable element, or a cycle;
- a non-gateway with other than one outgoing flow;
- a condition outside a gateway, or a gateway flow with neither a condition nor
  default status;
- an operator the condition evaluator lacks.

### Ad-hoc subprocesses

```json
{"id": "agent", "type": "adHocSubProcess", "taskType": "agent.decide", "maxIterations": 10,
 "resultVariable": "answer",
 "elements": [{"id": "search", "type": "serviceTask", "taskType": "tool.search", "resultVariable": "found"}]}
```

1. The decider (the handler for `taskType`) runs with the variables. Its
   `ctx.activities` holds the ids it may choose from.
2. `AdHocDecision(activate=(Activation(element_id="search", variables={...}), ...))`
   starts one job per activation, in parallel; the same activity may appear
   more than once. Each job sees the instance's variables with its activation's
   variables laid over them.
3. Once every job of the round has finished, the decider runs again with fresh
   attempts and sees their outputs.
4. `AdHocDecision(done=True, output={...})`, or activating nothing, completes the
   element with `output`.

The instance fails if:
- a round goes beyond `maxIterations`;
- an activation names an id outside `elements`;
- the decider returns something other than an `AdHocDecision`.

An activity's element id is `<subprocess>/<round>.<position>/<activity>`, for
example `agent/2.0/search`.

### BPMN XML

`parse_bpmn(xml, *, key=None)`, and `flow.deploy(xml)`, compile a BPMN 2.0
document to a `ProcessModel`. `key` picks the process when the document holds
more than one.

| Model field | Read from |
|---|---|
| `taskType`, `retries` | `zeebe:taskDefinition type="…" retries="…"`, or `flow:taskType` |
| `calledElement` | `calledElement`, or `zeebe:calledElement processId="…"`; `flow:version` pins a version |
| `resultVariable`, `maxIterations` | `flow:resultVariable`, `flow:maxIterations` (`xmlns:flow="https://forktex.com/schema/flow"`) |
| `condition` | `conditionExpression` holding JSON Logic |

Documentation, lanes, annotations and the diagram stay in the source and are
left out of the model.

`parse_bpmn` refuses, by name:
- any other element type;
- a FEEL condition;
- a document with a DOCTYPE.

The same process deployed as XML and as JSON is one version.

### Conditions

A gateway condition is [JSON Logic](https://jsonlogic.com) over the variables.
Supported operators:
- `var` (dotted paths, with an optional default) and `missing`;
- `==`, `===`, `!=`, `!==`;
- `<`, `<=`, `>`, `>=` (with three arguments, "between");
- `and`, `or`, `!`, `!!`, `if`, `in`.

It differs from the JavaScript reference in two ways:

- **Equality is strict.** `"1500"` never equals `1500`, and `true` never equals
  `1`.
- **Ordering compares like with like.** Numbers compare with numbers and strings
  with strings. Any other pair is false, so a variable of the wrong type sends
  the gateway to its default.

## `WorkflowInstance`

The fields:
- **Identity:** `instance_id`, `namespace`, `key`, `version`, `definition_id`.
- **State:** `status` (`active`, `completed`, `failed`, `cancelled`), `input`,
  `variables`, `metadata`, `error`, `cancel_reason`, `triggered_by` (`manual`,
  `schedule`, `call`).
- **Lineage:** `parent_instance_id`, `parent_element_id`.
- **Timing:** `started_at`, `finished_at`.
- **Elements:** `elements`, one `ElementRunInfo` per element visited.

| Call | Returns |
|---|---|
| `await instance.wait(timeout=None)` | The snapshot once terminal, or at the timeout. A timeout is not an error. |
| `await instance.refresh()` | A fresh snapshot. |
| `async for s in instance.stream()` | A snapshot per change, ending with the terminal one. |
| `await instance.events(*, after=0, limit=100)` | The history as `InstanceEventInfo` (`id`, `instance_id`, `ts`, `event_type`, `payload`), oldest first. Pass the last `id` seen as `after` to read on. |
| `await instance.cancel(reason=…)` | `True` if it took effect. Cancels active children and stops a running handler at its next heartbeat. `False` if the instance had already finished. |
| `instance.current_elements` | The elements holding the token: pending or running jobs, waiting tasks and calls. |
| `instance.element(element_id)` | That element's run, or `None`. |

An `ElementRunInfo` has `element_id`, `element_type`, `task_type`, `status`
(`pending`, `running`, `waiting`, `completed`, `failed`, `cancelled`), `input`,
`output`, `error`, `attempts`, `max_attempts`, `next_attempt_at`,
`started_at`, `finished_at`, `heartbeat_at` and `child_instance_id`.

The engine's events are `instance_started`, `element_started`,
`element_retried`, `element_completed`, `element_failed`, `task_waiting`,
`adhoc_round`, `instance_completed`, `instance_failed` and
`instance_cancelled`. A handler adds its own with `ctx.emit()`.

## `InstanceQuery`

Filters chain. Repeating a filter replaces it, except `.metadata()` and
`.variables()`, which accumulate.

| Filter | Matches |
|---|---|
| `.namespace(name)` | One namespace. |
| `.key(key, version=None)` | One key, optionally one version. |
| `.status(*statuses)` / `.triggered_by(*triggers)` | Any of these. |
| `.metadata(**kv)` / `.variables(**kv)` | JSONB containment. |
| `.current_element(*element_ids)` | The token is at one of these. |
| `.children_of(instance_id)` | Children started by call activities. |
| `.since(dt)` / `.until(dt)` | Started at or after / before. |
| `.limit(n)` | Page size; default 50. |

Terminals: `await q.fetch(cursor=None)` returns an `InstancePage` (`items`,
`has_more`, `next_cursor`), and `q.first()`, `q.count()` and `q.summary()` are awaited the
same way.

## `Ctx`

| Member | |
|---|---|
| `instance_id`, `namespace`, `key`, `version` | The instance and its definition. A handler shared across namespaces tells customers apart by `namespace`. |
| `element_id`, `task_type` | The element being run. |
| `attempt`, `max_attempts`, `final_attempt` | This attempt, counting from 1; whether a failure now fails the instance. |
| `metadata` | The instance's metadata. |
| `activities` | For an ad-hoc decider: the ids it may activate. |
| `await ctx.emit(event_type, payload=None)` | Append to `instance_event`. Raises `BadRequestError` on a `make_ctx()` context. |
| `await ctx.sleep(seconds)` | Sleep. Not durable: a crash mid-sleep retries the job. |

## `RuntimeConfig`

| Field | Default | Controls |
|---|---|---|
| `poll_interval` | `1.0` | Seconds between claims when idle, and between the leader's passes. A process claims again at once after it starts or advances an instance. |
| `election_interval` | `2.0` | Seconds a non-leader waits before standing again. |
| `heartbeat_interval` | `10.0` | Seconds between a running job's heartbeats. |
| `stale_threshold` | `60.0` | Heartbeat silence after which a job is reclaimed. Must exceed `heartbeat_interval`. Also how long a job may sit unclaimed before the leader warns. |
| `max_attempts` | `3` | Attempts for a task that declares none. |
| `backoff` | `(30.0, 120.0, 300.0)` | Seconds between attempts. The last entry repeats. |
| `max_concurrent_jobs` | `10` | Jobs this `Flow` runs at once. |
| `leader_lock_key` | `0` | The advisory lock key; `0` derives it from the schema. |
| `drain_timeout` | `25.0` | How long stopping waits for running jobs. |

A setting that cannot work raises `BadRequestError` naming it.

## `FlowExtension`

Any object with one or more of these methods:

| Hook | Called | Can |
|---|---|---|
| `before_start(key, version, variables, metadata)` | Before an instance is written | Return a dict merged into its metadata. |
| `after_complete(instance, variables)` | Once `completed` | Observe. |
| `after_fail(instance, error)` | Once `failed`; `error` is a `WorkflowFailed` | Observe. |

A terminal hook that raises is logged; the instance's outcome is unaffected.

## Testing helpers

| Helper | Gives |
|---|---|
| `make_ctx(*, namespace, key, version, element_id, attempt, max_attempts, metadata, instance_id, activities)` | A `Ctx` with no database, for awaiting a handler directly. |
| `isolated_flow(url)` | An async context manager: a migrated `Flow` on a throwaway schema, dropped on exit. |
| `run_to_completion(flow, key, *, variables=None, namespace="default", …)` | Start an instance and `run_pending` it. |
| `run_pending(flow, instance_id)` | Run the instance's jobs, and its children's, inline and without backoff, until it is terminal or waits on something else (such as a user task). |

## Errors

| Error | Raised when |
|---|---|
| `BadRequestError` | A definition, document, cron, condition or config that cannot run; a step or handler of another shape; variables that do not match a pipeline's pydantic state at `start()`. |
| `NotFoundError` | An undeployed key or version, an unknown instance, or no such user task. |
| `ConflictError` | Completing a user task that is not waiting. |
| `AlreadyExistsError` | A key or task type declared twice in one `Flow`, or two steps of one pipeline with one name. |
| `StateNotSerialisable` | Variables that JSONB cannot hold: at `start()`, or as a failed attempt. |
| `FlowClosed` | Any call after `close()`. |
| `WorkflowCancelled` | Raised by you in a handler, to cancel its instance. |
| `WorkflowFailed` | Handed to `after_fail`, carrying the instance's error. |

`FlowError` is the base of the library's own errors. The first four come from
`forktex.error`. All derive from `forktex.error.AppError`.
