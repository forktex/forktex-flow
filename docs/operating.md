# Operating `forktex-flow`

What the engine guarantees under failure, how leadership works, what to
watch, and the tables it owns. The consumer API is
[`docs/flow.md`](flow.md).

## Durability, precisely

**The memoisation key** is `(run_id, step_qualname, args_hash)` with a unique
constraint, where `args_hash` covers the step's **input state and its position**.
A completed row is returned without re-running the body.

**Resume replays from `run.input`.** `output` is written after every step for
observability but is never the resume point: resuming from a merged state would
feed a different state into the hash and miss every cached step, re-running side
effects that already happened.

**The hash normalises what it can.** Datetimes go through `to_iso`, which
converts to UTC first — so the same instant written with two different offsets
hashes identically. A resume cannot miss a cached step over a difference in the
caller's tzinfo. Dates and UUIDs have fallbacks; anything else JSONB cannot take
raises `StateNotSerialisable`: state round-trips through JSONB, and an
unserialisable value has no stable representation to key on.

**Retries are per step.** A retryable failure returns the step to `pending` with
`next_attempt_at` set. The last backoff entry repeats once attempts run past the
tuple, rather than collapsing back to the first.

**A skipped step records a completed row** with `skipped` set. It has to:
without it a resume re-evaluates the guard against a state that has since moved
on, and runs a step it had correctly skipped.

**Reclaim.** Running steps heartbeat every `heartbeat_interval`. A step silent
for longer than `stale_threshold` returns to `pending`; the run holding it is
swept from `running` back to `pending` so the next tick re-claims it. The cutoff
is computed on the **database** clock — comparing a database `heartbeat_at`
against an application-side `now()` puts two clocks on either side of the
comparison, and a live step gets reclaimed whenever the app clock runs ahead.

**Cancellation is cooperative, and between steps.** `cancel()` writes
`status='cancelled'`, and the executor re-reads the status before dispatching
each step — so a run that has not started stops before its first step, and a run
mid-flight stops before its *next* one. **The step already running completes**:
the library cannot interrupt arbitrary consumer code.

`cancel()` returns `False` when the run had already finished, and changes
nothing. A terminal status is final — `update_run_status` carries a
`WHERE status NOT IN (terminal)` guard.

**Claiming is a compare-and-swap.** `mark_step_running` updates
`WHERE status = 'pending'` and raises `StepClaimLost` when it matches nothing.
The loser abandons silently: whichever attempt won owns recording the outcome,
and a write from the loser would race it.

## Leadership

Every process competes for one session-scoped Postgres advisory lock, keyed on
the schema. Only the holder ticks; a worker that loses waits
`config.election_interval` before standing again — the bound on takeover time. The leader's death drops its connection and releases the lock,
so the next election has a new leader. The holder also revalidates the lock each
tick and stands down if it has gone: a terminated backend or a pooler recycle
releases it without the holder noticing.

A tick, in order: fire due schedules → reclaim stale steps → revive runs holding
a ready step → claim up to ten pending runs and dispatch each. Each phase is
isolated; one raising does not cost the tick the other two, which recover
independent kinds of stuck work.

## What to watch

**Logging.** Every module logs through `forktex.log.get_logger(__name__)`, so
the logger names are `forktex_flow.runtime.driver`, `forktex_flow.persist.steps`
and so on — configure them like any other. The driver logs at `info` when it
wins leadership and when it reclaims stale steps, at `warning` when it declines
a duplicate dispatch or an extension hook raises, and at `exception` when a tick
phase fails. Nothing logs per step; that is what `run_event` is for.

**What to watch.** The engine has no metrics endpoint — it has queries:

```python
# Runs that have been executing a given step for a while.
stuck = await flow.query().status("running").current_node("provision_vps").count()

# The backlog: anything pending is waiting for a leader to pick it up.
backlog = await flow.query().status("pending").count()

# Failures in the last hour, by workflow.
recent = await flow.query().status("failed").since(hour_ago).summary()
```

A `pending` count that grows without bound means no worker holds the lock — the
single most useful alert to have.

**Capacity.** Runs execute concurrently inside the one leader as coroutines,
bounded by `config.max_concurrent_runs` (default 10) — the driver claims only
what it has room for. Only the leader executes, so adding replicas buys
availability, not throughput. Past a single event loop's ceiling you need a
sharded lock, which this does not implement.

**Growth.** `run_event` is append-only and `step_run` keeps a row per step
invocation, so both grow with usage and neither is pruned for you. A retention
job deleting old terminal runs cascades to both — decide the policy before the
tables decide it for you.

**Deploying a new version.** A run records the version it started under and keeps
executing that definition, so a rolling deploy where both versions are briefly
registered is safe. Bump the version whenever the step list changes shape;
re-registering an existing `(name, version)` raises `AlreadyExistsError`.

## Operator actions

Three things an operator does that no route or method covers.

**Arm or disarm a schedule.** `@flow.scheduled(enabled=...)` is read only when
the row is first written, so after that the database is the only place the flag
lives. There is deliberately no API: arming a job that dispatches work is not
something a deploy should do on its own.

```sql
SELECT workflow_name, workflow_version, cron, enabled, last_fired_at, next_fire_at
  FROM forktex_flow.scheduled_run ORDER BY workflow_name;

UPDATE forktex_flow.scheduled_run SET enabled = true
 WHERE workflow_name = 'cloud.lifecycle.reap_expired_servers';
```

Read the rows before trusting any code that claims what is armed. A database
whose schema predates a schedule keeps whatever `enabled` it already had, and
nothing disarms it for you.

**Prune history.** Deleting a terminal run cascades to its `step_run` and
`run_event` rows, which is the whole retention policy:

```sql
DELETE FROM forktex_flow.run
 WHERE status IN ('completed', 'failed', 'cancelled')
   AND finished_at < now() - interval '90 days';
```

Only terminal runs: deleting a `running` one drops the step rows a resume would
have skipped, so the run restarts from the top and repeats side effects.

**Reset the schema.** There is no downgrade. A schema that cannot be migrated
forward is dropped and rebuilt by the next `Flow.init()`:

```sql
DROP SCHEMA forktex_flow CASCADE;
```

This discards run history and every `scheduled_run` row, so each schedule comes
back **disarmed**.

## Schema

| Table | Holds |
|---|---|
| `workflow` | `(name, version)` registry — what `scheduled_run` foreign-keys to. |
| `run` | One row per `flow.run()`. |
| `step_run` | One row per step invocation; the memoisation table. |
| `run_event` | Append-only progress log. |
| `scheduled_run` | Cron state: expression, `enabled`, `next_fire_at`. |
| `flow_schema_version` | Applied migrations. Not the runner's default name — deployed databases carry this one. |

The schema name is the only thing separating two Flows sharing a database, and
it carries into the `LISTEN`/`NOTIFY` channel as `{schema}_run` — that is what
`flow.stream(run_id)` subscribes to, so two Flows on one schema also share a
stream.

