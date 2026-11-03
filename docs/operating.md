# Operating `forktex-flow`

What the engine guarantees under failure, how processes share the work, what to
watch, and the tables it owns. The API is in the [reference](reference.md).

## Database

- PostgreSQL 14 or newer is the only supported database. Everything the engine
  knows lives there; the processes hold nothing that a restart loses.
- The database user needs to create the `forktex_flow` schema on first start
  (or own one created in advance). The engine creates and alters tables only in
  that schema.
- Connect directly or through a pooler in session mode. Leader election holds a
  session-level advisory lock, which transaction-mode pooling breaks.
- Every process pointed at the same database and schema shares one set of
  workflows. A different `schema=` is a separate, independent set.

## Durability

**The token moves inside one transaction.** Starting an instance, completing a
job and completing a user task each lock the instance's row and advance through
events and gateways until the next wait — a job, a user task, a child — in the
same transaction. A failure rolls the whole move back, so no element completes
without its successor being queued.

**An element runs at most once per instance.** `element_run` is unique on
`(instance_id, element_id)`, and definitions are acyclic, so a completed element
is never repeated. What can repeat is an *interrupted* job: delivery is
at-least-once.

**Claiming is `FOR UPDATE SKIP LOCKED`.** Any number of processes claim pending
jobs of the task types they handle without ever taking the same one, and each
claim counts an attempt.

**Retries are per job.** A failure with attempts left returns the job to
`pending` with `next_attempt_at` set, and no worker claims it before then. The
last backoff entry repeats once attempts run past the tuple. A failure on the
final attempt fails the element and the instance, carrying the traceback.

**Reclaim.** A running job heartbeats every `heartbeat_interval`. The leader
returns a job silent for longer than `stale_threshold` to `pending`, or fails it
when it has no attempts left. The cutoff is computed on the **database** clock.

**Cancellation.** `cancel()` marks the instance and its open elements
`cancelled`, cascades to active children, and returns `False` for an instance
that had already finished. A running handler learns at its next heartbeat and is
cancelled.

**Stopping.** `stop_driver()` stops claiming and gives running jobs
`drain_timeout` to finish. A job still running is cancelled and handed back as
`pending` without spending its attempt, so the next worker picks it up at once
rather than after `stale_threshold`. Keep the host's stop grace period above
`drain_timeout`.

## Sharing the work

Every driving `Flow` claims jobs for the task types it has handlers for, up to
`max_concurrent_jobs` at once, so replicas add throughput as well as
availability. A process polls every `poll_interval` when idle, and claims again
at once after it starts or advances an instance, so a pipeline's steps follow
each other without waiting for a poll. One claim takes as many ready jobs as the
process has room for.

A process can run a subset of the work by registering only some handlers. A job
whose type no running process handles stays `pending`; once one has waited
longer than `stale_threshold` with none of its type running, the leader logs a
warning naming the task type.

One process at a time also **leads**: the holder of a session-scoped advisory
lock, keyed on the schema, fires due schedules and reclaims stale jobs. A
non-leader stands again every `election_interval`, which bounds takeover time,
and the leader checks its lock each pass and stands down if Postgres released
it.

**Deploying.** An instance finishes on the version it started on, so two
versions can run side by side during a rolling deploy. A version that drops a
step leaves the old version's instances waiting at that step until some process
handles its task type again. Keep the old handler registered until those
instances finish, or cancel them.

## What to watch

**Logging** goes through `forktex.log` under `forktex_flow.*`.
- `info`: a process starts leading, reclaims jobs, or recovers after a failure.
- `warning`: a job type sits unclaimed, a drain ends with jobs still running, or
  an extension raises.
- A database outage logs one warning with its traceback per loop, then one line
  when it recovers.

Nothing logs per job; `instance_event` records that.

**Queries, not metrics:**

```python
# Instances waiting on a person.
awaiting_review = await flow.query().current_element("review").count()

# Failures in the last hour.
recent = await flow.query().status("failed").since(hour_ago).summary()
```

Pending jobs by type:

```sql
SELECT task_type, count(*) FROM forktex_flow.element_run
 WHERE status = 'pending' GROUP BY task_type ORDER BY 2 DESC;
```

**Growth.** `instance_event` is append-only and `element_run` keeps a row per
element visited; neither is pruned for you.

## Operator actions

### Arm or disarm a schedule

`enabled=` is read only when the row is first
written; afterwards the flag lives only in the database.

```sql
SELECT namespace, key, cron, enabled, last_fired_at, next_fire_at
  FROM forktex_flow.schedule ORDER BY key;

UPDATE forktex_flow.schedule SET enabled = true
 WHERE namespace = 'default' AND key = 'billing.close_day';
```

### Prune history

Deleting a terminal instance cascades to its elements, its
events and its children:

```sql
DELETE FROM forktex_flow.instance
 WHERE status IN ('completed', 'failed', 'cancelled')
   AND parent_instance_id IS NULL
   AND finished_at < now() - interval '90 days';
```

### Reset the schema

There is no downgrade. A schema that cannot be migrated
forward is dropped and rebuilt by the next `Flow.init()`, which also redeploys
every declared workflow; schedules come back as their declarations say.

```sql
DROP SCHEMA forktex_flow CASCADE;
```

## Schema

| Table | Holds |
|---|---|
| `definition` | One immutable row per `(namespace, key, version)`: the model, the source as written, its checksum. |
| `instance` | One execution of one version: status, input, variables, metadata, parent link. |
| `element_run` | One element of one instance; a row with a `task_type` is a job. |
| `instance_event` | Append-only progress log, tailable by `id`. |
| `schedule` | Cron state per `(namespace, key)`: expression, `enabled`, `next_fire_at`. |
| `schema_version` | Applied migrations. |

The schema name is the only thing separating two Flows sharing a database.
