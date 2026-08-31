-- The complete forktex_flow schema: five tables, in one migration. Applied
-- once on a fresh install; later migrations add only additive DDL, forward-only
-- (a bad migration is fixed by the next version, never by a downgrade).
--
-- {schema} is substituted by forktex.database.SchemaMigrationRunner with a
-- plain string replace, not str.format — the literal `{`/`}` in the JSONB
-- defaults below are left untouched.
--
-- This file and persist/models.py describe the same tables and must be changed
-- together. This one is what a deployed database actually has.

-- ── Workflow registry ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS {schema}.workflow (
    name             VARCHAR(255) NOT NULL,
    version          INTEGER      NOT NULL,
    ast_hash         VARCHAR(64),
    registered_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (name, version)
);

-- ── Run — one row per flow.run() ─────────────────────────────────────
CREATE TABLE IF NOT EXISTS {schema}.run (
    id                 UUID         PRIMARY KEY,
    workflow_name      VARCHAR(255) NOT NULL,
    workflow_version   INTEGER      NOT NULL,
    status             VARCHAR(16)  NOT NULL,
    input              JSONB        NOT NULL DEFAULT '{}'::jsonb,
    output             JSONB,
    error              TEXT,
    metadata           JSONB        NOT NULL DEFAULT '{}'::jsonb,
    triggered_by       VARCHAR(32)  NOT NULL DEFAULT 'manual',
    started_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    finished_at        TIMESTAMPTZ,
    cancelled_at       TIMESTAMPTZ,
    cancel_reason      TEXT,
    CONSTRAINT ck_run_status
        CHECK (status IN ('pending','running','completed','failed','cancelled')),
    -- Two ways a run starts: someone called flow.run(), or the driver fired a
    -- cron. Child runs ('spawn') went with the graph track; the value stayed in
    -- this list for a year after nothing could write it.
    CONSTRAINT ck_run_triggered_by
        CHECK (triggered_by IN ('manual','schedule'))
);

CREATE INDEX IF NOT EXISTS ix_run_workflow_name  ON {schema}.run (workflow_name);
CREATE INDEX IF NOT EXISTS ix_run_status         ON {schema}.run (status);
-- Containment indexes for the two JSONB filters the query builder offers:
-- `.metadata(**kv)` over `metadata`, `.state(**kv)` over `input`. jsonb_path_ops
-- on `input` because only `@>` is ever asked of it — a smaller index than the
-- default operator class, which also supports key-existence tests nothing uses.
CREATE INDEX IF NOT EXISTS ix_run_metadata_gin   ON {schema}.run USING GIN (metadata);
CREATE INDEX IF NOT EXISTS ix_run_input_gin      ON {schema}.run USING GIN (input jsonb_path_ops);

-- The keyset index. Both columns, in the paging order, so the row comparison
-- `(started_at, id) < (:ts, :id)` lands as an Index Cond rather than a Filter —
-- a Filter reads and discards every entry from the start of the index, which
-- returns correct rows and degrades with page depth until production notices.
-- Both columns are NOT NULL, which is what makes the comparison safe: a row
-- comparison involving NULL evaluates to NULL and silently drops the row.
CREATE INDEX IF NOT EXISTS ix_run_started_at_id
    ON {schema}.run (started_at DESC, id DESC);

-- ── Step run — one row per durable step invocation ───────────────────
CREATE TABLE IF NOT EXISTS {schema}.step_run (
    id                 UUID         PRIMARY KEY,
    run_id             UUID         NOT NULL REFERENCES {schema}.run(id) ON DELETE CASCADE,
    step_name          VARCHAR(255) NOT NULL,
    step_qualname      VARCHAR(512) NOT NULL,
    step_index         INTEGER      NOT NULL,
    args_hash          VARCHAR(64)  NOT NULL,
    status             VARCHAR(16)  NOT NULL,
    output             JSONB,
    error              TEXT,
    attempts           INTEGER      NOT NULL DEFAULT 0,
    max_attempts       INTEGER      NOT NULL,
    next_attempt_at    TIMESTAMPTZ,
    started_at         TIMESTAMPTZ,
    finished_at        TIMESTAMPTZ,
    heartbeat_at       TIMESTAMPTZ,
    CONSTRAINT ck_step_run_status
        CHECK (status IN ('pending','running','completed','failed','cancelled')),
    CONSTRAINT ck_step_run_attempts_non_negative CHECK (attempts >= 0),
    CONSTRAINT ck_step_run_max_attempts_positive CHECK (max_attempts >= 1),
    -- The memoisation key: a completed row here is what makes a resumed run
    -- skip a step instead of repeating its side effects. Uniqueness has to hold
    -- under concurrency, which only the database guarantees.
    CONSTRAINT uq_step_run_identity UNIQUE (run_id, step_qualname, args_hash)
);

CREATE INDEX IF NOT EXISTS ix_step_run_status_heartbeat
    ON {schema}.step_run (status, heartbeat_at);
CREATE INDEX IF NOT EXISTS ix_step_run_run_id_index
    ON {schema}.step_run (run_id, step_index);

-- ── Run event — append-only progress log ─────────────────────────────
-- EXTERNAL DEPENDENCE: ForkTex Cloud's SSE endpoint tails this table by raw
-- SQL, outside this package — `SELECT id, ts, event_type, payload FROM
-- forktex_flow.run_event WHERE run_id = :rid AND id > :last_id ORDER BY id`.
-- The table name, those four columns and the monotonic id are a cross-repo
-- contract; changing them breaks a consumer that never imports this code.
CREATE TABLE IF NOT EXISTS {schema}.run_event (
    id                 BIGSERIAL    PRIMARY KEY,
    run_id             UUID         NOT NULL REFERENCES {schema}.run(id) ON DELETE CASCADE,
    ts                 TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    event_type         VARCHAR(64)  NOT NULL,
    payload            JSONB        NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS ix_run_event_run_id_ts ON {schema}.run_event (run_id, ts);

-- ── Scheduled run — @flow.scheduled cron state ───────────────────────
CREATE TABLE IF NOT EXISTS {schema}.scheduled_run (
    workflow_name      VARCHAR(255) NOT NULL,
    workflow_version   INTEGER      NOT NULL,
    cron               VARCHAR(128) NOT NULL,
    enabled            BOOLEAN      NOT NULL DEFAULT TRUE,
    last_fired_at      TIMESTAMPTZ,
    next_fire_at       TIMESTAMPTZ  NOT NULL,
    PRIMARY KEY (workflow_name, workflow_version),
    FOREIGN KEY (workflow_name, workflow_version)
        REFERENCES {schema}.workflow(name, version) ON DELETE CASCADE
);
-- Partial: the due-scan only ever looks at enabled rows, so the disabled ones
-- do not belong in the index it walks.
CREATE INDEX IF NOT EXISTS ix_scheduled_run_due
    ON {schema}.scheduled_run (next_fire_at) WHERE enabled;
