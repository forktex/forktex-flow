-- One row per flow.run(), and the run-level liveness stamp recovery keys off.
--
-- `heartbeat_at` is stamped for the whole of execute_run. Recovery that can
-- only see a run through its step rows is blind between two steps, before the
-- first and after the last, which is where a dying worker strands a run.

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
    heartbeat_at       TIMESTAMPTZ,
    CONSTRAINT ck_run_status
        CHECK (status IN ('pending','running','completed','failed','cancelled')),
    -- Two ways a run starts: someone called flow.run(), or the driver fired a cron.
    CONSTRAINT ck_run_triggered_by
        CHECK (triggered_by IN ('manual','schedule'))
);

CREATE INDEX IF NOT EXISTS ix_run_workflow_name  ON {schema}.run (workflow_name);
CREATE INDEX IF NOT EXISTS ix_run_status         ON {schema}.run (status);

-- Containment indexes for the two JSONB filters the query builder offers:
-- `.metadata(**kv)` over `metadata`, `.state(**kv)` over `input`. jsonb_path_ops
-- on `input` since only `@>` is ever asked of it — a smaller index than the
-- default operator class, which also supports key-existence tests nothing uses.
CREATE INDEX IF NOT EXISTS ix_run_metadata_gin   ON {schema}.run USING GIN (metadata);
CREATE INDEX IF NOT EXISTS ix_run_input_gin      ON {schema}.run USING GIN (input jsonb_path_ops);

-- The keyset index. Both columns, in the paging order, so the row comparison
-- `(started_at, id) < (:ts, :id)` lands as an Index Cond rather than a Filter.
-- A Filter reads and discards every entry from the start of the index: correct
-- rows, degrading with page depth until production notices. Both columns are
-- NOT NULL, which is what makes the comparison safe — a row comparison
-- involving NULL evaluates to NULL and silently drops the row.
CREATE INDEX IF NOT EXISTS ix_run_started_at_id
    ON {schema}.run (started_at DESC, id DESC);

-- The stale-run reclaim scan.
CREATE INDEX IF NOT EXISTS ix_run_status_heartbeat
    ON {schema}.run (status, heartbeat_at);
