-- One row per durable step invocation: the memoisation table.
--
-- `skipped` and `status` are orthogonal facts, so they are separate columns
-- and a CHECK holds the pair to the states the executor can interpret. Carried
-- in the output payload instead, the marker collides with a value a step body
-- may legitimately return.

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
    skipped            BOOLEAN      NOT NULL DEFAULT FALSE,
    CONSTRAINT ck_step_run_status
        CHECK (status IN ('pending','running','completed','failed','cancelled')),
    CONSTRAINT ck_step_run_attempts_non_negative CHECK (attempts >= 0),
    CONSTRAINT ck_step_run_max_attempts_positive CHECK (max_attempts >= 1),
    CONSTRAINT ck_step_run_skipped_is_completed  CHECK (NOT skipped OR status = 'completed'),
    -- The memoisation key: a completed row here is what makes a resumed run
    -- skip a step instead of repeating its side effects. Uniqueness has to hold
    -- under concurrency, which only the database guarantees.
    CONSTRAINT uq_step_run_identity UNIQUE (run_id, step_qualname, args_hash)
);

CREATE INDEX IF NOT EXISTS ix_step_run_status_heartbeat
    ON {schema}.step_run (status, heartbeat_at);
CREATE INDEX IF NOT EXISTS ix_step_run_run_id_index
    ON {schema}.step_run (run_id, step_index);
