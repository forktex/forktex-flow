-- One element of one instance. Definitions are acyclic, so an element runs at most once per instance and
-- (instance_id, element_id) is unique: a completed row is what a resume never repeats.
--
-- A row with a `task_type` is a job: workers claim pending ones by type with FOR UPDATE SKIP LOCKED. `input` is
-- what an ad-hoc decider handed one activity, laid over the instance's variables when its handler runs.

CREATE TABLE IF NOT EXISTS {schema}.element_run (
    id                 UUID         PRIMARY KEY,
    instance_id        UUID         NOT NULL REFERENCES {schema}.instance(id) ON DELETE CASCADE,
    element_id         VARCHAR(255) NOT NULL,
    element_type       VARCHAR(32)  NOT NULL,
    task_type          VARCHAR(255),
    status             VARCHAR(16)  NOT NULL,
    output             JSONB,
    error              TEXT,
    attempts           INTEGER      NOT NULL DEFAULT 0,
    max_attempts       INTEGER      NOT NULL DEFAULT 1,
    next_attempt_at    TIMESTAMPTZ,
    started_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    finished_at        TIMESTAMPTZ,
    heartbeat_at       TIMESTAMPTZ,
    child_instance_id  UUID         REFERENCES {schema}.instance(id) ON DELETE SET NULL,
    input              JSONB,
    CONSTRAINT ck_element_run_status
        CHECK (status IN ('pending','running','waiting','completed','skipped','failed','cancelled')),
    CONSTRAINT ck_element_run_attempts CHECK (attempts >= 0 AND max_attempts >= 1),
    CONSTRAINT uq_element_run_identity UNIQUE (instance_id, element_id)
);

-- The claim scan and the stale-job scan.
CREATE INDEX IF NOT EXISTS ix_element_run_job
    ON {schema}.element_run (task_type, next_attempt_at) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS ix_element_run_running
    ON {schema}.element_run (heartbeat_at) WHERE status = 'running';
