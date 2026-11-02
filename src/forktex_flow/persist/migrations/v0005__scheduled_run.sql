-- @flow.scheduled cron state.
--
-- `enabled` is written when the row is first created and owned by the database
-- afterwards: no deploy re-arms or disarms a schedule behind an operator.

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
