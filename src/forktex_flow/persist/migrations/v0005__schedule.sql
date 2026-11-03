-- A cron on a definition key. It starts the key's latest version.
--
-- `enabled` is written when the row is first created and owned by the database afterwards: no deploy
-- re-arms or disarms a schedule behind an operator.

CREATE TABLE IF NOT EXISTS {schema}.schedule (
    namespace       VARCHAR(255) NOT NULL,
    key             VARCHAR(255) NOT NULL,
    cron            VARCHAR(128) NOT NULL,
    enabled         BOOLEAN      NOT NULL DEFAULT TRUE,
    last_fired_at   TIMESTAMPTZ,
    next_fire_at    TIMESTAMPTZ  NOT NULL,
    PRIMARY KEY (namespace, key)
);

CREATE INDEX IF NOT EXISTS ix_schedule_due ON {schema}.schedule (next_fire_at) WHERE enabled;
