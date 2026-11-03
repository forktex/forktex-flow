-- Append-only progress log, one row per event. The id is monotonic, so a reader tails it by `id > :last`.

CREATE TABLE IF NOT EXISTS {schema}.instance_event (
    id            BIGSERIAL    PRIMARY KEY,
    instance_id   UUID         NOT NULL REFERENCES {schema}.instance(id) ON DELETE CASCADE,
    ts            TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    event_type    VARCHAR(64)  NOT NULL,
    payload       JSONB        NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS ix_instance_event_instance_id ON {schema}.instance_event (instance_id, id);
