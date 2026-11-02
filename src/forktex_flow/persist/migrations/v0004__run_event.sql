-- Append-only progress log, one row per event.
--
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
