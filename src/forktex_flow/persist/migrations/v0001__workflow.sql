-- The workflow registry: one row per (name, version) a process declares.
--
-- First of the baseline, and the only table nothing references, so it has to
-- exist before `run` and `scheduled_run` can point at it. The runner applies
-- files in numeric order, which is what makes that dependency hold.
--
-- {schema} is substituted by forktex.database.SchemaMigrationRunner with a
-- plain string replace, not str.format, so literal `{`/`}` in a JSONB default
-- is left untouched.
--
-- This file and persist/models.py describe the same table and change together.
-- This one is what a deployed database actually has.

CREATE TABLE IF NOT EXISTS {schema}.workflow (
    name             VARCHAR(255) NOT NULL,
    version          INTEGER      NOT NULL,
    ast_hash         VARCHAR(64),
    registered_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (name, version)
);
