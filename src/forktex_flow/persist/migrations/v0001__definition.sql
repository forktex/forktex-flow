-- A process definition: one immutable row per (namespace, key, version).
--
-- `model` is what the engine executes; `source` is what the author wrote, kept verbatim so an editor can
-- reopen it. A redeploy of an unchanged model finds its checksum and creates nothing.
--
-- {schema} is substituted by forktex.database.SchemaMigrationRunner with a plain string replace.
-- This file and persist/models.py describe the same table and change together.

CREATE TABLE IF NOT EXISTS {schema}.definition (
    id           UUID         PRIMARY KEY,
    namespace    VARCHAR(255) NOT NULL,
    key          VARCHAR(255) NOT NULL,
    version      INTEGER      NOT NULL,
    checksum     VARCHAR(64)  NOT NULL,
    model        JSONB        NOT NULL,
    source       TEXT         NOT NULL,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    CONSTRAINT ck_definition_version_positive CHECK (version >= 1),
    CONSTRAINT uq_definition_version  UNIQUE (namespace, key, version),
    CONSTRAINT uq_definition_checksum UNIQUE (namespace, key, checksum)
);
