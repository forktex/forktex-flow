-- One execution of one definition version.
--
-- `input` is what the instance started with and never changes; `variables` is what its elements have
-- written since. `parent_*` links a call activity's child to the element waiting on it.

CREATE TABLE IF NOT EXISTS {schema}.instance (
    id                  UUID         PRIMARY KEY,
    namespace           VARCHAR(255) NOT NULL,
    key                 VARCHAR(255) NOT NULL,
    version             INTEGER      NOT NULL,
    definition_id       UUID         NOT NULL REFERENCES {schema}.definition(id),
    status              VARCHAR(16)  NOT NULL,
    input               JSONB        NOT NULL DEFAULT '{}'::jsonb,
    variables           JSONB        NOT NULL DEFAULT '{}'::jsonb,
    metadata            JSONB        NOT NULL DEFAULT '{}'::jsonb,
    error               TEXT,
    cancel_reason       TEXT,
    triggered_by        VARCHAR(16)  NOT NULL DEFAULT 'manual',
    parent_instance_id  UUID         REFERENCES {schema}.instance(id) ON DELETE CASCADE,
    parent_element_id   VARCHAR(255),
    started_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    finished_at         TIMESTAMPTZ,
    CONSTRAINT ck_instance_status CHECK (status IN ('active','completed','failed','cancelled')),
    CONSTRAINT ck_instance_triggered_by CHECK (triggered_by IN ('manual','schedule','call')),
    CONSTRAINT ck_instance_parent CHECK ((parent_instance_id IS NULL) = (parent_element_id IS NULL))
);

CREATE INDEX IF NOT EXISTS ix_instance_namespace_key ON {schema}.instance (namespace, key);
CREATE INDEX IF NOT EXISTS ix_instance_status        ON {schema}.instance (status);
CREATE INDEX IF NOT EXISTS ix_instance_parent        ON {schema}.instance (parent_instance_id);
CREATE INDEX IF NOT EXISTS ix_instance_metadata_gin  ON {schema}.instance USING GIN (metadata);
CREATE INDEX IF NOT EXISTS ix_instance_variables_gin ON {schema}.instance USING GIN (variables jsonb_path_ops);

-- The keyset index: both columns NOT NULL, in paging order, so `(started_at, id) < (...)` is an Index Cond.
CREATE INDEX IF NOT EXISTS ix_instance_started_at_id ON {schema}.instance (started_at DESC, id DESC);
