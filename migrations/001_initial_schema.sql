-- 001_initial_schema.sql
-- Internal schema for goalfy-data-connect-sql

-- =============================================
-- Source configurations
-- =============================================
CREATE TABLE IF NOT EXISTS data_sources (
    tenant_id       TEXT        NOT NULL,
    source_id       TEXT        NOT NULL,
    connector_type  TEXT        NOT NULL,  -- postgres | mysql | sqlserver
    host            TEXT        NOT NULL,
    port            INTEGER     NOT NULL,
    database_name   TEXT        NOT NULL,
    schema_name     TEXT,
    username        TEXT        NOT NULL,
    password_ref    TEXT        NOT NULL,  -- reference to secret, never plain text
    ssl_mode        TEXT        NOT NULL DEFAULT 'disable',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, source_id)
);

-- =============================================
-- Dataset configurations
-- =============================================
CREATE TABLE IF NOT EXISTS data_datasets (
    tenant_id           TEXT        NOT NULL,
    source_id           TEXT        NOT NULL,
    dataset_id          TEXT        NOT NULL,
    schema_name         TEXT,
    table_name          TEXT        NOT NULL,
    object_type         TEXT        NOT NULL DEFAULT 'table',  -- table | view
    sync_mode           TEXT        NOT NULL DEFAULT 'full',   -- full | incremental
    incremental_column  TEXT,
    incremental_type    TEXT,       -- date | key
    technical_key       TEXT,
    batch_size          INTEGER     NOT NULL DEFAULT 1000,
    enabled             BOOLEAN     NOT NULL DEFAULT true,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, source_id, dataset_id),
    FOREIGN KEY (tenant_id, source_id) REFERENCES data_sources(tenant_id, source_id)
);

-- =============================================
-- Sync execution tracking
-- =============================================
CREATE TABLE IF NOT EXISTS sync_executions (
    execution_id    TEXT        NOT NULL PRIMARY KEY,
    tenant_id       TEXT        NOT NULL,
    source_id       TEXT        NOT NULL,
    dataset_id      TEXT        NOT NULL,
    connector_type  TEXT        NOT NULL,
    trigger_type    TEXT        NOT NULL,
    status          TEXT        NOT NULL DEFAULT 'pending',
    total_records   INTEGER     NOT NULL DEFAULT 0,
    total_batches   INTEGER     NOT NULL DEFAULT 0,
    duration_ms     BIGINT      NOT NULL DEFAULT 0,
    error_message   TEXT,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at    TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_sync_executions_tenant
    ON sync_executions (tenant_id, source_id, dataset_id, started_at DESC);

-- =============================================
-- Incremental checkpoints
-- =============================================
CREATE TABLE IF NOT EXISTS sync_checkpoints (
    tenant_id   TEXT        NOT NULL,
    source_id   TEXT        NOT NULL,
    dataset_id  TEXT        NOT NULL,
    last_value  TEXT        NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, source_id, dataset_id)
);

-- =============================================
-- Raw ingestion staging
-- =============================================
CREATE TABLE IF NOT EXISTS raw_ingestion (
    tenant_id       TEXT        NOT NULL,
    source_id       TEXT        NOT NULL,
    dataset_id      TEXT        NOT NULL,
    execution_id    TEXT        NOT NULL,
    record_key      TEXT        NOT NULL,
    payload         JSONB       NOT NULL,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, source_id, dataset_id, record_key)
);

CREATE INDEX IF NOT EXISTS idx_raw_ingestion_execution
    ON raw_ingestion (execution_id);

-- =============================================
-- Normalized output layer
-- =============================================
CREATE TABLE IF NOT EXISTS normalized (
    tenant_id       TEXT        NOT NULL,
    source_id       TEXT        NOT NULL,
    dataset_id      TEXT        NOT NULL,
    execution_id    TEXT        NOT NULL,
    record_key      TEXT        NOT NULL,
    payload         JSONB       NOT NULL,
    normalized_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, source_id, dataset_id, record_key)
);

CREATE INDEX IF NOT EXISTS idx_normalized_execution
    ON normalized (execution_id);

CREATE INDEX IF NOT EXISTS idx_normalized_tenant_dataset
    ON normalized (tenant_id, source_id, dataset_id, normalized_at DESC);
