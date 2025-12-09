CREATE TABLE IF NOT EXISTS metadata.column_catalog (
    id BIGSERIAL PRIMARY KEY,
    schema_name VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    column_name VARCHAR(255) NOT NULL,
    db_engine VARCHAR(50) NOT NULL,
    connection_string TEXT NOT NULL,

    ordinal_position INTEGER NOT NULL,
    data_type VARCHAR(100) NOT NULL,
    character_maximum_length INTEGER,
    numeric_precision INTEGER,
    numeric_scale INTEGER,
    is_nullable BOOLEAN DEFAULT true,
    column_default TEXT,

    column_metadata JSONB,

    is_primary_key BOOLEAN DEFAULT false,
    is_foreign_key BOOLEAN DEFAULT false,
    is_unique BOOLEAN DEFAULT false,
    is_indexed BOOLEAN DEFAULT false,
    is_auto_increment BOOLEAN DEFAULT false,
    is_generated BOOLEAN DEFAULT false,

    null_count BIGINT,
    null_percentage NUMERIC(5,2),
    distinct_count BIGINT,
    distinct_percentage NUMERIC(5,2),
    min_value TEXT,
    max_value TEXT,
    avg_value NUMERIC,

    data_category VARCHAR(50),
    sensitivity_level VARCHAR(20),
    contains_pii BOOLEAN DEFAULT false,
    contains_phi BOOLEAN DEFAULT false,

    first_seen_at TIMESTAMP DEFAULT NOW(),
    last_seen_at TIMESTAMP DEFAULT NOW(),
    last_analyzed_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),

    CONSTRAINT uq_column_catalog UNIQUE (schema_name, table_name, column_name, db_engine, connection_string)
);

CREATE INDEX IF NOT EXISTS idx_column_catalog_table ON metadata.column_catalog(schema_name, table_name, db_engine);
CREATE INDEX IF NOT EXISTS idx_column_catalog_engine ON metadata.column_catalog(db_engine);
CREATE INDEX IF NOT EXISTS idx_column_catalog_pii ON metadata.column_catalog(contains_pii) WHERE contains_pii = true;
CREATE INDEX IF NOT EXISTS idx_column_catalog_data_type ON metadata.column_catalog(data_type);
CREATE INDEX IF NOT EXISTS idx_column_catalog_schema_table ON metadata.column_catalog(schema_name, table_name);

COMMENT ON TABLE metadata.column_catalog IS 'Catalog of all columns from all database sources with metadata, statistics, and classification';
COMMENT ON COLUMN metadata.column_catalog.column_metadata IS 'JSONB field containing engine-specific metadata and extended information';
COMMENT ON COLUMN metadata.column_catalog.contains_pii IS 'Indicates if column contains Personally Identifiable Information';
COMMENT ON COLUMN metadata.column_catalog.contains_phi IS 'Indicates if column contains Protected Health Information';

