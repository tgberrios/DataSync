-- ============================================================================
-- DATASYNC METADATA SCHEMA SETUP
-- ============================================================================
-- This script creates the complete metadata schema for the DataSync system
-- Version: 1.0
-- Created: 2024
-- Description: Core tables for managing data synchronization across multiple databases
-- ============================================================================

-- ============================================================================
-- 1. CONFIGURATION TABLE
-- ============================================================================
-- Stores system-wide configuration parameters
-- ============================================================================

CREATE TABLE metadata.config (
    key         VARCHAR(100) NOT NULL PRIMARY KEY,
    value       TEXT         NOT NULL,
    description TEXT,
    updated_at  TIMESTAMP    DEFAULT NOW()
);

-- Set table owner
ALTER TABLE metadata.config OWNER TO "tomy.berrios";

-- Add comments for documentation
COMMENT ON TABLE metadata.config IS 'System-wide configuration parameters for DataSync';
COMMENT ON COLUMN metadata.config.key IS 'Configuration parameter key (unique identifier)';
COMMENT ON COLUMN metadata.config.value IS 'Configuration parameter value';
COMMENT ON COLUMN metadata.config.description IS 'Human-readable description of the parameter';
COMMENT ON COLUMN metadata.config.updated_at IS 'Timestamp when the parameter was last updated';

-- ============================================================================
-- 2. CATALOG TABLE (CORE)
-- ============================================================================
-- Central catalog managing all synchronized tables across different databases
-- This is the heart of the DataSync system
-- ============================================================================

CREATE TABLE metadata.catalog (
    -- Core identification fields
    schema_name       VARCHAR NOT NULL,
    table_name        VARCHAR NOT NULL,
    db_engine         VARCHAR NOT NULL,
    connection_string VARCHAR NOT NULL,
    
    -- Synchronization control
    active            BOOLEAN     DEFAULT TRUE,
    status            VARCHAR     DEFAULT 'full_load'::CHARACTER VARYING,
    
    -- Synchronization tracking
    last_sync_time    TIMESTAMP,
    last_sync_column  VARCHAR,
    last_offset       INTEGER     DEFAULT 0,
    
    -- Additional metadata
    cluster_name      VARCHAR,
    updated_at        TIMESTAMP   DEFAULT NOW(),
    
    -- Primary key management
    pk_columns        TEXT,
    last_processed_pk TEXT,
    pk_strategy       VARCHAR(50) DEFAULT 'OFFSET'::CHARACTER VARYING,
    has_pk            BOOLEAN     DEFAULT FALSE,
    candidate_columns TEXT,
    
    -- Performance optimization
    table_size        BIGINT      DEFAULT 0,
    
    -- Primary key constraint
    CONSTRAINT catalog_pkey PRIMARY KEY (schema_name, table_name, db_engine)
);

-- Set table owner
ALTER TABLE metadata.catalog OWNER TO "tomy.berrios";

-- Comprehensive documentation
COMMENT ON TABLE metadata.catalog IS 'Central catalog for all tables managed by DataSync system';
COMMENT ON COLUMN metadata.catalog.schema_name IS 'Database schema name';
COMMENT ON COLUMN metadata.catalog.table_name IS 'Table name';
COMMENT ON COLUMN metadata.catalog.db_engine IS 'Source database engine (PostgreSQL, MongoDB, MSSQL, MariaDB)';
COMMENT ON COLUMN metadata.catalog.connection_string IS 'Database connection string for source database';
COMMENT ON COLUMN metadata.catalog.active IS 'Whether the table is actively synchronized';
COMMENT ON COLUMN metadata.catalog.status IS 'Current synchronization status: FULL_LOAD, LISTENING_CHANGES, PERFECT_MATCH, NO_DATA, ERROR';
COMMENT ON COLUMN metadata.catalog.last_sync_time IS 'Last successful synchronization timestamp';
COMMENT ON COLUMN metadata.catalog.last_sync_column IS 'Column used for incremental synchronization (usually timestamp column)';
COMMENT ON COLUMN metadata.catalog.last_offset IS 'Last offset value for incremental sync (row count processed)';
COMMENT ON COLUMN metadata.catalog.cluster_name IS 'Cluster or server name for grouping related tables';
COMMENT ON COLUMN metadata.catalog.pk_columns IS 'JSON array of primary key column names: ["id", "created_at"]';
COMMENT ON COLUMN metadata.catalog.last_processed_pk IS 'Last processed primary key value for cursor-based pagination';
COMMENT ON COLUMN metadata.catalog.pk_strategy IS 'Pagination strategy: PK, TEMPORAL_PK, ROWID, OFFSET';
COMMENT ON COLUMN metadata.catalog.has_pk IS 'Whether table has a real primary key';
COMMENT ON COLUMN metadata.catalog.candidate_columns IS 'JSON array of candidate columns for cursor-based pagination';
COMMENT ON COLUMN metadata.catalog.table_size IS 'Approximate number of records in the table for size-based ordering';

-- ============================================================================
-- CATALOG INDEXES
-- ============================================================================
-- Performance indexes for common query patterns
-- ============================================================================

-- Core lookup indexes
CREATE INDEX idx_catalog_schema_table ON metadata.catalog (schema_name, table_name);
CREATE INDEX idx_catalog_db_engine ON metadata.catalog (db_engine);

-- Status and activity indexes
CREATE INDEX idx_catalog_status ON metadata.catalog (status);
CREATE INDEX idx_catalog_active ON metadata.catalog (active);

-- Synchronization tracking indexes
CREATE INDEX idx_catalog_last_sync ON metadata.catalog (last_sync_time);
CREATE INDEX idx_catalog_last_processed_pk ON metadata.catalog (last_processed_pk);

-- Primary key strategy indexes
CREATE INDEX idx_catalog_pk_strategy ON metadata.catalog (pk_strategy);
CREATE INDEX idx_catalog_has_pk ON metadata.catalog (has_pk);

-- Performance optimization indexes
CREATE INDEX idx_catalog_table_size ON metadata.catalog (table_size);

-- ============================================================================
-- 3. DATA GOVERNANCE CATALOG TABLE
-- ============================================================================
-- Comprehensive metadata catalog for all tables in the DataLake
-- Provides governance, quality metrics, and business context
-- ============================================================================

CREATE TABLE metadata.data_governance_catalog (
    -- Primary key
    id                       SERIAL PRIMARY KEY,
    
    -- Core identification
    schema_name              VARCHAR(100) NOT NULL,
    table_name               VARCHAR(100) NOT NULL,
    
    -- Structural metadata
    total_columns            INTEGER,
    total_rows               BIGINT,
    table_size_mb            NUMERIC(10, 2),
    primary_key_columns      VARCHAR(200),
    index_count              INTEGER,
    constraint_count         INTEGER,
    
    -- Data quality metrics
    data_quality_score       NUMERIC(10, 2),
    null_percentage          NUMERIC(10, 2),
    duplicate_percentage     NUMERIC(10, 2),
    
    -- Source and lineage
    inferred_source_engine   VARCHAR(50),
    first_discovered         TIMESTAMP DEFAULT NOW(),
    last_analyzed            TIMESTAMP,
    last_accessed            TIMESTAMP,
    
    -- Usage analytics
    access_frequency         VARCHAR(20),
    query_count_daily        INTEGER,
    
    -- Business context
    data_category            VARCHAR(50),
    business_domain          VARCHAR(100),
    sensitivity_level        VARCHAR(20),
    
    -- Health and maintenance
    health_status            VARCHAR(20),
    last_vacuum              TIMESTAMP,
    fragmentation_percentage NUMERIC(10, 2),
    
    -- Audit timestamps
    created_at               TIMESTAMP DEFAULT NOW(),
    updated_at               TIMESTAMP DEFAULT NOW(),
    
    -- Unique constraint
    CONSTRAINT unique_table UNIQUE (schema_name, table_name)
);

-- Set table owner
ALTER TABLE metadata.data_governance_catalog OWNER TO "tomy.berrios";

-- Comprehensive documentation
COMMENT ON TABLE metadata.data_governance_catalog IS 'Comprehensive metadata catalog for all tables in the DataLake';
COMMENT ON COLUMN metadata.data_governance_catalog.id IS 'Unique identifier for the governance record';
COMMENT ON COLUMN metadata.data_governance_catalog.schema_name IS 'Database schema name';
COMMENT ON COLUMN metadata.data_governance_catalog.table_name IS 'Table name';
COMMENT ON COLUMN metadata.data_governance_catalog.total_columns IS 'Total number of columns in the table';
COMMENT ON COLUMN metadata.data_governance_catalog.total_rows IS 'Total number of rows in the table';
COMMENT ON COLUMN metadata.data_governance_catalog.table_size_mb IS 'Table size in megabytes';
COMMENT ON COLUMN metadata.data_governance_catalog.primary_key_columns IS 'Comma-separated list of primary key columns';
COMMENT ON COLUMN metadata.data_governance_catalog.index_count IS 'Number of indexes on the table';
COMMENT ON COLUMN metadata.data_governance_catalog.constraint_count IS 'Number of constraints on the table';
COMMENT ON COLUMN metadata.data_governance_catalog.data_quality_score IS 'Overall data quality score from 0-100';
COMMENT ON COLUMN metadata.data_governance_catalog.null_percentage IS 'Percentage of NULL values in the table';
COMMENT ON COLUMN metadata.data_governance_catalog.duplicate_percentage IS 'Percentage of duplicate rows in the table';
COMMENT ON COLUMN metadata.data_governance_catalog.inferred_source_engine IS 'Source database engine inferred from schema patterns';
COMMENT ON COLUMN metadata.data_governance_catalog.first_discovered IS 'When the table was first discovered by the system';
COMMENT ON COLUMN metadata.data_governance_catalog.last_analyzed IS 'Last time the table was analyzed for metadata';
COMMENT ON COLUMN metadata.data_governance_catalog.last_accessed IS 'Last time the table was accessed';
COMMENT ON COLUMN metadata.data_governance_catalog.access_frequency IS 'Table access frequency: HIGH (>100 queries/day), MEDIUM (10-100), LOW (<10)';
COMMENT ON COLUMN metadata.data_governance_catalog.query_count_daily IS 'Average number of queries per day';
COMMENT ON COLUMN metadata.data_governance_catalog.data_category IS 'Data category: TRANSACTIONAL, ANALYTICAL, REFERENCE';
COMMENT ON COLUMN metadata.data_governance_catalog.business_domain IS 'Business domain classification';
COMMENT ON COLUMN metadata.data_governance_catalog.sensitivity_level IS 'Data sensitivity: LOW, MEDIUM, HIGH';
COMMENT ON COLUMN metadata.data_governance_catalog.health_status IS 'Table health status: HEALTHY, WARNING, CRITICAL';
COMMENT ON COLUMN metadata.data_governance_catalog.last_vacuum IS 'Last time the table was vacuumed';
COMMENT ON COLUMN metadata.data_governance_catalog.fragmentation_percentage IS 'Table fragmentation percentage';

-- ============================================================================
-- DATA GOVERNANCE INDEXES
-- ============================================================================
-- Performance indexes for governance queries
-- ============================================================================

-- Core lookup indexes
CREATE INDEX idx_data_governance_schema_table ON metadata.data_governance_catalog (schema_name, table_name);
CREATE INDEX idx_data_governance_source_engine ON metadata.data_governance_catalog (inferred_source_engine);

-- Health and status indexes
CREATE INDEX idx_data_governance_health_status ON metadata.data_governance_catalog (health_status);

-- Business context indexes
CREATE INDEX idx_data_governance_data_category ON metadata.data_governance_catalog (data_category);
CREATE INDEX idx_data_governance_business_domain ON metadata.data_governance_catalog (business_domain);

-- ============================================================================
-- 4. TRANSFER METRICS TABLE
-- ============================================================================
-- Real-time metrics for data transfer operations
-- Tracks performance, throughput, and success rates
-- ============================================================================

CREATE TABLE metadata.transfer_metrics (
    -- Primary key
    id                       SERIAL PRIMARY KEY,
    
    -- Core identification
    schema_name              VARCHAR(100) NOT NULL,
    table_name               VARCHAR(100) NOT NULL,
    db_engine                VARCHAR(50)  NOT NULL,
    
    -- Transfer metrics
    records_transferred      BIGINT,
    bytes_transferred        BIGINT,
    memory_used_mb           NUMERIC(20, 2),
    io_operations_per_second INTEGER,
    
    -- Transfer metadata
    transfer_type            VARCHAR(20),
    status                   VARCHAR(20),
    error_message            TEXT,
    
    -- Timing information
    started_at               TIMESTAMP,
    completed_at             TIMESTAMP,
    created_at               TIMESTAMP DEFAULT NOW(),
    
    -- Generated column for partitioning
    created_date             DATE GENERATED ALWAYS AS (created_at::DATE) STORED,
    
    -- Unique constraint to prevent duplicate metrics per day
    CONSTRAINT unique_table_metrics UNIQUE (schema_name, table_name, db_engine, created_date)
);

-- Set table owner
ALTER TABLE metadata.transfer_metrics OWNER TO "tomy.berrios";

-- Comprehensive documentation
COMMENT ON TABLE metadata.transfer_metrics IS 'Real-time metrics for data transfer operations';
COMMENT ON COLUMN metadata.transfer_metrics.id IS 'Unique identifier for the metrics record';
COMMENT ON COLUMN metadata.transfer_metrics.schema_name IS 'Database schema name';
COMMENT ON COLUMN metadata.transfer_metrics.table_name IS 'Table name';
COMMENT ON COLUMN metadata.transfer_metrics.db_engine IS 'Source database engine';
COMMENT ON COLUMN metadata.transfer_metrics.records_transferred IS 'Number of records transferred';
COMMENT ON COLUMN metadata.transfer_metrics.bytes_transferred IS 'Total bytes transferred';
COMMENT ON COLUMN metadata.transfer_metrics.memory_used_mb IS 'Memory usage during transfer in MB';
COMMENT ON COLUMN metadata.transfer_metrics.io_operations_per_second IS 'IO operations per second during transfer';
COMMENT ON COLUMN metadata.transfer_metrics.transfer_type IS 'Type of transfer: FULL_LOAD, INCREMENTAL, SYNC';
COMMENT ON COLUMN metadata.transfer_metrics.status IS 'Transfer status: SUCCESS, FAILED, PENDING';
COMMENT ON COLUMN metadata.transfer_metrics.error_message IS 'Error message if transfer failed';
COMMENT ON COLUMN metadata.transfer_metrics.started_at IS 'When the transfer operation started';
COMMENT ON COLUMN metadata.transfer_metrics.completed_at IS 'When the transfer operation completed';
COMMENT ON COLUMN metadata.transfer_metrics.created_at IS 'When the metrics record was created';
COMMENT ON COLUMN metadata.transfer_metrics.created_date IS 'Generated date column for partitioning';

-- ============================================================================
-- TRANSFER METRICS INDEXES
-- ============================================================================
-- Performance indexes for metrics queries and reporting
-- ============================================================================

-- Core lookup indexes
CREATE INDEX idx_transfer_metrics_schema_table ON metadata.transfer_metrics (schema_name, table_name);
CREATE INDEX idx_transfer_metrics_db_engine ON metadata.transfer_metrics (db_engine);

-- Status and type indexes
CREATE INDEX idx_transfer_metrics_status ON metadata.transfer_metrics (status);
CREATE INDEX idx_transfer_metrics_transfer_type ON metadata.transfer_metrics (transfer_type);

-- Time-based indexes for reporting
CREATE INDEX idx_transfer_metrics_created_at ON metadata.transfer_metrics (created_at);

-- ============================================================================
-- 5. DATA QUALITY TABLE
-- ============================================================================
-- Comprehensive data quality metrics and validation results
-- Tracks data integrity, consistency, and quality scores over time
-- ============================================================================

CREATE TABLE metadata.data_quality (
    -- Primary key
    id                           BIGSERIAL PRIMARY KEY,
    
    -- Core identification
    schema_name                  VARCHAR(100)            NOT NULL,
    table_name                   VARCHAR(100)            NOT NULL,
    source_db_engine             VARCHAR(50)             NOT NULL,
    check_timestamp              TIMESTAMP DEFAULT NOW() NOT NULL,
    
    -- Row-level metrics
    total_rows                   BIGINT    DEFAULT 0     NOT NULL,
    null_count                   BIGINT    DEFAULT 0     NOT NULL,
    duplicate_count              BIGINT    DEFAULT 0     NOT NULL,
    
    -- Data integrity metrics
    data_checksum                VARCHAR(64),
    invalid_type_count           BIGINT    DEFAULT 0     NOT NULL,
    type_mismatch_details        JSONB,
    out_of_range_count           BIGINT    DEFAULT 0     NOT NULL,
    
    -- Constraint and referential integrity
    referential_integrity_errors BIGINT    DEFAULT 0     NOT NULL,
    constraint_violation_count   BIGINT    DEFAULT 0     NOT NULL,
    integrity_check_details      JSONB,
    
    -- Quality assessment
    validation_status            VARCHAR(20)             NOT NULL
        CONSTRAINT data_quality_validation_status_check
            CHECK (validation_status IN ('PASSED', 'FAILED', 'WARNING')),
    error_details                TEXT,
    quality_score                NUMERIC(5, 2)
        CONSTRAINT data_quality_quality_score_check
            CHECK (quality_score >= 0 AND quality_score <= 100),
    
    -- Audit timestamps
    created_at                   TIMESTAMP DEFAULT NOW() NOT NULL,
    updated_at                   TIMESTAMP DEFAULT NOW() NOT NULL,
    check_duration_ms            BIGINT    DEFAULT 0     NOT NULL,
    
    -- Unique constraint to prevent duplicate quality checks
    CONSTRAINT data_quality_table_check_unique UNIQUE (schema_name, table_name, check_timestamp)
);

-- Set table owner
ALTER TABLE metadata.data_quality OWNER TO "tomy.berrios";

-- Comprehensive documentation
COMMENT ON TABLE metadata.data_quality IS 'Comprehensive data quality metrics and validation results for synchronized tables';
COMMENT ON COLUMN metadata.data_quality.id IS 'Unique identifier for the quality check record';
COMMENT ON COLUMN metadata.data_quality.schema_name IS 'Database schema name';
COMMENT ON COLUMN metadata.data_quality.table_name IS 'Table name';
COMMENT ON COLUMN metadata.data_quality.source_db_engine IS 'Source database engine';
COMMENT ON COLUMN metadata.data_quality.check_timestamp IS 'When the quality check was performed';
COMMENT ON COLUMN metadata.data_quality.total_rows IS 'Total number of rows in the table';
COMMENT ON COLUMN metadata.data_quality.null_count IS 'Number of NULL values found';
COMMENT ON COLUMN metadata.data_quality.duplicate_count IS 'Number of duplicate rows found';
COMMENT ON COLUMN metadata.data_quality.data_checksum IS 'MD5 checksum of table data for integrity verification';
COMMENT ON COLUMN metadata.data_quality.invalid_type_count IS 'Number of values with invalid data types';
COMMENT ON COLUMN metadata.data_quality.type_mismatch_details IS 'JSON details of type mismatches found';
COMMENT ON COLUMN metadata.data_quality.out_of_range_count IS 'Number of values outside expected ranges';
COMMENT ON COLUMN metadata.data_quality.referential_integrity_errors IS 'Number of referential integrity violations';
COMMENT ON COLUMN metadata.data_quality.constraint_violation_count IS 'Number of constraint violations';
COMMENT ON COLUMN metadata.data_quality.integrity_check_details IS 'JSON details of integrity check results';
COMMENT ON COLUMN metadata.data_quality.validation_status IS 'Overall validation status: PASSED, FAILED, WARNING';
COMMENT ON COLUMN metadata.data_quality.error_details IS 'Detailed error information if validation failed';
COMMENT ON COLUMN metadata.data_quality.quality_score IS 'Overall data quality score (0-100)';
COMMENT ON COLUMN metadata.data_quality.created_at IS 'When the quality record was created';
COMMENT ON COLUMN metadata.data_quality.updated_at IS 'When the quality record was last updated';
COMMENT ON COLUMN metadata.data_quality.check_duration_ms IS 'Duration of the quality check in milliseconds';

-- ============================================================================
-- DATA QUALITY INDEXES
-- ============================================================================
-- Performance indexes for quality queries and reporting
-- ============================================================================

-- Core lookup index (optimized for time-series queries)
CREATE INDEX idx_data_quality_lookup ON metadata.data_quality (schema_name ASC, table_name ASC, check_timestamp DESC);

-- Status index for filtering by validation results
CREATE INDEX idx_data_quality_status ON metadata.data_quality (validation_status);

-- ============================================================================
-- SCHEMA COMPLETION SUMMARY
-- ============================================================================
-- 
-- This schema setup creates a comprehensive metadata management system for DataSync:
-- 
-- 1. metadata.config          - System configuration parameters
-- 2. metadata.catalog         - Core table synchronization management
-- 3. metadata.data_governance_catalog - Business context and governance
-- 4. metadata.transfer_metrics - Performance and throughput metrics
-- 5. metadata.data_quality    - Data integrity and quality metrics
-- 
-- All tables include:
-- - Comprehensive indexing for performance
-- - Detailed documentation and comments
-- - Proper constraints and data validation
-- - Audit trails with timestamps
-- - Ownership properly set
-- 
-- The schema supports:
-- - Multi-database synchronization (PostgreSQL, MongoDB, MSSQL, MariaDB)
-- - Real-time performance monitoring
-- - Data quality assessment
-- - Business governance and compliance
-- - Disaster recovery and consistency validation
-- 
-- ============================================================================

-- RESET ALL TABLES TO FULL_LOAD
UPDATE metadata.catalog
SET status = 'FULL_LOAD', last_offset = 0
where active = true

-- Discovery Tables

--MSQL
INSERT INTO metadata.catalog
(schema_name, table_name, cluster_name, db_engine, connection_string, last_sync_time, last_sync_column, status, last_offset, active)
VALUES ('dbo', 'RegistrosPrueba', '', 'MSSQL', 'DRIVER={ODBC Driver 18 for SQL Server};SERVER=10.12.240.66;DATABASE=PruebaDB1;UID=Datalake_User;PWD=keepprofessional;TrustServerCertificate=yes;', NOW(), '', 'FULL_LOAD', '0', true);


-- MariaDB Test Table
INSERT INTO metadata.catalog
(schema_name, table_name, cluster_name, db_engine, connection_string, last_sync_time, last_sync_column, status, last_offset, active)
VALUES ('SportBook', 'productos', '', 'MariaDB', 'host=localhost;user=root;password=Yucaquemada1;db=SportBook', NOW(), 'fecha_creacion', 'FULL_LOAD', '0', true);

-- PostgreSQL Test Table
INSERT INTO metadata.catalog
(schema_name, table_name, cluster_name, db_engine, connection_string, last_sync_time, last_sync_column, status, last_offset, active)
VALUES ('test_schema', 'test_table', '', 'PostgreSQL', 'host=localhost user=tomy.berrios password=Yucaquemada1 dbname=postgres', NOW(), 'created_at', 'FULL_LOAD', '0', true);

-- MongoDB Test Table
INSERT INTO metadata.catalog
(schema_name, table_name, cluster_name, db_engine, connection_string, last_sync_time, last_sync_column, status, last_offset, active)
VALUES ('mydatabase', 'usuarios', '', 'MongoDB', 'mongodb://localhost:27017', NOW(), '', 'FULL_LOAD', '0', true);

-- MSSQL Test Table
INSERT INTO metadata.catalog
(schema_name, table_name, cluster_name, db_engine, connection_string, last_sync_time, last_sync_column, status, last_offset, active)
VALUES ('dbo', 'customers', '', 'MSSQL', 'DRIVER={ODBC Driver 18 for SQL Server};SERVER=localhost;DATABASE=master;UID=sa;PWD=Yucaquemada1;TrustServerCertificate=yes;', NOW(), 'created_at', 'FULL_LOAD', '0', true);

