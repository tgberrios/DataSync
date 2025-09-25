-- =============================================
-- DataSync Database Schema Setup
-- =============================================

CREATE DATABASE IF NOT EXISTS DataLake;
-- Create metadata schema if not exists
CREATE SCHEMA IF NOT EXISTS metadata;

-- =============================================
-- CATALOG TABLE
-- =============================================

CREATE TABLE metadata.catalog
(
    schema_name       varchar not null,
    table_name        varchar not null,
    db_engine         varchar not null,
    connection_string varchar not null,
    active            boolean   default true,
    status            varchar   default 'full_load'::character varying,
    last_sync_time    timestamp,
    last_sync_column  varchar,
    last_offset       integer   default 0,
    cluster_name      varchar,
    updated_at        timestamp default now(),
    constraint catalog_new_pkey
        primary key (schema_name, table_name, db_engine)
);

-- Add comments to catalog table
COMMENT ON TABLE metadata.catalog IS 'Metadata catalog for all tables managed by DataSync system';
COMMENT ON COLUMN metadata.catalog.schema_name IS 'Database schema name';
COMMENT ON COLUMN metadata.catalog.table_name IS 'Table name';
COMMENT ON COLUMN metadata.catalog.db_engine IS 'Source database engine (PostgreSQL, MongoDB, MSSQL, MariaDB)';
COMMENT ON COLUMN metadata.catalog.connection_string IS 'Database connection string';
COMMENT ON COLUMN metadata.catalog.active IS 'Whether the table is actively synchronized';
COMMENT ON COLUMN metadata.catalog.status IS 'Current synchronization status';
COMMENT ON COLUMN metadata.catalog.last_sync_time IS 'Last successful synchronization timestamp';
COMMENT ON COLUMN metadata.catalog.last_sync_column IS 'Column used for incremental synchronization';
COMMENT ON COLUMN metadata.catalog.last_offset IS 'Last offset value for incremental sync';
COMMENT ON COLUMN metadata.catalog.cluster_name IS 'Cluster or server name';

-- Set table ownership
ALTER TABLE metadata.catalog OWNER TO "tomy.berrios";

-- Create indexes for catalog table
CREATE INDEX idx_catalog_schema_table ON metadata.catalog (schema_name, table_name);
CREATE INDEX idx_catalog_db_engine ON metadata.catalog (db_engine);
CREATE INDEX idx_catalog_status ON metadata.catalog (status);
CREATE INDEX idx_catalog_active ON metadata.catalog (active);
CREATE INDEX idx_catalog_last_sync ON metadata.catalog (last_sync_time);


-- =============================================
-- CONFIG TABLE
-- =============================================

CREATE TABLE metadata.config
(
    key         varchar(100) not null
        primary key,
    value       text         not null,
    description text,
    updated_at  timestamp default now()
);

alter table metadata.config
    owner to "tomy.berrios";


-- =============================================
-- DATA GOVERNANCE CATALOG TABLE
-- =============================================

CREATE TABLE metadata.data_governance_catalog
(
    id                       serial
        primary key,
    schema_name              varchar(100) not null,
    table_name               varchar(100) not null,
    total_columns            integer,
    total_rows               bigint,
    table_size_mb            numeric(10, 2),
    primary_key_columns      varchar(200),
    index_count              integer,
    constraint_count         integer,
    data_quality_score       numeric(10, 2),
    null_percentage          numeric(10, 2),
    duplicate_percentage     numeric(10, 2),
    inferred_source_engine   varchar(50),
    first_discovered         timestamp default now(),
    last_analyzed            timestamp,
    last_accessed            timestamp,
    access_frequency         varchar(20),
    query_count_daily        integer,
    data_category            varchar(50),
    business_domain          varchar(100),
    sensitivity_level        varchar(20),
    health_status            varchar(20),
    last_vacuum              timestamp,
    fragmentation_percentage numeric(10, 2),
    created_at               timestamp default now(),
    updated_at               timestamp default now(),
    constraint unique_table
        unique (schema_name, table_name)
);

-- Add comments to data governance catalog table
COMMENT ON TABLE metadata.data_governance_catalog IS 'Comprehensive metadata catalog for all tables in the DataLake';
COMMENT ON COLUMN metadata.data_governance_catalog.data_quality_score IS 'Overall data quality score from 0-100';
COMMENT ON COLUMN metadata.data_governance_catalog.inferred_source_engine IS 'Source database engine inferred from schema patterns';
COMMENT ON COLUMN metadata.data_governance_catalog.access_frequency IS 'Table access frequency: HIGH (>100 queries/day), MEDIUM (10-100), LOW (<10)';
COMMENT ON COLUMN metadata.data_governance_catalog.data_category IS 'Data category: TRANSACTIONAL, ANALYTICAL, REFERENCE';
COMMENT ON COLUMN metadata.data_governance_catalog.business_domain IS 'Business domain classification';
COMMENT ON COLUMN metadata.data_governance_catalog.sensitivity_level IS 'Data sensitivity: LOW, MEDIUM, HIGH';
COMMENT ON COLUMN metadata.data_governance_catalog.health_status IS 'Table health status: HEALTHY, WARNING, CRITICAL';

-- Set table ownership
ALTER TABLE metadata.data_governance_catalog OWNER TO "tomy.berrios";

-- Create indexes for data governance catalog table
CREATE INDEX idx_data_governance_schema_table ON metadata.data_governance_catalog (schema_name, table_name);
CREATE INDEX idx_data_governance_source_engine ON metadata.data_governance_catalog (inferred_source_engine);
CREATE INDEX idx_data_governance_health_status ON metadata.data_governance_catalog (health_status);
CREATE INDEX idx_data_governance_data_category ON metadata.data_governance_catalog (data_category);
CREATE INDEX idx_data_governance_business_domain ON metadata.data_governance_catalog (business_domain);


-- =============================================
-- DATA QUALITY TABLE
-- =============================================

CREATE TABLE metadata.data_quality
(
    id                           bigserial
        primary key,
    schema_name                  varchar(100)            not null,
    table_name                   varchar(100)            not null,
    source_db_engine             varchar(50)             not null,
    check_timestamp              timestamp default now() not null,
    total_rows                   bigint    default 0     not null,
    null_count                   bigint    default 0     not null,
    duplicate_count              bigint    default 0     not null,
    data_checksum                varchar(64),
    invalid_type_count           bigint    default 0     not null,
    type_mismatch_details        jsonb,
    out_of_range_count           bigint    default 0     not null,
    referential_integrity_errors bigint    default 0     not null,
    constraint_violation_count   bigint    default 0     not null,
    integrity_check_details      jsonb,
    validation_status            varchar(20)             not null
        constraint data_quality_validation_status_check
            check ((validation_status)::text = ANY
                   ((ARRAY ['PASSED'::character varying, 'FAILED'::character varying, 'WARNING'::character varying])::text[])),
    error_details                text,
    quality_score                numeric(5, 2)
        constraint data_quality_quality_score_check
            check ((quality_score >= (0)::numeric) AND (quality_score <= (100)::numeric)),
    created_at                   timestamp default now() not null,
    updated_at                   timestamp default now() not null,
    check_duration_ms            bigint    default 0     not null,
    constraint data_quality_table_check_unique
        unique (schema_name, table_name, check_timestamp)
);

-- Add comments to data quality table
COMMENT ON TABLE metadata.data_quality IS 'Stores data quality metrics and validation results for synchronized tables';

-- Set table ownership
ALTER TABLE metadata.data_quality OWNER TO "tomy.berrios";

-- Create indexes for data quality table
CREATE INDEX idx_data_quality_lookup ON metadata.data_quality (schema_name ASC, table_name ASC, check_timestamp DESC);
CREATE INDEX idx_data_quality_status ON metadata.data_quality (validation_status);



-- =============================================
-- TRANSFER METRICS TABLE
-- =============================================

CREATE TABLE metadata.transfer_metrics
(
    id                       serial
        primary key,
    schema_name              varchar(100) not null,
    table_name               varchar(100) not null,
    db_engine                varchar(50)  not null,
    records_transferred      bigint,
    bytes_transferred        bigint,
    transfer_duration_ms     integer,
    transfer_rate_per_second numeric(20, 2),
    chunk_size               integer,
    memory_used_mb           numeric(20, 2),
    cpu_usage_percent        numeric(5, 2),
    io_operations_per_second integer,
    transfer_type            varchar(20),
    status                   varchar(20),
    error_message            text,
    started_at               timestamp,
    completed_at             timestamp,
    created_at               timestamp default now(),
    created_date             date generated always as ((created_at)::date) stored,
    avg_latency_ms           numeric(20, 2),
    min_latency_ms           numeric(20, 2),
    max_latency_ms           numeric(20, 2),
    p95_latency_ms           numeric(20, 2),
    p99_latency_ms           numeric(20, 2),
    latency_samples          integer,
    constraint unique_table_metrics
        unique (schema_name, table_name, db_engine, created_date)
);

-- Add comments to transfer metrics table
COMMENT ON TABLE metadata.transfer_metrics IS 'Comprehensive metrics for data transfer operations';
COMMENT ON COLUMN metadata.transfer_metrics.records_transferred IS 'Number of records transferred in this operation';
COMMENT ON COLUMN metadata.transfer_metrics.bytes_transferred IS 'Number of bytes transferred in this operation';
COMMENT ON COLUMN metadata.transfer_metrics.transfer_duration_ms IS 'Duration of transfer operation in milliseconds';
COMMENT ON COLUMN metadata.transfer_metrics.transfer_rate_per_second IS 'Transfer rate in records per second';
COMMENT ON COLUMN metadata.transfer_metrics.chunk_size IS 'Chunk size used for transfer operation';
COMMENT ON COLUMN metadata.transfer_metrics.memory_used_mb IS 'Memory usage during transfer in MB';
COMMENT ON COLUMN metadata.transfer_metrics.cpu_usage_percent IS 'CPU usage percentage during transfer';
COMMENT ON COLUMN metadata.transfer_metrics.io_operations_per_second IS 'I/O operations per second during transfer';
COMMENT ON COLUMN metadata.transfer_metrics.transfer_type IS 'Type of transfer: FULL_LOAD, INCREMENTAL, SYNC';
COMMENT ON COLUMN metadata.transfer_metrics.status IS 'Transfer status: SUCCESS, FAILED, PARTIAL';
COMMENT ON COLUMN metadata.transfer_metrics.error_message IS 'Error message if transfer failed';
COMMENT ON COLUMN metadata.transfer_metrics.started_at IS 'When the transfer operation started';
COMMENT ON COLUMN metadata.transfer_metrics.completed_at IS 'When the transfer operation completed';

-- Set table ownership
ALTER TABLE metadata.transfer_metrics OWNER TO "tomy.berrios";

-- Create indexes for transfer metrics table
CREATE INDEX idx_transfer_metrics_schema_table ON metadata.transfer_metrics (schema_name, table_name);
CREATE INDEX idx_transfer_metrics_db_engine ON metadata.transfer_metrics (db_engine);
CREATE INDEX idx_transfer_metrics_status ON metadata.transfer_metrics (status);
CREATE INDEX idx_transfer_metrics_created_at ON metadata.transfer_metrics (created_at);
CREATE INDEX idx_transfer_metrics_transfer_type ON metadata.transfer_metrics (transfer_type);
CREATE INDEX idx_transfer_metrics_latency ON metadata.transfer_metrics (avg_latency_ms) WHERE (avg_latency_ms IS NOT NULL);

-- =============================================
-- CONFIG SYSTEM DataSync
-- =============================================

-- Insert default configuration values
INSERT INTO metadata.config (key, value, description) VALUES 
('chunk_size', '30000', 'Number of rows per chunk in synchronizations'),
('sync_interval', '5', 'Synchronization interval in seconds'),
('debug_show_timestamps', 'true', 'Show timestamps in log messages'),
('debug_show_thread_id', 'false', 'Show thread ID in log messages'),
('debug_show_file_line', 'false', 'Show file and line number in log messages'),
('pool_max_connections', '10', 'Maximum number of connections in the pool'),
('pool_min_connections', '2', 'Minimum number of connections in the pool'),
('pool_max_idle_time', '300', 'Maximum idle time for connections in seconds'),
('pool_auto_reconnect', 'true', 'Automatically reconnect failed connections'),
('debug_level', 'DEBUG', 'Debug level: DEBUG, INFO, WARN, ERROR, FATAL');

-- =============================================
-- END OF SETUP
-- =============================================
