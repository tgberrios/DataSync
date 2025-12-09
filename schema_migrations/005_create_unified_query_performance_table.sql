CREATE TABLE IF NOT EXISTS metadata.query_performance (
    id BIGSERIAL PRIMARY KEY,
    captured_at TIMESTAMP NOT NULL DEFAULT NOW(),
    
    source_type TEXT NOT NULL CHECK (source_type IN ('snapshot', 'activity')),
    
    pid INTEGER,
    dbname TEXT,
    username TEXT,
    application_name TEXT,
    client_addr INET,
    
    queryid BIGINT,
    query_text TEXT NOT NULL,
    query_fingerprint TEXT,
    
    state TEXT,
    wait_event_type TEXT,
    wait_event TEXT,
    
    calls BIGINT,
    total_time_ms DOUBLE PRECISION,
    mean_time_ms DOUBLE PRECISION,
    min_time_ms DOUBLE PRECISION,
    max_time_ms DOUBLE PRECISION,
    query_duration_ms DOUBLE PRECISION,
    
    rows_returned BIGINT,
    estimated_rows BIGINT,
    
    shared_blks_hit BIGINT,
    shared_blks_read BIGINT,
    shared_blks_dirtied BIGINT,
    shared_blks_written BIGINT,
    local_blks_hit BIGINT,
    local_blks_read BIGINT,
    local_blks_dirtied BIGINT,
    local_blks_written BIGINT,
    temp_blks_read BIGINT,
    temp_blks_written BIGINT,
    blk_read_time_ms DOUBLE PRECISION,
    blk_write_time_ms DOUBLE PRECISION,
    
    wal_records BIGINT,
    wal_fpi BIGINT,
    wal_bytes NUMERIC,
    
    operation_type TEXT,
    query_category TEXT,
    tables_count INTEGER,
    has_joins BOOLEAN DEFAULT FALSE,
    has_subqueries BOOLEAN DEFAULT FALSE,
    has_cte BOOLEAN DEFAULT FALSE,
    has_window_functions BOOLEAN DEFAULT FALSE,
    has_functions BOOLEAN DEFAULT FALSE,
    
    is_prepared BOOLEAN DEFAULT FALSE,
    plan_available BOOLEAN DEFAULT FALSE,
    estimated_cost DOUBLE PRECISION,
    execution_plan_hash TEXT,
    
    cache_hit_ratio DOUBLE PRECISION GENERATED ALWAYS AS (
        CASE 
            WHEN (shared_blks_hit + shared_blks_read) > 0 
            THEN (shared_blks_hit::DOUBLE PRECISION * 100.0) / (shared_blks_hit + shared_blks_read)
            ELSE NULL
        END
    ) STORED,
    
    io_efficiency DOUBLE PRECISION GENERATED ALWAYS AS (
        CASE 
            WHEN total_time_ms > 0 AND (shared_blks_read + temp_blks_read + shared_blks_written + temp_blks_written) > 0
            THEN (shared_blks_read + temp_blks_read + shared_blks_written + temp_blks_written)::DOUBLE PRECISION / total_time_ms
            WHEN query_duration_ms > 0 AND (shared_blks_read + temp_blks_read + shared_blks_written + temp_blks_written) > 0
            THEN (shared_blks_read + temp_blks_read + shared_blks_written + temp_blks_written)::DOUBLE PRECISION / query_duration_ms
            ELSE NULL
        END
    ) STORED,
    
    query_efficiency_score DOUBLE PRECISION GENERATED ALWAYS AS (
        CASE 
            WHEN source_type = 'snapshot' THEN
                CASE 
                    WHEN mean_time_ms < 100 THEN 100.0
                    WHEN mean_time_ms < 1000 THEN 80.0
                    WHEN mean_time_ms < 5000 THEN 60.0
                    ELSE 40.0
                END * 0.4 +
                CASE 
                    WHEN (shared_blks_hit + shared_blks_read) > 0 
                    THEN (shared_blks_hit::DOUBLE PRECISION * 100.0) / (shared_blks_hit + shared_blks_read)
                    ELSE 50.0
                END * 0.4 +
                CASE 
                    WHEN total_time_ms > 0 AND (shared_blks_read + temp_blks_read + shared_blks_written + temp_blks_written) > 0
                    THEN LEAST(100.0, 100.0 / ((shared_blks_read + temp_blks_read + shared_blks_written + temp_blks_written)::DOUBLE PRECISION / total_time_ms))
                    ELSE 50.0
                END * 0.2
            WHEN source_type = 'activity' THEN
                CASE 
                    WHEN query_duration_ms < 1000 THEN 100.0
                    WHEN query_duration_ms < 5000 THEN 80.0
                    WHEN query_duration_ms < 30000 THEN 60.0
                    ELSE 40.0
                END * 0.5 +
                CASE 
                    WHEN state = 'active' THEN 100.0
                    ELSE 60.0
                END * 0.3 +
                CASE 
                    WHEN wait_event_type IS NOT NULL AND wait_event_type != 'ClientRead' THEN 40.0
                    ELSE 100.0
                END * 0.2
            ELSE 50.0
        END
    ) STORED,
    
    is_long_running BOOLEAN GENERATED ALWAYS AS (
        CASE 
            WHEN source_type = 'activity' AND query_duration_ms > 60000 THEN TRUE
            WHEN source_type = 'snapshot' AND mean_time_ms > 5000 THEN TRUE
            ELSE FALSE
        END
    ) STORED,
    
    is_blocking BOOLEAN GENERATED ALWAYS AS (
        CASE 
            WHEN source_type = 'activity' AND wait_event_type IS NOT NULL 
                AND wait_event_type != 'ClientRead' AND wait_event_type != '' THEN TRUE
            ELSE FALSE
        END
    ) STORED,
    
    performance_tier TEXT GENERATED ALWAYS AS (
        CASE 
            WHEN source_type = 'snapshot' THEN
                CASE 
                    WHEN (CASE 
                            WHEN mean_time_ms < 100 THEN 100.0
                            WHEN mean_time_ms < 1000 THEN 80.0
                            WHEN mean_time_ms < 5000 THEN 60.0
                            ELSE 40.0
                        END * 0.4 +
                        CASE 
                            WHEN (shared_blks_hit + shared_blks_read) > 0 
                            THEN (shared_blks_hit::DOUBLE PRECISION * 100.0) / (shared_blks_hit + shared_blks_read)
                            ELSE 50.0
                        END * 0.4 +
                        CASE 
                            WHEN total_time_ms > 0 AND (shared_blks_read + temp_blks_read + shared_blks_written + temp_blks_written) > 0
                            THEN LEAST(100.0, 100.0 / ((shared_blks_read + temp_blks_read + shared_blks_written + temp_blks_written)::DOUBLE PRECISION / total_time_ms))
                            ELSE 50.0
                        END * 0.2) >= 80 THEN 'EXCELLENT'
                    WHEN (CASE 
                            WHEN mean_time_ms < 100 THEN 100.0
                            WHEN mean_time_ms < 1000 THEN 80.0
                            WHEN mean_time_ms < 5000 THEN 60.0
                            ELSE 40.0
                        END * 0.4 +
                        CASE 
                            WHEN (shared_blks_hit + shared_blks_read) > 0 
                            THEN (shared_blks_hit::DOUBLE PRECISION * 100.0) / (shared_blks_hit + shared_blks_read)
                            ELSE 50.0
                        END * 0.4 +
                        CASE 
                            WHEN total_time_ms > 0 AND (shared_blks_read + temp_blks_read + shared_blks_written + temp_blks_written) > 0
                            THEN LEAST(100.0, 100.0 / ((shared_blks_read + temp_blks_read + shared_blks_written + temp_blks_written)::DOUBLE PRECISION / total_time_ms))
                            ELSE 50.0
                        END * 0.2) >= 60 THEN 'GOOD'
                    WHEN (CASE 
                            WHEN mean_time_ms < 100 THEN 100.0
                            WHEN mean_time_ms < 1000 THEN 80.0
                            WHEN mean_time_ms < 5000 THEN 60.0
                            ELSE 40.0
                        END * 0.4 +
                        CASE 
                            WHEN (shared_blks_hit + shared_blks_read) > 0 
                            THEN (shared_blks_hit::DOUBLE PRECISION * 100.0) / (shared_blks_hit + shared_blks_read)
                            ELSE 50.0
                        END * 0.4 +
                        CASE 
                            WHEN total_time_ms > 0 AND (shared_blks_read + temp_blks_read + shared_blks_written + temp_blks_written) > 0
                            THEN LEAST(100.0, 100.0 / ((shared_blks_read + temp_blks_read + shared_blks_written + temp_blks_written)::DOUBLE PRECISION / total_time_ms))
                            ELSE 50.0
                        END * 0.2) >= 40 THEN 'FAIR'
                    ELSE 'POOR'
                END
            WHEN source_type = 'activity' THEN
                CASE 
                    WHEN (CASE 
                            WHEN query_duration_ms < 1000 THEN 100.0
                            WHEN query_duration_ms < 5000 THEN 80.0
                            WHEN query_duration_ms < 30000 THEN 60.0
                            ELSE 40.0
                        END * 0.5 +
                        CASE 
                            WHEN state = 'active' THEN 100.0
                            ELSE 60.0
                        END * 0.3 +
                        CASE 
                            WHEN wait_event_type IS NOT NULL AND wait_event_type != 'ClientRead' THEN 40.0
                            ELSE 100.0
                        END * 0.2) >= 80 THEN 'EXCELLENT'
                    WHEN (CASE 
                            WHEN query_duration_ms < 1000 THEN 100.0
                            WHEN query_duration_ms < 5000 THEN 80.0
                            WHEN query_duration_ms < 30000 THEN 60.0
                            ELSE 40.0
                        END * 0.5 +
                        CASE 
                            WHEN state = 'active' THEN 100.0
                            ELSE 60.0
                        END * 0.3 +
                        CASE 
                            WHEN wait_event_type IS NOT NULL AND wait_event_type != 'ClientRead' THEN 40.0
                            ELSE 100.0
                        END * 0.2) >= 60 THEN 'GOOD'
                    WHEN (CASE 
                            WHEN query_duration_ms < 1000 THEN 100.0
                            WHEN query_duration_ms < 5000 THEN 80.0
                            WHEN query_duration_ms < 30000 THEN 60.0
                            ELSE 40.0
                        END * 0.5 +
                        CASE 
                            WHEN state = 'active' THEN 100.0
                            ELSE 60.0
                        END * 0.3 +
                        CASE 
                            WHEN wait_event_type IS NOT NULL AND wait_event_type != 'ClientRead' THEN 40.0
                            ELSE 100.0
                        END * 0.2) >= 40 THEN 'FAIR'
                    ELSE 'POOR'
                END
            ELSE 'FAIR'
        END
    ) STORED
);

CREATE INDEX IF NOT EXISTS idx_qp_captured ON metadata.query_performance(captured_at);
CREATE INDEX IF NOT EXISTS idx_qp_source_type ON metadata.query_performance(source_type);
CREATE INDEX IF NOT EXISTS idx_qp_category ON metadata.query_performance(query_category);
CREATE INDEX IF NOT EXISTS idx_qp_operation ON metadata.query_performance(operation_type);
CREATE INDEX IF NOT EXISTS idx_qp_fingerprint ON metadata.query_performance(query_fingerprint);
CREATE INDEX IF NOT EXISTS idx_qp_efficiency ON metadata.query_performance(query_efficiency_score);
CREATE INDEX IF NOT EXISTS idx_qp_performance_tier ON metadata.query_performance(performance_tier);
CREATE INDEX IF NOT EXISTS idx_qp_long_running ON metadata.query_performance(is_long_running) WHERE is_long_running = TRUE;
CREATE INDEX IF NOT EXISTS idx_qp_blocking ON metadata.query_performance(is_blocking) WHERE is_blocking = TRUE;

COMMENT ON TABLE metadata.query_performance IS 'Unified table for query performance monitoring combining snapshots from pg_stat_statements and active queries from pg_stat_activity';
COMMENT ON COLUMN metadata.query_performance.source_type IS 'Type of source: snapshot (from pg_stat_statements) or activity (from pg_stat_activity)';
COMMENT ON COLUMN metadata.query_performance.cache_hit_ratio IS 'Calculated cache hit ratio percentage';
COMMENT ON COLUMN metadata.query_performance.io_efficiency IS 'Calculated IO efficiency metric';
COMMENT ON COLUMN metadata.query_performance.query_efficiency_score IS 'Calculated overall query efficiency score (0-100)';
COMMENT ON COLUMN metadata.query_performance.is_long_running IS 'Indicates if query is long-running';
COMMENT ON COLUMN metadata.query_performance.is_blocking IS 'Indicates if query is blocking other queries';
COMMENT ON COLUMN metadata.query_performance.performance_tier IS 'Performance tier: EXCELLENT, GOOD, FAIR, or POOR';

