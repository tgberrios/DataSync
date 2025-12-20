-- System Logs Migration Script
-- Creates table to store system resource metrics for historical tracking

BEGIN;

CREATE TABLE IF NOT EXISTS metadata.system_logs (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP DEFAULT NOW() NOT NULL,
    cpu_usage DECIMAL(5,2) NOT NULL,
    cpu_cores INTEGER NOT NULL,
    memory_used_gb DECIMAL(10,2) NOT NULL,
    memory_total_gb DECIMAL(10,2) NOT NULL,
    memory_percentage DECIMAL(5,2) NOT NULL,
    memory_rss_gb DECIMAL(10,2),
    memory_virtual_gb DECIMAL(10,2),
    network_iops DECIMAL(10,2),
    throughput_rps DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_system_logs_timestamp 
    ON metadata.system_logs(timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_system_logs_created_at 
    ON metadata.system_logs(created_at DESC);

COMMENT ON TABLE metadata.system_logs IS 'Stores historical system resource metrics for monitoring and analysis';

COMMIT;

