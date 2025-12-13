CREATE TABLE IF NOT EXISTS metadata.custom_jobs (
    id SERIAL PRIMARY KEY,
    job_name VARCHAR(255) UNIQUE NOT NULL,
    description TEXT,
    source_db_engine VARCHAR(50) NOT NULL,
    source_connection_string TEXT NOT NULL,
    query_sql TEXT NOT NULL,
    target_db_engine VARCHAR(50) NOT NULL,
    target_connection_string TEXT NOT NULL,
    target_schema VARCHAR(100) NOT NULL,
    target_table VARCHAR(100) NOT NULL,
    schedule_cron VARCHAR(100),
    active BOOLEAN NOT NULL DEFAULT true,
    enabled BOOLEAN NOT NULL DEFAULT true,
    transform_config JSONB DEFAULT '{}'::jsonb,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_custom_jobs_job_name ON metadata.custom_jobs (job_name);
CREATE INDEX IF NOT EXISTS idx_custom_jobs_active ON metadata.custom_jobs (active);
CREATE INDEX IF NOT EXISTS idx_custom_jobs_enabled ON metadata.custom_jobs (enabled);
CREATE INDEX IF NOT EXISTS idx_custom_jobs_schedule ON metadata.custom_jobs (schedule_cron) WHERE schedule_cron IS NOT NULL;

CREATE TABLE IF NOT EXISTS metadata.job_results (
    id SERIAL PRIMARY KEY,
    job_name VARCHAR(255) NOT NULL,
    process_log_id BIGINT,
    row_count BIGINT NOT NULL DEFAULT 0,
    result_sample JSONB,
    full_result_stored BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_job_results_job_name ON metadata.job_results (job_name);
CREATE INDEX IF NOT EXISTS idx_job_results_process_log_id ON metadata.job_results (process_log_id);
CREATE INDEX IF NOT EXISTS idx_job_results_created_at ON metadata.job_results (created_at);

