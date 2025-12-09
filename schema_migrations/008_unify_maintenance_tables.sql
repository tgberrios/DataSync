ALTER TABLE metadata.maintenance_control
ADD COLUMN IF NOT EXISTS server_name VARCHAR(255),
ADD COLUMN IF NOT EXISTS database_name VARCHAR(255);

UPDATE metadata.maintenance_control
SET db_engine = 'PostgreSQL'
WHERE db_engine IS NULL;

UPDATE metadata.maintenance_control
SET db_engine = 'MariaDB'
WHERE db_engine IS NULL AND connection_string LIKE '%mariadb%' OR connection_string LIKE '%mysql%';

INSERT INTO metadata.maintenance_control (
    maintenance_type, db_engine, connection_string, schema_name, object_name,
    object_type, auto_execute, enabled, priority, status, next_maintenance_date,
    last_maintenance_date, maintenance_duration_seconds, maintenance_count,
    first_detected_date, last_checked_date, result_message, error_details,
    server_name, database_name, metric_before, thresholds
)
SELECT 
    maintenance_type,
    'MSSQL' as db_engine,
    '' as connection_string,
    schema_name,
    object_name,
    object_type,
    true as auto_execute,
    true as enabled,
    priority,
    CASE 
        WHEN status = 'NEEDS_MAINTENANCE' THEN 'PENDING'
        WHEN status = 'ERROR' THEN 'FAILED'
        ELSE status
    END as status,
    next_maintenance_date,
    last_maintenance_date,
    maintenance_duration_seconds,
    maintenance_count,
    first_detected_date,
    last_checked_date,
    result_message,
    error_details,
    server_name,
    database_name,
    metric_before,
    '{}'::jsonb as thresholds
FROM metadata.maintenance_control_mssql
ON CONFLICT (maintenance_type, schema_name, object_name, object_type) DO NOTHING;

ALTER TABLE metadata.maintenance_metrics
ADD COLUMN IF NOT EXISTS server_name VARCHAR(255),
ADD COLUMN IF NOT EXISTS db_engine VARCHAR(50);

UPDATE metadata.maintenance_metrics
SET db_engine = 'PostgreSQL'
WHERE db_engine IS NULL;

INSERT INTO metadata.maintenance_metrics (
    execution_date, maintenance_type, total_detected, total_fixed,
    total_failed, total_skipped, total_duration_seconds,
    avg_duration_per_object, space_reclaimed_mb, objects_improved,
    server_name, db_engine
)
SELECT 
    execution_date,
    maintenance_type,
    total_detected,
    total_fixed,
    total_failed,
    total_skipped,
    total_duration_seconds,
    avg_duration_per_object,
    space_reclaimed_mb,
    objects_improved,
    server_name,
    'MSSQL' as db_engine
FROM metadata.maintenance_metrics_mssql
ON CONFLICT DO NOTHING;

DROP TABLE IF EXISTS metadata.maintenance_control_mssql CASCADE;
DROP TABLE IF EXISTS metadata.maintenance_metrics_mssql CASCADE;

CREATE INDEX IF NOT EXISTS idx_maintenance_control_server ON metadata.maintenance_control(server_name, database_name) WHERE server_name IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_maintenance_metrics_server ON metadata.maintenance_metrics(server_name, execution_date DESC) WHERE server_name IS NOT NULL;

COMMENT ON COLUMN metadata.maintenance_control.server_name IS 'Server name (mainly for MSSQL compatibility)';
COMMENT ON COLUMN metadata.maintenance_control.database_name IS 'Database name (mainly for MSSQL compatibility)';
COMMENT ON COLUMN metadata.maintenance_metrics.server_name IS 'Server name (mainly for MSSQL compatibility)';
COMMENT ON COLUMN metadata.maintenance_metrics.db_engine IS 'Database engine: PostgreSQL, MariaDB, or MSSQL';

