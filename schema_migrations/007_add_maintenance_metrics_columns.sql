ALTER TABLE metadata.maintenance_control 
ADD COLUMN IF NOT EXISTS db_engine VARCHAR(50),
ADD COLUMN IF NOT EXISTS connection_string TEXT,
ADD COLUMN IF NOT EXISTS auto_execute BOOLEAN DEFAULT true,
ADD COLUMN IF NOT EXISTS enabled BOOLEAN DEFAULT true,
ADD COLUMN IF NOT EXISTS thresholds JSONB,
ADD COLUMN IF NOT EXISTS maintenance_schedule JSONB;

ALTER TABLE metadata.maintenance_control
ADD COLUMN IF NOT EXISTS metrics_before JSONB,
ADD COLUMN IF NOT EXISTS metrics_after JSONB,
ADD COLUMN IF NOT EXISTS space_reclaimed_mb DOUBLE PRECISION DEFAULT 0,
ADD COLUMN IF NOT EXISTS performance_improvement_pct DOUBLE PRECISION DEFAULT 0,
ADD COLUMN IF NOT EXISTS fragmentation_before DOUBLE PRECISION,
ADD COLUMN IF NOT EXISTS fragmentation_after DOUBLE PRECISION,
ADD COLUMN IF NOT EXISTS dead_tuples_before BIGINT,
ADD COLUMN IF NOT EXISTS dead_tuples_after BIGINT,
ADD COLUMN IF NOT EXISTS index_size_before_mb DOUBLE PRECISION,
ADD COLUMN IF NOT EXISTS index_size_after_mb DOUBLE PRECISION,
ADD COLUMN IF NOT EXISTS table_size_before_mb DOUBLE PRECISION,
ADD COLUMN IF NOT EXISTS table_size_after_mb DOUBLE PRECISION,
ADD COLUMN IF NOT EXISTS query_performance_before DOUBLE PRECISION,
ADD COLUMN IF NOT EXISTS query_performance_after DOUBLE PRECISION,
ADD COLUMN IF NOT EXISTS impact_score DOUBLE PRECISION GENERATED ALWAYS AS (
    CASE 
        WHEN space_reclaimed_mb > 0 OR performance_improvement_pct > 0 THEN
            (COALESCE(space_reclaimed_mb, 0) / 100.0) * 0.4 +
            (COALESCE(performance_improvement_pct, 0) / 10.0) * 0.4 +
            (CASE 
                WHEN fragmentation_before IS NOT NULL AND fragmentation_after IS NOT NULL 
                THEN (fragmentation_before - fragmentation_after) / 10.0
                ELSE 0
            END) * 0.2
        ELSE 0
    END
) STORED;

CREATE INDEX IF NOT EXISTS idx_maintenance_control_engine ON metadata.maintenance_control(db_engine);
CREATE INDEX IF NOT EXISTS idx_maintenance_control_auto_execute ON metadata.maintenance_control(auto_execute, enabled, status);
CREATE INDEX IF NOT EXISTS idx_maintenance_control_impact ON metadata.maintenance_control(impact_score DESC) WHERE status = 'COMPLETED';

COMMENT ON COLUMN metadata.maintenance_control.db_engine IS 'Database engine: PostgreSQL, MariaDB, or MSSQL';
COMMENT ON COLUMN metadata.maintenance_control.connection_string IS 'Connection string to the source database';
COMMENT ON COLUMN metadata.maintenance_control.auto_execute IS 'If true, maintenance executes automatically. If false, manual execution only';
COMMENT ON COLUMN metadata.maintenance_control.enabled IS 'If false, maintenance is disabled for this object';
COMMENT ON COLUMN metadata.maintenance_control.thresholds IS 'JSON configuration of detection thresholds';
COMMENT ON COLUMN metadata.maintenance_control.maintenance_schedule IS 'JSON configuration of maintenance schedule';
COMMENT ON COLUMN metadata.maintenance_control.space_reclaimed_mb IS 'Space reclaimed in MB after maintenance';
COMMENT ON COLUMN metadata.maintenance_control.performance_improvement_pct IS 'Performance improvement percentage';
COMMENT ON COLUMN metadata.maintenance_control.impact_score IS 'Calculated impact score (0-100) based on space reclaimed, performance improvement, and fragmentation reduction';

