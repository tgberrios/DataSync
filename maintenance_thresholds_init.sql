INSERT INTO metadata.config (key, value, description)
VALUES (
  'maintenance_thresholds',
  '{
    "postgresql": {
      "vacuum": {
        "dead_tuples_threshold": 1000,
        "dead_tuples_percentage": 10.0,
        "days_since_last_vacuum": 7
      },
      "analyze": {
        "days_since_last_analyze": 1
      },
      "reindex": {
        "fragmentation_threshold": 30.0
      }
    },
    "mariadb": {
      "optimize": {
        "fragmentation_threshold": 20.0,
        "free_space_threshold_mb": 100
      },
      "analyze": {
        "days_since_last_analyze": 1
      }
    },
    "mssql": {
      "rebuild_index": {
        "fragmentation_threshold": 30.0
      },
      "reorganize_index": {
        "fragmentation_min": 10.0,
        "fragmentation_max": 30.0
      },
      "update_statistics": {
        "days_since_last_update": 1
      }
    }
  }',
  'Maintenance thresholds configuration for PostgreSQL, MariaDB, and MSSQL maintenance operations'
)
ON CONFLICT (key) DO UPDATE SET
  value = EXCLUDED.value,
  description = EXCLUDED.description,
  updated_at = NOW();

