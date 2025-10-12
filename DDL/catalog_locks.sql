CREATE TABLE IF NOT EXISTS metadata.catalog_locks (
    lock_name VARCHAR(255) PRIMARY KEY,
    acquired_at TIMESTAMP NOT NULL DEFAULT NOW(),
    acquired_by VARCHAR(255) NOT NULL,
    expires_at TIMESTAMP NOT NULL,
    session_id VARCHAR(255) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_catalog_locks_expires 
    ON metadata.catalog_locks(expires_at);

COMMENT ON TABLE metadata.catalog_locks IS 
    'Distributed locks for catalog operations to prevent race conditions';
COMMENT ON COLUMN metadata.catalog_locks.lock_name IS 
    'Name of the lock (e.g., catalog_sync, catalog_clean)';
COMMENT ON COLUMN metadata.catalog_locks.acquired_by IS 
    'Hostname or instance identifier that acquired the lock';
COMMENT ON COLUMN metadata.catalog_locks.expires_at IS 
    'When the lock expires (for automatic cleanup of dead locks)';
COMMENT ON COLUMN metadata.catalog_locks.session_id IS 
    'Unique session ID to prevent accidental lock release by other instances';

