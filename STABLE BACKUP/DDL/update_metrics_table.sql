-- Script simple para quitar columnas innecesarias de transfer_metrics
-- Ejecutar este script en tu base de datos PostgreSQL

-- Quitar las columnas que no se pueden obtener de forma realista
ALTER TABLE metadata.transfer_metrics 
DROP COLUMN IF EXISTS transfer_duration_ms,
DROP COLUMN IF EXISTS transfer_rate_per_second,
DROP COLUMN IF EXISTS chunk_size,
DROP COLUMN IF EXISTS cpu_usage_percent;

-- Actualizar comentarios de la tabla
COMMENT ON TABLE metadata.transfer_metrics IS 'Real database metrics for data transfer operations';
COMMENT ON COLUMN metadata.transfer_metrics.records_transferred IS 'Current live tuples in the table';
COMMENT ON COLUMN metadata.transfer_metrics.bytes_transferred IS 'Actual table size in bytes';
COMMENT ON COLUMN metadata.transfer_metrics.memory_used_mb IS 'Table size in MB';
COMMENT ON COLUMN metadata.transfer_metrics.io_operations_per_second IS 'Total database operations (inserts + updates + deletes)';
COMMENT ON COLUMN metadata.transfer_metrics.transfer_type IS 'Type of transfer: FULL_LOAD, INCREMENTAL, SYNC';
COMMENT ON COLUMN metadata.transfer_metrics.status IS 'Transfer status: SUCCESS, FAILED, PENDING';
COMMENT ON COLUMN metadata.transfer_metrics.error_message IS 'Error message if transfer failed';
COMMENT ON COLUMN metadata.transfer_metrics.started_at IS 'When the transfer operation started';
COMMENT ON COLUMN metadata.transfer_metrics.completed_at IS 'When the transfer operation completed';
