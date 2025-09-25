-- Eliminar columna data_checksum de la tabla metadata.data_quality
ALTER TABLE metadata.data_quality DROP COLUMN IF EXISTS data_checksum;