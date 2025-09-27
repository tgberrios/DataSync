-- Agregar columna SIZE a metadata.catalog para ordenamiento por tamaño de tabla
ALTER TABLE metadata.catalog ADD COLUMN IF NOT EXISTS table_size BIGINT DEFAULT 0;

-- Crear índice para optimizar ordenamiento por tamaño
CREATE INDEX IF NOT EXISTS idx_catalog_table_size ON metadata.catalog(table_size);

-- Comentario para documentar la columna
COMMENT ON COLUMN metadata.catalog.table_size IS 'Número aproximado de registros en la tabla para ordenamiento por tamaño';
