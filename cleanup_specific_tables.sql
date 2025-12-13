-- ============================================================
-- QUERY RÁPIDA PARA ELIMINAR LAS TABLAS ESPECÍFICAS
-- ============================================================

-- Eliminar de data_governance_catalog
DELETE FROM metadata.data_governance_catalog 
WHERE 
    (schema_name = 'DataSynth' AND table_name = 'testing')
    OR (schema_name = 'DataSytnth' AND table_name = 'holaaaa323');

-- Verificar que se eliminaron
SELECT 
    schema_name, 
    table_name 
FROM metadata.data_governance_catalog 
WHERE 
    schema_name ILIKE '%datasynth%' 
    OR schema_name ILIKE '%datasytnth%' 
    OR table_name ILIKE '%holaaaa323%' 
    OR table_name ILIKE '%testing%';

-- Si la query anterior no devuelve filas, entonces se eliminaron correctamente

