-- ============================================================
-- QUERIES PARA BUSCAR Y LIMPIAR TABLAS INEXISTENTES
-- ============================================================
-- 
-- NOTA: Ejecuta primero las queries de búsqueda (SELECT) para verificar
-- qué se va a eliminar antes de ejecutar los DELETE.
-- ============================================================

-- 1. BUSCAR TABLAS ESPECÍFICAS MENCIONADAS EN LOS ERRORES
SELECT 
    schema_name, 
    table_name, 
    db_engine, 
    connection_string,
    active,
    status
FROM metadata.catalog 
WHERE 
    (schema_name ILIKE '%datasynth%' 
     OR schema_name ILIKE '%datasytnth%' 
     OR table_name ILIKE '%holaaaa323%' 
     OR table_name ILIKE '%testing%')
ORDER BY db_engine, schema_name, table_name;

-- 2. ELIMINAR TABLAS ESPECÍFICAS DEL CATÁLOGO
-- (Ejecutar solo después de verificar que no existen)
DELETE FROM metadata.catalog 
WHERE 
    (schema_name ILIKE '%datasynth%' 
     OR schema_name ILIKE '%datasytnth%' 
     OR table_name ILIKE '%holaaaa323%' 
     OR table_name ILIKE '%testing%')
    AND db_engine IN ('PostgreSQL', 'MariaDB', 'MSSQL', 'MongoDB', 'Oracle');

-- 3. BUSCAR TODAS LAS TABLAS DE POSTGRESQL QUE NO EXISTEN
-- (Verifica contra information_schema)
SELECT 
    c.schema_name, 
    c.table_name,
    c.db_engine,
    c.connection_string
FROM metadata.catalog c
WHERE c.db_engine = 'PostgreSQL'
    AND NOT EXISTS (
        SELECT 1 
        FROM information_schema.tables t
        WHERE LOWER(t.table_schema) = LOWER(c.schema_name)
            AND LOWER(t.table_name) = LOWER(c.table_name)
    );

-- 4. ELIMINAR TABLAS DE POSTGRESQL QUE NO EXISTEN
DELETE FROM metadata.catalog 
WHERE db_engine = 'PostgreSQL'
    AND NOT EXISTS (
        SELECT 1 
        FROM information_schema.tables t
        WHERE LOWER(t.table_schema) = LOWER(metadata.catalog.schema_name)
            AND LOWER(t.table_name) = LOWER(metadata.catalog.table_name)
    );

-- 5. BUSCAR SCHEMAS INEXISTENTES EN POSTGRESQL
SELECT DISTINCT 
    schema_name
FROM metadata.catalog
WHERE db_engine = 'PostgreSQL'
    AND NOT EXISTS (
        SELECT 1 
        FROM information_schema.schemata s
        WHERE LOWER(s.schema_name) = LOWER(metadata.catalog.schema_name)
    );

-- 6. ELIMINAR TODAS LAS TABLAS DE SCHEMAS INEXISTENTES
DELETE FROM metadata.catalog 
WHERE db_engine = 'PostgreSQL'
    AND NOT EXISTS (
        SELECT 1 
        FROM information_schema.schemata s
        WHERE LOWER(s.schema_name) = LOWER(metadata.catalog.schema_name)
    );

-- 7. BUSCAR TABLAS CON NOMBRES DE SCHEMA/TABLA CON TYPOS
-- (Busca variaciones comunes de typos)
SELECT 
    schema_name, 
    table_name, 
    db_engine,
    connection_string
FROM metadata.catalog 
WHERE 
    schema_name ~* 'datasyn[th]{1,2}'  -- Busca datasynth, datasytnth, etc.
    OR schema_name ~* 'datasytnth'
ORDER BY schema_name, table_name;

-- 8. ELIMINAR TABLAS CON SCHEMAS CON TYPOS
DELETE FROM metadata.catalog 
WHERE 
    schema_name ~* 'datasyn[th]{1,2}'
    OR schema_name ~* 'datasytnth';

-- ============================================================
-- LIMPIAR REFERENCIAS EN OTRAS TABLAS DE METADATA
-- ============================================================

-- 9. BUSCAR EN column_catalog
SELECT 
    schema_name, 
    table_name, 
    db_engine,
    connection_string
FROM metadata.column_catalog 
WHERE 
    (schema_name ILIKE '%datasynth%' 
     OR schema_name ILIKE '%datasytnth%' 
     OR table_name ILIKE '%holaaaa323%' 
     OR table_name ILIKE '%testing%')
ORDER BY db_engine, schema_name, table_name;

-- 10. ELIMINAR DE column_catalog
DELETE FROM metadata.column_catalog 
WHERE 
    (schema_name ILIKE '%datasynth%' 
     OR schema_name ILIKE '%datasytnth%' 
     OR table_name ILIKE '%holaaaa323%' 
     OR table_name ILIKE '%testing%');

-- 11. BUSCAR EN data_governance_catalog (PostgreSQL)
SELECT 
    schema_name, 
    table_name
FROM metadata.data_governance_catalog 
WHERE 
    (schema_name ILIKE '%datasynth%' 
     OR schema_name ILIKE '%datasytnth%' 
     OR table_name ILIKE '%holaaaa323%' 
     OR table_name ILIKE '%testing%')
ORDER BY schema_name, table_name;

-- 12. ELIMINAR DE data_governance_catalog (PostgreSQL)
DELETE FROM metadata.data_governance_catalog 
WHERE 
    (schema_name ILIKE '%datasynth%' 
     OR schema_name ILIKE '%datasytnth%' 
     OR table_name ILIKE '%holaaaa323%' 
     OR table_name ILIKE '%testing%');

-- 13. BUSCAR EN query_store (si existe)
SELECT 
    schema_name, 
    table_name
FROM metadata.query_store 
WHERE 
    (schema_name ILIKE '%datasynth%' 
     OR schema_name ILIKE '%datasytnth%' 
     OR table_name ILIKE '%holaaaa323%' 
     OR table_name ILIKE '%testing%')
ORDER BY schema_name, table_name;

-- 14. ELIMINAR DE query_store
DELETE FROM metadata.query_store 
WHERE 
    (schema_name ILIKE '%datasynth%' 
     OR schema_name ILIKE '%datasytnth%' 
     OR table_name ILIKE '%holaaaa323%' 
     OR table_name ILIKE '%testing%');

-- 15. BUSCAR EN lineage (si existe)
SELECT 
    source_schema, 
    source_table
FROM metadata.postgres_lineage 
WHERE 
    (source_schema ILIKE '%datasynth%' 
     OR source_schema ILIKE '%datasytnth%' 
     OR source_table ILIKE '%holaaaa323%' 
     OR source_table ILIKE '%testing%')
UNION
SELECT 
    target_schema, 
    target_table
FROM metadata.postgres_lineage 
WHERE 
    (target_schema ILIKE '%datasynth%' 
     OR target_schema ILIKE '%datasytnth%' 
     OR target_table ILIKE '%holaaaa323%' 
     OR target_table ILIKE '%testing%');

-- 16. ELIMINAR DE lineage
DELETE FROM metadata.postgres_lineage 
WHERE 
    (source_schema ILIKE '%datasynth%' 
     OR source_schema ILIKE '%datasytnth%' 
     OR source_table ILIKE '%holaaaa323%' 
     OR source_table ILIKE '%testing%')
    OR (target_schema ILIKE '%datasynth%' 
        OR target_schema ILIKE '%datasytnth%' 
        OR target_table ILIKE '%holaaaa323%' 
        OR target_table ILIKE '%testing%');

-- ============================================================
-- QUERIES PARA VERIFICAR ANTES DE ELIMINAR
-- ============================================================

-- Ver cuántas tablas se eliminarían del catalog
SELECT 
    db_engine,
    COUNT(*) as total_tables_to_delete
FROM metadata.catalog 
WHERE 
    (schema_name ILIKE '%datasynth%' 
     OR schema_name ILIKE '%datasytnth%' 
     OR table_name ILIKE '%holaaaa323%' 
     OR table_name ILIKE '%testing%')
    AND db_engine IN ('PostgreSQL', 'MariaDB', 'MSSQL', 'MongoDB', 'Oracle')
GROUP BY db_engine;

-- Ver todas las tablas de PostgreSQL que no existen
SELECT 
    COUNT(*) as total_postgres_tables_to_delete
FROM metadata.catalog c
WHERE c.db_engine = 'PostgreSQL'
    AND NOT EXISTS (
        SELECT 1 
        FROM information_schema.tables t
        WHERE LOWER(t.table_schema) = LOWER(c.schema_name)
            AND LOWER(t.table_name) = LOWER(c.table_name)
    );

-- Ver cuántas referencias hay en column_catalog
SELECT 
    COUNT(*) as total_column_references
FROM metadata.column_catalog 
WHERE 
    (schema_name ILIKE '%datasynth%' 
     OR schema_name ILIKE '%datasytnth%' 
     OR table_name ILIKE '%holaaaa323%' 
     OR table_name ILIKE '%testing%');

-- ============================================================
-- LIMPIEZA COMPLETA EN UNA SOLA TRANSACCIÓN
-- ============================================================

-- 17. LIMPIAR TODO EN UNA TRANSACCIÓN (EJECUTAR CON CUIDADO)
BEGIN;

-- Eliminar del catalog
DELETE FROM metadata.catalog 
WHERE 
    (schema_name ILIKE '%datasynth%' 
     OR schema_name ILIKE '%datasytnth%' 
     OR table_name ILIKE '%holaaaa323%' 
     OR table_name ILIKE '%testing%');

-- Eliminar de column_catalog
DELETE FROM metadata.column_catalog 
WHERE 
    (schema_name ILIKE '%datasynth%' 
     OR schema_name ILIKE '%datasytnth%' 
     OR table_name ILIKE '%holaaaa323%' 
     OR table_name ILIKE '%testing%');

-- Eliminar de data_governance_catalog
DELETE FROM metadata.data_governance_catalog 
WHERE 
    (schema_name ILIKE '%datasynth%' 
     OR schema_name ILIKE '%datasytnth%' 
     OR table_name ILIKE '%holaaaa323%' 
     OR table_name ILIKE '%testing%');

-- Eliminar de query_store (si existe)
DELETE FROM metadata.query_store 
WHERE 
    (schema_name ILIKE '%datasynth%' 
     OR schema_name ILIKE '%datasytnth%' 
     OR table_name ILIKE '%holaaaa323%' 
     OR table_name ILIKE '%testing%');

-- Eliminar de lineage
DELETE FROM metadata.postgres_lineage 
WHERE 
    (source_schema ILIKE '%datasynth%' 
     OR source_schema ILIKE '%datasytnth%' 
     OR source_table ILIKE '%holaaaa323%' 
     OR source_table ILIKE '%testing%')
    OR (target_schema ILIKE '%datasynth%' 
        OR target_schema ILIKE '%datasytnth%' 
        OR target_table ILIKE '%holaaaa323%' 
        OR target_table ILIKE '%testing%');

-- Verificar antes de commit
SELECT 'Verificar resultados antes de hacer COMMIT' as warning;

-- Si todo está bien, ejecutar:
-- COMMIT;
-- Si algo está mal, ejecutar:
-- ROLLBACK;

