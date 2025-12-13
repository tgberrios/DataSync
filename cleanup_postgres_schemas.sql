-- ============================================================
-- QUERY PARA ELIMINAR SCHEMAS Y TABLAS FANTASMA EN POSTGRESQL
-- ============================================================

-- 1. VERIFICAR SCHEMAS PROBLEMÁTICOS
SELECT schema_name 
FROM information_schema.schemata 
WHERE schema_name ILIKE '%datasynth%' 
   OR schema_name ILIKE '%datasytnth%';

-- 2. VERIFICAR TABLAS EN ESOS SCHEMAS
SELECT schemaname, relname 
FROM pg_stat_user_tables 
WHERE schemaname ILIKE '%datasynth%' 
   OR schemaname ILIKE '%datasytnth%' 
   OR relname ILIKE '%testing%' 
   OR relname ILIKE '%holaaaa323%';

-- 3. ELIMINAR SCHEMAS Y TODAS SUS TABLAS (CASCADE)
-- ⚠️ CUIDADO: Esto eliminará todo en esos schemas
DROP SCHEMA IF EXISTS "DataSynth" CASCADE;
DROP SCHEMA IF EXISTS "DataSytnth" CASCADE;
DROP SCHEMA IF EXISTS "datasynth" CASCADE;

-- 4. VERIFICAR QUE SE ELIMINARON
SELECT schema_name 
FROM information_schema.schemata 
WHERE schema_name ILIKE '%datasynth%' 
   OR schema_name ILIKE '%datasytnth%';

-- Si la query anterior no devuelve filas, entonces se eliminaron correctamente

