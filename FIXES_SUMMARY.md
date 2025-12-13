# Resumen de Correcciones Aplicadas

## 1. Problema: Tablas Inexistentes (datasynth.testing, DataSytnth.holaaaa323)

### Causa

- Las tablas existían en PostgreSQL pero fueron eliminadas
- Quedaban referencias en `metadata.data_governance_catalog`
- Los procesos de governance consultan directamente `information_schema` y `pg_stat_user_tables`

### Solución Aplicada

- ✅ Eliminados schemas y tablas de PostgreSQL: `DROP SCHEMA DataSynth, DataSytnth, datasynth CASCADE`
- ✅ Eliminadas referencias de `metadata.data_governance_catalog`
- ✅ Agregadas funciones de limpieza automática: `cleanNonExistentOracleTables()` y `cleanNonExistentMongoDBTables()`

### Queries de Limpieza

Ver archivos:

- `cleanup_specific_tables.sql` - Elimina tablas específicas
- `cleanup_nonexistent_tables.sql` - Queries completas de limpieza
- `cleanup_postgres_schemas.sql` - Elimina schemas fantasma

## 2. Problema: Error "column 'name' does not exist" en test_incremental_sync

### Causa

- `dataFetcherThread` usaba `SELECT *` que devuelve TODAS las columnas de MSSQL
- `batchPreparerThread` construía INSERT usando `columnNames` de MSSQL
- Si la tabla destino en PostgreSQL tiene un esquema diferente (columnas faltantes), el INSERT falla

### Solución Aplicada

1. **dataFetcherThread**:

   - Ahora obtiene las columnas reales de PostgreSQL
   - Filtra `columnNames` para solo incluir columnas que existen en PostgreSQL
   - Usa columnas específicas en lugar de `SELECT *`

2. **batchPreparerThread**:
   - Obtiene las columnas reales de PostgreSQL
   - Filtra `columnNames` para solo incluir columnas que existen en PostgreSQL
   - Mapea correctamente los datos de `row` a las columnas válidas

### Cambios en el Código

- `include/sync/MSSQLToPostgres.h`:
  - `dataFetcherThread`: Filtra columnas antes de construir SELECT
  - `batchPreparerThread`: Filtra columnas y mapea datos correctamente

## 3. Otros Errores Corregidos

### Duplicate Keys en MSSQL

- ✅ Corregido: `batchPreparerThread` ahora usa UPSERT para tablas con PK

### Transaction Commit Después de Abort

- ✅ Corregido: Solo hace commit si no hubo abort

### Limpieza Automática

- ✅ Agregadas funciones para Oracle y MongoDB
- ✅ Integradas en `catalog_manager.cpp`

## Archivos Modificados

1. `include/sync/MSSQLToPostgres.h` - Filtrado de columnas y UPSERT
2. `src/sync/DatabaseToPostgresSync.cpp` - Manejo de transacciones
3. `src/catalog/metadata_repository.cpp` - Commit condicional
4. `include/catalog/catalog_cleaner.h` - Nuevas funciones
5. `src/catalog/catalog_cleaner.cpp` - Implementación de limpieza
6. `src/catalog/catalog_manager.cpp` - Integración de limpieza

## Próximos Pasos

1. **Para tablas inexistentes**: Los procesos de governance (`ColumnCatalogCollector`, `MaintenanceManager`) consultan directamente `information_schema`. Si persisten los errores, puede ser necesario agregar validación de existencia antes de procesar.

2. **Para test_incremental_sync**: El código ahora filtra columnas automáticamente. Si el error persiste, verificar:
   - Que `SchemaSync` se ejecute antes de procesar la tabla
   - Que las columnas en MSSQL y PostgreSQL estén sincronizadas
