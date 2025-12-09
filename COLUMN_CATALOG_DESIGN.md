# Diseño: Catálogo de Columnas (Column Catalog)

## Objetivo

Crear un catálogo completo de metadatos de columnas para todas las tablas en todas las fuentes de datos (PostgreSQL, MariaDB, MSSQL).

## Estructura de la Tabla

### Tabla: `metadata.column_catalog`

```sql
CREATE TABLE metadata.column_catalog (
    id BIGSERIAL PRIMARY KEY,
    schema_name VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    column_name VARCHAR(255) NOT NULL,
    db_engine VARCHAR(50) NOT NULL,
    connection_string TEXT NOT NULL,

    -- Metadatos básicos de columna
    ordinal_position INTEGER NOT NULL,
    data_type VARCHAR(100) NOT NULL,
    character_maximum_length INTEGER,
    numeric_precision INTEGER,
    numeric_scale INTEGER,
    is_nullable BOOLEAN DEFAULT true,
    column_default TEXT,

    -- Metadatos extendidos (JSON para flexibilidad)
    column_metadata JSONB,

    -- Clasificación y características
    is_primary_key BOOLEAN DEFAULT false,
    is_foreign_key BOOLEAN DEFAULT false,
    is_unique BOOLEAN DEFAULT false,
    is_indexed BOOLEAN DEFAULT false,
    is_auto_increment BOOLEAN DEFAULT false,
    is_generated BOOLEAN DEFAULT false,

    -- Estadísticas de datos
    null_count BIGINT,
    null_percentage NUMERIC(5,2),
    distinct_count BIGINT,
    distinct_percentage NUMERIC(5,2),
    min_value TEXT,
    max_value TEXT,
    avg_value NUMERIC,

    -- Clasificación de datos
    data_category VARCHAR(50),
    sensitivity_level VARCHAR(20),
    contains_pii BOOLEAN DEFAULT false,
    contains_phi BOOLEAN DEFAULT false,

    -- Timestamps
    first_seen_at TIMESTAMP DEFAULT NOW(),
    last_seen_at TIMESTAMP DEFAULT NOW(),
    last_analyzed_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),

    -- Constraints
    CONSTRAINT uq_column_catalog UNIQUE (schema_name, table_name, column_name, db_engine, connection_string)
);

CREATE INDEX idx_column_catalog_table ON metadata.column_catalog(schema_name, table_name, db_engine);
CREATE INDEX idx_column_catalog_engine ON metadata.column_catalog(db_engine);
CREATE INDEX idx_column_catalog_pii ON metadata.column_catalog(contains_pii) WHERE contains_pii = true;
CREATE INDEX idx_column_catalog_data_type ON metadata.column_catalog(data_type);
```

### Estructura del JSON `column_metadata`

```json
{
  "source_specific": {
    "postgresql": {
      "attnum": 1,
      "atttypid": 23,
      "attnotnull": false,
      "atthasdef": true,
      "attidentity": "",
      "collation": "default"
    },
    "mariadb": {
      "column_type": "int(11)",
      "column_key": "",
      "extra": "auto_increment",
      "privileges": "select,insert,update,references",
      "column_comment": ""
    },
    "mssql": {
      "system_type_id": 56,
      "user_type_id": 56,
      "max_length": 4,
      "precision": 10,
      "scale": 0,
      "is_computed": false,
      "is_sparse": false,
      "is_column_set": false
    }
  },
  "constraints": {
    "check_constraints": [],
    "default_constraint": "nextval('seq_name'::regclass)",
    "computed_formula": null
  },
  "indexes": [
    {
      "index_name": "idx_name",
      "index_type": "btree",
      "is_unique": false,
      "is_primary": true
    }
  ],
  "foreign_keys": [
    {
      "fk_name": "fk_constraint",
      "referenced_schema": "schema",
      "referenced_table": "table",
      "referenced_column": "column"
    }
  ]
}
```

## Módulo: ColumnCatalogCollector

### Estructura del Módulo

```
include/governance/ColumnCatalogCollector.h
src/governance/ColumnCatalogCollector.cpp
```

### Clase Principal

```cpp
class ColumnCatalogCollector {
public:
    ColumnCatalogCollector(const std::string &metadataConnectionString);
    ~ColumnCatalogCollector();

    void collectAllColumns();
    void storeColumnMetadata();
    void generateReport();

private:
    // Métodos de extracción por motor
    void collectPostgreSQLColumns(const std::string &connectionString);
    void collectMariaDBColumns(const std::string &connectionString);
    void collectMSSQLColumns(const std::string &connectionString);

    // Métodos auxiliares
    void analyzeColumnStatistics(const ColumnMetadata &column);
    void classifyColumn(ColumnMetadata &column);
    json buildColumnMetadataJSON(const ColumnMetadata &column, const std::string &engine);
};
```

### Estructura de Datos

```cpp
struct ColumnMetadata {
    std::string schema_name;
    std::string table_name;
    std::string column_name;
    std::string db_engine;
    std::string connection_string;

    int ordinal_position = 0;
    std::string data_type;
    int character_maximum_length = 0;
    int numeric_precision = 0;
    int numeric_scale = 0;
    bool is_nullable = true;
    std::string column_default;

    bool is_primary_key = false;
    bool is_foreign_key = false;
    bool is_unique = false;
    bool is_indexed = false;
    bool is_auto_increment = false;
    bool is_generated = false;

    long long null_count = 0;
    double null_percentage = 0.0;
    long long distinct_count = 0;
    double distinct_percentage = 0.0;
    std::string min_value;
    std::string max_value;
    double avg_value = 0.0;

    std::string data_category;
    std::string sensitivity_level;
    bool contains_pii = false;
    bool contains_phi = false;

    json column_metadata_json;
};
```

## Extracción por Motor

### PostgreSQL

**Query Principal:**

```sql
SELECT
    c.table_schema,
    c.table_name,
    c.column_name,
    c.ordinal_position,
    c.data_type,
    c.character_maximum_length,
    c.numeric_precision,
    c.numeric_scale,
    c.is_nullable,
    c.column_default,
    CASE WHEN pk.column_name IS NOT NULL THEN true ELSE false END as is_primary_key,
    CASE WHEN fk.column_name IS NOT NULL THEN true ELSE false END as is_foreign_key,
    CASE WHEN uq.column_name IS NOT NULL THEN true ELSE false END as is_unique,
    CASE WHEN idx.column_name IS NOT NULL THEN true ELSE false END as is_indexed,
    CASE WHEN c.is_identity = 'YES' THEN true ELSE false END as is_auto_increment,
    CASE WHEN c.is_generated = 'ALWAYS' THEN true ELSE false END as is_generated,
    a.attnum,
    a.atttypid,
    a.attnotnull,
    a.atthasdef,
    a.attidentity,
    a.attcollation
FROM information_schema.columns c
LEFT JOIN pg_attribute a ON a.attrelid = (c.table_schema||'.'||c.table_name)::regclass
    AND a.attname = c.column_name
LEFT JOIN (
    SELECT kcu.table_schema, kcu.table_name, kcu.column_name
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name
    WHERE tc.constraint_type = 'PRIMARY KEY'
) pk ON pk.table_schema = c.table_schema
    AND pk.table_name = c.table_name
    AND pk.column_name = c.column_name
LEFT JOIN (
    SELECT kcu.table_schema, kcu.table_name, kcu.column_name
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name
    WHERE tc.constraint_type = 'FOREIGN KEY'
) fk ON fk.table_schema = c.table_schema
    AND fk.table_name = c.table_name
    AND fk.column_name = c.column_name
LEFT JOIN (
    SELECT kcu.table_schema, kcu.table_name, kcu.column_name
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name
    WHERE tc.constraint_type = 'UNIQUE'
) uq ON uq.table_schema = c.table_schema
    AND uq.table_name = c.table_name
    AND uq.column_name = c.column_name
LEFT JOIN (
    SELECT schemaname, tablename, attname as column_name
    FROM pg_indexes pi
    JOIN pg_index pidx ON pi.indexname = pidx.indexrelid::regclass::text
    JOIN pg_attribute patt ON pidx.indexrelid = patt.attrelid
) idx ON idx.schemaname = c.table_schema
    AND idx.tablename = c.table_name
    AND idx.column_name = c.column_name
WHERE c.table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_toast', 'metadata')
ORDER BY c.table_schema, c.table_name, c.ordinal_position;
```

### MariaDB

**Query Principal:**

```sql
SELECT
    c.TABLE_SCHEMA,
    c.TABLE_NAME,
    c.COLUMN_NAME,
    c.ORDINAL_POSITION,
    c.DATA_TYPE,
    c.CHARACTER_MAXIMUM_LENGTH,
    c.NUMERIC_PRECISION,
    c.NUMERIC_SCALE,
    c.IS_NULLABLE,
    c.COLUMN_DEFAULT,
    c.COLUMN_TYPE,
    c.COLUMN_KEY,
    c.EXTRA,
    c.PRIVILEGES,
    c.COLUMN_COMMENT,
    CASE WHEN kcu.COLUMN_NAME IS NOT NULL AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY' THEN true ELSE false END as is_primary_key,
    CASE WHEN kcu.COLUMN_NAME IS NOT NULL AND tc.CONSTRAINT_TYPE = 'FOREIGN KEY' THEN true ELSE false END as is_foreign_key,
    CASE WHEN kcu.COLUMN_NAME IS NOT NULL AND tc.CONSTRAINT_TYPE = 'UNIQUE' THEN true ELSE false END as is_unique,
    CASE WHEN s.COLUMN_NAME IS NOT NULL THEN true ELSE false END as is_indexed,
    CASE WHEN c.EXTRA LIKE '%auto_increment%' THEN true ELSE false END as is_auto_increment,
    CASE WHEN c.GENERATION_EXPRESSION IS NOT NULL THEN true ELSE false END as is_generated
FROM INFORMATION_SCHEMA.COLUMNS c
LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
    ON c.TABLE_SCHEMA = kcu.TABLE_SCHEMA
    AND c.TABLE_NAME = kcu.TABLE_NAME
    AND c.COLUMN_NAME = kcu.COLUMN_NAME
LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
    ON kcu.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA
    AND kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
LEFT JOIN INFORMATION_SCHEMA.STATISTICS s
    ON c.TABLE_SCHEMA = s.TABLE_SCHEMA
    AND c.TABLE_NAME = s.TABLE_NAME
    AND c.COLUMN_NAME = s.COLUMN_NAME
WHERE c.TABLE_SCHEMA NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
ORDER BY c.TABLE_SCHEMA, c.TABLE_NAME, c.ORDINAL_POSITION;
```

### MSSQL

**Query Principal:**

```sql
SELECT
    OBJECT_SCHEMA_NAME(c.object_id) AS table_schema,
    OBJECT_NAME(c.object_id) AS table_name,
    c.name AS column_name,
    c.column_id AS ordinal_position,
    t.name AS data_type,
    c.max_length,
    c.precision,
    c.scale,
    c.is_nullable,
    dc.definition AS column_default,
    c.is_identity AS is_auto_increment,
    c.is_computed AS is_generated,
    c.system_type_id,
    c.user_type_id,
    c.is_sparse,
    c.is_column_set,
    CASE WHEN pk.column_name IS NOT NULL THEN 1 ELSE 0 END as is_primary_key,
    CASE WHEN fk.column_name IS NOT NULL THEN 1 ELSE 0 END as is_foreign_key,
    CASE WHEN uq.column_name IS NOT NULL THEN 1 ELSE 0 END as is_unique,
    CASE WHEN idx.column_name IS NOT NULL THEN 1 ELSE 0 END as is_indexed
FROM sys.columns c
INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
LEFT JOIN sys.default_constraints dc ON c.default_object_id = dc.object_id
LEFT JOIN (
    SELECT
        OBJECT_SCHEMA_NAME(k.parent_object_id) AS table_schema,
        OBJECT_NAME(k.parent_object_id) AS table_name,
        c.name AS column_name
    FROM sys.key_constraints k
    INNER JOIN sys.index_columns ic ON k.parent_object_id = ic.object_id AND k.unique_index_id = ic.index_id
    INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
    WHERE k.type = 'PK'
) pk ON OBJECT_SCHEMA_NAME(c.object_id) = pk.table_schema
    AND OBJECT_NAME(c.object_id) = pk.table_name
    AND c.name = pk.column_name
LEFT JOIN (
    SELECT
        OBJECT_SCHEMA_NAME(f.parent_object_id) AS table_schema,
        OBJECT_NAME(f.parent_object_id) AS table_name,
        c.name AS column_name
    FROM sys.foreign_keys f
    INNER JOIN sys.foreign_key_columns fkc ON f.object_id = fkc.constraint_object_id
    INNER JOIN sys.columns c ON fkc.parent_object_id = c.object_id AND fkc.parent_column_id = c.column_id
) fk ON OBJECT_SCHEMA_NAME(c.object_id) = fk.table_schema
    AND OBJECT_NAME(c.object_id) = fk.table_name
    AND c.name = fk.column_name
LEFT JOIN (
    SELECT
        OBJECT_SCHEMA_NAME(k.parent_object_id) AS table_schema,
        OBJECT_NAME(k.parent_object_id) AS table_name,
        c.name AS column_name
    FROM sys.key_constraints k
    INNER JOIN sys.index_columns ic ON k.parent_object_id = ic.object_id AND k.unique_index_id = ic.index_id
    INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
    WHERE k.type = 'UQ'
) uq ON OBJECT_SCHEMA_NAME(c.object_id) = uq.table_schema
    AND OBJECT_NAME(c.object_id) = uq.table_name
    AND c.name = uq.column_name
LEFT JOIN (
    SELECT DISTINCT
        OBJECT_SCHEMA_NAME(ic.object_id) AS table_schema,
        OBJECT_NAME(ic.object_id) AS table_name,
        c.name AS column_name
    FROM sys.index_columns ic
    INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
    WHERE ic.key_ordinal > 0
) idx ON OBJECT_SCHEMA_NAME(c.object_id) = idx.table_schema
    AND OBJECT_NAME(c.object_id) = idx.table_name
    AND c.name = idx.column_name
WHERE OBJECT_SCHEMA_NAME(c.object_id) NOT IN ('sys', 'INFORMATION_SCHEMA')
ORDER BY OBJECT_SCHEMA_NAME(c.object_id), OBJECT_NAME(c.object_id), c.column_id;
```

## Estadísticas de Columnas (Opcional)

Para cada columna, calcular:

- `null_count`: COUNT(\*) WHERE column IS NULL
- `null_percentage`: (null_count / total_rows) \* 100
- `distinct_count`: COUNT(DISTINCT column)
- `distinct_percentage`: (distinct_count / total_rows) \* 100
- `min_value`, `max_value`, `avg_value`: Para tipos numéricos

**Nota**: Estas estadísticas pueden ser costosas para tablas grandes. Considerar:

- Ejecutarlas solo para tablas pequeñas (< 1M filas)
- Ejecutarlas en modo muestreo (sample)
- Ejecutarlas en background/async

## Clasificación de Columnas

Usar `DataClassifier` existente para:

- Detectar PII (emails, phones, SSN, etc.)
- Detectar PHI (medical records)
- Clasificar por categoría (personal, financial, operational, etc.)
- Asignar nivel de sensibilidad (public, internal, confidential, restricted)

## Integración

### Flujo de Ejecución

1. **En `StreamingData::initializationThread()`** o **`DataGovernance::runDiscovery()`**:

   ```cpp
   ColumnCatalogCollector collector(DatabaseConfig::getPostgresConnectionString());
   collector.collectAllColumns();
   collector.storeColumnMetadata();
   collector.generateReport();
   ```

2. **Frecuencia**:
   - Ejecutar después de `DataGovernance::runDiscovery()`
   - O en un thread separado cada 24 horas
   - O manualmente cuando se necesite

### Orden de Ejecución

```
1. DataGovernance::runDiscovery()  // Descubre tablas
2. ColumnCatalogCollector::collectAllColumns()  // Descubre columnas
3. LineageExtractor (MSSQL/MariaDB)  // Descubre dependencias
```

## Ventajas de este Diseño

1. **Tabla Separada**: Más eficiente que JSON en `catalog`, permite queries específicas
2. **JSON Flexible**: `column_metadata` JSONB permite metadatos específicos por motor
3. **Estadísticas Opcionales**: No bloquea si son costosas
4. **Clasificación Automática**: Reutiliza `DataClassifier` existente
5. **Escalable**: Índices optimizados para búsquedas comunes
6. **Trazabilidad**: `first_seen_at`, `last_seen_at` para cambios de esquema

## Consideraciones

1. **Performance**: Para bases grandes, considerar:

   - Procesamiento en batches
   - Paralelización por motor
   - Caché de resultados

2. **Actualización**:

   - `ON CONFLICT` para actualizar columnas existentes
   - Detectar columnas eliminadas (marcar como inactivas o eliminar)

3. **Estadísticas**:
   - Opcional: solo para tablas activas
   - Muestreo para tablas grandes
   - Background processing

## Archivos a Crear

1. `schema_migrations/010_create_column_catalog_table.sql`
2. `include/governance/ColumnCatalogCollector.h`
3. `src/governance/ColumnCatalogCollector.cpp`
4. Actualizar `CMakeLists.txt`
5. Integrar en `DataGovernance::runDiscovery()` o `StreamingData`
