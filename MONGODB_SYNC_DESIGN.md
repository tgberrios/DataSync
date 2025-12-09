# Diseño: Soporte MongoDB para DataSync

## Objetivo

Agregar soporte completo para sincronización de MongoDB hacia PostgreSQL, usando estrategia TRUNCATE + FULL_LOAD diaria.

## Estrategia de Sincronización

### Características Principales

1. **Siempre TRUNCATE + FULL_LOAD**: No hay sincronización incremental
2. **Verificación diaria**: Compara `last_sync_time` con fecha actual
3. **Ejecución una vez al día**: Solo sincroniza si han pasado 24 horas desde la última sync
4. **Status fijo**: Todas las colecciones MongoDB tienen `status = 'FULL_LOAD'` en el catalog

## Arquitectura

### 1. MongoDB Engine (`MongoDBEngine`)

**Archivos:**

- `include/engines/mongodb_engine.h`
- `src/engines/mongodb_engine.cpp`

**Responsabilidades:**

- Conectar a MongoDB usando libmongoc
- Descubrir colecciones (equivalente a tablas)
- Obtener metadatos de colecciones (count, size, indexes)
- Extraer schema de documentos (campos y tipos)

**Métodos principales:**

```cpp
class MongoDBEngine {
public:
    explicit MongoDBEngine(const std::string &connectionString);
    ~MongoDBEngine();

    std::vector<CatalogTableInfo> discoverCollections();
    long long getCollectionCount(const std::string &database, const std::string &collection);
    std::vector<std::string> getCollectionFields(const std::string &database, const std::string &collection);
    bool isValid() const;

private:
    mongoc_client_t *client_;
    std::string connectionString_;
    std::string databaseName_;
    std::string host_;
    int port_;

    bool parseConnectionString(const std::string &connectionString);
};
```

**Connection String Format:**

```
mongodb://[username:password@]host[:port]/[database][?options]
```

### 2. MongoDB to PostgreSQL Sync (`MongoDBToPostgres`)

**Archivos:**

- `include/sync/MongoDBToPostgres.h`
- `src/sync/MongoDBToPostgres.cpp`

**Responsabilidades:**

- Transferir datos de MongoDB a PostgreSQL
- Convertir documentos BSON a filas PostgreSQL
- Manejar tipos de datos MongoDB -> PostgreSQL
- TRUNCATE antes de INSERT
- Verificar si debe sincronizar (última sync hace 24+ horas)

**Estructura:**

```cpp
class MongoDBToPostgres : public DatabaseToPostgresSync {
public:
    MongoDBToPostgres();
    ~MongoDBToPostgres();

    void transferDataMongoDBToPostgresParallel();
    void setupTableTargetMongoDBToPostgres();

protected:
    std::string cleanValueForPostgres(const std::string &value, const std::string &columnType) override;

private:
    bool shouldSyncCollection(const TableInfo &tableInfo);
    void truncateAndLoadCollection(const TableInfo &tableInfo);
    std::vector<std::vector<std::string>> fetchCollectionData(const TableInfo &tableInfo);
    void convertBSONToPostgresRow(bson_t *doc, const std::vector<std::string> &fields, std::vector<std::string> &row);
    std::string inferPostgreSQLType(const bson_value_t *value);
    void createPostgreSQLTable(const TableInfo &tableInfo, const std::vector<std::string> &fields, const std::vector<std::string> &fieldTypes);
};
```

**Lógica de Verificación Diaria:**

```cpp
bool MongoDBToPostgres::shouldSyncCollection(const TableInfo &tableInfo) {
    if (tableInfo.status != "FULL_LOAD") {
        return false;
    }

    if (tableInfo.last_sync_time.empty()) {
        return true; // Nunca sincronizado
    }

    // Parsear last_sync_time y comparar con fecha actual
    // Si han pasado 24+ horas, retornar true
    auto lastSync = parseTimestamp(tableInfo.last_sync_time);
    auto now = std::chrono::system_clock::now();
    auto hoursSinceLastSync = std::chrono::duration_cast<std::chrono::hours>(now - lastSync).count();

    return hoursSinceLastSync >= 24;
}
```

**Flujo de Sincronización:**

1. Obtener colecciones desde `metadata.catalog` donde `db_engine = 'MongoDB'` y `active = true`
2. Para cada colección:
   - Verificar `shouldSyncCollection()` (última sync hace 24+ horas)
   - Si debe sincronizar:
     - TRUNCATE tabla en PostgreSQL
     - Obtener todos los documentos de MongoDB
     - Convertir documentos BSON a filas PostgreSQL
     - INSERT masivo en PostgreSQL
     - Actualizar `last_sync_time` en catalog
     - Actualizar `status = 'FULL_LOAD'`

### 3. Catalog Manager Integration

**Método a agregar en `CatalogManager`:**

```cpp
void CatalogManager::syncCatalogMongoDBToPostgres() {
    // Similar a syncCatalogMariaDBToPostgres y syncCatalogMSSQLToPostgres
    // Descubre colecciones usando MongoDBEngine
    // Inserta/actualiza en metadata.catalog
}
```

### 4. StreamingData Integration

**Thread nuevo en `StreamingData`:**

```cpp
void StreamingData::mongoTransferThread() {
    Logger::info(LogCategory::MONITORING, "MongoDB transfer thread started");
    while (running) {
        try {
            // Verificar cada hora si hay colecciones que necesitan sync
            auto startTime = std::chrono::high_resolution_clock::now();
            mongoToPg.transferDataMongoDBToPostgresParallel();
            auto endTime = std::chrono::high_resolution_clock::now();

            auto duration = std::chrono::duration_cast<std::chrono::seconds>(endTime - startTime);
            Logger::info(LogCategory::MONITORING,
                        "MongoDB transfer cycle completed in " +
                        std::to_string(duration.count()) + " seconds");
        } catch (const std::exception &e) {
            Logger::error(LogCategory::MONITORING, "mongoTransferThread",
                         "ERROR in MongoDB transfer: " + std::string(e.what()));
        }

        // Verificar cada hora (3600 segundos)
        std::this_thread::sleep_for(std::chrono::seconds(3600));
    }
}
```

## Conversión de Tipos MongoDB -> PostgreSQL

| MongoDB Type | PostgreSQL Type | Notas                        |
| ------------ | --------------- | ---------------------------- |
| string       | TEXT            |                              |
| int32        | INTEGER         |                              |
| int64        | BIGINT          |                              |
| double       | NUMERIC         |                              |
| decimal128   | NUMERIC         |                              |
| bool         | BOOLEAN         |                              |
| date         | TIMESTAMP       |                              |
| objectId     | TEXT            | Almacenar como string        |
| array        | JSONB           | Arrays se convierten a JSONB |
| object       | JSONB           | Objetos anidados a JSONB     |
| binary       | BYTEA           |                              |
| null         | NULL            |                              |

## Estructura de Tabla en PostgreSQL

Para cada colección MongoDB, se crea una tabla en PostgreSQL con:

- `_id` (TEXT) - Primary key del documento MongoDB
- Campos planos del documento (si existen)
- `_document` (JSONB) - Documento completo para campos anidados/complejos
- `_created_at` (TIMESTAMP) - Timestamp de inserción en PostgreSQL
- `_updated_at` (TIMESTAMP) - Última actualización

**Ejemplo:**

```sql
CREATE TABLE IF NOT EXISTS mongodb_schema.users (
    _id TEXT PRIMARY KEY,
    name TEXT,
    email TEXT,
    age INTEGER,
    _document JSONB,
    _created_at TIMESTAMP DEFAULT NOW(),
    _updated_at TIMESTAMP DEFAULT NOW()
);
```

## Manejo de Documentos Anidados

**Estrategia:**

1. **Campos planos**: Si un campo es primitivo (string, number, bool), se crea una columna dedicada
2. **Campos anidados**: Si un campo es objeto o array, se almacena en `_document` JSONB
3. **Campos opcionales**: Si un documento no tiene un campo, la columna queda NULL

**Ejemplo de conversión:**

```json
// MongoDB Document
{
  "_id": "507f1f77bcf86cd799439011",
  "name": "John",
  "age": 30,
  "address": {
    "street": "123 Main St",
    "city": "NYC"
  },
  "tags": ["tag1", "tag2"]
}
```

```sql
-- PostgreSQL Row
_id: '507f1f77bcf86cd799439011'
name: 'John'
age: 30
_document: '{"address": {"street": "123 Main St", "city": "NYC"}, "tags": ["tag1", "tag2"]}'
```

## Catalog Entry para MongoDB

**Campos en `metadata.catalog`:**

- `schema_name`: Nombre de la base de datos MongoDB
- `table_name`: Nombre de la colección
- `db_engine`: `'MongoDB'`
- `status`: `'FULL_LOAD'` (siempre)
- `pk_strategy`: `'FULL_LOAD'` (siempre)
- `pk_columns`: `'_id'` (siempre)
- `has_pk`: `true` (siempre)
- `last_sync_time`: Timestamp de última sincronización
- `connection_string`: Connection string de MongoDB

## Verificación de Cambios Diarios

**Lógica:**

```cpp
bool shouldSync = false;

if (tableInfo.last_sync_time.empty()) {
    shouldSync = true; // Primera vez
} else {
    // Parsear timestamp
    std::tm tm = {};
    std::istringstream ss(tableInfo.last_sync_time);
    ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");

    auto lastSync = std::chrono::system_clock::from_time_t(std::mktime(&tm));
    auto now = std::chrono::system_clock::now();
    auto hours = std::chrono::duration_cast<std::chrono::hours>(now - lastSync).count();

    shouldSync = (hours >= 24);
}
```

## Consideraciones de Performance

1. **Batch Processing**: Insertar documentos en batches de 10,000-25,000
2. **Paralelización**: Procesar múltiples colecciones en paralelo
3. **Índices**: Crear índices en `_id` y campos comunes después del TRUNCATE
4. **Memory Management**: Liberar documentos BSON después de procesar
5. **Connection Pooling**: Reutilizar conexiones MongoDB cuando sea posible

## Manejo de Errores

1. **Conexión fallida**: Log error, continuar con siguiente colección
2. **Documento corrupto**: Log warning, saltar documento, continuar
3. **Error de conversión**: Usar valor por defecto, log warning
4. **TRUNCATE fallido**: Log error, no continuar con INSERT
5. **INSERT fallido**: Rollback, log error, marcar colección como error

## Archivos a Crear/Modificar

### Nuevos Archivos:

1. `include/engines/mongodb_engine.h`
2. `src/engines/mongodb_engine.cpp`
3. `include/sync/MongoDBToPostgres.h`
4. `src/sync/MongoDBToPostgres.cpp`

### Archivos a Modificar:

1. `include/sync/StreamingData.h` - Agregar `MongoDBToPostgres mongoToPg;` y `void mongoTransferThread();`
2. `src/sync/StreamingData.cpp` - Implementar `mongoTransferThread()` y agregar al `run()`
3. `include/catalog/catalog_manager.h` - Agregar `void syncCatalogMongoDBToPostgres();`
4. `src/catalog/catalog_manager.cpp` - Implementar `syncCatalogMongoDBToPostgres()`
5. `CMakeLists.txt` - Ya tiene `mongoc-1.0` y `bson-1.0`, verificar que esté correcto

## Dependencias

- `libmongoc-1.0` (ya en CMakeLists.txt)
- `libbson-1.0` (ya en CMakeLists.txt)
- PostgreSQL (pqxx) - ya existe
- JSON (nlohmann/json) - ya existe

## Testing

1. **Unit Tests**: Probar conversión de tipos, parsing de connection string
2. **Integration Tests**: Probar sync completo con MongoDB real
3. **Performance Tests**: Medir tiempo de sync para colecciones grandes
4. **Error Handling**: Probar manejo de errores (conexión fallida, documentos corruptos)

## Ventajas de este Diseño

1. **Simplicidad**: TRUNCATE + FULL_LOAD es más simple que incremental
2. **Consistencia**: Siempre datos frescos (máximo 24 horas de desfase)
3. **Robustez**: No hay problemas de sincronización incremental
4. **Mantenibilidad**: Código más simple, menos edge cases
5. **Escalabilidad**: Puede manejar colecciones grandes con batching

## Limitaciones

1. **No hay sync incremental**: Siempre recarga completa
2. **Overhead**: TRUNCATE + INSERT completo puede ser costoso para colecciones grandes
3. **Ventana de 24 horas**: Máximo desfase de 1 día
4. **No hay detección de cambios**: Siempre recarga todo, incluso si no hay cambios

## Próximos Pasos

1. Implementar `MongoDBEngine`
2. Implementar `MongoDBToPostgres`
3. Integrar en `CatalogManager`
4. Integrar en `StreamingData`
5. Probar con MongoDB real
6. Optimizar performance si es necesario
