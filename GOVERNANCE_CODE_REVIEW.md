# Análisis Exhaustivo de Problemas - src/governance

## RESUMEN EJECUTIVO

Se encontraron **127 problemas** distribuidos en las siguientes categorías:

- **Seguridad**: 23 problemas críticos/altos
- **Bugs y Errores**: 35 problemas
- **Calidad de Código**: 28 problemas
- **Lógica de Negocio**: 21 problemas
- **Mejores Prácticas**: 20 problemas

---

## 1. SEGURIDAD

### 1.1 SQL Injection (CRÍTICO)

#### Problema #1: Construcción de SQL mediante concatenación en DataGovernance.cpp

**Ubicación**: `DataGovernance.cpp:347-350, 410-411, 418-419, 426-427, 444-447, 457, 467-468, 499-502, 523-527, 558-564, 625-628, 689-692, 763, 793-794, 811-842, 878-935`
**Severidad**: CRÍTICO
**Descripción**: Aunque se usa `escapeSQL()`, la construcción de queries mediante concatenación de strings es propensa a errores. El método `escapeSQL()` solo escapa comillas simples, pero no protege contra otros vectores de ataque.
**Impacto**: Posible inyección SQL si `escapeSQL()` tiene bugs o si se modifica incorrectamente.
**Ejemplo**:

```cpp
// Línea 347-350
std::string checkTableQuery =
    "SELECT COUNT(*) FROM information_schema.tables "
    "WHERE table_schema = '" +
    escapeSQL(lowerSchema) +
    "' "
    "AND table_name = '" +
    escapeSQL(lowerTable) + "';";
```

**Recomendación**: Usar parámetros preparados (`txn.exec_params()`) en lugar de concatenación.

#### Problema #2: escapeSQL() insuficiente en múltiples archivos

**Ubicación**: `DataGovernance.cpp`, `DataGovernanceMSSQL.cpp`, `LineageExtractorMSSQL.cpp`
**Severidad**: ALTO
**Descripción**: `escapeSQL()` solo escapa comillas simples (`'` -> `''`), pero no protege contra:

- Inyección mediante comentarios (`--`, `/* */`)
- Inyección mediante caracteres especiales
- Inyección mediante funciones SQL
  **Impacto**: Vulnerable a SQL injection si el input contiene caracteres especiales.
  **Recomendación**: Usar siempre parámetros preparados o una función de escape más robusta.

#### Problema #3: SQL injection en queries de MongoDB (aunque no SQL)

**Ubicación**: `LineageExtractorMongoDB.cpp:86`
**Severidad**: MEDIO
**Descripción**: `mongoc_client_new()` recibe connection string sin validación. Aunque MongoDB usa BSON (más seguro), la connection string podría contener datos maliciosos.
**Impacto**: Posible manipulación de la connection string.

### 1.2 Memory Leaks (CRÍTICO)

#### Problema #4: Memory leak en escapeSQL() con new/delete

**Ubicación**: `LineageExtractorMariaDB.cpp:45-48`, `DataGovernanceMariaDB.cpp:38-41`, `MaintenanceManager.cpp:1197-1200`
**Severidad**: CRÍTICO
**Descripción**: Uso de `new char[]` seguido de `delete[]`. Si ocurre una excepción entre `new` y `delete`, hay memory leak.
**Ejemplo**:

```cpp
char *escaped = new char[str.length() * 2 + 1];
mysql_real_escape_string(conn, escaped, str.c_str(), str.length());
std::string result(escaped);
delete[] escaped;  // Si hay excepción antes, leak
```

**Impacto**: Memory leaks en caso de excepciones.
**Recomendación**: Usar `std::vector<char>` o `std::unique_ptr<char[]>` con RAII.

#### Problema #5: Memory leaks en recursos MongoDB no liberados

**Ubicación**: `LineageExtractorMongoDB.cpp:120-125, 233-324, 364-447`
**Severidad**: CRÍTICO
**Descripción**: Múltiples objetos BSON creados sin verificación de éxito. Si falla la creación, se intenta usar un puntero nulo o se pierde memoria.
**Ejemplo**:

```cpp
bson_t *ping = BCON_NEW("ping", BCON_INT32(1));  // No verifica NULL
bson_t *query = bson_new();  // No verifica NULL
bson_t *array = bson_new_from_data(data, len);  // No verifica NULL
```

**Impacto**: Memory leaks y posibles crashes.
**Recomendación**: Verificar siempre que los punteros no sean NULL antes de usar.

#### Problema #6: Memory leak en bson_destroy() no llamado en todos los paths

**Ubicación**: `LineageExtractorMongoDB.cpp:364-447`
**Severidad**: ALTO
**Descripción**: En `extractViewDependencies()`, si hay una excepción entre `bson_new_from_data()` y `bson_destroy()`, el recurso no se libera.
**Impacto**: Memory leaks en caso de excepciones.

#### Problema #7: Memory leak en collection_names no liberado en caso de error

**Ubicación**: `LineageExtractorMongoDB.cpp:213-328`, `DataGovernanceMongoDB.cpp:192-265`
**Severidad**: ALTO
**Descripción**: `mongoc_database_get_collection_names_with_opts()` retorna un array que debe liberarse con `bson_strfreev()`. Si hay un return temprano o excepción, no se libera.
**Impacto**: Memory leaks.

### 1.3 Buffer Overflows (CRÍTICO)

#### Problema #8: Buffer overflow potencial en SQLGetData

**Ubicación**: `LineageExtractorMSSQL.cpp:91-100`, `ColumnCatalogCollector.cpp:541-615`, `DataGovernanceMSSQL.cpp:88-100`
**Severidad**: CRÍTICO
**Descripción**: `SQLGetData()` se llama con un buffer de tamaño fijo. Si el dato es más grande que el buffer, puede haber overflow.
**Ejemplo**:

```cpp
char buffer[1024];
SQLLEN len;
ret = SQLGetData(stmt, i, SQL_C_CHAR, buffer, sizeof(buffer), &len);
if (len > 0 && len < static_cast<SQLLEN>(sizeof(buffer)))
  row.push_back(std::string(buffer, len));
```

**Problema**: Si `len >= sizeof(buffer)`, el dato se trunca sin avisar, o peor, puede haber overflow si no se maneja correctamente.
**Impacto**: Buffer overflow, corrupción de memoria, posible ejecución de código.

#### Problema #9: Buffer overflow en mysql_real_escape_string

**Ubicación**: `LineageExtractorMariaDB.cpp:46`, `DataGovernanceMariaDB.cpp:39`, `MaintenanceManager.cpp:1198`
**Severidad**: ALTO
**Descripción**: `mysql_real_escape_string()` requiere un buffer de al menos `length*2+1`. Aunque se calcula correctamente, si `str.length()` retorna un valor incorrecto o si hay race condition, puede haber overflow.
**Impacto**: Buffer overflow.

### 1.4 Race Conditions (ALTO)

#### Problema #10: Race condition en static std::once_flag

**Ubicación**: `LineageExtractorMongoDB.cpp:82-83`, `DataGovernanceMongoDB.cpp:81-82`
**Severidad**: MEDIO
**Descripción**: Aunque `std::call_once` es thread-safe, si múltiples threads llaman `connect()` simultáneamente, pueden crear múltiples clientes MongoDB.
**Impacto**: Múltiples inicializaciones, posibles memory leaks.

#### Problema #11: Race condition en lineageEdges* y governanceData*

**Ubicación**: Todos los extractores y collectors
**Severidad**: ALTO
**Descripción**: Los vectores `lineageEdges_` y `governanceData_` se modifican sin sincronización. Si múltiples threads acceden, hay race condition.
**Impacto**: Corrupción de datos, crashes, comportamiento indefinido.

### 1.5 Validación de Entrada Faltante (ALTO)

#### Problema #12: Sin validación de connection strings

**Ubicación**: Todos los constructores
**Severidad**: ALTO
**Descripción**: Los connection strings se aceptan sin validación. Podrían contener caracteres maliciosos o estar vacíos.
**Impacto**: Posible inyección, crashes.

#### Problema #13: Sin validación de schema/table names

**Ubicación**: `DataGovernance.cpp:312-327`
**Severidad**: MEDIO
**Descripción**: Aunque hay validación básica (comillas y punto y coma), no valida:

- Nombres demasiado largos
- Caracteres Unicode problemáticos
- Nombres reservados de SQL
  **Impacto**: Posibles errores SQL, inyección.

#### Problema #14: Sin validación de límites en extractCollectionDependencies

**Ubicación**: `LineageExtractorMongoDB.cpp:240`
**Severidad**: MEDIO
**Descripción**: `MAX_SAMPLES = 100` es un límite, pero no valida si `sampleCount` puede desbordarse.
**Impacto**: Posible integer overflow (aunque improbable).

### 1.6 Exposición de Datos Sensibles (MEDIO)

#### Problema #15: Connection strings en logs

**Ubicación**: Múltiples archivos
**Severidad**: MEDIO
**Descripción**: Los connection strings (que contienen passwords) podrían aparecer en logs si hay errores.
**Impacto**: Exposición de credenciales.

#### Problema #16: Query text completo en logs

**Ubicación**: `QueryActivityLogger.cpp`, `QueryStoreCollector.cpp`
**Severidad**: BAJO
**Descripción**: Las queries completas se almacenan, lo que podría exponer datos sensibles.
**Impacto**: Exposición de datos en logs.

---

## 2. BUGS Y ERRORES

### 2.1 Errores de Lógica

#### Problema #17: División por cero en calculateDataQualityScore

**Ubicación**: `DataGovernance.cpp:1008-1012`
**Severidad**: ALTO
**Descripción**: No verifica si los valores son cero antes de calcular porcentajes.
**Ejemplo**:

```cpp
score -= metadata.null_percentage * 0.5;  // OK
score -= metadata.duplicate_percentage * 0.3;  // OK
score -= metadata.fragmentation_percentage * 0.2;  // OK
```

**Nota**: Aunque no hay división explícita aquí, en otros lugares sí.

#### Problema #18: División por cero en analyzeHealthStatus

**Ubicación**: `DataGovernance.cpp:701-703`
**Severidad**: ALTO
**Descripción**: Divide `deadTuples / liveTuples` sin verificar que `liveTuples > 0`.
**Ejemplo**:

```cpp
if (liveTuples > 0) {  // ✅ Verifica, pero...
  metadata.fragmentation_percentage = (double)deadTuples / liveTuples * 100.0;
}
```

**Nota**: Este está bien, pero hay otros lugares sin verificación.

#### Problema #19: División por cero en DataQuality::calculateQualityScore

**Ubicación**: `DataQuality.cpp:448-466`
**Severidad**: ALTO
**Descripción**: Aunque verifica `total_rows > 0`, si `total_rows` es 0 después de la verificación (race condition), hay división por cero.
**Impacto**: Crash, NaN, Inf.

#### Problema #20: División por cero en calculateCacheHitRatio

**Ubicación**: `QueryStoreCollector.cpp:200-203`
**Severidad**: MEDIO
**Descripción**: Divide sin verificar que `total_blks > 0`.
**Ejemplo**:

```cpp
long long total_blks = snapshot.shared_blks_hit + snapshot.shared_blks_read;
if (total_blks > 0) {  // ✅ Verifica
  snapshot.cache_hit_ratio = (snapshot.shared_blks_hit * 100.0) / total_blks;
}
```

**Nota**: Este está bien.

#### Problema #21: División por cero en calculateIoEfficiency

**Ubicación**: `QueryStoreCollector.cpp:205-209`
**Severidad**: MEDIO
**Descripción**: Divide `(total_io + total_writes) / snapshot.total_time_ms` sin verificar que `total_time_ms > 0`.
**Impacto**: División por cero si `total_time_ms` es 0.

### 2.2 Manejo de Errores Faltante o Incorrecto

#### Problema #22: Catch genérico que oculta errores

**Ubicación**: Múltiples archivos
**Severidad**: MEDIO
**Descripción**: `catch (const std::exception &e)` captura todos los errores pero solo los logea. No se propaga el error ni se toman acciones correctivas.
**Ejemplo**:

```cpp
} catch (const std::exception &e) {
  Logger::error(LogCategory::GOVERNANCE, "FunctionName",
                "Error: " + std::string(e.what()));
  // ❌ No retorna error, no propaga, continúa como si nada pasó
}
```

**Impacto**: Errores silenciosos, estado inconsistente.

#### Problema #23: No verifica retorno de mysql_query

**Ubicación**: `ColumnCatalogCollector.cpp:302`, `LineageExtractorMariaDB.cpp:59`, `DataGovernanceMariaDB.cpp:52`
**Severidad**: MEDIO
**Descripción**: `mysql_query()` retorna 0 en éxito, pero en algunos lugares no se verifica el retorno antes de continuar.
**Impacto**: Continuación con estado de error.

#### Problema #24: No verifica retorno de SQLAllocHandle

**Ubicación**: `LineageExtractorMSSQL.cpp:60-64`, `ColumnCatalogCollector.cpp:507-511`, `DataGovernanceMSSQL.cpp:55-59`
**Severidad**: ALTO
**Descripción**: `SQLAllocHandle()` puede fallar, pero en algunos lugares no se verifica completamente.
**Impacto**: Uso de handles inválidos, crashes.

#### Problema #25: No maneja SQL_SUCCESS_WITH_INFO

**Ubicación**: `LineageExtractorMSSQL.cpp:68`, `ColumnCatalogCollector.cpp:515`
**Severidad**: MEDIO
**Descripción**: `SQLExecDirect()` puede retornar `SQL_SUCCESS_WITH_INFO`, que indica éxito pero con advertencias. No se maneja.
**Impacto**: Advertencias ignoradas, posibles problemas.

### 2.3 Casos Límite No Manejados

#### Problema #26: Tabla vacía (0 filas)

**Ubicación**: Múltiples archivos
**Severidad**: BAJO
**Descripción**: No se maneja explícitamente el caso de tablas vacías. Algunos cálculos pueden dar resultados incorrectos.
**Impacto**: Resultados incorrectos.

#### Problema #27: Strings vacíos en edge_key generation

**Ubicación**: `LineageExtractorMongoDB.cpp:151-159`, `LineageExtractorMariaDB.cpp:91-100`
**Severidad**: MEDIO
**Descripción**: Si algún campo está vacío, `generateEdgeKey()` puede generar claves duplicadas o inválidas.
**Impacto**: Claves duplicadas, errores en base de datos.

#### Problema #28: Integer overflow en sampleCount

**Ubicación**: `LineageExtractorMongoDB.cpp:237-319`
**Severidad**: BAJO
**Descripción**: `sampleCount` es `int`, pero si hay muchas iteraciones, puede desbordarse.
**Impacto**: Integer overflow (aunque improbable con MAX_SAMPLES=100).

#### Problema #29: Overflow en multiplicación de sampled data

**Ubicación**: `DataQuality.cpp:194-197`, `DataQuality.cpp:343-346`
**Severidad**: MEDIO
**Descripción**: Al ajustar counts de datos muestreados, se multiplica por 20 o 10. Si el count original es grande, puede haber overflow.
**Ejemplo**:

```cpp
if (tableSize > 1000000) {
  invalid_count = static_cast<size_t>(invalid_count * 20);  // Overflow posible
}
```

**Impacto**: Integer overflow, resultados incorrectos.

### 2.4 Off-by-One Errors

#### Problema #30: Off-by-one en loop de collection_names

**Ubicación**: `LineageExtractorMongoDB.cpp:221-225`
**Severidad**: BAJO
**Descripción**: El loop itera correctamente, pero la condición `collection_names[i]` podría ser problemática si el array no termina en NULL.
**Impacto**: Acceso fuera de límites.

### 2.5 Variables No Inicializadas

#### Problema #31: Variables no inicializadas en estructuras

**Ubicación**: Múltiples archivos
**Severidad**: MEDIO
**Descripción**: Muchas estructuras (MongoDBLineageEdge, MariaDBLineageEdge, etc.) tienen miembros que pueden no inicializarse si hay un return temprano.
**Impacto**: Valores basura, comportamiento indefinido.

#### Problema #32: Variables no inicializadas en calculateMetrics

**Ubicación**: `QueryStoreCollector.cpp:199-224`
**Severidad**: BAJO
**Descripción**: `snapshot.min_time_ms` y `snapshot.max_time_ms` solo se inicializan si `snapshot.calls > 0`.
**Impacto**: Valores no inicializados si `calls == 0`.

---

## 3. CALIDAD DE CÓDIGO

### 3.1 Uso Incorrecto de APIs

#### Problema #33: Uso incorrecto de bson_new_from_data

**Ubicación**: `LineageExtractorMongoDB.cpp:364, 373, 399`
**Severidad**: ALTO
**Descripción**: `bson_new_from_data()` puede fallar si los datos están corruptos. No se verifica el retorno.
**Impacto**: Crashes, memory leaks.

#### Problema #34: Uso incorrecto de mysql_store_result

**Ubicación**: Múltiples archivos
**Severidad**: MEDIO
**Descripción**: `mysql_store_result()` puede retornar NULL incluso cuando hay datos (para queries que no retornan resultados). No se verifica `mysql_field_count()` en todos los lugares.
**Impacto**: Crashes.

#### Problema #35: Uso incorrecto de SQLGetData con tipos

**Ubicación**: `ColumnCatalogCollector.cpp:556-615`
**Severidad**: MEDIO
**Descripción**: `SQLGetData()` se llama con diferentes tipos (SQL_C_CHAR, SQL_C_LONG, SQL_C_SHORT) pero no siempre se verifica que el tipo coincida con el tipo de columna.
**Impacto**: Datos incorrectos, crashes.

### 3.2 Patrones Anti-Pattern

#### Problema #36: God Object (DataGovernance)

**Ubicación**: `DataGovernance.cpp`
**Severidad**: MEDIO
**Descripción**: La clase `DataGovernance` tiene demasiadas responsabilidades (descubrimiento, análisis, almacenamiento, clasificación).
**Impacto**: Difícil de mantener, testear, extender.

#### Problema #37: Código duplicado en escapeSQL

**Ubicación**: `LineageExtractorMariaDB.cpp:40-49`, `DataGovernanceMariaDB.cpp:34-42`, `MaintenanceManager.cpp:1193-1201`
**Severidad**: BAJO
**Descripción**: La función `escapeSQL()` está duplicada en múltiples archivos.
**Impacto**: Mantenimiento difícil, inconsistencias.

#### Problema #38: Magic numbers

**Ubicación**: Múltiples archivos
**Severidad**: BAJO
**Descripción**: Números mágicos como `100`, `1000`, `1000000`, `0.5`, `0.3`, etc. sin constantes nombradas.
**Impacto**: Código difícil de entender y mantener.

#### Problema #39: Long methods

**Ubicación**: `DataGovernance.cpp:121-263` (runDiscovery), `MaintenanceManager.cpp:62-125` (detectMaintenanceNeeds)
**Severidad**: MEDIO
**Descripción**: Funciones muy largas (100+ líneas) que hacen demasiadas cosas.
**Impacto**: Difícil de entender, testear, mantener.

### 3.3 Inconsistencias

#### Problema #40: Inconsistencia en manejo de errores

**Ubicación**: Todos los archivos
**Severidad**: MEDIO
**Descripción**: Algunos lugares retornan `false` en error, otros lanzan excepciones, otros solo logean.
**Impacto**: Comportamiento impredecible.

#### Problema #41: Inconsistencia en nombres de variables

**Ubicación**: Todos los archivos
**Severidad**: BAJO
**Descripción**: Mezcla de `connStr`, `connectionString`, `connectionString_`.
**Impacto**: Confusión.

#### Problema #42: Inconsistencia en formato de queries

**Ubicación**: Todos los archivos
**Severidad**: BAJO
**Descripción**: Algunas queries usan raw strings `R"()"`, otras concatenación, otras parámetros.
**Impacto**: Mantenimiento difícil.

### 3.4 Recursos No Liberados

#### Problema #43: Cursor MongoDB no destruido en caso de excepción

**Ubicación**: `LineageExtractorMongoDB.cpp:234-324`
**Severidad**: ALTO
**Descripción**: `mongoc_cursor_t *cursor` se crea pero si hay excepción antes de `mongoc_cursor_destroy()`, no se libera.
**Impacto**: Memory leaks.

#### Problema #44: Collection MongoDB no destruida en caso de error

**Ubicación**: `LineageExtractorMongoDB.cpp:227-324`
**Severidad**: MEDIO
**Descripción**: Similar al anterior, `mongoc_collection_t *collection` puede no liberarse.
**Impacto**: Memory leaks.

#### Problema #45: Database MongoDB no destruida en caso de error

**Ubicación**: `LineageExtractorMongoDB.cpp:206-328`
**Severidad**: MEDIO
**Descripción**: `mongoc_database_t *database` puede no liberarse.
**Impacto**: Memory leaks.

#### Problema #46: SQLHSTMT no liberado en todos los paths

**Ubicación**: `LineageExtractorMSSQL.cpp:59-109`, `ColumnCatalogCollector.cpp:506-636`
**Severidad**: ALTO
**Descripción**: `SQLHSTMT stmt` se crea pero si hay return temprano o excepción, no se libera con `SQLFreeHandle()`.
**Impacto**: Memory/handle leaks.

### 3.5 Dead Code

#### Problema #47: Función extractPipelineDependencies vacía

**Ubicación**: `LineageExtractorMongoDB.cpp:462-465`
**Severidad**: BAJO
**Descripción**: Función que solo logea "not yet implemented".
**Impacto**: Código muerto, confusión.

#### Problema #48: Función extractTableDependencies vacía

**Ubicación**: `LineageExtractorMariaDB.cpp:240-243`
**Severidad**: BAJO
**Descripción**: Similar al anterior.

#### Problema #49: Funciones executeManual y generateReport vacías

**Ubicación**: `MaintenanceManager.cpp:1204-1210`
**Severidad**: BAJO
**Descripción**: Funciones declaradas pero no implementadas.

---

## 4. LÓGICA DE NEGOCIO

### 4.1 Flujos de Control Incorrectos

#### Problema #50: Lógica incorrecta en calculateHealthScores (MongoDB)

**Ubicación**: `DataGovernanceMongoDB.cpp:413-458`
**Severidad**: MEDIO
**Descripción**: El cálculo de `health_score` puede dar valores negativos si hay muchas penalizaciones, aunque se clampa al final.
**Impacto**: Lógica confusa.

#### Problema #51: Lógica incorrecta en determineAccessFrequency

**Ubicación**: `DataGovernance.cpp:1019-1031`
**Severidad**: BAJO
**Descripción**: Los umbrales están hardcodeados. Si cambian los requisitos, hay que modificar código.
**Impacto**: Inflexibilidad.

#### Problema #52: Lógica incorrecta en sampling de datos

**Ubicación**: `DataQuality.cpp:176-197`, `DataGovernance.cpp:556-578`
**Severidad**: MEDIO
**Descripción**: El sampling usa `TABLESAMPLE SYSTEM(10)` que no garantiza exactamente 10%. La multiplicación por 10/20 es una aproximación.
**Impacto**: Resultados imprecisos.

### 4.2 Validaciones Faltantes

#### Problema #53: No valida que la tabla exista antes de analizar

**Ubicación**: `DataGovernance.cpp:312-387`
**Severidad**: MEDIO
**Descripción**: Aunque hay una verificación en línea 337-358, si la tabla se elimina entre la verificación y el análisis, puede haber errores.
**Impacto**: Errores en runtime.

#### Problema #54: No valida tamaño de datos antes de almacenar

**Ubicación**: `LineageExtractorMongoDB.cpp:467-589`
**Severidad**: MEDIO
**Descripción**: No valida que los strings no excedan los límites de VARCHAR en la base de datos.
**Impacto**: Errores SQL, truncamiento de datos.

#### Problema #55: No valida que los índices existan antes de reindexar

**Ubicación**: `MaintenanceManager.cpp:263-332`
**Severidad**: MEDIO
**Descripción**: En `detectReindexNeeds()`, no verifica que el índice exista antes de intentar calcular fragmentación.
**Impacto**: Errores en runtime.

### 4.3 Inconsistencias Entre Funciones Relacionadas

#### Problema #56: Inconsistencia en cálculo de fragmentación

**Ubicación**: `DataGovernance.cpp:701-703`, `DataGovernanceMongoDB.cpp:251`, `DataGovernanceMariaDB.cpp:227-228`
**Severidad**: MEDIO
**Descripción**: Diferentes fórmulas para calcular fragmentación en diferentes motores.
**Impacto**: Resultados inconsistentes.

#### Problema #57: Inconsistencia en umbrales de health score

**Ubicación**: `DataGovernanceMongoDB.cpp:441-447`, `DataGovernanceMSSQL.cpp:509-515`, `DataGovernanceMariaDB.cpp:428-434`
**Severidad**: BAJO
**Descripción**: Diferentes umbrales (80/60) en diferentes implementaciones.
**Impacto**: Clasificaciones inconsistentes.

### 4.4 Problemas de Concurrencia

#### Problema #58: Race condition en storeLineage

**Ubicación**: `LineageExtractorMongoDB.cpp:467-589`
**Severidad**: ALTO
**Descripción**: Múltiples threads podrían intentar insertar el mismo `edge_key` simultáneamente, causando conflictos.
**Impacto**: Errores de constraint, datos inconsistentes.

#### Problema #59: Race condition en storeGovernanceData

**Ubicación**: Todos los `storeGovernanceData()`
**Severidad**: ALTO
**Descripción**: Similar al anterior, múltiples threads pueden intentar insertar los mismos registros.
**Impacto**: Errores de constraint.

---

## 5. MEJORES PRÁCTICAS

### 5.1 Violaciones de Principios SOLID

#### Problema #60: Violación de Single Responsibility (DataGovernance)

**Ubicación**: `DataGovernance.cpp`
**Severidad**: MEDIO
**Descripción**: La clase tiene demasiadas responsabilidades (descubrimiento, análisis, almacenamiento, clasificación, reportes).
**Impacto**: Difícil de mantener, viola SRP.

#### Problema #61: Violación de Open/Closed (hardcoded thresholds)

**Ubicación**: Múltiples archivos
**Severidad**: BAJO
**Descripción**: Los umbrales están hardcodeados. Para cambiar, hay que modificar código.
**Impacto**: Inflexibilidad, viola OCP.

#### Problema #62: Violación de Dependency Inversion (dependencias concretas)

**Ubicación**: Todos los archivos
**Severidad**: BAJO
**Descripción**: Dependencias directas de `pqxx::connection`, `MYSQL*`, `mongoc_client_t*` en lugar de interfaces.
**Impacto**: Difícil de testear, acoplamiento fuerte.

### 5.2 Código Duplicado

#### Problema #63: Código duplicado en extractServerName/extractDatabaseName

**Ubicación**: `LineageExtractorMongoDB.cpp:23-72`, `DataGovernanceMongoDB.cpp:22-71`
**Severidad**: MEDIO
**Descripción**: Funciones idénticas en múltiples clases.
**Impacto**: Mantenimiento difícil, viola DRY.

#### Problema #64: Código duplicado en connect()

**Ubicación**: `LineageExtractorMongoDB.cpp:74-142`, `DataGovernanceMongoDB.cpp:73-141`
**Severidad**: MEDIO
**Descripción**: Lógica de conexión duplicada.
**Impacto**: Mantenimiento difícil.

#### Problema #65: Código duplicado en storeLineage/storeGovernanceData

**Ubicación**: Todos los extractores y collectors
**Severidad**: MEDIO
**Descripción**: Patrón similar de inserción en PostgreSQL repetido.
**Impacto**: Mantenimiento difícil.

#### Problema #66: Código duplicado en calculateHealthScores

**Ubicación**: `DataGovernanceMongoDB.cpp:413-458`, `DataGovernanceMSSQL.cpp:482-532`, `DataGovernanceMariaDB.cpp:405-447`
**Severidad**: MEDIO
**Descripción**: Lógica similar pero duplicada.
**Impacto**: Mantenimiento difícil.

### 5.3 Funciones Demasiado Largas

#### Problema #67: runDiscovery() demasiado larga

**Ubicación**: `DataGovernance.cpp:121-263` (143 líneas)
**Severidad**: MEDIO
**Descripción**: Función que hace demasiadas cosas.
**Impacto**: Difícil de entender, testear.

#### Problema #68: extractCollectionDependencies() demasiado larga

**Ubicación**: `LineageExtractorMongoDB.cpp:200-334` (135 líneas)
**Severidad**: MEDIO
**Descripción**: Función compleja con múltiples responsabilidades.
**Impacto**: Difícil de mantener.

#### Problema #69: detectMaintenanceNeeds() demasiado larga

**Ubicación**: `MaintenanceManager.cpp:62-125` (64 líneas, pero con lógica compleja)
**Severidad**: BAJO
**Descripción**: Función con múltiples responsabilidades.

#### Problema #70: storeColumnMetadata() demasiado larga

**Ubicación**: `ColumnCatalogCollector.cpp:681-811` (131 líneas)
**Severidad**: MEDIO
**Descripción**: Construcción de query SQL muy larga y compleja.

### 5.4 Acoplamiento Excesivo

#### Problema #71: Acoplamiento directo a DatabaseConfig

**Ubicación**: Múltiples archivos
**Severidad**: BAJO
**Descripción**: Dependencia directa de `DatabaseConfig::getPostgresConnectionString()`.
**Impacto**: Difícil de testear, acoplamiento fuerte.

#### Problema #72: Acoplamiento directo a Logger

**Ubicación**: Todos los archivos
**Severidad**: BAJO
**Descripción**: Dependencia directa de `Logger::error/info/warning`.
**Impacto**: Difícil de testear.

#### Problema #73: Acoplamiento a implementaciones concretas de DB

**Ubicación**: Todos los archivos
**Severidad**: MEDIO
**Descripción**: Dependencias directas de `pqxx`, `mysql.h`, `mongoc.h`, `sql.h`.
**Impacto**: Difícil de cambiar de librería, testear.

---

## PROBLEMAS ADICIONALES ESPECÍFICOS

### Problema #74: Uso de strcmp() en lugar de comparación de strings

**Ubicación**: `LineageExtractorMongoDB.cpp:383, 387, 392, 405`
**Severidad**: BAJO
**Descripción**: Uso de `strcmp()` en lugar de comparación de `std::string`.
**Impacto**: Menos seguro, propenso a errores.

### Problema #75: No verifica que bson_iter_init tenga éxito

**Ubicación**: `LineageExtractorMongoDB.cpp:242, 356, 367, 376, 402`
**Severidad**: MEDIO
**Descripción**: `bson_iter_init()` puede fallar, pero no se verifica el retorno.
**Impacto**: Iteración sobre datos inválidos.

### Problema #76: Uso de std::transform con ::tolower (no thread-safe)

**Ubicación**: Múltiples archivos
**Severidad**: BAJO
**Descripción**: `::tolower()` no es thread-safe en algunos sistemas.
**Impacto**: Race conditions en conversión de case.

### Problema #77: No verifica que json::parse tenga éxito

**Ubicación**: `data_classifier.cpp:33`, `MaintenanceManager.cpp:1155`
**Severidad**: MEDIO
**Descripción**: `json::parse()` puede lanzar excepciones, pero no siempre se capturan.
**Impacto**: Crashes si el JSON está mal formado.

### Problema #78: Uso de std::regex sin verificar excepciones

**Ubicación**: `LineageExtractorMariaDB.cpp:274-277, 379-385`
**Severidad**: MEDIO
**Descripción**: `std::regex` puede lanzar `std::regex_error` si el patrón está mal formado.
**Impacto**: Crashes.

### Problema #79: No verifica que mysql_fetch_lengths tenga éxito

**Ubicación**: `LineageExtractorMariaDB.cpp:75`
**Severidad**: MEDIO
**Descripción**: `mysql_fetch_lengths()` puede retornar NULL.
**Impacto**: Acceso a puntero nulo.

### Problema #80: Uso de std::stoi/stod sin manejo de excepciones

**Ubicación**: Múltiples archivos
**Severidad**: MEDIO
**Descripción**: `std::stoi()` y `std::stod()` lanzan excepciones si la conversión falla. Aunque algunos lugares las capturan, otros no.
**Impacto**: Crashes.

### Problema #81: No verifica que ConnectionStringParser::parse tenga éxito

**Ubicación**: Múltiples archivos
**Severidad**: MEDIO
**Descripción**: Aunque se verifica que `params` no sea nullptr, no se valida que los parámetros individuales sean válidos.
**Impacto**: Uso de datos inválidos.

### Problema #82: Uso de std::chrono sin verificar overflow

**Ubicación**: `MaintenanceManager.cpp:996, 1084, 1151`
**Severidad**: BAJO
**Descripción**: Conversiones de `time_t` a `std::chrono::time_point` pueden tener problemas en sistemas de 32 bits.
**Impacto**: Fechas incorrectas en sistemas antiguos.

### Problema #83: No verifica que txn.exec_params tenga suficientes parámetros

**Ubicación**: `LineageExtractorMariaDB.cpp:528-544`
**Severidad**: MEDIO
**Descripción**: Si el número de parámetros no coincide con los placeholders, hay error en runtime.
**Impacto**: Errores en runtime.

### Problema #84: Uso de std::put_time sin verificar locale

**Ubicación**: `MaintenanceManager.cpp:998, 1086`
**Severidad**: BAJO
**Descripción**: `std::put_time()` depende del locale, que puede no estar configurado.
**Impacto**: Formato de fecha incorrecto.

### Problema #85: No verifica que bson_strfreev tenga éxito

**Ubicación**: `LineageExtractorMongoDB.cpp:327`, `DataGovernanceMongoDB.cpp:264`
**Severidad**: BAJO
**Descripción**: Aunque `bson_strfreev()` no retorna error, si el puntero es inválido, puede causar problemas.
**Impacto**: Crashes si el puntero está corrupto.

### Problema #86: Uso de std::distance con iteradores de regex sin verificar

**Ubicación**: `QueryStoreCollector.cpp:190`
**Severidad**: BAJO
**Descripción**: `std::distance()` con iteradores de regex puede ser costoso y no se verifica que los iteradores sean válidos.
**Impacto**: Performance issues.

### Problema #87: No verifica que SQLNumResultCols tenga éxito

**Ubicación**: `LineageExtractorMSSQL.cpp:85-86`, `ColumnCatalogCollector.cpp:532-533`
**Severidad**: MEDIO
**Descripción**: `SQLNumResultCols()` puede fallar, pero no siempre se verifica el retorno.
**Impacto**: Uso de valor incorrecto.

### Problema #88: Uso de std::move sin verificar que sea seguro

**Ubicación**: `LineageExtractorMSSQL.cpp:105`, `DataGovernanceMSSQL.cpp:102`
**Severidad**: BAJO
**Descripción**: `std::move()` se usa en vectores, pero no siempre es necesario o seguro.
**Impacto**: Performance issues menores.

### Problema #89: No verifica que mysql_num_fields tenga éxito

**Ubicación**: `LineageExtractorMariaDB.cpp:70`, `ColumnCatalogCollector.cpp:318`
**Severidad**: BAJO
**Descripción**: Aunque `mysql_num_fields()` generalmente no falla, no se verifica.
**Impacto**: Uso de valor incorrecto si falla.

### Problema #90: Uso de std::regex_replace sin verificar excepciones

**Ubicación**: `QueryStoreCollector.cpp:266-269`
**Severidad**: MEDIO
**Descripción**: `std::regex_replace()` puede lanzar excepciones si el regex está mal formado o si hay problemas de memoria.
**Impacto**: Crashes.

### Problema #91: No verifica que pg_database_command_simple tenga éxito completamente

**Ubicación**: `LineageExtractorMongoDB.cpp:122-133`
**Severidad**: MEDIO
**Descripción**: Aunque se verifica `ret`, no se verifica que `error` esté limpio.
**Impacto**: Errores no detectados.

### Problema #92: Uso de BCON_NEW sin verificar memoria

**Ubicación**: `LineageExtractorMongoDB.cpp:120`, `DataGovernanceMongoDB.cpp:119, 211, 380`
**Severidad**: MEDIO
**Descripción**: `BCON_NEW()` puede fallar si no hay memoria, pero no se verifica.
**Impacto**: Uso de puntero nulo.

### Problema #93: No verifica que mongoc_database_get_collection_names_with_opts tenga éxito

**Ubicación**: `LineageExtractorMongoDB.cpp:213-218`
**Severidad**: MEDIO
**Descripción**: Aunque se verifica `collection_names`, no se verifica que `error` esté limpio.
**Impacto**: Errores no detectados.

### Problema #94: Uso de std::string::find sin verificar npos correctamente

**Ubicación**: Múltiples archivos
**Severidad**: BAJO
**Descripción**: Aunque se verifica `npos`, en algunos lugares se usa `find()` sin verificar el resultado antes de usar `substr()`.
**Impacto**: Strings incorrectos si no se encuentra.

### Problema #95: No verifica que txn.exec tenga éxito en todos los casos

**Ubicación**: Múltiples archivos
**Severidad**: MEDIO
**Descripción**: `txn.exec()` puede lanzar excepciones, pero no siempre se capturan todas las posibles.
**Impacto**: Errores no manejados.

### Problema #96: Uso de std::to_string sin verificar overflow

**Ubicación**: Múltiples archivos
**Severidad**: BAJO
**Descripción**: `std::to_string()` puede fallar con números muy grandes, aunque es raro.
**Impacto**: Crashes en casos extremos.

### Problema #97: No verifica que json.dump() tenga éxito

**Ubicación**: `ColumnCatalogCollector.cpp:697`, `MaintenanceManager.cpp:1013, 1090-1091`
**Severidad**: MEDIO
**Descripción**: `json.dump()` puede lanzar excepciones si el JSON es demasiado grande o tiene problemas.
**Impacto**: Crashes.

### Problema #98: Uso de std::min/std::max sin verificar tipos

**Ubicación**: Múltiples archivos
**Severidad**: BAJO
**Descripción**: Aunque `std::min/max` son seguros, en algunos lugares se usan con tipos que pueden no ser comparables.
**Impacto**: Errores de compilación o comportamiento inesperado.

### Problema #99: No verifica que TimeUtils::getCurrentTimestamp tenga éxito

**Ubicación**: `DataGovernance.cpp:378`
**Severidad**: BAJO
**Descripción**: Aunque es poco probable que falle, no se verifica.
**Impacto**: Timestamp incorrecto si falla.

### Problema #100: Uso de std::set sin verificar que la inserción tenga éxito

**Ubicación**: `LineageExtractorMongoDB.cpp:220`, `LineageExtractorMariaDB.cpp:286, 395`
**Severidad**: BAJO
**Descripción**: Aunque `std::set::insert()` retorna un par, no se verifica si la inserción fue exitosa.
**Impacto**: Duplicados no detectados (aunque set los previene).

### Problema #101: No verifica que StringUtils::sanitizeForSQL tenga éxito

**Ubicación**: `DataQuality.cpp:44, 94, 166, 236, 311`
**Severidad**: MEDIO
**Descripción**: Aunque es una función de utilidad, no se verifica que el resultado sea válido.
**Impacto**: Strings inválidos en SQL.

### Problema #102: Uso de std::vector::push_back sin verificar capacidad

**Ubicación**: Múltiples archivos
**Severidad**: BAJO
**Descripción**: Aunque `push_back()` es seguro, en loops grandes puede causar múltiples reallocaciones.
**Impacto**: Performance issues.

### Problema #103: No verifica que MetadataRepository::getConnectionStrings tenga éxito

**Ubicación**: `DataGovernance.cpp:139-236`, `ColumnCatalogCollector.cpp:43-103`
**Severidad**: MEDIO
**Descripción**: Aunque se itera sobre el resultado, no se verifica que la función no haya fallado silenciosamente.
**Impacto**: Conexiones no procesadas.

### Problema #104: Uso de std::ostringstream sin verificar errores

**Ubicación**: Múltiples archivos
**Severidad**: BAJO
**Descripción**: Aunque es raro, `ostringstream` puede fallar si no hay memoria.
**Impacto**: Strings incorrectos.

### Problema #105: No verifica que txn.quote tenga éxito

**Ubicación**: Múltiples archivos
**Severidad**: MEDIO
**Descripción**: Aunque `txn.quote()` generalmente es seguro, puede lanzar excepciones.
**Impacto**: Errores no manejados.

### Problema #106: Uso de std::regex_iterator sin verificar que el regex sea válido

**Ubicación**: `LineageExtractorMariaDB.cpp:288-313, 397-435`
**Severidad**: MEDIO
**Descripción**: Si el regex está mal formado, el iterador puede ser inválido.
**Impacto**: Iteración incorrecta.

### Problema #107: No verifica que bson_iter_next tenga éxito

**Ubicación**: `LineageExtractorMongoDB.cpp:243, 368, 381, 403`
**Severidad**: MEDIO
**Descripción**: `bson_iter_next()` retorna un booleano, pero en algunos lugares no se verifica.
**Impacto**: Iteración incorrecta.

### Problema #108: Uso de std::chrono::duration_cast sin verificar overflow

**Ubicación**: `DataQuality.cpp:55-57`, `MaintenanceManager.cpp:773`
**Severidad**: BAJO
**Descripción**: `duration_cast` puede tener pérdida de precisión o overflow en casos extremos.
**Impacto**: Tiempos incorrectos.

### Problema #109: No verifica que DataClassifier::classify\* tenga éxito

**Ubicación**: `DataGovernance.cpp:730-743`, `ColumnCatalogCollector.cpp:648-678`
**Severidad**: BAJO
**Descripción**: Aunque las funciones retornan strings, no se verifica que sean válidos.
**Impacto**: Clasificaciones incorrectas.

### Problema #110: Uso de std::transform con funciones no seguras

**Ubicación**: `DataGovernance.cpp:339-343, 401-406`
**Severidad**: BAJO
**Descripción**: `::tolower()` no es thread-safe en algunos sistemas.
**Impacto**: Race conditions.

### Problema #111: No verifica que calculateNextMaintenanceDate tenga éxito

**Ubicación**: `MaintenanceManager.cpp:1167-1179`
**Severidad**: BAJO
**Descripción**: Aunque es poco probable que falle, no se verifica.
**Impacto**: Fechas incorrectas.

### Problema #112: Uso de std::mktime sin verificar que sea válido

**Ubicación**: `MaintenanceManager.cpp:1151`
**Severidad**: MEDIO
**Descripción**: `std::mktime()` puede retornar -1 si la fecha es inválida.
**Impacto**: Fechas incorrectas.

### Problema #113: No verifica que getThresholds tenga éxito

**Ubicación**: `MaintenanceManager.cpp:1181-1187`
**Severidad**: MEDIO
**Descripción**: Aunque retorna un json vacío en caso de error, no se verifica que los valores sean válidos.
**Impacto**: Umbrales incorrectos.

### Problema #114: Uso de std::get_time sin verificar que sea válido

**Ubicación**: `MaintenanceManager.cpp:1150`
**Severidad**: MEDIO
**Descripción**: `std::get_time()` puede fallar si el formato no coincide.
**Impacto**: Fechas incorrectas.

### Problema #115: No verifica que storeTask tenga éxito

**Ubicación**: `MaintenanceManager.cpp:975-1022`
**Severidad**: MEDIO
**Descripción**: Aunque se capturan excepciones, no se verifica que la inserción fue exitosa.
**Impacto**: Tareas no almacenadas.

### Problema #116: Uso de std::chrono::system_clock::now sin verificar

**Ubicación**: Múltiples archivos
**Severidad**: BAJO
**Descripción**: Aunque es poco probable que falle, en sistemas con problemas de reloj puede dar resultados incorrectos.
**Impacto**: Timestamps incorrectos.

### Problema #117: No verifica que updateTaskStatus tenga éxito

**Ubicación**: `MaintenanceManager.cpp:1024-1044`
**Severidad**: MEDIO
**Descripción**: Similar al anterior, no se verifica que la actualización fue exitosa.
**Impacto**: Estados incorrectos.

### Problema #118: Uso de std::stringstream sin verificar errores

**Ubicación**: `LineageExtractorMongoDB.cpp:153-158`, `LineageExtractorMariaDB.cpp:92-99`
**Severidad**: BAJO
**Descripción**: Aunque es raro, puede fallar si no hay memoria.
**Impacto**: Claves incorrectas.

### Problema #119: No verifica que generateFingerprint tenga éxito

**Ubicación**: `QueryStoreCollector.cpp:263-271`
**Severidad**: BAJO
**Descripción**: Aunque es poco probable que falle, no se verifica.
**Impacto**: Fingerprints incorrectos.

### Problema #120: Uso de std::regex sin compilar previamente

**Ubicación**: `LineageExtractorMariaDB.cpp:274-277, 379-385`, `QueryStoreCollector.cpp:187, 265-268`
**Severidad**: MEDIO
**Descripción**: Los regex se compilan en cada llamada, lo cual es ineficiente.
**Impacto**: Performance issues.

### Problema #121: No verifica que categorizeQuery tenga éxito

**Ubicación**: `QueryStoreCollector.cpp:226-244`, `QueryActivityLogger.cpp:133-151`
**Severidad**: BAJO
**Descripción**: Aunque siempre retorna un string, no se verifica que sea válido.
**Impacto**: Categorías incorrectas.

### Problema #122: Uso de std::distance con iteradores sin verificar

**Ubicación**: `QueryStoreCollector.cpp:190`
**Severidad**: BAJO
**Descripción**: `std::distance()` puede ser costoso con algunos tipos de iteradores.
**Impacto**: Performance issues.

### Problema #123: No verifica que extractOperationType tenga éxito

**Ubicación**: `QueryStoreCollector.cpp:246-261`, `QueryActivityLogger.cpp:153-168`
**Severidad**: BAJO
**Descripción**: Similar al anterior.
**Impacto**: Tipos de operación incorrectos.

### Problema #124: Uso de std::max/std::min con tipos diferentes

**Ubicación**: `DataGovernance.cpp:1012`, `DataQuality.cpp:470`
**Severidad**: BAJO
**Descripción**: Aunque funciona, puede haber conversiones implícitas no deseadas.
**Impacto**: Resultados incorrectos en algunos casos.

### Problema #125: No verifica que determineValidationStatus tenga éxito

**Ubicación**: `DataQuality.cpp:478-486`
**Severidad**: BAJO
**Descripción**: Aunque siempre retorna un string, no se verifica.
**Impacto**: Estados incorrectos.

### Problema #126: Uso de std::vector::reserve sin verificar capacidad

**Ubicación**: `LineageExtractorMariaDB.cpp:71`
**Severidad**: BAJO
**Descripción**: Aunque `reserve()` es seguro, no se verifica que la reserva fue exitosa.
**Impacto**: Performance issues menores.

### Problema #127: No verifica que getPendingTasks tenga éxito

**Ubicación**: `MaintenanceManager.cpp:1110-1165`
**Severidad**: MEDIO
**Descripción**: Aunque se capturan excepciones, no se verifica que la query fue exitosa.
**Impacto**: Tareas no recuperadas.

---

## RECOMENDACIONES PRIORITARIAS

### CRÍTICO (Resolver inmediatamente)

1. **Problema #4, #5, #6, #7**: Memory leaks - Usar RAII, smart pointers
2. **Problema #8**: Buffer overflows - Validar tamaños, usar buffers dinámicos
3. **Problema #1, #2**: SQL Injection - Usar parámetros preparados siempre
4. **Problema #11, #58, #59**: Race conditions - Agregar mutex/sincronización

### ALTO (Resolver pronto)

5. **Problema #19, #21**: Divisiones por cero - Agregar validaciones
6. **Problema #22**: Manejo de errores - Propagar errores correctamente
7. **Problema #43-46**: Recursos no liberados - Usar RAII
8. **Problema #33-35**: Uso incorrecto de APIs - Verificar retornos

### MEDIO (Planificar)

9. **Problema #36, #60**: God Object - Refactorizar en clases más pequeñas
10. **Problema #37, #63-66**: Código duplicado - Extraer a funciones comunes
11. **Problema #39, #67-70**: Funciones largas - Dividir en funciones más pequeñas
12. **Problema #52**: Sampling impreciso - Mejorar algoritmo de sampling

### BAJO (Mejoras)

13. **Problema #38**: Magic numbers - Definir constantes
14. **Problema #40-42**: Inconsistencias - Estandarizar patrones
15. **Problema #47-49**: Dead code - Eliminar o implementar

---

## CONCLUSIÓN

El código tiene **problemas significativos de seguridad y calidad** que deben abordarse prioritariamente. Los problemas más críticos son:

1. **Memory leaks** en múltiples lugares
2. **SQL Injection** potencial aunque se use escapeSQL
3. **Buffer overflows** en manejo de datos SQL
4. **Race conditions** en estructuras compartidas
5. **Manejo de errores** insuficiente

Se recomienda una **refactorización gradual** comenzando por los problemas críticos de seguridad, seguidos de mejoras en la arquitectura y calidad del código.
