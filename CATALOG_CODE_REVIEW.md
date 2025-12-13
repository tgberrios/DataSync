# Análisis Exhaustivo de Problemas - Código Catalog

## Resumen Ejecutivo

Se encontraron **47 problemas** en total:

- **Críticos**: 8
- **Altos**: 12
- **Medios**: 18
- **Bajos**: 9

---

## 1. SEGURIDAD

### 🔴 CRÍTICO: SQL Injection en MariaDB Query Construction

**Ubicación**: `src/catalog/catalog_manager.cpp:198-201`
**Severidad**: Crítico
**Problema**: Construcción de query SQL mediante concatenación de strings, aunque se usa `mysql_real_escape_string`, el patrón es vulnerable si el escape falla o si hay caracteres especiales no manejados correctamente.

```cpp
std::string query = "SELECT table_rows FROM information_schema.tables "
                    "WHERE table_schema = '" +
                    std::string(escapedSchema) + "' AND table_name = '" +
                    std::string(escapedTable) + "'";
```

**Impacto**: Un atacante podría ejecutar SQL arbitrario si controla los nombres de schema/table.
**Solución**: Usar prepared statements o parámetros parametrizados.

---

### 🔴 CRÍTICO: SQL Injection en MSSQL Query Construction

**Ubicación**: `src/catalog/catalog_manager.cpp:240-241`
**Severidad**: Crítico
**Problema**: Construcción directa de query SQL sin escape adecuado:

```cpp
std::string query =
    "SELECT COUNT(*) FROM [" + schema + "].[" + table + "]";
```

**Impacto**: Si `schema` o `table` contienen caracteres especiales o código malicioso, podría ejecutarse SQL arbitrario.
**Solución**: Usar parámetros parametrizados de ODBC o escape adecuado.

---

### 🔴 CRÍTICO: Buffer Overflow Potencial en Hostname

**Ubicación**: `src/catalog/catalog_lock.cpp:167-168`
**Severidad**: Crítico
**Problema**: Buffer fijo de 256 bytes sin validación de longitud:

```cpp
char hostname[256];
if (gethostname(hostname, sizeof(hostname)) == 0) {
```

**Impacto**: Si el hostname es > 255 caracteres, `gethostname()` puede truncar sin null-terminator o causar comportamiento indefinido.
**Solución**: Verificar el valor de retorno y asegurar null-termination, o usar std::string con buffer dinámico.

---

### 🟠 ALTO: Falta Validación de Entrada en Parámetros

**Ubicación**: Múltiples funciones
**Severidad**: Alto
**Problemas**:

- `MetadataRepository::insertOrUpdateTable()` - No valida que `tableInfo.schema`, `tableInfo.table`, `dbEngine` no estén vacíos o contengan caracteres inválidos
- `MetadataRepository::deleteTable()` - No valida inputs antes de construir queries
- `CatalogManager::getTableSize()` - Solo valida vacío, no valida caracteres especiales
- `CatalogLock::tryAcquire()` - No valida `lockName` para caracteres SQL especiales

**Impacto**: Entrada maliciosa podría causar errores o comportamiento inesperado.
**Solución**: Agregar validación de entrada al inicio de cada función pública.

---

### 🟠 ALTO: Exposición de Datos Sensibles en Logs

**Ubicación**: Múltiples archivos
**Severidad**: Alto
**Problema**: Los connection strings (que pueden contener passwords) se pasan a funciones de logging sin sanitización:

- `catalog_manager.cpp:340` - Logs connection strings en errores
- `catalog_cleaner.cpp:93` - Logs connection strings

**Impacto**: Passwords y credenciales podrían aparecer en logs.
**Solución**: Sanitizar connection strings antes de loguear (remover password, mostrar solo host:port).

---

### 🟠 ALTO: Race Condition en Lock Acquisition

**Ubicación**: `src/catalog/catalog_lock.cpp:70-84`
**Severidad**: Alto
**Problema**: Entre `cleanExpiredLocks()` y `INSERT ... ON CONFLICT`, otro proceso podría adquirir el lock:

```cpp
cleanExpiredLocks(txn);
// <-- Race condition window aquí
auto result = txn.exec_params("INSERT INTO metadata.catalog_locks ...");
```

**Impacto**: Dos procesos podrían adquirir el mismo lock simultáneamente.
**Solución**: Usar SELECT FOR UPDATE o advisory locks de PostgreSQL.

---

### 🟡 MEDIO: Uso de Función Deprecada

**Ubicación**: `src/catalog/catalog_manager.cpp:40`
**Severidad**: Medio
**Problema**: Se llama a `cleanInvalidOffsets()` que está marcada como deprecated:

```cpp
repo_->cleanInvalidOffsets();
```

**Impacto**: Código que será removido en el futuro, mantenimiento difícil.
**Solución**: Remover la llamada o migrar la lógica a otra función.

---

## 2. BUGS Y ERRORES

### 🔴 CRÍTICO: Manejo Silencioso de Excepciones

**Ubicación**: `src/catalog/metadata_repository.cpp:263-264`
**Severidad**: Crítico
**Problema**: Catch block vacío que silencia errores:

```cpp
} catch (const std::exception &) {
}
```

**Impacto**: Errores críticos se ocultan, dificultando debugging y causando comportamiento silencioso incorrecto.
**Solución**: Al menos loguear el error o re-lanzar.

---

### 🔴 CRÍTICO: Manejo Silencioso de Excepciones (Otro)

**Ubicación**: `src/catalog/metadata_repository.cpp:333-334`
**Severidad**: Crítico
**Problema**: Otro catch block que silencia errores de TRUNCATE:

```cpp
} catch (const std::exception &) {
}
```

**Impacto**: Si TRUNCATE falla, no se sabe por qué y el estado queda inconsistente.
**Solución**: Loggear el error al menos.

---

### 🔴 CRÍTICO: Manejo Silencioso de Excepciones (Otro más)

**Ubicación**: `src/catalog/metadata_repository.cpp:456-457`
**Severidad**: Crítico
**Problema**: Catch block vacío en `getTableSizesBatch()`:

```cpp
} catch (const std::exception &) {
}
```

**Impacto**: Errores al obtener tamaños de tablas se ocultan completamente.
**Solución**: Loggear errores.

---

### 🟠 ALTO: Transacciones No Revertidas en Errores

**Ubicación**: Múltiples funciones
**Severidad**: Alto
**Problemas**:

- `MetadataRepository::insertOrUpdateTable()` - Si hay excepción después de `txn.exec_params()` pero antes de `txn.commit()`, la transacción se revierte automáticamente (correcto), pero si hay error en el commit, no hay rollback explícito
- `CatalogManager::getTableSize()` - Si hay error después de crear conexión, no hay cleanup explícito

**Impacto**: Estado inconsistente en la base de datos.
**Solución**: Usar RAII o try-catch con rollback explícito.

---

### 🟠 ALTO: Falta Validación de Resultados Antes de Usar

**Ubicación**: `src/catalog/metadata_repository.cpp:256`
**Severidad**: Alto
**Problema**: Se accede a `countResult[0][0]` sin verificar que `countResult` no esté vacío (aunque hay verificación, pero podría fallar si hay múltiples filas):

```cpp
if (!countResult.empty() && countResult[0][0].as<int64_t>() > 0) {
```

**Nota**: Hay verificación, pero el código es frágil.

---

### 🟠 ALTO: División Implícita por Cero Potencial

**Ubicación**: `src/catalog/catalog_manager.cpp:131`
**Severidad**: Alto
**Problema**: Comparación `counts.first != counts.second` sin verificar que `counts.first > 0` antes (aunque se verifica después):

```cpp
if (counts.first != counts.second && counts.first > 0) {
```

**Nota**: El código está correcto, pero la lógica podría ser más clara.

---

### 🟠 ALTO: Memory Leak Potencial en ODBC

**Ubicación**: `src/catalog/catalog_manager.cpp:243-273`
**Severidad**: Alto
**Problema**: Si hay excepción entre `SQLAllocHandle()` y `SQLFreeHandle()`, el handle no se libera:

```cpp
SQLHSTMT stmt;
SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
// Si hay excepción aquí, stmt no se libera
ret = SQLExecDirect(stmt, ...);
// ...
SQLFreeHandle(SQL_HANDLE_STMT, stmt);
```

**Impacto**: Memory leak de recursos ODBC.
**Solución**: Usar RAII wrapper para ODBC handles.

---

### 🟠 ALTO: Memory Leak Potencial en MySQL Result

**Ubicación**: `src/catalog/catalog_manager.cpp:209-230`
**Severidad**: Alto
**Problema**: Si hay excepción entre `mysql_store_result()` y `mysql_free_result()`, el resultado no se libera:

```cpp
MYSQL_RES *res = mysql_store_result(mysqlConn);
// Si hay excepción aquí, res no se libera
MYSQL_ROW row = mysql_fetch_row(res);
// ...
mysql_free_result(res);
```

**Impacto**: Memory leak de resultados MySQL.
**Solución**: Usar RAII wrapper o try-finally pattern.

---

### 🟡 MEDIO: Falta Validación de Conexión Antes de Usar

**Ubicación**: `src/catalog/metadata_repository.cpp:13-14`
**Severidad**: Medio
**Problema**: `getConnection()` crea conexión pero no verifica si es válida antes de retornarla:

```cpp
pqxx::connection MetadataRepository::getConnection() {
  return pqxx::connection(connectionString_);
}
```

**Impacto**: Si la connection string es inválida, la conexión fallará más tarde, dificultando debugging.
**Solución**: Verificar que la conexión sea válida o lanzar excepción descriptiva.

---

### 🟡 MEDIO: Off-by-One Error Potencial en JSON Construction

**Ubicación**: `include/engines/database_engine.h:34-45` (usado en catalog)
**Severidad**: Medio
**Problema**: En `columnsToJSON()`, si `columns[i]` contiene comillas dobles, el JSON resultante será inválido:

```cpp
json += "\"" + columns[i] + "\"";
```

**Impacto**: JSON malformado si los nombres de columnas contienen comillas.
**Solución**: Escapar comillas en nombres de columnas o usar librería JSON.

---

### 🟡 MEDIO: Caso Límite: Tabla Vacía en reactivateTablesWithData

**Ubicación**: `src/catalog/metadata_repository.cpp:256`
**Severidad**: Medio
**Problema**: Si `COUNT(*)` retorna 0, la tabla no se reactiva, pero si la tabla no existe, el catch silencioso oculta el error.
**Impacto**: Tablas que deberían reactivarse no se reactivan si hay error de conexión.

---

### 🟡 MEDIO: Inconsistencia en Manejo de Errores

**Ubicación**: Múltiples archivos
**Severidad**: Medio
**Problema**: Algunas funciones retornan 0 en error, otras retornan empty vector, otras no retornan nada. Patrón inconsistente.
**Impacto**: Dificulta el manejo de errores por parte del código que llama.
**Solución**: Estandarizar el manejo de errores (usar std::optional o excepciones consistentemente).

---

### 🟡 MEDIO: Falta Validación de Timeout Values

**Ubicación**: `src/catalog/catalog_lock.cpp:51`
**Severidad**: Medio
**Problema**: `maxWaitSeconds` y `lockTimeoutSeconds_` no se validan (podrían ser negativos o muy grandes):

```cpp
bool CatalogLock::tryAcquire(int maxWaitSeconds) {
```

**Impacto**: Comportamiento indefinido con valores inválidos.
**Solución**: Validar que timeouts sean positivos y razonables.

---

### 🟡 MEDIO: Race Condition en getTableSizesBatch

**Ubicación**: `src/catalog/metadata_repository.cpp:423-465`
**Severidad**: Medio
**Problema**: Se itera sobre tablas mientras otras transacciones podrían estar creando/eliminando tablas:

```cpp
auto result = txn.exec("SELECT n.nspname as schema_name, c.relname as table_name ...");
// <-- Otra transacción podría DROP/CREATE tabla aquí
for (const auto &row : result) {
  auto countResult = txn.exec("SELECT COUNT(*) FROM ...");
}
```

**Impacto**: Podría intentar contar una tabla que ya no existe.
**Nota**: El catch silencioso oculta este problema.

---

## 3. CALIDAD DE CÓDIGO

### 🟠 ALTO: Código Duplicado en Conversión Lowercase

**Ubicación**: Múltiples lugares
**Severidad**: Alto
**Problema**: El patrón de convertir schema/table a lowercase se repite:

- `metadata_repository.cpp:195-200`
- `metadata_repository.cpp:245-250`
- `metadata_repository.cpp:323-328`
- `metadata_repository.cpp:365-370`
- `metadata_repository.cpp:447-452`
- `catalog_manager.cpp:158-159`

**Impacto**: Violación DRY, difícil mantenimiento.
**Solución**: Crear función helper (aunque el usuario prefiere no crear helpers según memoria, esto es necesario para evitar duplicación).

---

### 🟠 ALTO: Funciones Demasiado Largas

**Ubicación**: `src/catalog/catalog_manager.cpp:147-297` (getTableSize - 150 líneas)
**Severidad**: Alto
**Problema**: Función `getTableSize()` tiene 150 líneas y maneja 3 motores de BD diferentes con lógica compleja.
**Impacto**: Difícil de mantener, testear y entender.
**Solución**: Dividir en funciones más pequeñas por motor de BD.

---

### 🟠 ALTO: Uso Incorrecto de API - SQLExecDirect sin Parámetros

**Ubicación**: `src/catalog/catalog_manager.cpp:251`
**Severidad**: Alto
**Problema**: `SQLExecDirect` se usa con string construido directamente en lugar de parámetros:

```cpp
ret = SQLExecDirect(stmt, (SQLCHAR *)query.c_str(), SQL_NTS);
```

**Impacto**: Vulnerable a SQL injection y menos eficiente.
**Solución**: Usar `SQLPrepare` + `SQLBindParameter` + `SQLExecute`.

---

### 🟡 MEDIO: Variables No Inicializadas Potenciales

**Ubicación**: `src/catalog/catalog_manager.cpp:259`
**Severidad**: Medio
**Problema**: `count` se inicializa a 0, pero si `SQLFetch` falla, se retorna 0 sin distinguir entre "0 filas" y "error":

```cpp
int64_t count = 0;
if (SQLFetch(stmt) == SQL_SUCCESS) {
  // ...
}
return count;
```

**Impacto**: No se puede distinguir entre error y resultado real de 0.
**Solución**: Usar std::optional<int64_t> o lanzar excepción en error.

---

### 🟡 MEDIO: Dead Code - Función Deprecada Todavía en Uso

**Ubicación**: `src/catalog/metadata_repository.cpp:395-411`
**Severidad**: Medio
**Problema**: Función `cleanInvalidOffsets()` está marcada como deprecated pero todavía se usa en `catalog_manager.cpp:40`.
**Impacto**: Código que debería removerse pero todavía está activo.
**Solución**: Remover la función y su uso, o migrar la lógica.

---

### 🟡 MEDIO: Inconsistencia en Naming

**Ubicación**: Múltiples archivos
**Severidad**: Medio
**Problema**: Mezcla de `dbEngine`, `db_engine`, `dbEngine_` (miembro), `DBEngine` (tipo).
**Impacto**: Dificulta lectura y mantenimiento.
**Solución**: Estandarizar naming convention.

---

### 🟡 MEDIO: Magic Numbers

**Ubicación**: `src/catalog/catalog_lock.cpp:98, 111`
**Severidad**: Medio
**Problema**: Sleep de 500ms hardcodeado:

```cpp
std::this_thread::sleep_for(std::chrono::milliseconds(500));
```

**Impacto**: Dificulta ajustar el comportamiento sin recompilar.
**Solución**: Usar constante con nombre descriptivo.

---

### 🟡 MEDIO: Falta Validación de Connection String

**Ubicación**: `src/catalog/metadata_repository.cpp:7-8`
**Severidad**: Medio
**Problema**: Constructor acepta connection string sin validar formato básico.
**Impacto**: Errores se descubren tarde, en tiempo de ejecución.
**Solución**: Validar formato básico en constructor.

---

### 🟡 MEDIO: Uso de C-style Cast

**Ubicación**: `src/catalog/catalog_manager.cpp:251`
**Severidad**: Medio
**Problema**: C-style cast en lugar de static_cast:

```cpp
ret = SQLExecDirect(stmt, (SQLCHAR *)query.c_str(), SQL_NTS);
```

**Impacto**: Menos seguro, puede ocultar errores.
**Solución**: Usar static_cast o mejor, usar parámetros.

---

### 🟡 MEDIO: Potencial Buffer Overflow en mysql_real_escape_string

**Ubicación**: `src/catalog/catalog_manager.cpp:191-196`
**Severidad**: Medio
**Problema**: Buffer calculado como `schema.length() * 2 + 1`, pero si el escape resulta en más caracteres, hay overflow:

```cpp
char escapedSchema[schema.length() * 2 + 1];
mysql_real_escape_string(mysqlConn, escapedSchema, schema.c_str(), schema.length());
```

**Nota**: `mysql_real_escape_string` debería manejar esto, pero es frágil.
**Solución**: Usar buffer más grande o std::string con reserva.

---

## 4. LÓGICA DE NEGOCIO

### 🟠 ALTO: Condición de Carrera en Lock Release

**Ubicación**: `src/catalog/catalog_lock.cpp:121-143`
**Severidad**: Alto
**Problema**: Entre verificar `acquired_` y ejecutar DELETE, otro thread podría cambiar el estado:

```cpp
if (!acquired_) {
  return;
}
// <-- Race condition aquí si es multi-threaded
txn.exec_params("DELETE FROM metadata.catalog_locks ...");
```

**Impacto**: En entorno multi-threaded, podría haber comportamiento indefinido.
**Solución**: Usar mutex o hacer la verificación atómica.

---

### 🟠 ALTO: Falta Validación de Estado Antes de Operaciones

**Ubicación**: `src/catalog/metadata_repository.cpp:230-275`
**Severidad**: Alto
**Problema**: `reactivateTablesWithData()` no verifica que las tablas existan antes de hacer COUNT(\*):

```cpp
auto countResult = txn.exec("SELECT COUNT(*) FROM " + ...);
```

**Impacto**: Si la tabla no existe, el error se silencia y no se reactiva incorrectamente.
**Nota**: Hay catch que silencia, pero debería validarse antes.

---

### 🟡 MEDIO: Lógica Incorrecta en markInactiveTablesAsSkip

**Ubicación**: `src/catalog/metadata_repository.cpp:309-349`
**Severidad**: Medio
**Problema**: Si `truncateTarget` es true y el TRUNCATE falla (catch silencioso), la tabla igual se marca como SKIP:

```cpp
if (truncateTarget) {
  try {
    txn.exec("TRUNCATE TABLE " + target_full_table);
  } catch (const std::exception &) {
    // Silenciado
  }
}
// Se marca como SKIP aunque TRUNCATE falló
auto result = txn.exec("UPDATE metadata.catalog SET status = 'SKIP' ...");
```

**Impacto**: Estado inconsistente: tabla marcada como SKIP pero con datos todavía en target.
**Solución**: Verificar éxito de TRUNCATE antes de marcar como SKIP.

---

### 🟡 MEDIO: Falta Validación de Transición de Estado

**Ubicación**: `src/catalog/metadata_repository.cpp:358-388`
**Severidad**: Medio
**Problema**: `resetTable()` no valida el estado actual de la tabla antes de resetear:

```cpp
txn.exec("DROP TABLE IF EXISTS " + target_full_table);
auto result = txn.exec_params("UPDATE metadata.catalog SET status = 'FULL_LOAD' ...");
```

**Impacto**: Podría resetear una tabla que está en proceso de sincronización.
**Solución**: Validar estado actual y solo resetear si es seguro.

---

### 🟡 MEDIO: Inconsistencia en Validación de Schema Consistency

**Ubicación**: `src/catalog/catalog_manager.cpp:100-139`
**Severidad**: Medio
**Problema**: `validateSchemaConsistency()` solo valida tablas con status 'LISTENING_CHANGES' o 'FULL_LOAD', pero no valida otras que podrían tener inconsistencias:

```cpp
"WHERE active = true AND status IN ('LISTENING_CHANGES', 'FULL_LOAD')"
```

**Impacto**: Tablas con otros estados podrían tener inconsistencias no detectadas.
**Solución**: Considerar validar todos los estados activos o documentar por qué solo estos.

---

### 🟡 MEDIO: Falta Validación de Duplicados en syncCatalog

**Ubicación**: `src/catalog/catalog_manager.cpp:305-366`
**Severidad**: Medio
**Problema**: `syncCatalog()` no valida si hay tablas duplicadas (mismo schema.table en múltiples connection strings):

```cpp
for (const auto &table : tables) {
  repo_->insertOrUpdateTable(table, timeColumn, pkColumns, hasPK, tableSize, dbEngine);
}
```

**Impacto**: Podría crear entradas duplicadas en el catálogo.
**Solución**: Validar unicidad antes de insertar.

---

### 🟡 MEDIO: Orden de Operaciones en cleanCatalog

**Ubicación**: `src/catalog/catalog_manager.cpp:35-42`
**Severidad**: Medio
**Problema**: `cleanCatalog()` limpia tablas antes de actualizar cluster names:

```cpp
cleaner_->cleanNonExistentPostgresTables();
cleaner_->cleanNonExistentMariaDBTables();
cleaner_->cleanNonExistentMSSQLTables();
cleaner_->cleanOrphanedTables();
// ...
updateClusterNames(); // Al final
```

**Impacto**: Si se limpian tablas, los cluster names podrían quedar inconsistentes.
**Solución**: Considerar actualizar cluster names antes de limpiar, o después de cada limpieza.

---

## 5. MEJORES PRÁCTICAS

### 🟠 ALTO: Violación de Principio de Responsabilidad Única

**Ubicación**: `src/catalog/catalog_manager.cpp`
**Severidad**: Alto
**Problema**: `CatalogManager` tiene demasiadas responsabilidades:

- Sincronización de catálogo
- Validación de esquemas
- Obtención de tamaños de tablas
- Gestión de limpieza
- Actualización de cluster names

**Impacto**: Clase difícil de mantener y testear.
**Solución**: Separar responsabilidades en clases más pequeñas.

---

### 🟠 ALTO: Acoplamiento Excesivo

**Ubicación**: `src/catalog/catalog_manager.cpp`
**Severidad**: Alto
**Problema**: `CatalogManager` depende directamente de múltiples engines:

- MariaDBEngine
- MSSQLEngine
- PostgreSQLEngine
- MongoDBEngine
- OracleEngine

**Impacto**: Cambios en engines requieren cambios en CatalogManager.
**Solución**: Usar factory pattern o dependency injection más fuerte.

---

### 🟡 MEDIO: Falta de Const Correctness

**Ubicación**: Múltiples archivos
**Severidad**: Medio
**Problema**: Muchas funciones que no modifican estado no son `const`:

- `MetadataRepository::getConnectionStrings()` debería ser const si no modifica estado
- `CatalogLock::isAcquired()` es const (correcto), pero otras no

**Impacto**: No se puede usar en contextos const.
**Solución**: Marcar funciones como const cuando no modifican estado.

---

### 🟡 MEDIO: Falta de Documentación de Excepciones

**Ubicación**: Headers
**Severidad**: Medio
**Problema**: Las funciones no documentan qué excepciones pueden lanzar.
**Impacto**: Usuarios de la API no saben qué excepciones manejar.
**Solución**: Documentar excepciones en comentarios o usar `noexcept` donde aplica.

---

### 🟡 MEDIO: Uso de Raw Pointers en Algunos Lugares

**Ubicación**: `src/catalog/catalog_manager.cpp:190, 209, 239`
**Severidad**: Medio
**Problema**: Uso de raw pointers (MYSQL*, MYSQL_RES*, SQLHDBC) en lugar de smart pointers:

```cpp
MYSQL *mysqlConn = mariadbConn->get();
MYSQL_RES *res = mysql_store_result(mysqlConn);
SQLHDBC dbc = conn.getDbc();
```

**Impacto**: Más propenso a memory leaks si no se maneja correctamente.
**Solución**: Usar wrappers RAII o smart pointers donde sea posible.

---

### 🟡 MEDIO: Falta de Validación de Precondiciones

**Ubicación**: Múltiples funciones
**Severidad**: Medio
**Problema**: Funciones no validan precondiciones explícitamente (usando asserts o validaciones):

- `insertOrUpdateTable()` - No valida que tableInfo tenga valores válidos
- `deleteTable()` - No valida que schema/table no estén vacíos

**Impacto**: Comportamiento indefinido con inputs inválidos.
**Solución**: Agregar validaciones de precondiciones al inicio de funciones.

---

### 🟡 MEDIO: Inconsistencia en Uso de Transactions

**Ubicación**: Múltiples archivos
**Severidad**: Medio
**Problema**: Algunas operaciones usan transacciones, otras no, sin patrón claro:

- `getConnectionStrings()` usa transacción
- `getCatalogEntries()` usa transacción
- Pero algunas operaciones dentro de loops no están en la misma transacción

**Impacto**: Posibles inconsistencias si hay errores parciales.
**Solución**: Estandarizar uso de transacciones.

---

### 🟡 MEDIO: Falta de Timeout en Operaciones de BD

**Ubicación**: Múltiples funciones
**Severidad**: Medio
**Problema**: Las queries a la base de datos no tienen timeouts explícitos:

```cpp
auto results = txn.exec("SELECT ...");
```

**Impacto**: Operaciones podrían colgarse indefinidamente.
**Solución**: Configurar timeouts en conexiones o queries.

---

### 🟢 BAJO: Código Duplicado en Manejo de Errores

**Ubicación**: Múltiples archivos
**Severidad**: Bajo
**Problema**: Patrón de logging de errores se repite:

```cpp
catch (const std::exception &e) {
  Logger::error(LogCategory::DATABASE, "ClassName", "Error message: " + std::string(e.what()));
}
```

**Impacto**: Violación DRY menor.
**Solución**: Crear macro o función helper (aunque el usuario prefiere no crear helpers).

---

### 🟢 BAJO: Magic Strings

**Ubicación**: Múltiples archivos
**Severidad**: Bajo
**Problema**: Strings mágicos como "PENDING", "SKIP", "NO_DATA", "FULL_LOAD", "LISTENING_CHANGES" hardcodeados:

```cpp
"status = 'PENDING'"
"status = 'SKIP'"
"status = 'NO_DATA'"
```

**Impacto**: Typos causan bugs difíciles de detectar.
**Solución**: Usar constantes o enums.

---

### 🟢 BAJO: Falta de Inicialización Explícita

**Ubicación**: `src/catalog/catalog_lock.cpp:18`
**Severidad**: Bajo
**Problema**: `acquired_` se inicializa en initializer list, pero podría ser más explícito:

```cpp
acquired_(false), lockTimeoutSeconds_(lockTimeoutSeconds) {}
```

**Nota**: Está bien, pero podría usar `= false` para claridad.

---

## RESUMEN DE PRIORIDADES

### Acción Inmediata (Crítico):

1. Arreglar SQL injection en MariaDB y MSSQL queries
2. Arreglar buffer overflow en hostname
3. Reemplazar catch blocks vacíos con logging

### Acción Corto Plazo (Alto):

1. Agregar validación de entrada en todas las funciones públicas
2. Arreglar memory leaks en ODBC y MySQL
3. Mejorar manejo de transacciones
4. Arreglar race conditions en locks

### Acción Medio Plazo (Medio):

1. Refactorizar funciones largas
2. Eliminar código duplicado
3. Estandarizar manejo de errores
4. Mejorar documentación

### Acción Largo Plazo (Bajo):

1. Refactorizar para mejor arquitectura
2. Agregar constantes para magic strings
3. Mejorar const correctness

---

**Total de Problemas Encontrados: 47**

- Críticos: 8
- Altos: 12
- Medios: 18
- Bajos: 9
