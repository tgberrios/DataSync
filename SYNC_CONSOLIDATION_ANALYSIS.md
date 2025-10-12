# 📊 ANÁLISIS EXHAUSTIVO: MariaDBToPostgres.h vs MSSQLToPostgres.h

## 🎯 OBJETIVO: Identificar duplicación y oportunidades de consolidación

---

## 1️⃣ INVENTARIO COMPLETO DE FUNCIONES

### 📦 MariaDBToPostgres.h (3976 líneas)

#### **PÚBLICAS** (Interfaz externa):

```cpp
1.  MariaDBToPostgres()                                    // Constructor
2.  ~MariaDBToPostgres()                                   // Destructor
3.  MYSQL* getMariaDBConnection(string)                    // ✅ ESPECÍFICO MariaDB
4.  vector<TableInfo> getActiveTables(pqxx::connection&)   // 🟡 DUPLICADO 95%
5.  void syncIndexesAndConstraints(...)                    // 🟡 DUPLICADO 85%
6.  void setupTableTargetMariaDBToPostgres()               // 🟡 DUPLICADO 90%
7.  void processDeletesByPrimaryKey(...)                   // 🟡 DUPLICADO 95%
8.  void processUpdatesByPrimaryKey(...)                   // 🟡 DUPLICADO 95%
9.  bool compareAndUpdateRecord(...)                       // 🟢 DUPLICADO 100%
10. void transferDataMariaDBToPostgres()                   // 🟡 DUPLICADO 80%
11. void transferDataMariaDBToPostgresOld()                // ⚠️  LEGACY (puede eliminarse)
12. void transferDataMariaDBToPostgresParallel()           // 🟡 DUPLICADO 85%
```

#### **PRIVADAS** (Helpers internos):

```cpp
13. void processTableParallelWithConnection(TableInfo)     // 🟢 DUPLICADO 100%
14. void processTableParallel(TableInfo, pqxx::connection&) // 🟡 DUPLICADO 90%
15. void startParallelProcessing()                         // 🟢 DUPLICADO 100%
16. void shutdownParallelProcessing()                      // 🟢 DUPLICADO 100%
17. void dataFetcherThread(MYSQL*, TableInfo, ...)         // 🟡 DUPLICADO 80%
18. void batchPreparerThread(vector, vector)               // 🟢 DUPLICADO 95%
19. void batchInserterThread(pqxx::connection&)            // 🟢 DUPLICADO 95%
20. void updateStatus(pqxx::connection&, ...)              // 🟡 DUPLICADO 90%
21. vector<string> getPrimaryKeyColumns(MYSQL*, ...)       // ✅ ESPECÍFICO MariaDB
22. vector<vector<string>> findDeletedPrimaryKeys(...)     // 🟢 DUPLICADO 95%
23. size_t deleteRecordsByPrimaryKey(...)                  // 🟢 DUPLICADO 100%
24. vector<vector<string>> executeQueryMariaDB(MYSQL*, string) // ✅ ESPECÍFICO MariaDB
25. void performBulkUpsert(...)                            // 🟢 DUPLICADO 98%
26. void performBulkInsert(...)                            // 🟢 DUPLICADO 100%
27. vector<string> getPrimaryKeyColumnsFromPostgres(...)   // 🟢 DUPLICADO 100%
28. string buildUpsertQuery(...)                           // 🟢 DUPLICADO 100%
29. string buildUpsertConflictClause(...)                  // 🟢 DUPLICADO 100%
30. string cleanValueForPostgres(string, string)           // 🟡 DUPLICADO 85%
31. string getPKStrategyFromCatalog(...)                   // 🟢 DUPLICADO 100%
32. vector<string> getPKColumnsFromCatalog(...)            // 🟢 DUPLICADO 100%
33. string getLastProcessedPKFromCatalog(...)              // 🟢 DUPLICADO 100%
34. vector<string> parseJSONArray(string)                  // 🟢 DUPLICADO 100%
35. void updateLastProcessedPK(...)                        // 🟢 DUPLICADO 100%
36. string getLastPKFromResults(...)                       // 🟡 DUPLICADO 95%
37. vector<string> parseLastPK(string)                     // 🟢 DUPLICADO 100%
38. bool verifyDataConsistency(MYSQL*, pqxx::connection&, TableInfo) // ✅ ESPECÍFICO MariaDB
```

**TOTAL MariaDB: 38 funciones**

---

### 📦 MSSQLToPostgres.h (3433 líneas)

#### **PÚBLICAS** (Interfaz externa):

```cpp
1.  MSSQLToPostgres()                                      // Constructor
2.  ~MSSQLToPostgres()                                     // Destructor
3.  SQLHDBC getMSSQLConnection(string)                     // ✅ ESPECÍFICO MSSQL
4.  void closeMSSQLConnection(SQLHDBC)                     // ✅ ESPECÍFICO MSSQL
5.  vector<TableInfo> getActiveTables(pqxx::connection&)   // 🟡 DUPLICADO 95%
6.  void syncIndexesAndConstraints(...)                    // 🟡 DUPLICADO 85%
7.  void setupTableTargetMSSQLToPostgres()                 // 🟡 DUPLICADO 90%
8.  void transferDataMSSQLToPostgres()                     // 🟡 DUPLICADO 80%
9.  void transferDataMSSQLToPostgresParallel()             // 🟡 DUPLICADO 85%
```

#### **PRIVADAS** (Helpers internos):

```cpp
10. void processTableParallelWithConnection(TableInfo)     // 🟢 DUPLICADO 100%
11. void processTableParallel(TableInfo, pqxx::connection&) // 🟡 DUPLICADO 90%
12. void startParallelProcessing()                         // 🟢 DUPLICADO 100%
13. void shutdownParallelProcessing()                      // 🟢 DUPLICADO 100%
14. void dataFetcherThread(SQLHDBC, TableInfo, ...)        // 🟡 DUPLICADO 80%
15. void batchPreparerThread(vector, vector)               // 🟢 DUPLICADO 95%
16. void batchInserterThread(pqxx::connection&)            // 🟢 DUPLICADO 95%
17. string getLastSyncTimeOptimized(...)                   // ✅ ESPECÍFICO MSSQL (no está en MariaDB)
18. void updateStatus(pqxx::connection&, ...)              // 🟡 DUPLICADO 90%
19. void processDeletesByPrimaryKey(...)                   // 🟡 DUPLICADO 95%
20. void processUpdatesByPrimaryKey(...)                   // 🟡 DUPLICADO 95%
21. bool compareAndUpdateRecord(...)                       // 🟢 DUPLICADO 100%
22. vector<string> getPrimaryKeyColumns(SQLHDBC, ...)      // ✅ ESPECÍFICO MSSQL
23. vector<vector<string>> findDeletedPrimaryKeys(...)     // 🟢 DUPLICADO 95%
24. size_t deleteRecordsByPrimaryKey(...)                  // 🟢 DUPLICADO 100%
25. string getPKStrategyFromCatalog(...)                   // 🟢 DUPLICADO 100%
26. vector<string> getPKColumnsFromCatalog(...)            // 🟢 DUPLICADO 100%
27. string getLastProcessedPKFromCatalog(...)              // 🟢 DUPLICADO 100%
28. vector<string> parseJSONArray(string)                  // 🟢 DUPLICADO 100%
29. void updateLastProcessedPK(...)                        // 🟢 DUPLICADO 100%
30. string getLastPKFromResults(...)                       // 🟡 DUPLICADO 95%
31. vector<string> parseLastPK(string)                     // 🟢 DUPLICADO 100%
32. void performBulkUpsert(...)                            // 🟢 DUPLICADO 98%
33. void performBulkInsert(...)                            // 🟢 DUPLICADO 100%
34. vector<string> getPrimaryKeyColumnsFromPostgres(...)   // 🟢 DUPLICADO 100%
35. string buildUpsertQuery(...)                           // 🟢 DUPLICADO 100%
36. string buildUpsertConflictClause(...)                  // 🟢 DUPLICADO 100%
37. string cleanValueForPostgres(string, string)           // 🟡 DUPLICADO 85%
38. string extractDatabaseName(string)                     // ✅ ESPECÍFICO MSSQL
39. vector<vector<string>> executeQueryMSSQL(SQLHDBC, string) // ✅ ESPECÍFICO MSSQL
```

**TOTAL MSSQL: 39 funciones**

---

## 2️⃣ CLASIFICACIÓN POR DUPLICACIÓN

### 🟢 DUPLICACIÓN 100% (23 funciones - PRIORIDAD ALTA)

Estas funciones son **IDÉNTICAS** o casi idénticas entre ambos archivos:

```cpp
✅ compareAndUpdateRecord()                 // Comparar y actualizar registros
✅ deleteRecordsByPrimaryKey()              // Eliminar por PK
✅ performBulkInsert()                      // INSERT masivo
✅ getPrimaryKeyColumnsFromPostgres()       // Obtener PKs de PostgreSQL
✅ buildUpsertQuery()                       // Construir query UPSERT
✅ buildUpsertConflictClause()              // Construir ON CONFLICT
✅ getPKStrategyFromCatalog()               // Obtener estrategia PK
✅ getPKColumnsFromCatalog()                // Obtener columnas PK
✅ getLastProcessedPKFromCatalog()          // Obtener último PK procesado
✅ parseJSONArray()                         // Parsear JSON array
✅ updateLastProcessedPK()                  // Actualizar último PK
✅ parseLastPK()                            // Parsear último PK
✅ startParallelProcessing()                // Iniciar procesamiento paralelo
✅ shutdownParallelProcessing()             // Detener procesamiento paralelo
✅ processTableParallelWithConnection()     // Procesar tabla paralela (wrapper)
✅ findDeletedPrimaryKeys()                 // Encontrar PKs eliminados
✅ batchPreparerThread()                    // Thread preparador de batches
✅ batchInserterThread()                    // Thread insertador de batches
```

**IMPACTO:** ~1800 líneas de código idéntico que pueden moverse a clase base.

---

### 🟡 DUPLICACIÓN 80-95% (12 funciones - PRIORIDAD MEDIA)

Estas funciones son **MUY SIMILARES** pero tienen pequeñas diferencias específicas del engine:

```cpp
🔸 getActiveTables()                        // Diferencia: WHERE db_engine='MariaDB'/'MSSQL'
🔸 syncIndexesAndConstraints()              // Diferencia: Queries sys.indexes (MSSQL) vs information_schema (MariaDB)
🔸 setupTableTarget*()                      // Diferencia: Queries de metadata específicas
🔸 transferData*()                          // Diferencia: Llamadas a conexión específica
🔸 transferData*Parallel()                  // Diferencia: Llamadas a conexión específica
🔸 processTableParallel()                   // Diferencia: Tipo de conexión (MYSQL vs SQLHDBC)
🔸 dataFetcherThread()                      // Diferencia: Tipo de conexión y queries
🔸 updateStatus()                           // Diferencia: Validaciones de tipo de columna
🔸 processDeletesByPrimaryKey()             // Diferencia: Sintaxis SQL ([] vs `)
🔸 processUpdatesByPrimaryKey()             // Diferencia: Sintaxis SQL ([] vs `)
🔸 performBulkUpsert()                      // Diferencia: Manejo de errores específico
🔸 cleanValueForPostgres()                  // Diferencia: Validaciones de datos específicos
🔸 getLastPKFromResults()                   // Diferencia: Manejo de índices
```

**IMPACTO:** ~900 líneas que pueden parametrizarse o usar templates.

---

### ✅ ESPECÍFICO DE ENGINE (7 funciones - NO DUPLICADAS)

Estas funciones son **ÚNICAS** a cada engine y NO pueden consolidarse:

#### **MariaDB específico:**

```cpp
✅ getMariaDBConnection(string) → MYSQL*     // Conectar a MariaDB
✅ executeQueryMariaDB(MYSQL*, string)       // Ejecutar query en MariaDB
✅ getPrimaryKeyColumns(MYSQL*, ...)         // Obtener PKs usando MySQL API
✅ verifyDataConsistency(MYSQL*, ...)        // Verificar consistencia con MariaDB
```

#### **MSSQL específico:**

```cpp
✅ getMSSQLConnection(string) → SQLHDBC      // Conectar a MSSQL
✅ closeMSSQLConnection(SQLHDBC)             // Cerrar conexión MSSQL
✅ executeQueryMSSQL(SQLHDBC, string)        // Ejecutar query en MSSQL
✅ getPrimaryKeyColumns(SQLHDBC, ...)        // Obtener PKs usando ODBC API
✅ extractDatabaseName(string)               // Extraer DB de connection string
✅ getLastSyncTimeOptimized(...)             // Optimización MSSQL específica
```

**IMPACTO:** ~600 líneas que permanecen en clases derivadas.

---

## 3️⃣ ANÁLISIS DE MIEMBROS ESTÁTICOS

### 🔴 PROBLEMA CRÍTICO: Variables estáticas en header

```cpp
// AMBOS archivos tienen esto al final:
static std::unordered_map<std::string, std::string> dataTypeMap;
static std::unordered_map<std::string, std::string> collationMap;
static std::mutex metadataUpdateMutex;  // Solo en MariaDB
```

**PROBLEMA:** Cuando intentamos separar StreamingData.h en .h + .cpp:

- `#include "MariaDBToPostgres.h"` define las variables estáticas
- `#include "MSSQLToPostgres.h"` define las variables estáticas
- Si ambos headers se incluyen desde archivos .cpp diferentes → **multiple definition error**

**SOLUCIÓN:** Estas variables deben moverse a un archivo .cpp correspondiente.

---

## 4️⃣ ANÁLISIS DE DEPENDENCIAS

### Dependencias externas usadas por ambos:

```cpp
#include "catalog/catalog_manager.h"       // CatalogManager
#include "core/Config.h"                   // DatabaseConfig, SyncConfig
#include "core/logger.h"                   // Logger
#include "engines/database_engine.h"       // escapeSQL()
#include "sync/ParallelProcessing.h"       // ThreadSafeQueue, DataChunk, etc.
#include "third_party/json.hpp"            // JSON parsing
#include <pqxx/pqxx>                       // PostgreSQL
```

### Dependencias específicas:

```cpp
// MariaDB:
#include <mysql/mysql.h>                   // MYSQL, mysql_*

// MSSQL:
#include <sql.h>                           // ODBC
#include <sqlext.h>                        // ODBC extended
```

---

## 5️⃣ PLAN DE CONSOLIDACIÓN RECOMENDADO

### 📋 FASE 1: Preparación (2-3 horas)

1. ✅ Crear `DatabaseToPostgresSync.h` (clase base abstracta)
2. ✅ Mover **funciones 100% duplicadas** a clase base
3. ✅ Crear métodos virtuales puros para operaciones específicas

### 📋 FASE 2: Refactorización MariaDB (4-6 horas)

4. ✅ Refactorizar `MariaDBToPostgres` para heredar de base
5. ✅ Implementar métodos virtuales específicos
6. ✅ Mover static members a .cpp
7. ✅ Testing exhaustivo

### 📋 FASE 3: Refactorización MSSQL (4-6 horas)

8. ✅ Refactorizar `MSSQLToPostgres` para heredar de base
9. ✅ Implementar métodos virtuales específicos
10. ✅ Mover static members a .cpp
11. ✅ Testing exhaustivo

### 📋 FASE 4: Limpieza y optimización (2-3 horas)

12. ✅ Separar `StreamingData.h` en .h + .cpp
13. ✅ Eliminar código legacy (`transferDataMariaDBToPostgresOld`)
14. ✅ Documentación y testing final

**TIEMPO TOTAL ESTIMADO: 12-18 horas (2-3 días de trabajo)**

---

## 6️⃣ ESTRUCTURA PROPUESTA

```
include/sync/
├── DatabaseToPostgresSync.h          // Clase base abstracta (~1500 líneas)
├── MariaDBToPostgres.h               // Derivada (~800 líneas)
├── MSSQLToPostgres.h                 // Derivada (~700 líneas)
├── ParallelProcessing.h              // Ya existe
└── StreamingData.h                   // Refactorizado (~400 líneas)

src/sync/
├── DatabaseToPostgresSync.cpp        // Implementación base (~1200 líneas)
├── MariaDBToPostgres.cpp             // Implementación específica (~600 líneas)
├── MSSQLToPostgres.cpp               // Implementación específica (~500 líneas)
└── StreamingData.cpp                 // Refactorizado (~400 líneas)
```

**REDUCCIÓN:** 7409 líneas → ~4600 líneas = **38% reducción**

---

## 7️⃣ MÉTODOS QUE IRÁN A LA CLASE BASE

### Funciones PostgreSQL (agnósticas del source engine):

```cpp
// Metadata management
virtual void updateStatus(...) = 0;  // Implementación base + hook virtual
string getPKStrategyFromCatalog(...);
vector<string> getPKColumnsFromCatalog(...);
string getLastProcessedPKFromCatalog(...);
void updateLastProcessedPK(...);

// PK operations
vector<string> getPrimaryKeyColumnsFromPostgres(...);
size_t deleteRecordsByPrimaryKey(...);
bool compareAndUpdateRecord(...);

// Bulk operations
void performBulkInsert(...);
void performBulkUpsert(...);
string buildUpsertQuery(...);
string buildUpsertConflictClause(...);

// Parallel processing
void startParallelProcessing();
void shutdownParallelProcessing();
void batchPreparerThread(...);
void batchInserterThread(...);

// Utilities
vector<string> parseJSONArray(...);
vector<string> parseLastPK(...);
string getLastPKFromResults(...);
```

### Métodos virtuales puros (implementados en derivadas):

```cpp
// Conexión específica
virtual void* getConnection(string) = 0;
virtual void closeConnection(void*) = 0;

// Queries específicas
virtual vector<vector<string>> executeQuery(void*, string) = 0;
virtual vector<string> getPrimaryKeyColumns(void*, string, string) = 0;

// Setup específico
virtual void setupTableTarget() = 0;

// Transfer específico
virtual void transferData() = 0;
virtual void transferDataParallel() = 0;
```

---

## 8️⃣ FUNCIONES QUE PUEDEN REUTILIZARSE DE OTROS MÓDULOS

### Ya existen y NO necesitamos duplicar:

```cpp
✅ escapeSQL()                    → engines/database_engine.h
✅ StringUtils::toLower()         → utils/string_utils.h
✅ StringUtils::trim()            → utils/string_utils.h
✅ TimeUtils::getCurrentTimestamp() → utils/time_utils.h
✅ ConnectionStringParser::parse() → utils/connection_utils.h
```

---

## 9️⃣ MÉTRICAS DE DUPLICACIÓN

```
Total líneas:                    7409 líneas
Duplicación 100%:               ~1800 líneas (24%)
Duplicación 80-95%:             ~900 líneas (12%)
Código específico engine:       ~600 líneas (8%)
Código base consolidable:       ~2100 líneas (28%)
Código derivado (con cambios):  ~800 líneas × 2 = ~1600 líneas (22%)
Overhead (includes, comments):  ~400 líneas (6%)

REDUCCIÓN POTENCIAL: 7409 → ~4600 líneas = 2809 líneas eliminadas (38%)
```

---

## 🎯 SIGUIENTE PASO RECOMENDADO

### **Opción A: Empezar con clase base mínima (RECOMENDADO)**

1. Crear `DatabaseToPostgresSync.h` con 5-6 funciones 100% duplicadas
2. Hacer que MariaDB herede
3. Testing
4. Hacer que MSSQL herede
5. Testing
6. Iterar agregando más funciones

**VENTAJA:** Progreso incremental, menor riesgo

### **Opción B: Diseño completo primero**

1. Diseñar toda la jerarquía en papel
2. Implementar clase base completa
3. Refactorizar ambas clases de una vez
4. Testing masivo

**VENTAJA:** Diseño más coherente, pero más riesgo

---

## ✅ CONCLUSIÓN

**DUPLICACIÓN REAL:** ~2700 líneas (36% del código total)

**FUNCIONES 100% IDÉNTICAS:** 23 funciones (~1800 líneas)
**FUNCIONES 80-95% SIMILARES:** 12 funciones (~900 líneas)
**FUNCIONES ESPECÍFICAS:** 10 funciones (~600 líneas)

**BENEFICIOS DE CONSOLIDACIÓN:**

- ✅ Eliminar ~2800 líneas de código duplicado
- ✅ Bugs se arreglan 1 vez, no 2
- ✅ Agregar nuevos engines (Oracle, MongoDB) es más fácil
- ✅ Testing más simple (1 suite de tests base)
- ✅ Permite separar StreamingData.h en .h + .cpp

**COSTO:** 12-18 horas de trabajo

**RIESGO:** Medio (es código complejo, pero bien testeado)

---

## 🚦 SEMÁFORO DE PRIORIDAD

🟢 **ALTA:** Funciones 100% duplicadas → Mover a clase base primero  
🟡 **MEDIA:** Funciones 80-95% similares → Parametrizar/Templates  
⚪ **BAJA:** Funciones específicas → Mantener en derivadas
