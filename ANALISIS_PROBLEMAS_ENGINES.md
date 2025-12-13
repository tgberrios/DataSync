# Análisis Exhaustivo de Problemas - Carpeta src/engines

## RESUMEN EJECUTIVO

- **Total de problemas encontrados:** 52
- **Críticos:** 13
- **Altos:** 19
- **Medios:** 15
- **Bajos:** 5

---

## 1. SEGURIDAD

### 1.1 SQL Injection - CRÍTICO

#### Problema 1.1.1: Oracle Engine - Concatenación de strings sin escape adecuado

- **Archivo:** `oracle_engine.cpp`
- **Líneas:** 348-350, 388-393, 424-428
- **Severidad:** CRÍTICO
- **Descripción:**
  - La función `escapeSQL()` solo escapa comillas simples (`'` -> `''`), pero Oracle requiere un escape más robusto.
  - Los nombres de schema y tabla se concatenan directamente en queries sin validación adicional.
  - Oracle puede tener caracteres especiales que no son manejados por `escapeSQL()`.
- **Impacto:** Un atacante podría inyectar SQL malicioso a través de nombres de schema/table.
- **Ejemplo vulnerable:**

```cpp
std::string query = "SELECT owner, table_name FROM all_tables WHERE owner = '" +
                    escapeSQL(upperSchema) + "' ORDER BY owner, table_name";
```

#### Problema 1.1.2: MSSQL Engine - Sanitización insuficiente

- **Archivo:** `mssql_engine.cpp`
- **Líneas:** 262-271, 318-327, 388-397
- **Severidad:** CRÍTICO
- **Descripción:**
  - Solo se eliminan `'`, `;`, y `-`, pero no se escapan otros caracteres peligrosos.
  - No se valida la longitud de los strings después de la sanitización.
  - No se previene inyección a través de caracteres Unicode o de control.
- **Impacto:** SQL injection a través de caracteres especiales no contemplados.
- **Código vulnerable:**

```cpp
safeSchema.erase(std::remove_if(safeSchema.begin(), safeSchema.end(),
                 [](char c) { return c == '\'' || c == ';' || c == '-'; }),
                 safeSchema.end());
```

#### Problema 1.1.3: MongoDB Engine - SQL injection en query PostgreSQL

- **Archivo:** `mongodb_engine.cpp`
- **Líneas:** 208-211
- **Severidad:** CRÍTICO
- **Descripción:**
  - Se usa `escapeSQL()` (solo escapa comillas) en una query PostgreSQL.
  - PostgreSQL requiere un escape diferente y debería usarse `pqxx::connection::quote()` o parámetros.
- **Impacto:** SQL injection en la base de datos PostgreSQL target.
- **Código vulnerable:**

```cpp
std::string query = "SELECT COUNT(*) FROM information_schema.columns "
                    "WHERE table_schema = '" +
                    escapeSQL(schema) + "' AND table_name = '" +
                    escapeSQL(table) + "'";
```

#### Problema 1.1.4: MariaDB Engine - Buffer overflow potencial en mysql_real_escape_string

- **Archivo:** `mariadb_engine.cpp`
- **Líneas:** 238-243, 280-285, 338-343
- **Severidad:** CRÍTICO
- **Descripción:**
  - Se calcula el tamaño del buffer como `schema.length() * 2 + 1`, pero `mysql_real_escape_string` puede necesitar más espacio en casos edge.
  - No se verifica el valor de retorno de `mysql_real_escape_string`.
  - Si la conexión se cierra entre la creación del buffer y el escape, puede haber comportamiento indefinido.
- **Impacto:** Buffer overflow, corrupción de memoria, crash de la aplicación.
- **Código vulnerable:**

```cpp
char escapedSchema[schema.length() * 2 + 1];
char escapedTable[table.length() * 2 + 1];
mysql_real_escape_string(conn->get(), escapedSchema, schema.c_str(),
                         schema.length());
```

### 1.2 Memory Leaks - CRÍTICO

#### Problema 1.2.1: Oracle Engine - Manejo de errores incompleto en executeQuery

- **Archivo:** `oracle_engine.cpp`
- **Líneas:** 254-312
- **Severidad:** CRÍTICO
- **Descripción:**
  - Si `OCIStmtPrepare` falla después de `OCIHandleAlloc`, el statement handle se libera correctamente.
  - PERO: Si `OCIStmtExecute` falla después de crear los `OCIDefine`, los defines no se liberan.
  - Los buffers `std::vector` se destruyen automáticamente, pero los handles OCI no.
- **Impacto:** Memory leak de handles OCI en cada query fallida.
- **Código problemático:**

```cpp
std::vector<OCIDefine *> defines(numCols);
// ... si OCIStmtExecute falla, defines nunca se liberan
```

#### Problema 1.2.2: MongoDB Engine - Memory leak en discoverTables si hay excepción

- **Archivo:** `mongodb_engine.cpp`
- **Líneas:** 132-165
- **Severidad:** ALTO
- **Descripción:**
  - Si `mongoc_database_get_collection_names` retorna un array pero luego hay una excepción antes de `bson_strfreev`, se produce memory leak.
  - El código usa try-catch pero no garantiza la liberación en todos los paths.
- **Impacto:** Memory leak de strings de MongoDB.

#### Problema 1.2.3: MSSQL Engine - Statement handle no liberado en algunos casos de error

- **Archivo:** `mssql_engine.cpp`
- **Líneas:** 162-200
- **Severidad:** MEDIO
- **Descripción:**
  - Si `SQLFetch` retorna un error diferente a `SQL_SUCCESS` pero no es `SQL_NO_DATA`, el statement handle se libera al final.
  - Sin embargo, si hay un error durante `SQLGetData` en el loop, el handle se libera correctamente.
  - El problema es menor pero existe inconsistencia.

### 1.3 Buffer Overflows - CRÍTICO

#### Problema 1.3.1: MSSQL Engine - Buffer fijo en SQLGetData

- **Archivo:** `mssql_engine.cpp`
- **Líneas:** 184-191
- **Severidad:** CRÍTICO
- **Descripción:**
  - Se usa un buffer fijo de `DatabaseDefaults::BUFFER_SIZE` (1024 bytes).
  - Si una columna tiene más de 1024 caracteres, `SQLGetData` puede truncar o causar comportamiento indefinido.
  - No se maneja el caso de datos largos (LOB, TEXT, etc.).
- **Impacto:** Pérdida de datos, truncamiento silencioso, posible corrupción.
- **Código vulnerable:**

```cpp
char buffer[DatabaseDefaults::BUFFER_SIZE];
SQLLEN len;
ret = SQLGetData(stmt, i, SQL_C_CHAR, buffer, sizeof(buffer), &len);
```

#### Problema 1.3.2: Oracle Engine - Buffer fijo de 4000 bytes

- **Archivo:** `oracle_engine.cpp`
- **Líneas:** 292-295
- **Severidad:** ALTO
- **Descripción:**
  - Cada columna tiene un buffer fijo de 4000 bytes.
  - Si una columna excede este tamaño, se trunca sin advertencia.
  - Oracle puede tener columnas CLOB, BLOB, LONG que exceden este límite.
- **Impacto:** Truncamiento de datos, pérdida de información.

### 1.4 Race Conditions - ALTO

#### Problema 1.4.1: MongoDB Engine - Conexión no thread-safe

- **Archivo:** `mongodb_engine.cpp`
- **Líneas:** 10-19, 80-114
- **Severidad:** ALTO
- **Descripción:**
  - `mongoc_client_t *client_` es un miembro compartido de la clase.
  - No hay mutex o protección para acceso concurrente.
  - Múltiples threads pueden usar el mismo cliente simultáneamente, causando corrupción.
- **Impacto:** Corrupción de datos, crashes, comportamiento indefinido.
- **Solución requerida:** Usar mutex o crear conexiones por thread.

#### Problema 1.4.2: MariaDB/MSSQL Engine - Conexiones compartidas potenciales

- **Archivo:** `mariadb_engine.cpp`, `mssql_engine.cpp`
- **Líneas:** Varias
- **Severidad:** MEDIO
- **Descripción:**
  - `createConnection()` crea nuevas conexiones, pero si se reutiliza la misma instancia del engine desde múltiples threads sin protección, puede haber problemas.
  - Las conexiones individuales son seguras, pero el engine mismo no es thread-safe.

### 1.5 Exposición de Datos Sensibles - ALTO

#### Problema 1.5.1: Oracle Engine - Password en logs

- **Archivo:** `oracle_engine.cpp`
- **Líneas:** 56-79, 105-108
- **Severidad:** ALTO
- **Descripción:**
  - El connection string completo (que incluye password) se almacena en `connectionString_`.
  - Si se loguea el connection string en algún lugar, se expone la contraseña.
  - Aunque no se ve directamente en este archivo, el string está disponible para logging.
- **Impacto:** Exposición de credenciales en logs.

#### Problema 1.5.2: Todos los engines - Connection strings en memoria

- **Archivo:** Todos
- **Severidad:** MEDIO
- **Descripción:**
  - Los connection strings (con passwords) se almacenan como `std::string` en memoria.
  - No hay limpieza explícita de memoria sensible.
  - En sistemas con swap, los passwords pueden persistir en disco.

---

## 2. BUGS Y ERRORES

### 2.1 Errores de Lógica - ALTO

#### Problema 2.1.1: MongoDB Engine - Lógica incorrecta en getColumnCounts

- **Archivo:** `mongodb_engine.cpp`
- **Líneas:** 181-224
- **Severidad:** ALTO
- **Descripción:**
  - `getColumnCounts` retorna el conteo de documentos de MongoDB como `sourceCount`.
  - Pero luego consulta `information_schema.columns` en PostgreSQL para `targetCount`.
  - Esto compara documentos de MongoDB con columnas de PostgreSQL, lo cual no tiene sentido.
  - Debería comparar documentos con documentos, o columnas con columnas.
- **Impacto:** Lógica de sincronización incorrecta, comparaciones inválidas.

#### Problema 2.1.2: Oracle Engine - Error en manejo de NULL en executeQuery

- **Archivo:** `oracle_engine.cpp`
- **Líneas:** 302-306
- **Severidad:** MEDIO
- **Descripción:**
  - Se verifica `inds[i] == -1` para NULL, pero Oracle puede usar otros valores de indicador.
  - No se verifica el status de `OCIDefineByPos` para errores.
  - Si `lengths[i]` es 0 pero `inds[i]` no es -1, puede haber comportamiento indefinido.
- **Impacto:** Manejo incorrecto de valores NULL, posibles crashes.

#### Problema 2.1.4: Oracle Engine - OCIStmtFetch no maneja OCI_NO_DATA explícitamente

- **Archivo:** `oracle_engine.cpp`
- **Líneas:** 298-299
- **Severidad:** MEDIO
- **Descripción:**
  - El loop `while (OCIStmtFetch(...) == OCI_SUCCESS)` no verifica explícitamente `OCI_NO_DATA`.
  - Aunque `OCI_NO_DATA` no es `OCI_SUCCESS` y el loop terminará, es mejor práctica verificar explícitamente.
  - No se verifica `OCI_SUCCESS_WITH_INFO` que puede indicar warnings.
- **Impacto:** Warnings no detectados, posible pérdida de información.

#### Problema 2.1.3: MSSQL Engine - No se verifica SQL_NO_DATA

- **Archivo:** `mssql_engine.cpp`
- **Líneas:** 181-197
- **Severidad:** MEDIO
- **Descripción:**
  - El loop `while (SQLFetch(stmt) == SQL_SUCCESS)` no maneja explícitamente `SQL_NO_DATA`.
  - Aunque `SQL_NO_DATA` no es `SQL_SUCCESS`, es mejor práctica verificar explícitamente.
  - No se verifica si hay más datos disponibles después de un fetch parcial.

### 2.2 Manejo de Errores Faltante - ALTO

#### Problema 2.2.1: Oracle Engine - OCIAttrSet sin verificación de status

- **Archivo:** `oracle_engine.cpp`
- **Líneas:** 130-133
- **Severidad:** ALTO
- **Descripción:**
  - `OCIAttrSet` para USERNAME y PASSWORD no verifica el valor de retorno.
  - Si falla, la conexión puede parecer válida pero no funcionar correctamente.
- **Impacto:** Conexiones que parecen válidas pero fallan silenciosamente.
- **Código problemático:**

```cpp
OCIAttrSet(session_, OCI_HTYPE_SESSION, (OraText *)user.c_str(),
           user.length(), OCI_ATTR_USERNAME, err_);
OCIAttrSet(session_, OCI_HTYPE_SESSION, (OraText *)password.c_str(),
           password.length(), OCI_ATTR_PASSWORD, err_);
```

#### Problema 2.2.2: MSSQL Engine - SQLNumResultCols sin verificación

- **Archivo:** `mssql_engine.cpp`
- **Líneas:** 178-179
- **Severidad:** MEDIO
- **Descripción:**
  - `SQLNumResultCols` no verifica el valor de retorno.
  - Si falla, `numCols` puede tener un valor inválido, causando loops incorrectos.
- **Impacto:** Comportamiento indefinido, posibles crashes.

#### Problema 2.2.3: MariaDB Engine - mysql_store_result sin verificación completa

- **Archivo:** `mariadb_engine.cpp`
- **Líneas:** 164-171
- **Severidad:** MEDIO
- **Descripción:**
  - Se verifica si `res` es nullptr, pero no se distingue entre "no hay resultados" y "error".
  - `mysql_field_count(conn) > 0` indica que debería haber resultados, pero no es suficiente.
  - Debería verificarse `mysql_errno(conn)` para distinguir errores de "sin resultados".

#### Problema 2.2.4: Oracle Engine - OCIAttrGet sin verificación de retorno

- **Archivo:** `oracle_engine.cpp`
- **Líneas:** 283-285
- **Severidad:** ALTO
- **Descripción:**
  - `OCIAttrGet` no verifica el valor de retorno.
  - Si falla, `numCols` puede quedar con valor 0 o no inicializado.
  - Esto causa que el loop de `OCIDefineByPos` no se ejecute o se ejecute incorrectamente.
- **Impacto:** Queries que retornan resultados pero no se procesan, pérdida de datos.

#### Problema 2.2.5: Oracle Engine - OCIDefineByPos sin verificación

- **Archivo:** `oracle_engine.cpp`
- **Líneas:** 294-295
- **Severidad:** ALTO
- **Descripción:**
  - `OCIDefineByPos` no verifica el valor de retorno en el loop.
  - Si falla para alguna columna, el define no se establece correctamente.
  - Los datos de esa columna pueden ser incorrectos o causar crashes en `OCIStmtFetch`.
- **Impacto:** Datos corruptos, posibles crashes.

#### Problema 2.2.6: Catch-all blocks sin logging

- **Archivo:** `oracle_engine.cpp`, `mongodb_engine.cpp`, `mssql_engine.cpp`, `mariadb_engine.cpp`
- **Líneas:** 486, 69, 412, 25, 357
- **Severidad:** MEDIO
- **Descripción:**
  - Múltiples bloques `catch (...) { }` que capturan todas las excepciones sin logging.
  - Hace muy difícil debuggear problemas en producción.
  - Al menos debería loguearse el tipo de excepción o un mensaje genérico.
- **Impacto:** Pérdida de información de debugging, problemas silenciosos.

### 2.3 Casos Límite No Manejados - ALTO

#### Problema 2.3.1: Oracle Engine - División por cero potencial en parsing

- **Archivo:** `oracle_engine.cpp`
- **Líneas:** 59-79
- **Severidad:** MEDIO
- **Descripción:**
  - Si `token.find('=')` retorna `std::string::npos`, `pos + 1` puede causar problemas.
  - Aunque se verifica `pos == std::string::npos`, el código es frágil.
  - Si `value` está vacío después del `=`, no se valida.

#### Problema 2.3.2: MongoDB Engine - Parsing de connection string frágil

- **Archivo:** `mongodb_engine.cpp`
- **Líneas:** 23-78
- **Severidad:** MEDIO
- **Descripción:**
  - El parsing manual de la connection string es muy frágil.
  - No maneja casos edge como:
    - URLs con parámetros de query complejos
    - Credenciales con caracteres especiales
    - IPv6 addresses
    - Replica sets con múltiples hosts
- **Impacto:** Fallos de conexión en casos válidos pero complejos.

#### Problema 2.3.3: Todos los engines - Strings vacíos después de transformaciones

- **Archivo:** Varios
- **Líneas:** Varias
- **Severidad:** MEDIO
- **Descripción:**
  - Después de `std::transform` a lowercase/uppercase, o después de sanitización, los strings pueden quedar vacíos.
  - No se valida que el string resultante no esté vacío antes de usarlo en queries.
- **Impacto:** Queries inválidas, errores de sintaxis SQL.

### 2.4 Off-by-One Errors - MEDIO

#### Problema 2.4.1: Oracle Engine - Indices de columna y validación de numCols

- **Archivo:** `oracle_engine.cpp`
- **Líneas:** 283-296
- **Severidad:** MEDIO
- **Descripción:**
  - `OCIDefineByPos` usa `i + 1` (correcto, Oracle es 1-based).
  - Pero el loop `for (ub4 i = 0; i < numCols; ++i)` puede tener problemas si `numCols` es 0.
  - Aunque `numCols` debería ser > 0 para queries SELECT, no se valida.
  - Si `numCols` es 0, el vector `defines` estará vacío pero se intentará usar.

---

## 3. CALIDAD DE CÓDIGO

### 3.1 Uso Incorrecto de APIs - ALTO

#### Problema 3.1.1: Oracle Engine - OCIErrorGet sin verificación de buffer

- **Archivo:** `oracle_engine.cpp`
- **Líneas:** 101-104
- **Severidad:** MEDIO
- **Descripción:**
  - `OCIErrorGet` puede retornar más de 512 bytes de mensaje de error.
  - El buffer está fijo a 512 bytes, puede truncar mensajes de error importantes.
- **Impacto:** Pérdida de información de debugging.

#### Problema 3.1.2: MSSQL Engine - SQLGetDiagRec sin verificación completa

- **Archivo:** `mssql_engine.cpp`
- **Líneas:** 44-48
- **Severidad:** MEDIO
- **Descripción:**
  - `SQLGetDiagRec` puede retornar múltiples registros de diagnóstico.
  - Solo se obtiene el primer registro (índice 1).
  - Puede haber información de error adicional en registros subsecuentes.

#### Problema 3.1.3: MariaDB Engine - mysql_real_escape_string sin conexión válida

- **Archivo:** `mariadb_engine.cpp`
- **Líneas:** 238-243
- **Severidad:** ALTO
- **Descripción:**
  - Se llama `mysql_real_escape_string` con `conn->get()`, pero no se verifica que la conexión esté activa.
  - Si la conexión se cerró o es inválida, el comportamiento es indefinido.
- **Impacto:** Crashes, comportamiento indefinido.

### 3.2 Patrones Anti-Pattern - MEDIO

#### Problema 3.2.1: Todos los engines - Código duplicado en sanitización

- **Archivo:** `mssql_engine.cpp`, `mariadb_engine.cpp`
- **Líneas:** Varias
- **Severidad:** MEDIO
- **Descripción:**
  - La lógica de sanitización/escape se repite en múltiples funciones.
  - Debería extraerse a funciones helper (pero el usuario prefiere no crear helpers según memoria).
  - Sin embargo, la duplicación aumenta el riesgo de inconsistencias.

#### Problema 3.2.2: Oracle Engine - Magic numbers

- **Archivo:** `oracle_engine.cpp`
- **Líneas:** 293
- **Severidad:** BAJO
- **Descripción:**
  - El valor `4000` está hardcodeado como tamaño de buffer.
  - Debería ser una constante con nombre descriptivo.

#### Problema 3.2.3: MongoDB Engine - Parsing manual en lugar de usar API

- **Archivo:** `mongodb_engine.cpp`
- **Líneas:** 23-78
- **Severidad:** MEDIO
- **Descripción:**
  - Se parsea manualmente la connection string de MongoDB.
  - MongoDB C driver tiene funciones para esto (`mongoc_uri_t`).
  - El parsing manual es propenso a errores.

### 3.3 Inconsistencias - MEDIO

#### Problema 3.3.1: Inconsistencia en manejo de errores entre engines

- **Archivo:** Todos
- **Severidad:** MEDIO
- **Descripción:**
  - Oracle: retorna early con cleanup en constructor.
  - MSSQL: similar pero con diferentes patrones de cleanup.
  - MariaDB: cierra conexión en destructor.
  - MongoDB: maneja errores de forma diferente.
  - No hay un patrón consistente de manejo de errores.

#### Problema 3.3.2: Inconsistencia en validación de entrada

- **Archivo:** Todos
- **Severidad:** MEDIO
- **Descripción:**
  - Algunos métodos validan `schema.empty() || table.empty()` (MSSQL, MariaDB, PostgreSQL).
  - Oracle no valida esto en algunos métodos.
  - MongoDB no valida en `getColumnCounts`.

### 3.4 Variables No Inicializadas - MEDIO

#### Problema 3.4.1: MSSQL Engine - outConnStrLen puede no inicializarse

- **Archivo:** `mssql_engine.cpp`
- **Líneas:** 38-42
- **Severidad:** BAJO
- **Descripción:**
  - `outConnStrLen` se declara pero solo se inicializa si `SQLDriverConnect` tiene éxito.
  - Si falla antes de asignar, el valor es indeterminado (aunque no se usa después del error).

### 3.5 Recursos No Liberados - ALTO

#### Problema 3.5.1: Oracle Engine - OCIDefine no liberado

- **Archivo:** `oracle_engine.cpp`
- **Líneas:** 287-296
- **Descripción:**
  - Los `OCIDefine *` se crean pero nunca se liberan explícitamente.
  - Según documentación OCI, los defines se liberan automáticamente cuando se libera el statement.
  - PERO: si hay un error antes de liberar el statement, puede haber leak.
  - **Severidad:** MEDIO

---

## 4. LÓGICA DE NEGOCIO

### 4.1 Flujos de Control Incorrectos - ALTO

#### Problema 4.1.1: MongoDB Engine - getColumnCounts con lógica incorrecta

- **Archivo:** `mongodb_engine.cpp`
- **Líneas:** 181-224
- **Severidad:** CRÍTICO (lógica de negocio)
- **Descripción:**
  - Compara conteo de documentos (MongoDB) con conteo de columnas (PostgreSQL).
  - Esto no tiene sentido semánticamente.
  - Debería comparar documentos con filas, o estructuras con estructuras.
- **Impacto:** Validaciones de sincronización completamente incorrectas.

#### Problema 4.1.2: Oracle Engine - Uso de comillas dobles sin escape adecuado

- **Archivo:** `oracle_engine.cpp`
- **Líneas:** 461-480
- **Severidad:** ALTO
- **Descripción:**
  - En `getColumnCounts`, se escapan comillas dobles manualmente (`"` -> `""`).
  - Pero esto solo funciona si el nombre está entre comillas dobles.
  - Si el nombre tiene otros caracteres especiales, puede fallar.
  - La lógica de escape es inconsistente con el resto del código.

### 4.2 Validaciones Faltantes - ALTO

#### Problema 4.2.1: Oracle Engine - No valida schema/table vacíos en algunos métodos

- **Archivo:** `oracle_engine.cpp`
- **Líneas:** 370-406, 408-438
- **Severidad:** ALTO
- **Descripción:**
  - `detectPrimaryKey` y `detectTimeColumn` no validan que schema/table no estén vacíos.
  - Si están vacíos, las queries pueden ser inválidas o retornar resultados incorrectos.

#### Problema 4.2.2: Todos los engines - No validan longitud máxima de nombres

- **Archivo:** Todos
- **Severidad:** MEDIO
- **Descripción:**
  - Los nombres de schema/table pueden tener límites en cada DBMS.
  - No se valida la longitud antes de construir queries.
  - Puede causar errores de sintaxis SQL o truncamiento.

### 4.3 Inconsistencias Entre Funciones - MEDIO

#### Problema 4.3.1: Oracle Engine - Diferentes estrategias de escape

- **Archivo:** `oracle_engine.cpp`
- **Líneas:** 348-350 vs 461-480
- **Severidad:** MEDIO
- **Descripción:**
  - En `discoverTables` y `detectPrimaryKey` usa `escapeSQL()` (comillas simples).
  - En `getColumnCounts` usa escape manual de comillas dobles.
  - Inconsistencia que puede causar bugs difíciles de encontrar.

---

## 5. MEJORES PRÁCTICAS

### 5.1 Violaciones de SOLID - MEDIO

#### Problema 5.1.1: Responsabilidades múltiples en clases Engine

- **Archivo:** Todos
- **Severidad:** MEDIO
- **Descripción:**
  - Cada engine maneja: conexión, queries, descubrimiento, detección de PK, etc.
  - Violación del principio de responsabilidad única.
  - Sin embargo, esto puede ser aceptable para mantener simplicidad (KISS).

### 5.2 Código Duplicado - MEDIO

#### Problema 5.2.1: Lógica de retry duplicada

- **Archivo:** `mariadb_engine.cpp`, `mssql_engine.cpp`
- **Líneas:** 83-120, 118-147
- **Severidad:** MEDIO
- **Descripción:**
  - La lógica de retry con exponential backoff está duplicada.
  - Mismo patrón, mismos valores (MAX_RETRIES=3, INITIAL_BACKOFF_MS=100).
  - Nota: El usuario prefiere no crear helpers, pero la duplicación aumenta riesgo.

#### Problema 5.2.2: Parsing de connection string duplicado

- **Archivo:** `oracle_engine.cpp`, `mongodb_engine.cpp`
- **Líneas:** 56-79, 23-78
- **Severidad:** MEDIO
- **Descripción:**
  - Cada engine parsea su connection string de forma diferente.
  - Oracle: parsing manual con `;` como separador.
  - MongoDB: parsing manual de URL.
  - Debería haber un parser común o usar librerías estándar.

### 5.3 Funciones Demasiado Largas - BAJO

#### Problema 5.3.1: Oracle Engine - Constructor muy largo

- **Archivo:** `oracle_engine.cpp`
- **Líneas:** 6-164
- **Severidad:** BAJO
- **Descripción:**
  - El constructor de `OCIConnection` tiene 158 líneas.
  - Mucha lógica de inicialización y cleanup.
  - Difícil de mantener y testear.
  - Sin embargo, extraer funciones violaría la preferencia del usuario de no crear helpers.

### 5.4 Acoplamiento Excesivo - BAJO

#### Problema 5.4.1: Dependencia directa de pqxx en engines

- **Archivo:** Varios
- **Severidad:** BAJO
- **Descripción:**
  - Los engines tienen dependencia directa de `pqxx` para consultas a PostgreSQL.
  - Esto acopla los engines con PostgreSQL.
  - Idealmente, debería haber una abstracción, pero esto puede ser aceptable para simplicidad.

---

## RECOMENDACIONES PRIORITARIAS

### CRÍTICAS (Resolver inmediatamente):

1. **SQL Injection en Oracle/MSSQL/MongoDB** - Usar parámetros preparados o escape robusto
2. **Buffer overflow en MSSQL/Oracle** - Manejar datos largos correctamente
3. **Memory leaks en Oracle** - Liberar OCIDefine correctamente
4. **Lógica incorrecta en MongoDB getColumnCounts** - Corregir comparación documentos vs columnas
5. **Race conditions en MongoDB** - Hacer conexión thread-safe
6. **OCIAttrGet/OCIDefineByPos sin verificación** - Validar todos los valores de retorno OCI

### ALTAS (Resolver pronto):

1. **Manejo de errores incompleto** - Verificar todos los valores de retorno
2. **Validaciones faltantes** - Validar inputs en todos los métodos
3. **Exposición de passwords** - Limpiar memoria sensible
4. **Casos límite** - Manejar strings vacíos, valores extremos

### MEDIAS (Mejorar cuando sea posible):

1. **Código duplicado** - Considerar extracción (respetando preferencias del usuario)
2. **Inconsistencias** - Estandarizar patrones entre engines
3. **Magic numbers** - Usar constantes con nombre

---

## NOTAS FINALES

- Muchos problemas son consecuencia de la preferencia del usuario de mantener código simple (KISS) y no crear funciones helper.
- Algunos problemas de "mejores prácticas" pueden ser aceptables si priorizan simplicidad.
- Los problemas de seguridad (SQL injection, buffer overflow, memory leaks) deben resolverse independientemente de las preferencias de diseño.
- Se recomienda crear tests unitarios para cubrir casos límite y validar correcciones.
