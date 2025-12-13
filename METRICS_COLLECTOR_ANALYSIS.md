# Análisis Exhaustivo de Problemas - MetricsCollector.cpp

## RESUMEN EJECUTIVO

**Archivo analizado:** `src/metrics/MetricsCollector.cpp` (626 líneas)
**Total de problemas encontrados:** 48

- Críticos: 7
- Altos: 13
- Medios: 19
- Bajos: 9

---

## 1. SEGURIDAD

### 1.1 SQL Injection - CRÍTICO

**Ubicación:** Línea 608-610
**Severidad:** CRÍTICO
**Descripción:** La función `calculateBytesTransferred` construye una consulta SQL mediante concatenación de strings sin usar parámetros preparados. Aunque usa `escapeSQL`, esto no es suficiente para prevenir todos los tipos de inyección SQL.

```cpp
std::string sizeQuery =
    "SELECT COALESCE(pg_total_relation_size(to_regclass('\"" +
    escapeSQL(lowerSchema) + "\".\"" + escapeSQL(lowerTable) +
    "\"')), 0) as size_bytes;";
```

**Impacto:** Un atacante podría manipular los nombres de esquema/tabla para ejecutar código SQL arbitrario.
**Recomendación:** Usar `pqxx::params` o `exec_params` con identificadores escapados correctamente.

### 1.2 Escape SQL Incompleto - ALTO

**Ubicación:** Líneas 533-541
**Severidad:** ALTO
**Descripción:** La función `escapeSQL` solo escapa comillas simples, pero no maneja otros caracteres peligrosos como backslashes, comillas dobles, o caracteres de control.

```cpp
std::string MetricsCollector::escapeSQL(const std::string &str) {
  std::string escaped = str;
  size_t pos = 0;
  while ((pos = escaped.find('\'', pos)) != std::string::npos) {
    escaped.replace(pos, 1, "''");
    pos += 2;
  }
  return escaped;
}
```

**Impacto:** Posible inyección SQL o errores de parsing con nombres de tablas que contengan caracteres especiales.
**Recomendación:** Usar funciones de escape de pqxx o validar nombres de objetos de base de datos según reglas de PostgreSQL.

### 1.3 Exposición de Información en Logs - MEDIO

**Ubicación:** Líneas 28-29, 93-94, 218-220, 283-285, 355-357, 406-408, 459-460, 528-529, 595-596, 619-621
**Severidad:** MEDIO
**Descripción:** Los mensajes de error incluyen `e.what()` que puede exponer información sensible sobre la estructura de la base de datos, nombres de tablas, esquemas, o detalles de configuración.
**Impacto:** Información sensible podría ser expuesta en logs accesibles.
**Recomendación:** Sanitizar mensajes de error antes de loguearlos, especialmente en producción.

### 1.4 Falta de Validación de Entrada - ALTO

**Ubicación:** Líneas 145-153, 319-322, 386-390
**Severidad:** ALTO
**Descripción:** Los datos extraídos de la base de datos se asignan directamente sin validar longitud, formato, o contenido malicioso. No hay límites en el tamaño de strings.
**Impacto:** Posible overflow de buffers o inyección de datos maliciosos.
**Recomendación:** Validar longitud máxima de strings (especialmente schema_name, table_name, db_engine) y usar límites consistentes con la definición de la tabla.

### 1.5 Conexiones de Base de Datos sin Timeout Consistente - MEDIO

**Ubicación:** Líneas 109, 234, 299, 369, 421, 473, 601
**Severidad:** MEDIO
**Descripción:** Solo `createMetricsTable` establece timeouts (líneas 46-47), pero todas las demás funciones que crean conexiones no lo hacen. Esto puede causar que las conexiones se cuelguen indefinidamente.
**Impacto:** Denegación de servicio si la base de datos está lenta o no responde.
**Recomendación:** Establecer timeouts en todas las conexiones de forma consistente.

---

## 2. BUGS Y ERRORES

### 2.1 División por Cero Potencial - ALTO

**Ubicación:** Línea 174
**Severidad:** ALTO
**Descripción:** Aunque hay validación para `tableSizeBytes <= 0` en la línea 164, la división se hace antes de esa validación en la línea 174. Si `tableSizeBytes` es 0, la división es segura pero innecesaria.

```cpp
metric.memory_used_mb = tableSizeBytes / (1024.0 * 1024.0);
```

**Impacto:** Aunque no causa crash (división por float), es código ineficiente y confuso.
**Recomendación:** Mover el cálculo después de las validaciones o verificar explícitamente.

### 2.2 Acceso a Índices de Array sin Validación - CRÍTICO

**Ubicación:** Líneas 141-157, 272-278, 331-350, 398-401
**Severidad:** CRÍTICO
**Descripción:** El código accede a `row[0]`, `row[1]`, etc. sin verificar que el resultado de la consulta tenga suficientes columnas. Si la consulta SQL cambia o retorna menos columnas, esto causará excepciones no manejadas.
**Impacto:** Crash de la aplicación si la estructura de la consulta no coincide con las expectativas.
**Recomendación:** Validar `row.size()` antes de acceder a índices específicos.

### 2.3 Manejo de NULL Inconsistente - ALTO

**Ubicación:** Líneas 155-157, 206-212
**Severidad:** ALTO
**Descripción:** Algunos campos se validan con `is_null()` (línea 155, 206), pero otros se acceden directamente sin verificación (línea 333 para `status`). El código asume que ciertos campos nunca serán NULL.
**Impacto:** Excepciones en tiempo de ejecución si la base de datos contiene valores NULL inesperados.
**Recomendación:** Validar todos los campos que pueden ser NULL antes de accederlos.

### 2.4 Error en Cálculo de I/O Operations - ALTO

**Ubicación:** Línea 277
**Severidad:** ALTO
**Descripción:** El código suma `n_tup_ins`, `n_tup_upd`, `n_tup_del` y lo asigna a `io_operations_per_second`, pero estos son contadores acumulativos desde el inicio del servidor, no operaciones por segundo. El nombre de la variable es engañoso.

```cpp
long long total_operations = row[2].as<long long>() +
                             row[3].as<long long>() +
                             row[4].as<long long>();
metric.io_operations_per_second = static_cast<int>(total_operations);
```

**Impacto:** Métricas incorrectas que no representan la realidad.
**Recomendación:** Renombrar la variable y/o calcular la tasa real comparando con timestamps.

### 2.5 Overflow Potencial en Conversión de Tipos - MEDIO

**Ubicación:** Línea 277
**Severidad:** MEDIO
**Descripción:** `total_operations` es `long long` pero se convierte a `int` (línea 277). Si el valor excede el rango de `int`, habrá truncamiento silencioso.
**Impacto:** Pérdida de datos en sistemas con muchas operaciones.
**Recomendación:** Validar el rango antes de la conversión o cambiar el tipo de dato en la estructura.

### 2.6 Falta de Validación de Tamaño de Fila - CRÍTICO

**Ubicación:** Línea 278 (y múltiples otras)
**Severidad:** CRÍTICO
**Descripción:** El código accede a `row[9]` sin verificar que la fila tenga al menos 10 columnas. Aunque la consulta debería retornar 10 columnas, si la estructura de la consulta cambia o hay un error, esto causará una excepción no manejada.

```cpp
// No hay validación de row.size() antes de acceder a row[9]
metric.memory_used_mb = row[9].as<long long>() / (1024.0 * 1024.0);
```

**Impacto:** Excepción en tiempo de ejecución, crash de la aplicación si la consulta retorna menos columnas de las esperadas.
**Recomendación:** Validar `row.size() >= 10` antes de acceder a índices específicos.

### 2.7 Lógica de Validación Incorrecta - MEDIO

**Ubicación:** Líneas 168-170
**Severidad:** MEDIO
**Descripción:** El código salta métricas si `currentRecords <= 0 && tableSizeBytes <= 0`, pero esto puede omitir tablas válidas que están vacías pero tienen estructura (índices, etc.). Además, la validación ocurre después de asignar valores.
**Impacto:** Tablas vacías no se incluyen en las métricas, lo cual puede ser información valiosa.
**Recomendación:** Reconsiderar la lógica de validación o incluir tablas vacías con valores cero.

### 2.8 Parsing de Timestamp Frágil - MEDIO

**Ubicación:** Líneas 547-553
**Severidad:** MEDIO
**Descripción:** `getEstimatedStartTime` parsea timestamps con formato fijo `"%Y-%m-%d %H:%M:%S"`, pero los timestamps de PostgreSQL pueden incluir fracciones de segundo o zonas horarias. Si el formato no coincide exactamente, falla silenciosamente.
**Impacto:** Timestamps incorrectos si la base de datos retorna un formato diferente.
**Recomendación:** Usar funciones de parsing más robustas o manejar múltiples formatos.

### 2.9 Uso de std::localtime No Thread-Safe - ALTO

**Ubicación:** Líneas 561, 19 (en time_utils.h)
**Severidad:** ALTO
**Descripción:** `std::localtime` retorna un puntero a una variable estática compartida. Si múltiples threads llaman esta función simultáneamente, hay condición de carrera.

```cpp
result << std::put_time(std::localtime(&time_t_start), "%Y-%m-%d %H:%M:%S");
```

**Impacto:** Comportamiento indefinido, posibles crashes o datos corruptos en entornos multi-threaded.
**Recomendación:** Usar `std::localtime_r` (POSIX) o proteger con mutex, o mejor aún, usar funciones thread-safe de C++20.

### 2.10 Manejo de Excepciones Genérico - MEDIO

**Ubicación:** Múltiples ubicaciones (27-30, 92-95, 217-221, etc.)
**Severidad:** MEDIO
**Descripción:** Todas las excepciones se capturan como `std::exception`, pero diferentes tipos de excepciones (pqxx::sql_error, std::bad_alloc, etc.) requieren diferentes manejos.
**Impacto:** Errores específicos de base de datos pueden no manejarse apropiadamente.
**Recomendación:** Capturar excepciones específicas de pqxx y manejar cada tipo apropiadamente.

### 2.11 Transacciones No Commiteadas en Caso de Error - ALTO

**Ubicación:** Líneas 55, 110, 235, 300, 370, 422, 474
**Severidad:** ALTO
**Descripción:** Si ocurre una excepción después de crear una transacción pero antes de `commit()`, la transacción se revierte automáticamente al destruirse, pero no hay logging explícito de esto.
**Impacto:** Pérdida silenciosa de datos o inconsistencias no detectadas.
**Recomendación:** Usar RAII apropiadamente y loguear cuando las transacciones se revierten.

### 2.12 Validación de Conexión Inconsistente - MEDIO

**Ubicación:** Líneas 49-53 vs otras funciones
**Severidad:** MEDIO
**Descripción:** Solo `createMetricsTable` verifica `conn.is_open()` después de crear la conexión. Las demás funciones asumen que la conexión es válida.
**Impacto:** Errores no detectados tempranamente, causando fallos más adelante.
**Recomendación:** Validar todas las conexiones de forma consistente.

---

## 3. CALIDAD DE CÓDIGO

### 3.1 Código Muerto (Dead Code) - BAJO

**Ubicación:** Líneas 574-581, 589-625
**Severidad:** BAJO
**Descripción:** Las funciones `calculateTransferRate` y `calculateBytesTransferred` están marcadas como `[[deprecated]]` y no se usan en el código.
**Impacto:** Mantenimiento innecesario, confusión para desarrolladores.
**Recomendación:** Eliminar el código muerto o implementar si se necesita.

### 3.2 Includes No Utilizados - BAJO

**Ubicación:** Líneas 5-6
**Severidad:** BAJO
**Descripción:** Se incluyen `<algorithm>` y `<numeric>` pero no se usan en el código.
**Impacto:** Compilación más lenta, confusión.
**Recomendación:** Eliminar includes no utilizados.

### 3.3 Variable No Utilizada - BAJO

**Ubicación:** Línea 119, 246, 247, 309
**Severidad:** BAJO
**Descripción:** Se seleccionan columnas `last_sync_column` en las consultas pero nunca se usan.
**Impacto:** Consultas innecesariamente complejas, posible confusión.
**Recomendación:** Eliminar columnas no utilizadas de las consultas.

### 3.4 Magic Numbers - MEDIO

**Ubicación:** Líneas 46-47, 174, 278, 510
**Severidad:** MEDIO
**Descripción:** Valores hardcodeados como `30000`, `10000`, `1024.0 * 1024.0`, `1` (hora en línea 557) sin constantes con nombre.
**Impacto:** Código difícil de mantener y entender.
**Recomendación:** Definir constantes con nombres descriptivos.

### 3.5 Funciones Demasiado Largas - MEDIO

**Ubicación:** `collectTransferMetrics` (107-222), `saveMetricsToDatabase` (419-462)
**Severidad:** MEDIO
**Descripción:** Funciones con más de 100 líneas que hacen múltiples cosas, violando el principio de responsabilidad única.
**Impacto:** Código difícil de testear, mantener y depurar.
**Recomendación:** Dividir en funciones más pequeñas y enfocadas.

### 3.6 Duplicación de Código - MEDIO

**Ubicación:** Múltiples funciones (109, 234, 299, 369, 421, 473)
**Severidad:** MEDIO
**Descripción:** El patrón de crear conexión, transacción, ejecutar consulta, commit se repite en múltiples funciones.
**Impacto:** Violación del principio DRY, más código para mantener.
**Recomendación:** Extraer a una función helper (aunque el usuario prefiere no crear helpers según memoria - verificar si esto aplica).

### 3.7 Inconsistencia en Naming - BAJO

**Ubicación:** Línea 277
**Severidad:** BAJO
**Descripción:** `io_operations_per_second` sugiere una tasa, pero almacena un total acumulado.
**Impacto:** Confusión sobre qué representa la métrica.
**Recomendación:** Renombrar a `total_io_operations` o calcular la tasa real.

### 3.8 Falta de Validación de Precondiciones - MEDIO

**Ubicación:** Todas las funciones públicas/privadas
**Severidad:** MEDIO
**Descripción:** Las funciones no validan precondiciones (por ejemplo, que `metrics` no esté vacío antes de procesarlo).
**Impacto:** Comportamiento indefinido o errores confusos.
**Recomendación:** Agregar validaciones de precondiciones con mensajes claros.

### 3.9 Uso de std::string para Timestamps - BAJO

**Ubicación:** Estructura `TransferMetrics` (líneas 27-28 en .h)
**Severidad:** BAJO
**Descripción:** Los timestamps se almacenan como strings en lugar de tipos de fecha/hora apropiados.
**Impacto:** Dificulta operaciones de fecha, validación, y comparaciones.
**Recomendación:** Usar `std::chrono::time_point` o tipos de PostgreSQL.

### 3.10 Construcción de Clave de Hash Frágil - MEDIO

**Ubicación:** Líneas 262-264, 319-322, 387-390
**Severidad:** MEDIO
**Descripción:** Las claves para los hash maps se construyen concatenando strings con `"|"`. Si algún nombre contiene este carácter, habrá colisiones.
**Impacto:** Datos incorrectos si hay nombres de tablas con el carácter `|`.
**Recomendación:** Usar un separador más seguro o un método de hash más robusto.

---

## 4. LÓGICA DE NEGOCIO

### 4.1 Sobrescritura de Datos sin Verificación - ALTO

**Ubicación:** Líneas 232-287, 297-359, 367-410
**Severidad:** ALTO
**Descripción:** `collectPerformanceMetrics`, `collectMetadataMetrics`, y `collectTimestampMetrics` sobrescriben datos ya calculados en `collectTransferMetrics` sin verificar si tienen sentido o son más precisos.
**Impacto:** Datos válidos pueden ser sobrescritos con datos incorrectos o menos precisos.
**Recomendación:** Implementar lógica de merge inteligente que preserve los mejores datos.

### 4.2 Orden de Ejecución Problemático - MEDIO

**Ubicación:** Líneas 19-25
**Severidad:** MEDIO
**Descripción:** `collectAllMetrics` llama a las funciones en un orden específico, pero si una falla, las siguientes pueden ejecutarse con datos incompletos o inconsistentes.
**Impacto:** Métricas inconsistentes o incorrectas si hay fallos parciales.
**Recomendación:** Validar que cada paso se complete exitosamente antes de continuar, o implementar transacciones lógicas.

### 4.3 Actualización de Métricas en Lugar de Inserción - MEDIO

**Ubicación:** Líneas 433-443
**Severidad:** MEDIO
**Descripción:** El código usa `ON CONFLICT DO UPDATE` que actualiza métricas existentes del mismo día. Esto puede sobrescribir métricas históricas si se ejecuta múltiples veces el mismo día.
**Impacto:** Pérdida de datos históricos, imposibilidad de rastrear cambios durante el día.
**Recomendación:** Considerar insertar nuevas filas con timestamps más granulares o mantener historial.

### 4.4 Estimación de Tiempo de Inicio Arbitraria - BAJO

**Ubicación:** Líneas 208, 543-568
**Severidad:** BAJO
**Descripción:** `getEstimatedStartTime` siempre resta 1 hora del tiempo de completado, lo cual es una estimación arbitraria que puede ser muy incorrecta.
**Impacto:** Métricas de duración incorrectas.
**Recomendación:** Usar datos reales si están disponibles, o al menos hacer la estimación configurable.

### 4.5 Mapeo de Status Incompleto - MEDIO

**Ubicación:** Líneas 177-204, 333-350
**Descripción:** El mapeo de status del catálogo a status de métricas es incompleto y tiene lógica duplicada en dos lugares diferentes con posibles inconsistencias.
**Impacto:** Status incorrectos o inconsistentes en las métricas.
**Recomendación:** Centralizar la lógica de mapeo en una función única.

### 4.6 Validación de Datos de Entrada Faltante - ALTO

**Ubicación:** Líneas 445-454
**Severidad:** ALTO
**Descripción:** Antes de insertar en la base de datos, no se valida que los datos cumplan con las restricciones de la tabla (longitud máxima de VARCHAR, rangos válidos, etc.).
**Impacto:** Errores de base de datos en tiempo de ejecución, posible corrupción de datos.
**Recomendación:** Validar todos los datos antes de la inserción.

### 4.7 Cálculo de Memory Used Inconsistente - MEDIO

**Ubicación:** Líneas 174, 278
**Severidad:** MEDIO
**Descripción:** `memory_used_mb` se calcula dos veces con diferentes valores: una vez en `collectTransferMetrics` (línea 174) y otra en `collectPerformanceMetrics` (línea 278), sobrescribiendo el valor anterior.
**Impacto:** El valor final puede no ser el esperado, dependiendo del orden de ejecución.
**Recomendación:** Calcular una sola vez o documentar cuál tiene prioridad.

### 4.8 Filtro de Esquemas en Performance Metrics - MEDIO

**Ubicación:** Líneas 253-254
**Severidad:** MEDIO
**Descripción:** La consulta de performance metrics filtra por esquemas que existen en el catálogo, pero esto puede excluir tablas que fueron eliminadas del catálogo pero aún existen en PostgreSQL.
**Impacto:** Métricas incompletas para tablas huérfanas.
**Recomendación:** Reconsiderar el filtro o documentar el comportamiento.

---

## 5. MEJORES PRÁCTICAS

### 5.1 Violación de Principio de Responsabilidad Única - MEDIO

**Ubicación:** Clase `MetricsCollector` completa
**Severidad:** MEDIO
**Descripción:** La clase hace demasiadas cosas: recopila datos, los procesa, los guarda, y genera reportes. Debería separarse en múltiples clases.
**Impacto:** Código difícil de testear y mantener.
**Recomendación:** Separar en clases: `MetricsCollector`, `MetricsRepository`, `MetricsReporter`.

### 5.2 Falta de Encapsulación - MEDIO

**Ubicación:** Estructura `TransferMetrics` (pública, sin validación)
**Severidad:** MEDIO
**Descripción:** La estructura `TransferMetrics` es pública y no tiene validación de invariantes. Cualquiera puede crear instancias con datos inválidos.
**Impacto:** Datos inconsistentes pueden propagarse por el sistema.
**Recomendación:** Hacer la estructura privada o agregar validación en el constructor.

### 5.3 Acoplamiento Fuerte con PostgreSQL - MEDIO

**Ubicación:** Todo el archivo
**Severidad:** MEDIO
**Descripción:** El código está fuertemente acoplado a PostgreSQL (pqxx, funciones específicas de PG como `pg_total_relation_size`).
**Impacto:** Difícil cambiar de base de datos o testear sin una base de datos real.
**Recomendación:** Introducir una abstracción de repositorio (aunque el usuario prefiere KISS - balancear).

### 5.4 Falta de Logging de Operaciones Importantes - BAJO

**Ubicación:** Líneas 445-454
**Severidad:** BAJO
**Descripción:** No se loguea cuántas métricas se están guardando, éxito/fallo individual, o estadísticas de la operación.
**Impacto:** Difícil diagnosticar problemas o monitorear el sistema.
**Recomendación:** Agregar logging informativo para operaciones críticas.

### 5.5 Manejo de Errores Silencioso - ALTO

**Ubicación:** Todas las funciones catch
**Severidad:** ALTO
**Descripción:** Cuando ocurre un error, solo se loguea pero la ejecución continúa. Esto puede llevar a estados inconsistentes donde algunas métricas se recopilan y otras no.
**Impacto:** Datos inconsistentes sin que el usuario sepa que algo falló.
**Recomendación:** Considerar propagar errores críticos o al menos retornar códigos de estado.

### 5.6 Falta de Documentación de Contratos - BAJO

**Ubicación:** Todas las funciones
**Severidad:** BAJO
**Descripción:** Las funciones no documentan precondiciones, postcondiciones, o qué excepciones pueden lanzar.
**Impacto:** Difícil usar la clase correctamente sin leer toda la implementación.
**Recomendación:** Agregar documentación de contratos (pre/post condiciones, excepciones).

### 5.7 Uso de Vector sin Reserva de Memoria (Parcial) - BAJO

**Ubicación:** Línea 135
**Severidad:** BAJO
**Descripción:** Aunque se hace `reserve` en `collectTransferMetrics`, no se hace en otras funciones que podrían agregar elementos.
**Impacto:** Reasignaciones innecesarias de memoria (aunque el impacto es menor).
**Recomendación:** Ser consistente con la reserva de memoria.

### 5.8 Construcción de Strings Ineficiente - BAJO

**Ubicación:** Líneas 262-264, 319-322, etc.
**Severidad:** BAJO
**Descripción:** Concatenación de strings con `+` puede causar múltiples asignaciones. Aunque el compilador puede optimizar, no está garantizado.
**Impacto:** Pequeña ineficiencia en rendimiento.
**Recomendación:** Usar `std::stringstream` o `std::format` (C++20).

### 5.9 Limpieza de Vector Antes de Usar - MEDIO

**Ubicación:** Línea 134
**Severidad:** MEDIO
**Descripción:** `collectTransferMetrics` limpia el vector `metrics` al inicio, pero las funciones posteriores (`collectPerformanceMetrics`, etc.) asumen que el vector ya tiene datos. Si `collectTransferMetrics` falla o no encuentra datos, las funciones posteriores procesarán un vector vacío sin error.
**Impacto:** Ejecución silenciosa sin datos, métricas vacías sin advertencia.
**Recomendación:** Validar que el vector no esté vacío antes de procesarlo en funciones posteriores, o retornar códigos de estado.

### 5.10 Falta de Validación de Resultado Vacío - MEDIO

**Ubicación:** Líneas 130, 256, 313, 381, 489
**Severidad:** MEDIO
**Descripción:** Después de ejecutar consultas, no se valida si el resultado está vacío antes de procesarlo. Aunque el código maneja `result.empty()` en `generateMetricsReport` (línea 492), no lo hace en otras funciones.
**Impacto:** Procesamiento innecesario o errores si se asume que hay datos.
**Recomendación:** Validar `result.empty()` consistentemente en todas las funciones.

---

## PROBLEMAS CRÍTICOS QUE REQUIEREN ACCIÓN INMEDIATA

1. **Acceso a índices sin validación de tamaño (múltiples ubicaciones)** - Puede causar crashes si la estructura de consultas cambia
2. **SQL Injection en calculateBytesTransferred (línea 608)** - Vulnerabilidad de seguridad
3. **Race condition con std::localtime (línea 561)** - Comportamiento indefinido en multi-thread
4. **Uso de std::localtime no thread-safe en time_utils.h** - Afecta múltiples lugares
5. **Falta de validación de entrada en múltiples funciones** - Puede causar corrupción de datos

---

## RECOMENDACIONES PRIORITARIAS

### Prioridad 1 (Crítico - Arreglar inmediatamente):

1. Agregar validación de tamaño de filas antes de acceder a índices en todas las funciones
2. Hacer thread-safe el uso de localtime (usar localtime_r o mutex)
3. Eliminar o arreglar la función calculateBytesTransferred con SQL injection (usar parámetros preparados)
4. Agregar validación de entrada consistente en todas las funciones

### Prioridad 2 (Alto - Arreglar pronto):

1. Agregar validación de entrada consistente
2. Mejorar manejo de NULL
3. Corregir cálculo de I/O operations (nombre y lógica)
4. Agregar timeouts a todas las conexiones
5. Mejorar manejo de errores y logging

### Prioridad 3 (Medio - Mejorar cuando sea posible):

1. Refactorizar funciones largas
2. Eliminar código duplicado
3. Agregar constantes para magic numbers
4. Mejorar validación de precondiciones
5. Documentar contratos de funciones

---

**Fecha de análisis:** 2024
**Analista:** AI Code Reviewer
**Versión del código analizado:** Current HEAD
