# Sistema de Mantenimiento de Bases de Datos - Diseño

## 1. Arquitectura General

### 1.1 Tabla de Control: `metadata.maintenance_control`

**Estructura actual (ya existe):**

- `id` - Identificador único
- `maintenance_type` - Tipo de mantenimiento (VACUUM, ANALYZE, REINDEX, etc.)
- `schema_name` - Schema de la base de datos
- `object_name` - Nombre del objeto (tabla, índice, etc.)
- `object_type` - Tipo de objeto (TABLE, INDEX, etc.)
- `status` - Estado (PENDING, RUNNING, COMPLETED, FAILED, SKIPPED)
- `priority` - Prioridad (1-10, mayor = más urgente)
- `last_maintenance_date` - Última vez que se ejecutó
- `next_maintenance_date` - Próxima ejecución programada
- `maintenance_duration_seconds` - Duración de la última ejecución
- `maintenance_count` - Contador de ejecuciones
- `result_message` - Mensaje de resultado
- `error_details` - Detalles de errores

**Columnas a agregar:**

- `db_engine` VARCHAR(50) - Motor de BD (PostgreSQL, MariaDB, MSSQL)
- `connection_string` TEXT - Connection string para conectar a la BD
- `auto_execute` BOOLEAN DEFAULT true - Si true ejecuta automáticamente, si false solo detecta
- `enabled` BOOLEAN DEFAULT true - Si está habilitado o no
- `maintenance_schedule` JSONB - Configuración de horarios (opcional)
- `thresholds` JSONB - Umbrales para detección (ej: fragmentation > 30%)

### 1.2 Tipos de Mantenimiento por Engine

#### PostgreSQL:

1. **VACUUM** - Limpieza de dead tuples

   - Detección: `n_dead_tup > threshold` o `last_vacuum` muy antiguo
   - Comando: `VACUUM ANALYZE schema.table`
   - Variantes: `VACUUM FULL` (más agresivo, requiere lock exclusivo)

2. **ANALYZE** - Actualización de estadísticas

   - Detección: `last_autoanalyze` muy antiguo o estadísticas desactualizadas
   - Comando: `ANALYZE schema.table`

3. **REINDEX** - Reconstrucción de índices

   - Detección: Fragmentación alta o índices corruptos
   - Comando: `REINDEX TABLE schema.table` o `REINDEX INDEX schema.index`

4. **CLUSTER** - Reordenamiento físico (opcional)
   - Detección: Tabla muy fragmentada
   - Comando: `CLUSTER schema.table USING index_name`

#### MariaDB:

1. **OPTIMIZE TABLE** - Optimización de tablas

   - Detección: Fragmentación alta o espacio libre excesivo
   - Comando: `OPTIMIZE TABLE schema.table`

2. **ANALYZE TABLE** - Actualización de estadísticas

   - Detección: Estadísticas desactualizadas
   - Comando: `ANALYZE TABLE schema.table`

3. **REPAIR TABLE** - Reparación (solo si es necesario)
   - Detección: Tabla corrupta
   - Comando: `REPAIR TABLE schema.table`

#### MSSQL:

1. **UPDATE STATISTICS** - Actualización de estadísticas

   - Detección: Estadísticas desactualizadas
   - Comando: `UPDATE STATISTICS schema.table`

2. **REBUILD INDEX** - Reconstrucción completa de índices

   - Detección: Fragmentación > 30%
   - Comando: `ALTER INDEX index_name ON schema.table REBUILD`

3. **REORGANIZE INDEX** - Reorganización de índices

   - Detección: Fragmentación 10-30%
   - Comando: `ALTER INDEX index_name ON schema.table REORGANIZE`

4. **DBCC SHRINKFILE** - Reducción de archivos (opcional, peligroso)
   - Detección: Archivos muy grandes con mucho espacio libre
   - Comando: `DBCC SHRINKFILE (file_name, target_size)`

## 2. Flujo de Trabajo

### 2.1 Detección (Siempre se ejecuta)

```
1. Para cada engine (PostgreSQL, MariaDB, MSSQL):
   a. Obtener connection strings desde metadata.catalog
   b. Conectar a cada base de datos
   c. Detectar necesidades de mantenimiento:
      - Consultar métricas (fragmentación, dead tuples, etc.)
      - Comparar con umbrales configurados
      - Calcular prioridad
   d. Insertar/Actualizar en metadata.maintenance_control
```

### 2.2 Ejecución (Solo si auto_execute = true)

```
1. Consultar metadata.maintenance_control:
   WHERE status = 'PENDING'
     AND auto_execute = true
     AND enabled = true
     AND next_maintenance_date <= NOW()
   ORDER BY priority DESC, next_maintenance_date ASC

2. Para cada tarea pendiente:
   a. Actualizar status = 'RUNNING'
   b. Ejecutar comando de mantenimiento según engine
   c. Medir duración
   d. Actualizar:
      - status = 'COMPLETED' o 'FAILED'
      - last_maintenance_date = NOW()
      - next_maintenance_date = calcular próxima ejecución
      - maintenance_duration_seconds
      - result_message / error_details
      - maintenance_count += 1

3. Almacenar métricas en metadata.maintenance_metrics
```

### 2.3 Modo Manual (auto_execute = false)

```
- Solo detecta y registra en maintenance_control
- NO ejecuta comandos
- Permite revisión manual antes de ejecutar
- Puede ejecutarse manualmente vía API o comando específico
```

## 3. Estructura de Clases

### 3.1 MaintenanceManager (Orquestador Principal)

```cpp
class MaintenanceManager {
  // Métodos públicos
  void detectMaintenanceNeeds();  // Detecta para todos los engines
  void executeMaintenance();     // Ejecuta solo si auto_execute = true
  void executeManual(int maintenanceId); // Ejecuta manualmente un mantenimiento específico

  // Métodos privados por engine
  void detectPostgreSQLMaintenance(const std::string &connStr);
  void detectMariaDBMaintenance(const std::string &connStr);
  void detectMSSQLMaintenance(const std::string &connStr);

  // Métodos de ejecución por engine
  void executePostgreSQLMaintenance(const MaintenanceTask &task);
  void executeMariaDBMaintenance(const MaintenanceTask &task);
  void executeMSSQLMaintenance(const MaintenanceTask &task);

  // Métodos de detección específicos
  void detectVacuumNeeds(pqxx::connection &conn);
  void detectAnalyzeNeeds(pqxx::connection &conn);
  void detectReindexNeeds(pqxx::connection &conn);
  void detectOptimizeNeeds(MYSQL *conn);
  void detectRebuildIndexNeeds(SQLHDBC conn);

  // Utilidades
  int calculatePriority(const MaintenanceMetrics &metrics);
  void storeMetrics(const MaintenanceExecution &execution);
};
```

### 3.2 Estructura de Datos

```cpp
struct MaintenanceTask {
  int id;
  std::string maintenance_type;  // VACUUM, ANALYZE, REINDEX, etc.
  std::string db_engine;         // PostgreSQL, MariaDB, MSSQL
  std::string connection_string;
  std::string schema_name;
  std::string object_name;
  std::string object_type;        // TABLE, INDEX
  bool auto_execute;
  bool enabled;
  int priority;
  std::string status;             // PENDING, RUNNING, COMPLETED, FAILED
  std::chrono::system_clock::time_point next_maintenance_date;
  json thresholds;                // Umbrales específicos
};

struct MaintenanceMetrics {
  double fragmentation_pct;
  long long dead_tuples;
  long long live_tuples;
  std::string last_vacuum;
  std::string last_analyze;
  // ... más métricas según engine
};
```

## 4. Configuración de Umbrales

### 4.1 Umbrales por Defecto

```json
{
  "postgresql": {
    "vacuum": {
      "dead_tuples_threshold": 1000,
      "dead_tuples_percentage": 10.0,
      "days_since_last_vacuum": 7
    },
    "analyze": {
      "days_since_last_analyze": 1
    },
    "reindex": {
      "fragmentation_threshold": 30.0
    }
  },
  "mariadb": {
    "optimize": {
      "fragmentation_threshold": 20.0,
      "free_space_threshold_mb": 100
    }
  },
  "mssql": {
    "rebuild_index": {
      "fragmentation_threshold": 30.0
    },
    "reorganize_index": {
      "fragmentation_min": 10.0,
      "fragmentation_max": 30.0
    }
  }
}
```

## 5. Integración en el Sistema

### 5.1 Thread de Mantenimiento

- Se ejecuta periódicamente (cada X horas, configurable)
- Llama a `detectMaintenanceNeeds()` siempre
- Llama a `executeMaintenance()` solo si hay tareas con `auto_execute = true`

### 5.2 API/Comandos Manuales

- Endpoint para listar tareas pendientes
- Endpoint para ejecutar manualmente una tarea específica
- Endpoint para cambiar `auto_execute` de una tarea

## 6. Seguridad y Consideraciones

### 6.1 Locks y Bloqueos

- VACUUM FULL requiere lock exclusivo → ejecutar en horarios de bajo tráfico
- REINDEX puede bloquear lecturas → ejecutar con cuidado
- OPTIMIZE TABLE en MariaDB puede tomar mucho tiempo

### 6.2 Priorización

- Prioridad 10: Crítico (tablas corruptas, índices rotos)
- Prioridad 7-9: Alto (fragmentación muy alta, dead tuples excesivos)
- Prioridad 4-6: Medio (mantenimiento rutinario)
- Prioridad 1-3: Bajo (optimizaciones menores)

### 6.3 Horarios

- Mantenimientos pesados (VACUUM FULL, REBUILD) → horarios nocturnos
- Mantenimientos ligeros (ANALYZE) → pueden ejecutarse durante el día
- Configurable por tipo de mantenimiento

## 7. Métricas y Reportes

### 7.1 Almacenamiento

- `metadata.maintenance_metrics` - Métricas agregadas por día/tipo
- `metadata.maintenance_control` - Estado individual de cada tarea

### 7.2 Reportes

- Tareas pendientes por prioridad
- Tareas fallidas que requieren atención
- Efectividad del mantenimiento (espacio recuperado, mejora de performance)
- Tiempo total de mantenimiento por día

## 8. Ejemplo de Uso

### 8.1 Modo Automático (auto_execute = true)

```sql
-- El sistema detecta y ejecuta automáticamente
INSERT INTO metadata.maintenance_control
  (maintenance_type, db_engine, schema_name, object_name,
   object_type, auto_execute, priority, status)
VALUES
  ('VACUUM', 'PostgreSQL', 'public', 'users',
   'TABLE', true, 7, 'PENDING');
-- El sistema ejecutará VACUUM automáticamente cuando llegue next_maintenance_date
```

### 8.2 Modo Manual (auto_execute = false)

```sql
-- El sistema detecta pero NO ejecuta
INSERT INTO metadata.maintenance_control
  (maintenance_type, db_engine, schema_name, object_name,
   object_type, auto_execute, priority, status)
VALUES
  ('REBUILD INDEX', 'MSSQL', 'dbo', 'idx_users_email',
   'INDEX', false, 8, 'PENDING');
-- Requiere ejecución manual vía API o comando
```

## 9. Preguntas para Ajustar

1. **Frecuencia de detección**: ¿Cada cuánto tiempo se ejecuta la detección? (sugerencia: cada 6 horas)

2. **Frecuencia de ejecución**: ¿Cada cuánto tiempo se ejecutan los mantenimientos automáticos? (sugerencia: diario en horario nocturno)

3. **Umbrales**: ¿Los umbrales propuestos están bien o prefieres otros valores?

4. **Horarios**: ¿Hay horarios específicos donde NO se debe ejecutar mantenimiento?

5. **Límites**: ¿Límite de tareas ejecutadas por ciclo? (ej: máximo 10 tareas por ejecución)

6. **Notificaciones**: ¿Se debe notificar cuando hay tareas críticas pendientes?

7. **Rollback**: ¿Se debe intentar rollback si un mantenimiento falla? (la mayoría son DDL, no se puede hacer rollback)
