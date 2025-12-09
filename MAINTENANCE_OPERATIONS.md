# Mantenimientos por Motor de Base de Datos

## PostgreSQL

### 1. VACUUM

- **Tipo**: `VACUUM`
- **Objeto**: Tablas
- **Descripción**: Limpia tuplas muertas y actualiza estadísticas
- **Comando ejecutado**: `VACUUM ANALYZE schema.table`
- **Criterios de detección**:
  - Tuplas muertas > 1000 (threshold)
  - Porcentaje de tuplas muertas > 10%
  - Último VACUUM hace más de 7 días
- **Prioridad**: Calculada según métricas (dead tuples, tamaño de tabla)

### 2. ANALYZE

- **Tipo**: `ANALYZE`
- **Objeto**: Tablas
- **Descripción**: Actualiza estadísticas del optimizador de consultas
- **Comando ejecutado**: `ANALYZE schema.table`
- **Criterios de detección**:
  - Último ANALYZE hace más de 1 día
- **Prioridad**: 5 (fija)

### 3. REINDEX

- **Tipo**: `REINDEX`
- **Objeto**: Índices o Tablas completas
- **Descripción**: Reconstruye índices fragmentados
- **Comando ejecutado**:
  - `REINDEX INDEX schema.index` (para índices específicos)
  - `REINDEX TABLE schema.table` (para todos los índices de una tabla)
- **Criterios de detección**:
  - Fragmentación de índice > 30%
- **Prioridad**: 8 si fragmentación > 50%, 6 si fragmentación 30-50%

---

## MariaDB

### 1. OPTIMIZE TABLE

- **Tipo**: `OPTIMIZE TABLE`
- **Objeto**: Tablas
- **Descripción**: Reorganiza y optimiza tablas, reduce fragmentación
- **Comando ejecutado**: `OPTIMIZE TABLE schema.table`
- **Criterios de detección**:
  - Fragmentación > 20%
  - Espacio libre > 100 MB
- **Prioridad**: 7 si fragmentación > 30%, 5 si fragmentación 20-30%

### 2. ANALYZE TABLE

- **Tipo**: `ANALYZE TABLE`
- **Objeto**: Tablas
- **Descripción**: Actualiza estadísticas de distribución de datos
- **Comando ejecutado**: `ANALYZE TABLE schema.table`
- **Criterios de detección**:
  - Último ANALYZE hace más de 1 día
  - Tabla actualizada recientemente sin ANALYZE
- **Prioridad**: 4 (fija)

---

## MSSQL

### 1. UPDATE STATISTICS

- **Tipo**: `UPDATE STATISTICS`
- **Objeto**: Tablas (estadísticas)
- **Descripción**: Actualiza estadísticas de distribución de datos para el optimizador
- **Comando ejecutado**: `UPDATE STATISTICS schema.table`
- **Criterios de detección**:
  - Estadísticas nunca actualizadas (NULL)
  - Última actualización hace más de 1 día
- **Prioridad**: 5 (fija)

### 2. REBUILD INDEX

- **Tipo**: `REBUILD INDEX`
- **Objeto**: Índices
- **Descripción**: Reconstruye completamente índices fragmentados (más agresivo)
- **Comando ejecutado**: `ALTER INDEX index_name ON schema.table REBUILD`
- **Criterios de detección**:
  - Fragmentación > 30%
  - Páginas del índice > 100
- **Prioridad**: 8 si fragmentación > 50%, 6 si fragmentación 30-50%

### 3. REORGANIZE INDEX

- **Tipo**: `REORGANIZE INDEX`
- **Objeto**: Índices
- **Descripción**: Reorganiza índices fragmentados (menos agresivo, online)
- **Comando ejecutado**: `ALTER INDEX index_name ON schema.table REORGANIZE`
- **Criterios de detección**:
  - Fragmentación entre 10% y 30%
  - Páginas del índice > 100
- **Prioridad**: 5 (fija)

---

## Resumen por Motor

| Motor          | Operaciones | Objetos                                            |
| -------------- | ----------- | -------------------------------------------------- |
| **PostgreSQL** | 3           | VACUUM, ANALYZE, REINDEX                           |
| **MariaDB**    | 2           | OPTIMIZE TABLE, ANALYZE TABLE                      |
| **MSSQL**      | 3           | UPDATE STATISTICS, REBUILD INDEX, REORGANIZE INDEX |

## Frecuencia de Detección

- **Detección automática**: Cada 6 horas (configurado en `StreamingData::maintenanceThread()`)
- **Ejecución automática**: Solo si `auto_execute = true` y `enabled = true`
- **Ejecución manual**: Puede ejecutarse manualmente desde la tabla `metadata.maintenance_control`

## Métricas Calculadas

Todas las operaciones de mantenimiento calculan métricas de impacto:

- `space_reclaimed_mb`: Espacio recuperado en MB
- `performance_improvement_pct`: Mejora de performance en porcentaje
- `fragmentation_before/after`: Fragmentación antes y después
- `dead_tuples_before/after`: Tuplas muertas (PostgreSQL)
- `table_size_before/after_mb`: Tamaño de tabla antes y después
- `index_size_before/after_mb`: Tamaño de índice antes y después
- `impact_score`: Puntuación calculada de impacto del mantenimiento
