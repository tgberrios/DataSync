# Testing Guide: FASE 2.1 - Big Data Processing

## Resumen de Implementación

Todas las integraciones de FASE 2.1 han sido completadas. Este documento describe cómo probar las funcionalidades implementadas.

## Componentes Implementados

### 1. SparkEngine
- **Ubicación**: `include/engines/spark_engine.h`, `src/engines/spark_engine.cpp`
- **Funcionalidad**: Wrapper de Apache Spark con soporte para Spark Connect y spark-submit
- **Testing**:
  ```cpp
  SparkEngine::SparkConfig config;
  config.appName = "test-app";
  config.masterUrl = "local[*]";
  SparkEngine engine(config);
  engine.initialize();
  // Verificar: engine.isAvailable() debe retornar true si Spark está instalado
  ```

### 2. DistributedProcessingManager
- **Ubicación**: `include/sync/DistributedProcessingManager.h`, `src/sync/DistributedProcessingManager.cpp`
- **Funcionalidad**: Decisión automática local vs distribuido
- **Testing**:
  ```cpp
  DistributedProcessingManager::ProcessingConfig config;
  DistributedProcessingManager manager(config);
  manager.initialize();
  
  DistributedProcessingManager::ProcessingTask task;
  task.taskId = "test-task";
  task.estimatedRows = 2000000; // > threshold
  auto decision = manager.shouldUseDistributed(task);
  // Verificar: decision.useDistributed debe ser true para datos grandes
  ```

### 3. SparkTranslator
- **Ubicación**: `include/transformations/spark_translator.h`, `src/transformations/spark_translator.cpp`
- **Funcionalidad**: Traduce transformaciones DataSync a Spark SQL
- **Testing**:
  ```cpp
  json joinConfig = {
    {"type", "join"},
    {"config", {
      {"join_type", "inner"},
      {"left_table", "table1"},
      {"right_table", "table2"},
      {"left_columns", {"id"}},
      {"right_columns", {"id"}}
    }}
  };
  auto result = SparkTranslator::translateTransformation(joinConfig);
  // Verificar: result.sparkSQL contiene SQL válido
  ```

### 4. DistributedJoinExecutor
- **Ubicación**: `include/sync/DistributedJoinExecutor.h`, `src/sync/DistributedJoinExecutor.cpp`
- **Funcionalidad**: Ejecuta joins distribuidos con diferentes algoritmos
- **Testing**:
  ```cpp
  auto sparkEngine = std::make_shared<SparkEngine>(config);
  DistributedJoinExecutor executor(sparkEngine);
  
  DistributedJoinExecutor::JoinConfig joinConfig;
  joinConfig.leftTable = "table1";
  joinConfig.rightTable = "table2";
  joinConfig.leftTableSizeMB = 5; // < broadcast threshold
  auto algorithm = executor.detectBestAlgorithm(joinConfig);
  // Verificar: algorithm debe ser BROADCAST para tablas pequeñas
  ```

### 5. PartitioningManager
- **Ubicación**: `include/sync/PartitioningManager.h`, `src/sync/PartitioningManager.cpp`
- **Funcionalidad**: Detección automática de particiones
- **Testing**:
  ```cpp
  std::vector<std::string> columns = {"id", "created_date", "region"};
  std::vector<std::string> types = {"INTEGER", "TIMESTAMP", "VARCHAR"};
  auto result = PartitioningManager::detectPartitions("schema", "table", columns, types);
  // Verificar: result.hasPartitions debe ser true si hay columnas de fecha
  ```

### 6. SchemaEvolutionManager
- **Ubicación**: `include/governance/SchemaEvolutionManager.h`, `src/governance/SchemaEvolutionManager.cpp`
- **Funcionalidad**: Manejo de cambios de esquema
- **Testing**:
  ```cpp
  SchemaEvolutionManager::SchemaVersion oldSchema, newSchema;
  // ... configurar schemas
  auto changes = SchemaEvolutionManager::detectChanges(oldSchema, newSchema);
  auto compatibility = SchemaEvolutionManager::determineCompatibility(changes);
  // Verificar: compatibility debe ser BACKWARD_COMPATIBLE para columnas agregadas
  ```

### 7. MergeStrategyExecutor
- **Ubicación**: `include/sync/MergeStrategyExecutor.h`, `src/sync/MergeStrategyExecutor.cpp`
- **Funcionalidad**: Ejecuta diferentes estrategias de merge
- **Testing**:
  ```cpp
  auto sparkEngine = std::make_shared<SparkEngine>(config);
  MergeStrategyExecutor executor(sparkEngine);
  
  MergeStrategyExecutor::MergeConfig mergeConfig;
  mergeConfig.targetTable = "target";
  mergeConfig.sourceTable = "source";
  mergeConfig.strategy = MergeStrategyExecutor::MergeStrategy::UPSERT;
  auto result = executor.executeMerge(mergeConfig);
  // Verificar: result.success debe ser true si merge se ejecuta correctamente
  ```

### 8. IncrementalProcessor
- **Ubicación**: `include/sync/IncrementalProcessor.h`, `src/sync/IncrementalProcessor.cpp`
- **Funcionalidad**: Procesamiento incremental inteligente
- **Testing**:
  ```cpp
  IncrementalProcessor::IncrementalConfig config;
  config.tableName = "test_table";
  config.timestampColumn = "updated_at";
  config.method = IncrementalProcessor::ChangeDetectionMethod::TIMESTAMP;
  auto result = IncrementalProcessor::detectChanges(config);
  // Verificar: result.hasChanges debe detectar cambios desde última ejecución
  ```

### 9. CDCStrategyManager
- **Ubicación**: `include/sync/CDCStrategyManager.h`, `src/sync/CDCStrategyManager.cpp`
- **Funcionalidad**: Decisión automática de estrategia CDC
- **Testing**:
  ```cpp
  CDCStrategyManager::DatabaseCapabilities caps;
  caps.hasBinlog = true;
  CDCStrategyManager::ChangeVolume volume;
  volume.changesPerHour = 500000;
  auto strategy = CDCStrategyManager::selectCDCStrategy("MariaDB", caps, volume, 
                                                         LatencyRequirement{});
  // Verificar: strategy debe ser NATIVE_BINLOG para MariaDB con binlog
  ```

### 10. DeltaLakeEngine e IcebergEngine
- **Ubicación**: `include/engines/delta_lake_engine.h`, `include/engines/iceberg_engine.h`
- **Funcionalidad**: Integración con Delta Lake e Iceberg
- **Testing**: Requiere Spark y Delta Lake/Iceberg instalados

### 11. MapReduceEngine
- **Ubicación**: `include/engines/mapreduce_engine.h`, `src/engines/mapreduce_engine.cpp`
- **Funcionalidad**: Integración con Hadoop MapReduce (legacy)
- **Testing**: Requiere Hadoop instalado

## Integraciones Completadas

### WorkflowExecutor
- **Cambios**: Integrado `DistributedProcessingManager`
- **Configuración**: Agregar `"use_distributed": true` o `"processing_mode": "distributed"` en task config
- **Testing**: Ejecutar workflow con tareas grandes y verificar que use Spark si está disponible

### TransformationEngine
- **Cambios**: Integrado `SparkTranslator` y soporte para `executePipelineWithSpark`
- **Configuración**: Agregar `"use_spark": true` en pipeline config
- **Testing**: Ejecutar pipeline con múltiples transformaciones y verificar traducción a Spark SQL

### DatabaseToPostgresSync
- **Cambios**: Integrado `PartitioningManager` y `DistributedProcessingManager`
- **Funcionalidad**: Detección automática de particiones y decisión de procesamiento distribuido
- **Testing**: Sincronizar tabla grande y verificar detección de particiones y uso de Spark

### DataWarehouseBuilder
- **Cambios**: Integrado `MergeStrategyExecutor` y `PartitioningManager`
- **Funcionalidad**: SCD Type 1 usa UPSERT de MergeStrategyExecutor, soporte para particionado
- **Testing**: Construir warehouse con dimensión TYPE_1 y verificar uso de merge strategy

## Testing End-to-End

### Test 1: Workflow con Procesamiento Distribuido
```json
{
  "workflow_name": "test_distributed",
  "tasks": [
    {
      "task_name": "large_sync",
      "task_type": "SYNC",
      "task_config": {
        "use_distributed": true,
        "estimated_rows": 2000000
      }
    }
  ]
}
```
**Verificar**: Workflow debe usar Spark si está disponible

### Test 2: Pipeline de Transformaciones con Spark
```json
{
  "transformations": [
    {"type": "join", "config": {...}},
    {"type": "aggregate", "config": {...}},
    {"type": "filter", "config": {...}}
  ],
  "use_spark": true
}
```
**Verificar**: Pipeline debe traducirse a Spark SQL y ejecutarse en Spark

### Test 3: Warehouse Build con Merge Strategies
```json
{
  "warehouse_name": "test_warehouse",
  "dimensions": [
    {
      "dimension_name": "customer",
      "scd_type": "TYPE_1"
    }
  ]
}
```
**Verificar**: TYPE_1 debe usar MergeStrategyExecutor::UPSERT

### Test 4: Detección de Particiones
- Crear tabla con columna `created_date` (TIMESTAMP)
- Ejecutar sync
- **Verificar**: PartitioningManager debe detectar partición por fecha

### Test 5: Decisión Automática CDC
- Configurar sync para MariaDB con binlog habilitado
- **Verificar**: CDCStrategyManager debe seleccionar NATIVE_BINLOG

## Notas de Compilación

1. **Spark**: Instalar Apache Spark y asegurar que `spark-submit` esté en PATH
2. **Hadoop** (opcional): Instalar Hadoop para MapReduceEngine
3. **Delta Lake/Iceberg**: Se detectan automáticamente si Spark está disponible

## Próximos Pasos

- Agregar tests unitarios para cada componente
- Implementar tests de integración end-to-end
- Performance benchmarks comparando local vs distribuido
- Documentación de configuración avanzada
