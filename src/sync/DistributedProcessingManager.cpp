#include "sync/DistributedProcessingManager.h"
#include "sync/TableProcessorThreadPool.h"
#include "core/logger.h"
#include <chrono>
#include <algorithm>
#include <cmath>

DistributedProcessingManager::DistributedProcessingManager(const ProcessingConfig& config)
    : config_(config) {
  Logger::info(LogCategory::SYSTEM, "DistributedProcessingManager",
               "Initializing DistributedProcessingManager");
}

DistributedProcessingManager::~DistributedProcessingManager() {
  shutdown();
}

bool DistributedProcessingManager::initialize() {
  if (initialized_) {
    Logger::warning(LogCategory::SYSTEM, "DistributedProcessingManager",
                    "Already initialized");
    return true;
  }

  if (!validateConfig()) {
    Logger::error(LogCategory::SYSTEM, "DistributedProcessingManager",
                 "Invalid configuration");
    return false;
  }

  // Inicializar thread pool para procesamiento local
  threadPool_ = std::make_unique<TableProcessorThreadPool>(0); // 0 = auto-detect workers
  threadPool_->enableMonitoring(true);

  // Inicializar Spark engine si está disponible
  if (config_.forceMode != ProcessingConfig::ForceMode::LOCAL_ONLY) {
    sparkEngine_ = std::make_unique<SparkEngine>(config_.sparkConfig);
    if (sparkEngine_->initialize()) {
      Logger::info(LogCategory::SYSTEM, "DistributedProcessingManager",
                   "Spark engine initialized successfully");
    } else {
      Logger::warning(LogCategory::SYSTEM, "DistributedProcessingManager",
                      "Spark engine initialization failed, will use local processing only");
      sparkEngine_.reset();
    }
  }

  initialized_ = true;
  Logger::info(LogCategory::SYSTEM, "DistributedProcessingManager",
               "DistributedProcessingManager initialized successfully");
  return true;
}

void DistributedProcessingManager::shutdown() {
  if (!initialized_) {
    return;
  }

  if (threadPool_) {
    threadPool_->shutdown();
    threadPool_.reset();
  }

  if (sparkEngine_) {
    sparkEngine_->shutdown();
    sparkEngine_.reset();
  }

  initialized_ = false;
  Logger::info(LogCategory::SYSTEM, "DistributedProcessingManager",
               "DistributedProcessingManager shutdown");
}

bool DistributedProcessingManager::validateConfig() const {
  if (config_.distributedThresholdRows <= 0) {
    Logger::error(LogCategory::SYSTEM, "DistributedProcessingManager",
                 "distributedThresholdRows must be > 0");
    return false;
  }

  if (config_.distributedThresholdSizeMB <= 0) {
    Logger::error(LogCategory::SYSTEM, "DistributedProcessingManager",
                 "distributedThresholdSizeMB must be > 0");
    return false;
  }

  return true;
}

DistributedProcessingManager::ProcessingDecision
DistributedProcessingManager::shouldUseDistributed(const ProcessingTask& task) {
  ProcessingDecision decision;
  decision.estimatedRows = estimateRowCount(task);
  decision.estimatedSizeMB = estimateDataSize(task);
  decision.complexityScore = calculateComplexity(task);

  // Forzar modo si está configurado
  if (config_.forceMode == ProcessingConfig::ForceMode::LOCAL_ONLY) {
    decision.useDistributed = false;
    decision.reason = "Forced to local processing";
    return decision;
  }

  if (config_.forceMode == ProcessingConfig::ForceMode::DISTRIBUTED_ONLY) {
    if (isSparkAvailable()) {
      decision.useDistributed = true;
      decision.reason = "Forced to distributed processing";
      return decision;
    } else {
      decision.useDistributed = false;
      decision.reason = "Forced to distributed but Spark not available, falling back to local";
      return decision;
    }
  }

  // Decisión automática basada en thresholds
  // Verificar si Spark está disponible
  if (!isSparkAvailable()) {
    decision.useDistributed = false;
    decision.reason = "Spark not available";
    return decision;
  }

  // Criterio 1: Tamaño de datos
  bool sizeThreshold = decision.estimatedSizeMB >= config_.distributedThresholdSizeMB ||
                       decision.estimatedRows >= config_.distributedThresholdRows;

  // Criterio 2: Complejidad
  // Tareas complejas (joins, aggregations grandes) se benefician más de Spark
  bool complexityThreshold = decision.complexityScore >= 50;

  // Criterio 3: Tipo de tarea
  // Algunas tareas se benefician más de procesamiento distribuido
  bool taskTypeBenefit = task.taskType == "join" || 
                         task.taskType == "aggregate" ||
                         task.taskType == "transform" ||
                         task.taskType == "warehouse_build";

  // Decisión: usar distribuido si cumple thresholds o es tarea compleja
  decision.useDistributed = sizeThreshold || (complexityThreshold && taskTypeBenefit);

  if (decision.useDistributed) {
    if (sizeThreshold) {
      decision.reason = "Data size exceeds threshold (" + 
                       std::to_string(decision.estimatedSizeMB) + " MB, " +
                       std::to_string(decision.estimatedRows) + " rows)";
    } else {
      decision.reason = "High complexity task (score: " + 
                       std::to_string(decision.complexityScore) + ")";
    }
  } else {
    decision.reason = "Small data size and low complexity, using local processing";
  }

  Logger::info(LogCategory::SYSTEM, "DistributedProcessingManager",
               "Processing decision for task " + task.taskId + ": " +
               (decision.useDistributed ? "DISTRIBUTED" : "LOCAL") + 
               " - " + decision.reason);

  return decision;
}

DistributedProcessingManager::ProcessingResult
DistributedProcessingManager::executeTask(const ProcessingTask& task) {
  if (!initialized_) {
    Logger::error(LogCategory::SYSTEM, "DistributedProcessingManager",
                 "Not initialized");
    return ProcessingResult{false, task.taskId, "", 0, "", "Not initialized", json::object()};
  }

  auto startTime = std::chrono::steady_clock::now();

  // Decidir modo de procesamiento
  ProcessingDecision decision = shouldUseDistributed(task);

  ProcessingResult result;
  result.taskId = task.taskId;

  if (decision.useDistributed) {
    result = executeTaskDistributed(task);
    result.executionMode = "distributed";
    distributedTasksExecuted_++;
    distributedRowsProcessed_ += result.rowsProcessed;
  } else {
    // Para procesamiento local, necesitamos un processor function
    // Por ahora, retornamos error si no se proporciona
    result.success = false;
    result.executionMode = "local";
    result.errorMessage = "Local processing requires processor function, use executeTaskLocal()";
  }

  auto endTime = std::chrono::steady_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime).count();

  if (decision.useDistributed) {
    distributedExecutionTimeMs_ += duration;
  } else {
    localExecutionTimeMs_ += duration;
  }

  result.metadata["execution_time_ms"] = duration;
  result.metadata["decision_reason"] = decision.reason;
  result.metadata["estimated_rows"] = decision.estimatedRows;
  result.metadata["estimated_size_mb"] = decision.estimatedSizeMB;
  result.metadata["complexity_score"] = decision.complexityScore;

  return result;
}

DistributedProcessingManager::ProcessingResult
DistributedProcessingManager::executeTaskLocal(const ProcessingTask& task,
                                                std::function<void()> localProcessor) {
  if (!initialized_ || !threadPool_) {
    Logger::error(LogCategory::SYSTEM, "DistributedProcessingManager",
                 "Not initialized or thread pool not available");
    return ProcessingResult{false, task.taskId, "local", 0, "", "Not initialized", json::object()};
  }

  auto startTime = std::chrono::steady_clock::now();

  ProcessingResult result;
  result.taskId = task.taskId;
  result.executionMode = "local";

  try {
    // Ejecutar procesador local
    localProcessor();
    
    result.success = true;
    result.rowsProcessed = estimateRowCount(task);
    result.outputPath = task.outputPath;
    
    localTasksExecuted_++;
    localRowsProcessed_ += result.rowsProcessed;
  } catch (const std::exception& e) {
    result.success = false;
    result.errorMessage = std::string(e.what());
    Logger::error(LogCategory::SYSTEM, "DistributedProcessingManager",
                 "Local task execution failed: " + result.errorMessage);
  }

  auto endTime = std::chrono::steady_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime).count();
  localExecutionTimeMs_ += duration;

  result.metadata["execution_time_ms"] = duration;
  return result;
}

DistributedProcessingManager::ProcessingResult
DistributedProcessingManager::executeTaskDistributed(const ProcessingTask& task) {
  if (!sparkEngine_ || !sparkEngine_->isAvailable()) {
    Logger::error(LogCategory::SYSTEM, "DistributedProcessingManager",
                 "Spark engine not available");
    return ProcessingResult{false, task.taskId, "distributed", 0, "", 
                           "Spark not available", json::object()};
  }

  ProcessingResult result;
  result.taskId = task.taskId;
  result.executionMode = "distributed";

  try {
    // Crear Spark job desde la tarea
    SparkEngine::SparkJob sparkJob;
    sparkJob.jobId = task.taskId;
    sparkJob.inputPath = task.inputPath;
    sparkJob.outputPath = task.outputPath;
    sparkJob.transformationConfig = task.config;
    
    // Determinar formato de salida
    if (task.outputPath.find(".parquet") != std::string::npos) {
      sparkJob.outputFormat = "parquet";
    } else if (task.outputPath.find(".csv") != std::string::npos) {
      sparkJob.outputFormat = "csv";
    } else if (task.outputPath.find(".json") != std::string::npos) {
      sparkJob.outputFormat = "json";
    } else {
      sparkJob.outputFormat = "parquet"; // Default
    }

    // Generar SQL query si está disponible en config
    if (task.config.contains("sql_query")) {
      sparkJob.sqlQuery = task.config["sql_query"].get<std::string>();
    } else {
      // Generar SQL básico desde la configuración
      sparkJob.sqlQuery = "SELECT * FROM input_table";
    }

    // Ejecutar job Spark
    SparkEngine::SparkJobResult sparkResult = sparkEngine_->executeJob(sparkJob);

    result.success = sparkResult.success;
    result.rowsProcessed = sparkResult.rowsProcessed;
    result.outputPath = sparkResult.outputPath;
    result.errorMessage = sparkResult.errorMessage;
    result.metadata = sparkResult.metadata;

    if (!result.success) {
      Logger::error(LogCategory::SYSTEM, "DistributedProcessingManager",
                   "Spark job failed: " + result.errorMessage);
    }
  } catch (const std::exception& e) {
    result.success = false;
    result.errorMessage = std::string(e.what());
    Logger::error(LogCategory::SYSTEM, "DistributedProcessingManager",
                 "Error executing distributed task: " + result.errorMessage);
  }

  return result;
}

int DistributedProcessingManager::calculateComplexity(const ProcessingTask& task) const {
  int complexity = 0;

  // Basado en tipo de tarea
  if (task.taskType == "join") {
    complexity += 30;
  } else if (task.taskType == "aggregate") {
    complexity += 25;
  } else if (task.taskType == "transform") {
    complexity += 20;
  } else if (task.taskType == "warehouse_build") {
    complexity += 40;
  }

  // Basado en número de transformaciones en config
  if (task.config.contains("transformations") && 
      task.config["transformations"].is_array()) {
    complexity += task.config["transformations"].size() * 5;
  }

  // Basado en tamaño estimado
  int64_t sizeMB = estimateDataSize(task);
  if (sizeMB > 1000) {
    complexity += 20;
  } else if (sizeMB > 100) {
    complexity += 10;
  }

  // Basado en número de filas estimadas
  int64_t rows = estimateRowCount(task);
  if (rows > 10000000) {  // 10M rows
    complexity += 15;
  } else if (rows > 1000000) {  // 1M rows
    complexity += 10;
  }

  return std::min(complexity, 100); // Cap at 100
}

int64_t DistributedProcessingManager::estimateDataSize(const ProcessingTask& task) const {
  // Si ya está estimado en la tarea, usarlo
  if (task.estimatedSizeMB > 0) {
    return task.estimatedSizeMB;
  }

  // Estimar basado en número de filas (asumiendo ~1KB por fila promedio)
  int64_t rows = estimateRowCount(task);
  return rows / 1000; // Aproximadamente 1MB por 1000 filas
}

int64_t DistributedProcessingManager::estimateRowCount(const ProcessingTask& task) const {
  // Si ya está estimado en la tarea, usarlo
  if (task.estimatedRows > 0) {
    return task.estimatedRows;
  }

  // Si hay metadata en config, intentar extraer
  if (task.config.contains("estimated_rows")) {
    return task.config["estimated_rows"].get<int64_t>();
  }

  // Default: estimación conservadora
  return 10000;
}

bool DistributedProcessingManager::isSparkAvailable() const {
  return sparkEngine_ && sparkEngine_->isAvailable();
}

json DistributedProcessingManager::getStatistics() const {
  json stats;
  stats["local_tasks_executed"] = localTasksExecuted_.load();
  stats["distributed_tasks_executed"] = distributedTasksExecuted_.load();
  stats["local_rows_processed"] = localRowsProcessed_.load();
  stats["distributed_rows_processed"] = distributedRowsProcessed_.load();
  stats["local_execution_time_ms"] = localExecutionTimeMs_.load();
  stats["distributed_execution_time_ms"] = distributedExecutionTimeMs_.load();
  
  int64_t totalTasks = localTasksExecuted_.load() + distributedTasksExecuted_.load();
  if (totalTasks > 0) {
    stats["local_percentage"] = (100.0 * localTasksExecuted_.load()) / totalTasks;
    stats["distributed_percentage"] = (100.0 * distributedTasksExecuted_.load()) / totalTasks;
  }

  stats["spark_available"] = isSparkAvailable();
  
  return stats;
}
