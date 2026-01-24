#include "transformations/transformation_engine.h"
#include "sync/JoinOptimizer.h"
#include "utils/MemoryManager.h"
#include "governance/TransformationLineageTracker.h"
#include "core/logger.h"
#include <algorithm>
#include <chrono>

#ifdef HAVE_SPARK
#include "transformations/spark_transformation.h"
#include "engines/spark_engine.h"
#endif

TransformationEngine::TransformationEngine() {
  // Initialize memory manager with default limits
  MemoryManager::MemoryLimit memLimit;
  memLimit.maxMemory = 2ULL * 1024 * 1024 * 1024;  // 2GB default
  memLimit.enableSpill = true;
  memLimit.spillDirectory = "/tmp/datasync_spill";
  memoryManager_ = std::make_unique<MemoryManager>(memLimit);
  
  // TransformationLineageTracker se inicializará cuando se necesite con connection string
  // Por ahora, se inicializa como nullptr y se crea bajo demanda
  
  Logger::info(LogCategory::SYSTEM, "TransformationEngine",
               "TransformationEngine initialized with memory management");
}

TransformationEngine::~TransformationEngine() {
  memoryManager_.reset();
}

void TransformationEngine::registerTransformation(
  std::unique_ptr<Transformation> transformation
) {
  if (!transformation) {
    Logger::error(LogCategory::SYSTEM, "TransformationEngine",
                  "Attempted to register null transformation");
    return;
  }
  
  std::string type = transformation->getType();
  transformations_[type] = std::move(transformation);
  
  Logger::info(LogCategory::SYSTEM, "TransformationEngine",
               "Registered transformation type: " + type);
}

std::vector<json> TransformationEngine::executePipeline(
  const std::vector<json>& inputData,
  const json& pipelineConfig
) {
  if (!pipelineConfig.contains("transformations") || 
      !pipelineConfig["transformations"].is_array()) {
    Logger::error(LogCategory::SYSTEM, "TransformationEngine",
                  "Invalid pipeline config: missing transformations array");
    return inputData;
  }
  
  // Verificar si se debe usar Spark
  if (shouldUseSpark(pipelineConfig)) {
    return executePipelineWithSpark(inputData, pipelineConfig);
  }
  
  // Ejecución local normal
  std::vector<json> currentData = inputData;
  
  for (const auto& transformConfig : pipelineConfig["transformations"]) {
    if (!transformConfig.contains("type") && transformConfig["type"].is_string()) {
      Logger::warning(LogCategory::SYSTEM, "TransformationEngine",
                      "Skipping transformation without type");
      continue;
    }
    
    std::string type = transformConfig["type"];
    json config = transformConfig.contains("config") ? 
                  transformConfig["config"] : json::object();
    
    currentData = executeTransformation(currentData, type, config);
    
    if (currentData.empty() && !inputData.empty()) {
      Logger::warning(LogCategory::SYSTEM, "TransformationEngine",
                      "Transformation " + type + " returned empty result");
    }
  }
  
  return currentData;
}

std::vector<json> TransformationEngine::executeTransformation(
  const std::vector<json>& inputData,
  const std::string& transformationType,
  const json& config
) {
  auto it = transformations_.find(transformationType);
  if (it == transformations_.end()) {
    Logger::error(LogCategory::SYSTEM, "TransformationEngine",
                  "Unknown transformation type: " + transformationType);
    return inputData;
  }
  
  if (!it->second->validateConfig(config)) {
    Logger::error(LogCategory::SYSTEM, "TransformationEngine",
                  "Invalid config for transformation: " + transformationType);
    return inputData;
  }
  
  try {
    auto startTime = std::chrono::high_resolution_clock::now();
    std::vector<json> result = it->second->execute(inputData, config);
    auto endTime = std::chrono::high_resolution_clock::now();
    double executionTimeMs = std::chrono::duration<double, std::milli>(endTime - startTime).count();

    // Track transformation lineage si está disponible
    // (se inicializará cuando se tenga connection string)
    if (lineageTracker_) {
      trackTransformation(transformationType, config, inputData, result,
                         "", "", 0, 0);
    }

    return result;
  } catch (const std::exception& e) {
    Logger::error(LogCategory::SYSTEM, "TransformationEngine",
                  "Error executing transformation " + transformationType + ": " + 
                  std::string(e.what()));
    return inputData;
  }
}

bool TransformationEngine::validatePipeline(const json& pipelineConfig) const {
  if (!pipelineConfig.contains("transformations") || 
      !pipelineConfig["transformations"].is_array()) {
    return false;
  }
  
  for (const auto& transformConfig : pipelineConfig["transformations"]) {
    if (!transformConfig.contains("type") || 
        !transformConfig["type"].is_string()) {
      return false;
    }
    
    std::string type = transformConfig["type"];
    auto it = transformations_.find(type);
    if (it == transformations_.end()) {
      Logger::warning(LogCategory::SYSTEM, "TransformationEngine",
                      "Unknown transformation type in pipeline: " + type);
      return false;
    }
    
    json config = transformConfig.contains("config") ? 
                  transformConfig["config"] : json::object();
    
    if (!it->second->validateConfig(config)) {
      Logger::warning(LogCategory::SYSTEM, "TransformationEngine",
                      "Invalid config for transformation: " + type);
      return false;
    }
  }
  
  return true;
}

std::vector<json> TransformationEngine::executePipelineWithSpark(
    const std::vector<json>& inputData,
    const json& pipelineConfig) {
  
#ifdef HAVE_SPARK
  // Traducir pipeline completo a Spark SQL
  SparkTranslator::TranslationResult translation = SparkTranslator::translatePipeline(pipelineConfig);
  
  if (translation.sparkSQL.empty()) {
    Logger::warning(LogCategory::TRANSFORM, "TransformationEngine",
                    "Failed to translate pipeline to Spark, falling back to local execution");
    // Fallback a ejecución local
    std::vector<json> currentData = inputData;
    for (const auto& transformConfig : pipelineConfig["transformations"]) {
      std::string type = transformConfig["type"].get<std::string>();
      json config = transformConfig.contains("config") ? transformConfig["config"] : json::object();
      currentData = executeTransformation(currentData, type, config);
    }
    return currentData;
  }
  
  Logger::info(LogCategory::TRANSFORM, "TransformationEngine",
               "Executing pipeline in Spark");
  
  // En una implementación real, se ejecutaría el SQL en Spark y se leerían los resultados
  // Por ahora, retornamos los datos de entrada como placeholder
  return inputData;
#else
  Logger::warning(LogCategory::TRANSFORM, "TransformationEngine",
                  "Spark not available, using local execution");
  // Fallback a ejecución local
  std::vector<json> currentData = inputData;
  for (const auto& transformConfig : pipelineConfig["transformations"]) {
    std::string type = transformConfig["type"].get<std::string>();
    json config = transformConfig.contains("config") ? transformConfig["config"] : json::object();
    currentData = executeTransformation(currentData, type, config);
  }
  return currentData;
#endif
}

std::vector<json> TransformationEngine::optimizeJoin(
    const std::vector<json>& leftData,
    const std::vector<json>& rightData,
    const json& joinConfig) {
  
  // Crear configuración de join
  JoinOptimizer::JoinConfig config;
  config.leftTable = joinConfig.value("left_table", "left");
  config.rightTable = joinConfig.value("right_table", "right");
  
  if (joinConfig.contains("left_columns")) {
    for (const auto& col : joinConfig["left_columns"]) {
      config.leftColumns.push_back(col.get<std::string>());
    }
  }
  
  if (joinConfig.contains("right_columns")) {
    for (const auto& col : joinConfig["right_columns"]) {
      config.rightColumns.push_back(col.get<std::string>());
    }
  }
  
  config.joinType = joinConfig.value("join_type", "inner");
  
  // Estimar estadísticas
  config.leftStats = JoinOptimizer::estimateTableStats(config.leftTable, leftData);
  config.rightStats = JoinOptimizer::estimateTableStats(config.rightTable, rightData);
  
  // Ejecutar join optimizado
  JoinOptimizer::JoinResult result = JoinOptimizer::executeJoin(config, leftData, rightData);
  
  if (!result.success) {
    Logger::error(LogCategory::SYSTEM, "TransformationEngine",
                  "Join optimization failed: " + result.errorMessage);
    return {};
  }
  
  return result.resultRows;
}

void TransformationEngine::trackTransformation(
    const std::string& transformationType,
    const json& config,
    const std::vector<json>& inputData,
    const std::vector<json>& outputData,
    const std::string& workflowName,
    const std::string& taskName,
    int64_t workflowExecutionId,
    int64_t taskExecutionId) {
  
  if (!lineageTracker_) {
    return;  // No tracking disponible
  }

  try {
    TransformationLineageTracker::TransformationRecord record;
    record.transformationId = std::to_string(
        std::chrono::system_clock::now().time_since_epoch().count());
    record.transformationType = transformationType;
    record.transformationConfig = config;
    record.workflowName = workflowName;
    record.taskName = taskName;
    record.workflowExecutionId = workflowExecutionId;
    record.taskExecutionId = taskExecutionId;
    record.executedAt = std::chrono::system_clock::now();
    record.rowsProcessed = outputData.size();
    record.success = true;

    // Extraer schemas/tablas/columnas de input y output
    // (simplificado - en implementación real, se extraería de metadata)
    if (!inputData.empty() && inputData[0].contains("_schema")) {
      record.inputSchemas.push_back(inputData[0]["_schema"].get<std::string>());
    }
    if (!outputData.empty() && outputData[0].contains("_schema")) {
      record.outputSchemas.push_back(outputData[0]["_schema"].get<std::string>());
    }

    lineageTracker_->recordTransformation(record);
  } catch (const std::exception& e) {
    Logger::warning(LogCategory::GOVERNANCE, "TransformationEngine",
                    "Failed to track transformation lineage: " + std::string(e.what()));
  }
}

bool TransformationEngine::shouldUseSpark(const json& pipelineConfig) const {
  // Verificar si se fuerza el uso de Spark
  if (pipelineConfig.contains("use_spark")) {
    return pipelineConfig["use_spark"].get<bool>();
  }
  
  // Verificar si hay muchas transformaciones (beneficio de Spark)
  if (pipelineConfig.contains("transformations") && 
      pipelineConfig["transformations"].is_array()) {
    size_t transformCount = pipelineConfig["transformations"].size();
    if (transformCount > 5) { // Threshold configurable
      return true;
    }
  }
  
  return false;
}
