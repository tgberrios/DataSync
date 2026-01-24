#include "transformations/transformation_engine.h"
#include "core/logger.h"
#include <algorithm>

#ifdef HAVE_SPARK
#include "transformations/spark_transformation.h"
#include "engines/spark_engine.h"
#endif

TransformationEngine::TransformationEngine() = default;

TransformationEngine::~TransformationEngine() = default;

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
    return it->second->execute(inputData, config);
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
