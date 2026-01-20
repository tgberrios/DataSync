#include "transformations/transformation_engine.h"
#include "core/logger.h"
#include <algorithm>

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
