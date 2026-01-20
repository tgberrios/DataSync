#include "transformations/sequence_generator_transformation.h"
#include "core/logger.h"

SequenceGeneratorTransformation::SequenceGeneratorTransformation() = default;

bool SequenceGeneratorTransformation::validateConfig(const json& config) const {
  if (!config.contains("target_column") || !config["target_column"].is_string()) {
    Logger::error(LogCategory::TRANSFER, "SequenceGeneratorTransformation",
                  "Missing or invalid target_column in config");
    return false;
  }
  
  return true;
}

std::vector<json> SequenceGeneratorTransformation::execute(
  const std::vector<json>& inputData,
  const json& config
) {
  if (inputData.empty()) {
    return inputData;
  }
  
  if (!validateConfig(config)) {
    Logger::error(LogCategory::TRANSFER, "SequenceGeneratorTransformation",
                  "Invalid config, returning input data unchanged");
    return inputData;
  }
  
  std::string targetColumn = config["target_column"];
  int64_t startValue = config.value("start_value", 1);
  int64_t increment = config.value("increment", 1);
  
  std::vector<json> result;
  result.reserve(inputData.size());
  
  for (size_t i = 0; i < inputData.size(); ++i) {
    json outputRow = inputData[i];
    int64_t sequenceValue = generateSequence(i, startValue, increment);
    outputRow[targetColumn] = sequenceValue;
    result.push_back(outputRow);
  }
  
  Logger::info(LogCategory::TRANSFER, "SequenceGeneratorTransformation",
               "Generated sequence for " + std::to_string(inputData.size()) + " rows");
  
  return result;
}

int64_t SequenceGeneratorTransformation::generateSequence(
  int rowIndex,
  int64_t startValue,
  int64_t increment
) {
  return startValue + (rowIndex * increment);
}
