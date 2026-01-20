#include "transformations/normalizer_transformation.h"
#include "core/logger.h"

NormalizerTransformation::NormalizerTransformation() = default;

bool NormalizerTransformation::validateConfig(const json& config) const {
  if (!config.contains("columns_to_denormalize") || 
      !config["columns_to_denormalize"].is_array()) {
    Logger::error(LogCategory::TRANSFER, "NormalizerTransformation",
                  "Missing or invalid columns_to_denormalize in config");
    return false;
  }
  
  auto columns = config["columns_to_denormalize"];
  if (columns.empty()) {
    Logger::error(LogCategory::TRANSFER, "NormalizerTransformation",
                  "columns_to_denormalize array cannot be empty");
    return false;
  }
  
  if (!config.contains("key_column_name") || !config["key_column_name"].is_string()) {
    Logger::error(LogCategory::TRANSFER, "NormalizerTransformation",
                  "Missing or invalid key_column_name in config");
    return false;
  }
  
  if (!config.contains("value_column_name") || !config["value_column_name"].is_string()) {
    Logger::error(LogCategory::TRANSFER, "NormalizerTransformation",
                  "Missing or invalid value_column_name in config");
    return false;
  }
  
  return true;
}

std::vector<json> NormalizerTransformation::execute(
  const std::vector<json>& inputData,
  const json& config
) {
  if (inputData.empty()) {
    return inputData;
  }
  
  if (!validateConfig(config)) {
    Logger::error(LogCategory::TRANSFER, "NormalizerTransformation",
                  "Invalid config, returning input data unchanged");
    return inputData;
  }
  
  std::vector<std::string> columnsToDenormalize;
  for (const auto& col : config["columns_to_denormalize"]) {
    columnsToDenormalize.push_back(col.get<std::string>());
  }
  
  std::string keyColumnName = config["key_column_name"];
  std::string valueColumnName = config["value_column_name"];
  
  std::vector<json> result;
  
  for (const auto& row : inputData) {
    std::vector<json> denormalizedRows = denormalizeRow(
      row,
      columnsToDenormalize,
      keyColumnName,
      valueColumnName
    );
    result.insert(result.end(), denormalizedRows.begin(), denormalizedRows.end());
  }
  
  Logger::info(LogCategory::TRANSFER, "NormalizerTransformation",
               "Denormalized " + std::to_string(inputData.size()) + 
               " rows into " + std::to_string(result.size()) + " rows");
  
  return result;
}

std::vector<json> NormalizerTransformation::denormalizeRow(
  const json& row,
  const std::vector<std::string>& columnsToDenormalize,
  const std::string& keyColumnName,
  const std::string& valueColumnName
) {
  std::vector<json> result;
  
  for (const auto& column : columnsToDenormalize) {
    json newRow;
    
    // Copy all non-denormalized columns
    for (auto it = row.begin(); it != row.end(); ++it) {
      std::string colName = it.key();
      bool isDenormalizedColumn = false;
      for (const auto& denormCol : columnsToDenormalize) {
        if (colName == denormCol) {
          isDenormalizedColumn = true;
          break;
        }
      }
      
      if (!isDenormalizedColumn) {
        newRow[colName] = it.value();
      }
    }
    
    // Add key and value columns
    newRow[keyColumnName] = column;
    if (row.contains(column)) {
      newRow[valueColumnName] = row[column];
    } else {
      newRow[valueColumnName] = json(nullptr);
    }
    
    result.push_back(newRow);
  }
  
  return result;
}
