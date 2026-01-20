#include "transformations/union_transformation.h"
#include "core/logger.h"
#include <algorithm>
#include <sstream>
#include <set>

UnionTransformation::UnionTransformation() = default;

bool UnionTransformation::validateConfig(const json& config) const {
  if (!config.contains("additional_data") || !config["additional_data"].is_array()) {
    Logger::error(LogCategory::TRANSFER, "UnionTransformation",
                  "Missing or invalid additional_data in config");
    return false;
  }
  
  // union_type is optional, defaults to UNION_ALL
  if (config.contains("union_type") && !config["union_type"].is_string()) {
    Logger::error(LogCategory::TRANSFER, "UnionTransformation",
                  "union_type must be a string if provided");
    return false;
  }
  
  return true;
}

std::vector<json> UnionTransformation::execute(
  const std::vector<json>& inputData,
  const json& config
) {
  if (inputData.empty() && (!config.contains("additional_data") || 
      config["additional_data"].empty())) {
    return inputData;
  }
  
  if (!validateConfig(config)) {
    Logger::error(LogCategory::TRANSFER, "UnionTransformation",
                  "Invalid config, returning input data unchanged");
    return inputData;
  }
  
  // Get additional data sources
  std::vector<std::vector<json>> additionalData;
  for (const auto& dataSource : config["additional_data"]) {
    if (!dataSource.is_array()) {
      Logger::warning(LogCategory::TRANSFER, "UnionTransformation",
                      "Skipping invalid data source in additional_data");
      continue;
    }
    
    std::vector<json> sourceRows;
    for (const auto& row : dataSource) {
      sourceRows.push_back(row);
    }
    additionalData.push_back(sourceRows);
  }
  
  UnionType unionType = parseUnionType(config.value("union_type", "union_all"));
  
  std::vector<json> result;
  
  if (unionType == UnionType::UNION) {
    result = performUnion(inputData, additionalData);
  } else {
    result = performUnionAll(inputData, additionalData);
  }
  
  Logger::info(LogCategory::TRANSFER, "UnionTransformation",
               "Unioned " + std::to_string(inputData.size()) + " input rows with " +
               std::to_string(additionalData.size()) + " additional sources, result: " +
               std::to_string(result.size()) + " rows");
  
  return result;
}

std::vector<std::string> UnionTransformation::getAllColumns(
  const std::vector<json>& inputData,
  const std::vector<std::vector<json>>& additionalData
) {
  std::set<std::string> allColumnsSet;
  
  for (const auto& row : inputData) {
    for (auto& [key, value] : row.items()) {
      allColumnsSet.insert(key);
    }
  }
  
  for (const auto& dataSource : additionalData) {
    for (const auto& row : dataSource) {
      for (auto& [key, value] : row.items()) {
        allColumnsSet.insert(key);
      }
    }
  }
  
  return std::vector<std::string>(allColumnsSet.begin(), allColumnsSet.end());
}

std::vector<json> UnionTransformation::performUnion(
  const std::vector<json>& inputData,
  const std::vector<std::vector<json>>& additionalData
) {
  std::vector<std::string> allColumns = getAllColumns(inputData, additionalData);
  std::set<std::string> seenSignatures;
  std::vector<json> result;
  
  // Process input data
  for (const auto& row : inputData) {
    json normalized = normalizeRow(row, allColumns);
    std::string signature = createRowSignature(normalized);
    
    if (seenSignatures.find(signature) == seenSignatures.end()) {
      seenSignatures.insert(signature);
      result.push_back(normalized);
    }
  }
  
  // Process additional data sources
  for (const auto& dataSource : additionalData) {
    for (const auto& row : dataSource) {
      json normalized = normalizeRow(row, allColumns);
      std::string signature = createRowSignature(normalized);
      
      if (seenSignatures.find(signature) == seenSignatures.end()) {
        seenSignatures.insert(signature);
        result.push_back(normalized);
      }
    }
  }
  
  return result;
}

std::vector<json> UnionTransformation::performUnionAll(
  const std::vector<json>& inputData,
  const std::vector<std::vector<json>>& additionalData
) {
  std::vector<std::string> allColumns = getAllColumns(inputData, additionalData);
  std::vector<json> result;
  
  // Process input data
  for (const auto& row : inputData) {
    result.push_back(normalizeRow(row, allColumns));
  }
  
  // Process additional data sources
  for (const auto& dataSource : additionalData) {
    for (const auto& row : dataSource) {
      result.push_back(normalizeRow(row, allColumns));
    }
  }
  
  return result;
}

json UnionTransformation::normalizeRow(
  const json& row,
  const std::vector<std::string>& allColumns
) {
  json normalized;
  
  for (const auto& column : allColumns) {
    if (row.contains(column)) {
      normalized[column] = row[column];
    } else {
      normalized[column] = nullptr;
    }
  }
  
  return normalized;
}

std::string UnionTransformation::createRowSignature(const json& row) {
  std::ostringstream sig;
  bool first = true;
  
  for (auto& [key, value] : row.items()) {
    if (!first) sig << "|||";
    sig << key << "=" << value.dump();
    first = false;
  }
  
  return sig.str();
}

UnionTransformation::UnionType UnionTransformation::parseUnionType(const std::string& unionTypeStr) {
  if (unionTypeStr == "union") return UnionType::UNION;
  if (unionTypeStr == "union_all") return UnionType::UNION_ALL;
  return UnionType::UNION_ALL; // default
}

std::string UnionTransformation::generateUnionSQL(
  const std::string& firstQuery,
  const std::vector<std::string>& additionalQueries,
  UnionType unionType
) {
  std::ostringstream sql;
  
  sql << "(" << firstQuery << ")";
  
  for (const auto& query : additionalQueries) {
    if (unionType == UnionType::UNION) {
      sql << " UNION ";
    } else {
      sql << " UNION ALL ";
    }
    sql << "(" << query << ")";
  }
  
  return sql.str();
}
