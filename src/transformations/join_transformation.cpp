#include "transformations/join_transformation.h"
#include "core/logger.h"
#include <algorithm>
#include <sstream>
#include <set>

JoinTransformation::JoinTransformation() = default;

bool JoinTransformation::validateConfig(const json& config) const {
  if (!config.contains("right_data") || !config["right_data"].is_array()) {
    Logger::error(LogCategory::TRANSFER, "JoinTransformation",
                  "Missing or invalid right_data in config");
    return false;
  }
  
  if (!config.contains("join_type") || !config["join_type"].is_string()) {
    Logger::error(LogCategory::TRANSFER, "JoinTransformation",
                  "Missing or invalid join_type in config");
    return false;
  }
  
  if (!config.contains("left_columns") || !config["left_columns"].is_array()) {
    Logger::error(LogCategory::TRANSFER, "JoinTransformation",
                  "Missing or invalid left_columns in config");
    return false;
  }
  
  if (!config.contains("right_columns") || !config["right_columns"].is_array()) {
    Logger::error(LogCategory::TRANSFER, "JoinTransformation",
                  "Missing or invalid right_columns in config");
    return false;
  }
  
  auto leftCols = config["left_columns"];
  auto rightCols = config["right_columns"];
  
  if (leftCols.size() != rightCols.size()) {
    Logger::error(LogCategory::TRANSFER, "JoinTransformation",
                  "left_columns and right_columns must have same size");
    return false;
  }
  
  std::string joinType = config["join_type"];
  std::vector<std::string> validTypes = {"inner", "left", "right", "full_outer"};
  if (std::find(validTypes.begin(), validTypes.end(), joinType) == validTypes.end()) {
    Logger::error(LogCategory::TRANSFER, "JoinTransformation",
                  "Invalid join_type: " + joinType);
    return false;
  }
  
  return true;
}

std::vector<json> JoinTransformation::execute(
  const std::vector<json>& inputData,
  const json& config
) {
  if (inputData.empty()) {
    return inputData;
  }
  
  if (!validateConfig(config)) {
    Logger::error(LogCategory::TRANSFER, "JoinTransformation",
                  "Invalid config, returning input data unchanged");
    return inputData;
  }
  
  std::vector<json> rightData;
  for (const auto& row : config["right_data"]) {
    rightData.push_back(row);
  }
  
  std::vector<std::string> leftColumns;
  for (const auto& col : config["left_columns"]) {
    leftColumns.push_back(col.get<std::string>());
  }
  
  std::vector<std::string> rightColumns;
  for (const auto& col : config["right_columns"]) {
    rightColumns.push_back(col.get<std::string>());
  }
  
  JoinType joinType = parseJoinType(config["join_type"]);
  
  std::vector<json> result;
  
  switch (joinType) {
    case JoinType::INNER:
      result = performInnerJoin(inputData, rightData, leftColumns, rightColumns);
      break;
    case JoinType::LEFT:
      result = performLeftJoin(inputData, rightData, leftColumns, rightColumns);
      break;
    case JoinType::RIGHT:
      result = performRightJoin(inputData, rightData, leftColumns, rightColumns);
      break;
    case JoinType::FULL_OUTER:
      result = performFullOuterJoin(inputData, rightData, leftColumns, rightColumns);
      break;
  }
  
  Logger::info(LogCategory::TRANSFER, "JoinTransformation",
               "Joined " + std::to_string(inputData.size()) + " left rows with " +
               std::to_string(rightData.size()) + " right rows, result: " +
               std::to_string(result.size()) + " rows");
  
  return result;
}

std::string JoinTransformation::createJoinKey(
  const json& row,
  const std::vector<std::string>& columns
) {
  std::ostringstream keyStream;
  for (size_t i = 0; i < columns.size(); ++i) {
    if (i > 0) keyStream << "|||";
    if (row.contains(columns[i])) {
      keyStream << row[columns[i]].dump();
    } else {
      keyStream << "NULL";
    }
  }
  return keyStream.str();
}

std::vector<json> JoinTransformation::performInnerJoin(
  const std::vector<json>& leftData,
  const std::vector<json>& rightData,
  const std::vector<std::string>& leftColumns,
  const std::vector<std::string>& rightColumns
) {
  // Build index for right data
  std::map<std::string, std::vector<json>> rightIndex;
  for (const auto& rightRow : rightData) {
    std::string key = createJoinKey(rightRow, rightColumns);
    rightIndex[key].push_back(rightRow);
  }
  
  std::vector<json> result;
  
  // Join left with right
  for (const auto& leftRow : leftData) {
    std::string key = createJoinKey(leftRow, leftColumns);
    auto it = rightIndex.find(key);
    if (it != rightIndex.end()) {
      for (const auto& rightRow : it->second) {
        result.push_back(mergeRows(leftRow, rightRow));
      }
    }
  }
  
  return result;
}

std::vector<json> JoinTransformation::performLeftJoin(
  const std::vector<json>& leftData,
  const std::vector<json>& rightData,
  const std::vector<std::string>& leftColumns,
  const std::vector<std::string>& rightColumns
) {
  // Build index for right data
  std::map<std::string, std::vector<json>> rightIndex;
  for (const auto& rightRow : rightData) {
    std::string key = createJoinKey(rightRow, rightColumns);
    rightIndex[key].push_back(rightRow);
  }
  
  std::vector<json> result;
  
  // Join left with right
  for (const auto& leftRow : leftData) {
    std::string key = createJoinKey(leftRow, leftColumns);
    auto it = rightIndex.find(key);
    if (it != rightIndex.end()) {
      for (const auto& rightRow : it->second) {
        result.push_back(mergeRows(leftRow, rightRow));
      }
    } else {
      // No match - left row with null right columns
      json merged = leftRow;
      for (const auto& rightRow : rightData) {
        for (auto& [key, value] : rightRow.items()) {
          if (std::find(rightColumns.begin(), rightColumns.end(), key) == rightColumns.end()) {
            merged[key] = nullptr;
          }
        }
      }
      result.push_back(merged);
    }
  }
  
  return result;
}

std::vector<json> JoinTransformation::performRightJoin(
  const std::vector<json>& leftData,
  const std::vector<json>& rightData,
  const std::vector<std::string>& leftColumns,
  const std::vector<std::string>& rightColumns
) {
  // Build index for left data
  std::map<std::string, std::vector<json>> leftIndex;
  for (const auto& leftRow : leftData) {
    std::string key = createJoinKey(leftRow, leftColumns);
    leftIndex[key].push_back(leftRow);
  }
  
  std::vector<json> result;
  std::set<std::string> matchedLeftKeys;
  
  // Join right with left
  for (const auto& rightRow : rightData) {
    std::string key = createJoinKey(rightRow, rightColumns);
    auto it = leftIndex.find(key);
    if (it != leftIndex.end()) {
      matchedLeftKeys.insert(key);
      for (const auto& leftRow : it->second) {
        result.push_back(mergeRows(leftRow, rightRow));
      }
    } else {
      // No match - right row with null left columns
      json merged = rightRow;
      for (const auto& leftRow : leftData) {
        for (auto& [key, value] : leftRow.items()) {
          if (std::find(leftColumns.begin(), leftColumns.end(), key) == leftColumns.end()) {
            merged[key] = nullptr;
          }
        }
      }
      result.push_back(merged);
    }
  }
  
  return result;
}

std::vector<json> JoinTransformation::performFullOuterJoin(
  const std::vector<json>& leftData,
  const std::vector<json>& rightData,
  const std::vector<std::string>& leftColumns,
  const std::vector<std::string>& rightColumns
) {
  // Build indexes
  std::map<std::string, std::vector<json>> leftIndex;
  for (const auto& leftRow : leftData) {
    std::string key = createJoinKey(leftRow, leftColumns);
    leftIndex[key].push_back(leftRow);
  }
  
  std::map<std::string, std::vector<json>> rightIndex;
  for (const auto& rightRow : rightData) {
    std::string key = createJoinKey(rightRow, rightColumns);
    rightIndex[key].push_back(rightRow);
  }
  
  std::vector<json> result;
  std::set<std::string> matchedKeys;
  
  // Process matches and left-only rows
  for (const auto& leftRow : leftData) {
    std::string key = createJoinKey(leftRow, leftColumns);
    auto it = rightIndex.find(key);
    if (it != rightIndex.end()) {
      matchedKeys.insert(key);
      for (const auto& rightRow : it->second) {
        result.push_back(mergeRows(leftRow, rightRow));
      }
    } else {
      // Left-only row
      json merged = leftRow;
      for (const auto& rightRow : rightData) {
        for (auto& [key, value] : rightRow.items()) {
          if (std::find(rightColumns.begin(), rightColumns.end(), key) == rightColumns.end()) {
            merged[key] = nullptr;
          }
        }
      }
      result.push_back(merged);
    }
  }
  
  // Process right-only rows
  for (const auto& rightRow : rightData) {
    std::string key = createJoinKey(rightRow, rightColumns);
    if (matchedKeys.find(key) == matchedKeys.end()) {
      // Right-only row
      json merged = rightRow;
      for (const auto& leftRow : leftData) {
        for (auto& [key, value] : leftRow.items()) {
          if (std::find(leftColumns.begin(), leftColumns.end(), key) == leftColumns.end()) {
            merged[key] = nullptr;
          }
        }
      }
      result.push_back(merged);
    }
  }
  
  return result;
}

json JoinTransformation::mergeRows(
  const json& leftRow,
  const json& rightRow,
  const std::string& leftPrefix,
  const std::string& rightPrefix
) {
  json merged = leftRow;
  
  for (auto& [key, value] : rightRow.items()) {
    std::string mergedKey = rightPrefix.empty() ? key : rightPrefix + "_" + key;
    merged[mergedKey] = value;
  }
  
  return merged;
}

JoinTransformation::JoinType JoinTransformation::parseJoinType(const std::string& joinTypeStr) {
  if (joinTypeStr == "inner") return JoinType::INNER;
  if (joinTypeStr == "left") return JoinType::LEFT;
  if (joinTypeStr == "right") return JoinType::RIGHT;
  if (joinTypeStr == "full_outer") return JoinType::FULL_OUTER;
  return JoinType::INNER; // default
}

std::string JoinTransformation::generateJoinSQL(
  const std::string& leftQuery,
  const std::string& rightQuery,
  JoinType joinType,
  const std::vector<std::string>& leftColumns,
  const std::vector<std::string>& rightColumns
) {
  std::ostringstream sql;
  
  sql << "SELECT * FROM (" << leftQuery << ") AS left_table ";
  
  switch (joinType) {
    case JoinType::INNER:
      sql << "INNER JOIN ";
      break;
    case JoinType::LEFT:
      sql << "LEFT JOIN ";
      break;
    case JoinType::RIGHT:
      sql << "RIGHT JOIN ";
      break;
    case JoinType::FULL_OUTER:
      sql << "FULL OUTER JOIN ";
      break;
  }
  
  sql << "(" << rightQuery << ") AS right_table ON ";
  
  for (size_t i = 0; i < leftColumns.size(); ++i) {
    if (i > 0) sql << " AND ";
    sql << "left_table.\"" << leftColumns[i] << "\" = right_table.\"" << rightColumns[i] << "\"";
  }
  
  return sql.str();
}
