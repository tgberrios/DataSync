#include "transformations/aggregate_transformation.h"
#include "core/logger.h"
#include <algorithm>
#include <cmath>
#include <sstream>
#include <numeric>

AggregateTransformation::AggregateTransformation() = default;

bool AggregateTransformation::validateConfig(const json& config) const {
  if (!config.contains("aggregations") || !config["aggregations"].is_array()) {
    Logger::error(LogCategory::TRANSFER, "AggregateTransformation",
                  "Missing or invalid aggregations in config");
    return false;
  }
  
  auto aggregations = config["aggregations"];
  if (aggregations.empty()) {
    Logger::error(LogCategory::TRANSFER, "AggregateTransformation",
                  "aggregations array cannot be empty");
    return false;
  }
  
  for (const auto& agg : aggregations) {
    if (!agg.contains("column") || !agg["column"].is_string()) {
      Logger::error(LogCategory::TRANSFER, "AggregateTransformation",
                    "Missing or invalid column in aggregation");
      return false;
    }
    
    if (!agg.contains("function") || !agg["function"].is_string()) {
      Logger::error(LogCategory::TRANSFER, "AggregateTransformation",
                    "Missing or invalid function in aggregation");
      return false;
    }
    
    std::string function = agg["function"];
    std::vector<std::string> validFunctions = {
      "sum", "count", "avg", "min", "max", "stddev", "variance", "percentile"
    };
    
    if (function == "percentile" && (!agg.contains("percentile_value") || 
        !agg["percentile_value"].is_number())) {
      Logger::error(LogCategory::TRANSFER, "AggregateTransformation",
                    "percentile function requires percentile_value");
      return false;
    }
    
    if (std::find(validFunctions.begin(), validFunctions.end(), function) == 
        validFunctions.end()) {
      Logger::error(LogCategory::TRANSFER, "AggregateTransformation",
                    "Invalid aggregation function: " + function);
      return false;
    }
  }
  
  // groupBy is optional
  if (config.contains("group_by") && !config["group_by"].is_array()) {
    Logger::error(LogCategory::TRANSFER, "AggregateTransformation",
                  "group_by must be an array if provided");
    return false;
  }
  
  return true;
}

std::vector<json> AggregateTransformation::execute(
  const std::vector<json>& inputData,
  const json& config
) {
  if (inputData.empty()) {
    return inputData;
  }
  
  if (!validateConfig(config)) {
    Logger::error(LogCategory::TRANSFER, "AggregateTransformation",
                  "Invalid config, returning input data unchanged");
    return inputData;
  }
  
  // Get group by columns
  std::vector<std::string> groupByColumns;
  if (config.contains("group_by") && config["group_by"].is_array()) {
    for (const auto& col : config["group_by"]) {
      groupByColumns.push_back(col.get<std::string>());
    }
  }
  
  // Get aggregations
  json aggregations = config["aggregations"];
  
  // Group data
  std::map<std::string, std::vector<json>> groups;
  
  if (groupByColumns.empty()) {
    // No grouping - aggregate all data into one group
    groups[""] = inputData;
  } else {
    // Group by specified columns
    for (const auto& row : inputData) {
      std::ostringstream keyStream;
      for (size_t i = 0; i < groupByColumns.size(); ++i) {
        if (i > 0) keyStream << "|||";
        if (row.contains(groupByColumns[i])) {
          keyStream << row[groupByColumns[i]].dump();
        } else {
          keyStream << "NULL";
        }
      }
      std::string key = keyStream.str();
      groups[key].push_back(row);
    }
  }
  
  // Aggregate each group
  std::vector<json> result;
  result.reserve(groups.size());
  
  for (const auto& [groupKey, groupData] : groups) {
    json aggregatedRow = aggregateGroup(groupData, groupByColumns, aggregations);
    result.push_back(aggregatedRow);
  }
  
  Logger::info(LogCategory::TRANSFER, "AggregateTransformation",
               "Aggregated " + std::to_string(inputData.size()) + 
               " rows into " + std::to_string(result.size()) + " groups");
  
  return result;
}

json AggregateTransformation::aggregateGroup(
  const std::vector<json>& groupData,
  const std::vector<std::string>& groupByColumns,
  const json& aggregationConfig
) {
  json result;
  
  // Add group by columns to result
  if (!groupData.empty()) {
    const json& firstRow = groupData[0];
    for (const auto& col : groupByColumns) {
      if (firstRow.contains(col)) {
        result[col] = firstRow[col];
      }
    }
  }
  
  // Calculate aggregations
  for (const auto& agg : aggregationConfig) {
    std::string column = agg["column"];
    std::string function = agg["function"];
    std::string alias = agg.value("alias", column + "_" + function);
    
    double value = calculateAggregation(groupData, column, function, agg);
    result[alias] = value;
  }
  
  return result;
}

double AggregateTransformation::calculateAggregation(
  const std::vector<json>& groupData,
  const std::string& column,
  const std::string& function,
  const json& aggConfig
) {
  std::vector<double> values;
  
  for (const auto& row : groupData) {
    if (row.contains(column)) {
      double val = getNumericValue(row[column]);
      if (!std::isnan(val)) {
        values.push_back(val);
      }
    }
  }
  
  if (values.empty()) {
    return 0.0;
  }
  
  if (function == "sum") {
    return std::accumulate(values.begin(), values.end(), 0.0);
  } else if (function == "count") {
    return static_cast<double>(values.size());
  } else if (function == "avg") {
    double sum = std::accumulate(values.begin(), values.end(), 0.0);
    return sum / values.size();
  } else if (function == "min") {
    return *std::min_element(values.begin(), values.end());
  } else if (function == "max") {
    return *std::max_element(values.begin(), values.end());
  } else if (function == "stddev") {
    if (values.size() < 2) return 0.0;
    double mean = std::accumulate(values.begin(), values.end(), 0.0) / values.size();
    double variance = 0.0;
    for (double val : values) {
      variance += (val - mean) * (val - mean);
    }
    variance /= (values.size() - 1);
    return std::sqrt(variance);
  } else if (function == "variance") {
    if (values.size() < 2) return 0.0;
    double mean = std::accumulate(values.begin(), values.end(), 0.0) / values.size();
    double variance = 0.0;
    for (double val : values) {
      variance += (val - mean) * (val - mean);
    }
    return variance / (values.size() - 1);
  } else if (function == "percentile") {
    if (values.empty()) return 0.0;
    std::sort(values.begin(), values.end());
    double percentile = aggConfig.value("percentile_value", 0.5); // Default to median (50th percentile)
    if (percentile < 0.0) percentile = 0.0;
    if (percentile > 1.0) percentile = 1.0;
    size_t index = static_cast<size_t>((values.size() - 1) * percentile);
    if (index >= values.size()) index = values.size() - 1;
    return values[index];
  }
  
  return 0.0;
}

double AggregateTransformation::getNumericValue(const json& value) {
  if (value.is_number()) {
    return value.get<double>();
  } else if (value.is_string()) {
    try {
      return std::stod(value.get<std::string>());
    } catch (...) {
      return std::nan("");
    }
  } else if (value.is_null()) {
    return std::nan("");
  }
  return std::nan("");
}

std::string AggregateTransformation::generateAggregateSQL(
  const std::string& sourceQuery,
  const std::vector<std::string>& groupByColumns,
  const json& aggregationConfig
) {
  std::ostringstream sql;
  
  sql << "SELECT ";
  
  // Add group by columns
  for (size_t i = 0; i < groupByColumns.size(); ++i) {
    if (i > 0) sql << ", ";
    sql << "\"" << groupByColumns[i] << "\"";
  }
  
  // Add aggregations
  for (const auto& agg : aggregationConfig) {
    if (!groupByColumns.empty() || agg != aggregationConfig[0]) {
      sql << ", ";
    }
    
    std::string column = agg["column"];
    std::string function = agg["function"];
    std::string alias = agg.value("alias", column + "_" + function);
    
    sql << function << "(\"" << column << "\") AS \"" << alias << "\"";
  }
  
  sql << " FROM (" << sourceQuery << ") AS source";
  
  if (!groupByColumns.empty()) {
    sql << " GROUP BY ";
    for (size_t i = 0; i < groupByColumns.size(); ++i) {
      if (i > 0) sql << ", ";
      sql << "\"" << groupByColumns[i] << "\"";
    }
  }
  
  return sql.str();
}
