#include "transformations/window_functions_transformation.h"
#include "core/logger.h"
#include <algorithm>
#include <sstream>

WindowFunctionsTransformation::WindowFunctionsTransformation() = default;

bool WindowFunctionsTransformation::validateConfig(const json& config) const {
  if (!config.contains("windows") || !config["windows"].is_array()) {
    Logger::error(LogCategory::TRANSFER, "WindowFunctionsTransformation",
                  "Missing or invalid windows in config");
    return false;
  }
  
  auto windows = config["windows"];
  if (windows.empty()) {
    Logger::error(LogCategory::TRANSFER, "WindowFunctionsTransformation",
                  "windows array cannot be empty");
    return false;
  }
  
  std::vector<std::string> validFunctions = {
    "row_number", "lag", "lead", "first_value", "last_value", "rank", "dense_rank"
  };
  
  for (const auto& win : windows) {
    if (!win.is_object()) {
      Logger::error(LogCategory::TRANSFER, "WindowFunctionsTransformation",
                    "Window must be an object");
      return false;
    }
    
    if (!win.contains("function") || !win["function"].is_string()) {
      Logger::error(LogCategory::TRANSFER, "WindowFunctionsTransformation",
                    "Missing or invalid function in window");
      return false;
    }
    
    std::string funcStr = win["function"];
    if (std::find(validFunctions.begin(), validFunctions.end(), funcStr) == 
        validFunctions.end()) {
      Logger::error(LogCategory::TRANSFER, "WindowFunctionsTransformation",
                    "Invalid window function: " + funcStr);
      return false;
    }
    
    if (!win.contains("target_column") || !win["target_column"].is_string()) {
      Logger::error(LogCategory::TRANSFER, "WindowFunctionsTransformation",
                    "Missing or invalid target_column in window");
      return false;
    }
    
    if (!win.contains("source_column") || !win["source_column"].is_string()) {
      Logger::error(LogCategory::TRANSFER, "WindowFunctionsTransformation",
                    "Missing or invalid source_column in window");
      return false;
    }
  }
  
  return true;
}

std::vector<json> WindowFunctionsTransformation::execute(
  const std::vector<json>& inputData,
  const json& config
) {
  if (inputData.empty()) {
    return inputData;
  }
  
  if (!validateConfig(config)) {
    Logger::error(LogCategory::TRANSFER, "WindowFunctionsTransformation",
                  "Invalid config, returning input data unchanged");
    return inputData;
  }
  
  // Parse window configurations
  std::vector<WindowConfig> windowConfigs;
  for (const auto& win : config["windows"]) {
    WindowConfig wc;
    wc.function = parseWindowFunction(win["function"]);
    wc.targetColumn = win["target_column"];
    wc.sourceColumn = win["source_column"];
    wc.offset = win.value("offset", 1);
    wc.defaultValue = win.value("default_value", json(nullptr));
    
    if (win.contains("partition_by") && win["partition_by"].is_array()) {
      for (const auto& col : win["partition_by"]) {
        wc.partitionBy.push_back(col.get<std::string>());
      }
    }
    
    if (win.contains("order_by") && win["order_by"].is_array()) {
      for (const auto& col : win["order_by"]) {
        wc.orderBy.push_back(col.get<std::string>());
      }
    }
    
    windowConfigs.push_back(wc);
  }
  
  // Apply each window function
  std::vector<json> result = inputData;
  
  for (const auto& windowConfig : windowConfigs) {
    if (windowConfig.partitionBy.empty()) {
      // No partitioning - apply to entire dataset
      applyWindowFunction(result, windowConfig);
    } else {
      // Partition data
      std::map<std::string, std::vector<size_t>> partitions;
      
      for (size_t i = 0; i < result.size(); ++i) {
        std::string key = getPartitionKey(result[i], windowConfig.partitionBy);
        partitions[key].push_back(i);
      }
      
      // Apply window function to each partition
      for (auto& [key, indices] : partitions) {
        std::vector<json> partitionData;
        for (size_t idx : indices) {
          partitionData.push_back(result[idx]);
        }
        
        applyWindowFunction(partitionData, windowConfig);
        
        // Update result
        for (size_t i = 0; i < indices.size(); ++i) {
          result[indices[i]] = partitionData[i];
        }
      }
    }
  }
  
  Logger::info(LogCategory::TRANSFER, "WindowFunctionsTransformation",
               "Applied window functions to " + std::to_string(inputData.size()) + " rows");
  
  return result;
}

void WindowFunctionsTransformation::applyWindowFunction(
  std::vector<json>& partitionData,
  const WindowConfig& windowConfig
) {
  // Sort if order_by is specified
  if (!windowConfig.orderBy.empty()) {
    std::sort(partitionData.begin(), partitionData.end(),
      [&windowConfig, this](const json& a, const json& b) {
        return compareRows(a, b, windowConfig.orderBy);
      }
    );
  }
  
  // Apply window function
  switch (windowConfig.function) {
    case WindowFunction::ROW_NUMBER: {
      int rowNum = 1;
      for (auto& row : partitionData) {
        row[windowConfig.targetColumn] = rowNum++;
      }
      break;
    }
    
    case WindowFunction::LAG: {
      for (size_t i = 0; i < partitionData.size(); ++i) {
        if (i >= static_cast<size_t>(windowConfig.offset)) {
          json lagValue = partitionData[i - windowConfig.offset][windowConfig.sourceColumn];
          partitionData[i][windowConfig.targetColumn] = lagValue;
        } else {
          partitionData[i][windowConfig.targetColumn] = windowConfig.defaultValue;
        }
      }
      break;
    }
    
    case WindowFunction::LEAD: {
      for (size_t i = 0; i < partitionData.size(); ++i) {
        if (i + windowConfig.offset < partitionData.size()) {
          json leadValue = partitionData[i + windowConfig.offset][windowConfig.sourceColumn];
          partitionData[i][windowConfig.targetColumn] = leadValue;
        } else {
          partitionData[i][windowConfig.targetColumn] = windowConfig.defaultValue;
        }
      }
      break;
    }
    
    case WindowFunction::FIRST_VALUE: {
      if (!partitionData.empty()) {
        json firstValue = partitionData[0][windowConfig.sourceColumn];
        for (auto& row : partitionData) {
          row[windowConfig.targetColumn] = firstValue;
        }
      }
      break;
    }
    
    case WindowFunction::LAST_VALUE: {
      if (!partitionData.empty()) {
        json lastValue = partitionData[partitionData.size() - 1][windowConfig.sourceColumn];
        for (auto& row : partitionData) {
          row[windowConfig.targetColumn] = lastValue;
        }
      }
      break;
    }
    
    case WindowFunction::RANK: {
      int rank = 1;
      for (size_t i = 0; i < partitionData.size(); ++i) {
        if (i > 0 && partitionData[i][windowConfig.sourceColumn] != 
                     partitionData[i-1][windowConfig.sourceColumn]) {
          rank = i + 1;
        }
        partitionData[i][windowConfig.targetColumn] = rank;
      }
      break;
    }
    
    case WindowFunction::DENSE_RANK: {
      int denseRank = 1;
      for (size_t i = 0; i < partitionData.size(); ++i) {
        if (i > 0 && partitionData[i][windowConfig.sourceColumn] != 
                     partitionData[i-1][windowConfig.sourceColumn]) {
          denseRank++;
        }
        partitionData[i][windowConfig.targetColumn] = denseRank;
      }
      break;
    }
  }
}

std::string WindowFunctionsTransformation::getPartitionKey(
  const json& row,
  const std::vector<std::string>& partitionBy
) {
  std::ostringstream keyStream;
  for (size_t i = 0; i < partitionBy.size(); ++i) {
    if (i > 0) keyStream << "|||";
    if (row.contains(partitionBy[i])) {
      keyStream << row[partitionBy[i]].dump();
    } else {
      keyStream << "NULL";
    }
  }
  return keyStream.str();
}

bool WindowFunctionsTransformation::compareRows(
  const json& row1,
  const json& row2,
  const std::vector<std::string>& orderBy
) {
  for (const auto& col : orderBy) {
    json val1 = row1.value(col, json(nullptr));
    json val2 = row2.value(col, json(nullptr));
    
    if (val1.is_null() && val2.is_null()) continue;
    if (val1.is_null()) return true;
    if (val2.is_null()) return false;
    
    if (val1.is_number() && val2.is_number()) {
      double d1 = val1.get<double>();
      double d2 = val2.get<double>();
      if (d1 < d2) return true;
      if (d1 > d2) return false;
      continue;
    }
    
    if (val1.is_string() && val2.is_string()) {
      int cmp = val1.get<std::string>().compare(val2.get<std::string>());
      if (cmp < 0) return true;
      if (cmp > 0) return false;
      continue;
    }
  }
  
  return false;
}

WindowFunctionsTransformation::WindowFunction WindowFunctionsTransformation::parseWindowFunction(
  const std::string& funcStr
) {
  std::string lower = funcStr;
  std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);
  
  if (lower == "row_number") return WindowFunction::ROW_NUMBER;
  if (lower == "lag") return WindowFunction::LAG;
  if (lower == "lead") return WindowFunction::LEAD;
  if (lower == "first_value") return WindowFunction::FIRST_VALUE;
  if (lower == "last_value") return WindowFunction::LAST_VALUE;
  if (lower == "rank") return WindowFunction::RANK;
  if (lower == "dense_rank") return WindowFunction::DENSE_RANK;
  
  return WindowFunction::ROW_NUMBER; // default
}
