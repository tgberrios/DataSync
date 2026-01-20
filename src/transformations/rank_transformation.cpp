#include "transformations/rank_transformation.h"
#include "core/logger.h"
#include <algorithm>
#include <map>

RankTransformation::RankTransformation() = default;

bool RankTransformation::validateConfig(const json& config) const {
  if (!config.contains("rank_type") || !config["rank_type"].is_string()) {
    Logger::error(LogCategory::TRANSFER, "RankTransformation",
                  "Missing or invalid rank_type in config");
    return false;
  }
  
  if (!config.contains("order_column") || !config["order_column"].is_string()) {
    Logger::error(LogCategory::TRANSFER, "RankTransformation",
                  "Missing or invalid order_column in config");
    return false;
  }
  
  std::string rankType = config["rank_type"];
  std::vector<std::string> validTypes = {
    "top_n", "bottom_n", "rank", "dense_rank", "row_number"
  };
  
  if (std::find(validTypes.begin(), validTypes.end(), rankType) == validTypes.end()) {
    Logger::error(LogCategory::TRANSFER, "RankTransformation",
                  "Invalid rank_type: " + rankType);
    return false;
  }
  
  if ((rankType == "top_n" || rankType == "bottom_n") && 
      (!config.contains("n") || !config["n"].is_number())) {
    Logger::error(LogCategory::TRANSFER, "RankTransformation",
                  "Missing or invalid n for top_n/bottom_n");
    return false;
  }
  
  return true;
}

std::vector<json> RankTransformation::execute(
  const std::vector<json>& inputData,
  const json& config
) {
  if (inputData.empty()) {
    return inputData;
  }
  
  if (!validateConfig(config)) {
    Logger::error(LogCategory::TRANSFER, "RankTransformation",
                  "Invalid config, returning input data unchanged");
    return inputData;
  }
  
  RankType rankType = parseRankType(config["rank_type"]);
  std::string orderColumn = config["order_column"];
  int n = config.value("n", 10);
  
  std::vector<std::string> partitionColumns;
  if (config.contains("partition_by") && config["partition_by"].is_array()) {
    for (const auto& col : config["partition_by"]) {
      partitionColumns.push_back(col.get<std::string>());
    }
  }
  
  return performRanking(inputData, rankType, n, orderColumn, partitionColumns);
}

std::vector<json> RankTransformation::performRanking(
  const std::vector<json>& inputData,
  RankType rankType,
  int n,
  const std::string& orderColumn,
  const std::vector<std::string>& partitionColumns
) {
  if (partitionColumns.empty()) {
    // No partitioning - rank entire dataset
    std::vector<json> sortedData = inputData;
    
    std::sort(sortedData.begin(), sortedData.end(),
      [&orderColumn, this](const json& a, const json& b) {
        return compareForRanking(a, b, orderColumn);
      }
    );
    
    if (rankType == RankType::TOP_N) {
      if (n > 0 && n < static_cast<int>(sortedData.size())) {
        sortedData.resize(n);
      }
      return sortedData;
    } else if (rankType == RankType::BOTTOM_N) {
      if (n > 0 && n < static_cast<int>(sortedData.size())) {
        std::reverse(sortedData.begin(), sortedData.end());
        sortedData.resize(n);
        std::reverse(sortedData.begin(), sortedData.end());
      }
      return sortedData;
    } else {
      // Add rank column
      std::vector<json> result = sortedData;
      int rank = 1;
      for (auto& row : result) {
        row["_rank"] = rank++;
      }
      return result;
    }
  } else {
    // Partition by columns
    std::map<std::string, std::vector<json>> partitions;
    
    for (const auto& row : inputData) {
      std::ostringstream keyStream;
      for (size_t i = 0; i < partitionColumns.size(); ++i) {
        if (i > 0) keyStream << "|||";
        if (row.contains(partitionColumns[i])) {
          keyStream << row[partitionColumns[i]].dump();
        } else {
          keyStream << "NULL";
        }
      }
      std::string key = keyStream.str();
      partitions[key].push_back(row);
    }
    
    std::vector<json> result;
    
    for (auto& [key, partitionData] : partitions) {
      std::sort(partitionData.begin(), partitionData.end(),
        [&orderColumn, this](const json& a, const json& b) {
          return compareForRanking(a, b, orderColumn);
        }
      );
      
      if (rankType == RankType::TOP_N) {
        if (n > 0 && n < static_cast<int>(partitionData.size())) {
          partitionData.resize(n);
        }
      } else if (rankType == RankType::BOTTOM_N) {
        if (n > 0 && n < static_cast<int>(partitionData.size())) {
          std::reverse(partitionData.begin(), partitionData.end());
          partitionData.resize(n);
          std::reverse(partitionData.begin(), partitionData.end());
        }
      } else {
        int rank = 1;
        for (auto& row : partitionData) {
          row["_rank"] = rank++;
        }
      }
      
      result.insert(result.end(), partitionData.begin(), partitionData.end());
    }
    
    return result;
  }
}

bool RankTransformation::compareForRanking(
  const json& row1,
  const json& row2,
  const std::string& orderColumn
) {
  json val1 = row1.value(orderColumn, json(nullptr));
  json val2 = row2.value(orderColumn, json(nullptr));
  
  if (val1.is_null() && val2.is_null()) return false;
  if (val1.is_null()) return true;
  if (val2.is_null()) return false;
  
  if (val1.is_number() && val2.is_number()) {
    return val1.get<double>() < val2.get<double>();
  }
  
  if (val1.is_string() && val2.is_string()) {
    return val1.get<std::string>() < val2.get<std::string>();
  }
  
  return val1.dump() < val2.dump();
}

RankTransformation::RankType RankTransformation::parseRankType(const std::string& typeStr) {
  std::string lower = typeStr;
  std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);
  
  if (lower == "top_n") return RankType::TOP_N;
  if (lower == "bottom_n") return RankType::BOTTOM_N;
  if (lower == "rank") return RankType::RANK;
  if (lower == "dense_rank") return RankType::DENSE_RANK;
  if (lower == "row_number") return RankType::ROW_NUMBER;
  
  return RankType::TOP_N; // default
}
