#include "transformations/deduplication_transformation.h"
#include "core/logger.h"
#include <algorithm>
#include <cmath>
#include <limits>

DeduplicationTransformation::DeduplicationTransformation() = default;

bool DeduplicationTransformation::validateConfig(const json& config) const {
  if (!config.contains("key_columns") || !config["key_columns"].is_array()) {
    Logger::error(LogCategory::TRANSFER, "DeduplicationTransformation",
                  "Missing or invalid key_columns in config");
    return false;
  }
  
  auto keyColumns = config["key_columns"];
  if (keyColumns.empty()) {
    Logger::error(LogCategory::TRANSFER, "DeduplicationTransformation",
                  "key_columns array cannot be empty");
    return false;
  }
  
  if (!config.contains("method") || !config["method"].is_string()) {
    Logger::error(LogCategory::TRANSFER, "DeduplicationTransformation",
                  "Missing or invalid method in config");
    return false;
  }
  
  std::string method = config["method"];
  std::vector<std::string> validMethods = {
    "exact", "fuzzy", "similarity"
  };
  
  if (std::find(validMethods.begin(), validMethods.end(), method) == 
      validMethods.end()) {
    Logger::error(LogCategory::TRANSFER, "DeduplicationTransformation",
                  "Invalid method: " + method);
    return false;
  }
  
  return true;
}

std::vector<json> DeduplicationTransformation::execute(
  const std::vector<json>& inputData,
  const json& config
) {
  if (inputData.empty()) {
    return inputData;
  }
  
  if (!validateConfig(config)) {
    Logger::error(LogCategory::TRANSFER, "DeduplicationTransformation",
                  "Invalid config, returning input data unchanged");
    return inputData;
  }
  
  std::vector<std::string> keyColumns;
  for (const auto& col : config["key_columns"]) {
    keyColumns.push_back(col.get<std::string>());
  }
  
  std::string method = config["method"];
  double similarityThreshold = config.value("similarity_threshold", 0.8);
  std::string keepStrategy = config.value("keep_strategy", "first");
  
  std::vector<json> result;
  
  if (method == "exact") {
    // Exact matching using hash map
    std::set<std::string> seenKeys;
    
    for (const auto& row : inputData) {
      std::string key = generateKey(row, keyColumns);
      
      if (seenKeys.find(key) == seenKeys.end()) {
        seenKeys.insert(key);
        result.push_back(row);
      }
    }
  } else if (method == "fuzzy" || method == "similarity") {
    // Fuzzy matching - compare each row with all previous rows
    result.push_back(inputData[0]);
    
    for (size_t i = 1; i < inputData.size(); ++i) {
      bool isDuplicate = false;
      
      for (const auto& existingRow : result) {
        if (areDuplicates(inputData[i], existingRow, keyColumns, similarityThreshold)) {
          isDuplicate = true;
          break;
        }
      }
      
      if (!isDuplicate) {
        result.push_back(inputData[i]);
      }
    }
  }
  
  Logger::info(LogCategory::TRANSFER, "DeduplicationTransformation",
               "Removed " + std::to_string(inputData.size() - result.size()) + 
               " duplicates from " + std::to_string(inputData.size()) + " rows");
  
  return result;
}

int DeduplicationTransformation::levenshteinDistance(
  const std::string& s1,
  const std::string& s2
) {
  size_t m = s1.length();
  size_t n = s2.length();
  
  if (m == 0) return static_cast<int>(n);
  if (n == 0) return static_cast<int>(m);
  
  std::vector<std::vector<int>> dp(m + 1, std::vector<int>(n + 1));
  
  for (size_t i = 0; i <= m; ++i) {
    dp[i][0] = static_cast<int>(i);
  }
  
  for (size_t j = 0; j <= n; ++j) {
    dp[0][j] = static_cast<int>(j);
  }
  
  for (size_t i = 1; i <= m; ++i) {
    for (size_t j = 1; j <= n; ++j) {
      int cost = (s1[i - 1] == s2[j - 1]) ? 0 : 1;
      dp[i][j] = std::min({
        dp[i - 1][j] + 1,      // deletion
        dp[i][j - 1] + 1,      // insertion
        dp[i - 1][j - 1] + cost // substitution
      });
    }
  }
  
  return dp[m][n];
}

double DeduplicationTransformation::calculateSimilarity(
  const std::string& s1,
  const std::string& s2
) {
  if (s1.empty() && s2.empty()) return 1.0;
  if (s1.empty() || s2.empty()) return 0.0;
  
  int distance = levenshteinDistance(s1, s2);
  int maxLen = std::max(s1.length(), s2.length());
  
  return 1.0 - (static_cast<double>(distance) / static_cast<double>(maxLen));
}

bool DeduplicationTransformation::areDuplicates(
  const json& row1,
  const json& row2,
  const std::vector<std::string>& keyColumns,
  double similarityThreshold
) {
  for (const auto& col : keyColumns) {
    if (!row1.contains(col) || !row2.contains(col)) {
      return false;
    }
    
    std::string val1 = normalizeString(
      row1[col].is_string() ? row1[col].get<std::string>() : row1[col].dump()
    );
    std::string val2 = normalizeString(
      row2[col].is_string() ? row2[col].get<std::string>() : row2[col].dump()
    );
    
    double similarity = calculateSimilarity(val1, val2);
    
    if (similarity < similarityThreshold) {
      return false;
    }
  }
  
  return true;
}

std::string DeduplicationTransformation::generateKey(
  const json& row,
  const std::vector<std::string>& columns
) {
  std::ostringstream keyStream;
  
  for (size_t i = 0; i < columns.size(); ++i) {
    if (i > 0) keyStream << "|||";
    
    if (row.contains(columns[i])) {
      std::string value = row[columns[i]].is_string() ? 
                          row[columns[i]].get<std::string>() : 
                          row[columns[i]].dump();
      keyStream << normalizeString(value);
    } else {
      keyStream << "NULL";
    }
  }
  
  return keyStream.str();
}

std::string DeduplicationTransformation::normalizeString(const std::string& str) {
  std::string result = str;
  
  // Convert to lowercase
  std::transform(result.begin(), result.end(), result.begin(), ::tolower);
  
  // Remove whitespace
  result.erase(std::remove_if(result.begin(), result.end(), ::isspace), result.end());
  
  // Remove special characters (keep alphanumeric only)
  result.erase(std::remove_if(result.begin(), result.end(), 
    [](char c) { return !std::isalnum(c); }), result.end());
  
  return result;
}
