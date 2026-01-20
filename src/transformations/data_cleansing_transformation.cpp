#include "transformations/data_cleansing_transformation.h"
#include "core/logger.h"
#include <algorithm>
#include <regex>
#include <cctype>

DataCleansingTransformation::DataCleansingTransformation() = default;

bool DataCleansingTransformation::validateConfig(const json& config) const {
  if (!config.contains("rules") || !config["rules"].is_array()) {
    Logger::error(LogCategory::TRANSFER, "DataCleansingTransformation",
                  "Missing or invalid rules in config");
    return false;
  }
  
  auto rules = config["rules"];
  if (rules.empty()) {
    Logger::error(LogCategory::TRANSFER, "DataCleansingTransformation",
                  "rules array cannot be empty");
    return false;
  }
  
  std::vector<std::string> validOperations = {
    "trim", "uppercase", "lowercase", "remove_special", 
    "remove_whitespace", "remove_leading_zeros", "normalize_whitespace"
  };
  
  for (const auto& rule : rules) {
    if (!rule.is_object()) {
      Logger::error(LogCategory::TRANSFER, "DataCleansingTransformation",
                    "Rule must be an object");
      return false;
    }
    
    if (!rule.contains("column") || !rule["column"].is_string()) {
      Logger::error(LogCategory::TRANSFER, "DataCleansingTransformation",
                    "Missing or invalid column in rule");
      return false;
    }
    
    if (!rule.contains("operations") || !rule["operations"].is_array()) {
      Logger::error(LogCategory::TRANSFER, "DataCleansingTransformation",
                    "Missing or invalid operations in rule");
      return false;
    }
    
    auto operations = rule["operations"];
    for (const auto& op : operations) {
      if (!op.is_string()) {
        Logger::error(LogCategory::TRANSFER, "DataCleansingTransformation",
                      "Operation must be a string");
        return false;
      }
      
      std::string opStr = op.get<std::string>();
      if (std::find(validOperations.begin(), validOperations.end(), opStr) == 
          validOperations.end()) {
        Logger::error(LogCategory::TRANSFER, "DataCleansingTransformation",
                      "Invalid operation: " + opStr);
        return false;
      }
    }
  }
  
  return true;
}

std::vector<json> DataCleansingTransformation::execute(
  const std::vector<json>& inputData,
  const json& config
) {
  if (inputData.empty()) {
    return inputData;
  }
  
  if (!validateConfig(config)) {
    Logger::error(LogCategory::TRANSFER, "DataCleansingTransformation",
                  "Invalid config, returning input data unchanged");
    return inputData;
  }
  
  std::vector<json> result;
  result.reserve(inputData.size());
  
  for (const auto& row : inputData) {
    json outputRow = row;
    
    // Apply cleansing rules
    for (const auto& rule : config["rules"]) {
      std::string column = rule["column"];
      
      if (!outputRow.contains(column)) {
        continue;
      }
      
      std::vector<std::string> operations;
      for (const auto& op : rule["operations"]) {
        operations.push_back(op.get<std::string>());
      }
      
      json cleanedValue = applyCleansing(outputRow[column], operations);
      outputRow[column] = cleanedValue;
    }
    
    result.push_back(outputRow);
  }
  
  Logger::info(LogCategory::TRANSFER, "DataCleansingTransformation",
               "Cleansed " + std::to_string(inputData.size()) + " rows");
  
  return result;
}

json DataCleansingTransformation::applyCleansing(
  const json& value,
  const std::vector<std::string>& operations
) {
  if (value.is_null()) {
    return value;
  }
  
  if (!value.is_string()) {
    // Convert to string if not already
    std::string strValue = value.dump();
    json stringValue = strValue;
    return applyCleansing(stringValue, operations);
  }
  
  std::string str = value.get<std::string>();
  
  for (const auto& op : operations) {
    if (op == "trim") {
      str = trim(str);
    } else if (op == "uppercase") {
      str = upperCase(str);
    } else if (op == "lowercase") {
      str = lowerCase(str);
    } else if (op == "remove_special") {
      str = removeSpecialChars(str);
    } else if (op == "remove_whitespace") {
      str = removeWhitespace(str);
    } else if (op == "remove_leading_zeros") {
      str = removeLeadingZeros(str);
    } else if (op == "normalize_whitespace") {
      str = normalizeWhitespace(str);
    }
  }
  
  return json(str);
}

std::string DataCleansingTransformation::trim(const std::string& str) {
  size_t first = str.find_first_not_of(" \t\n\r");
  if (first == std::string::npos) return "";
  size_t last = str.find_last_not_of(" \t\n\r");
  return str.substr(first, (last - first + 1));
}

std::string DataCleansingTransformation::upperCase(const std::string& str) {
  std::string result = str;
  std::transform(result.begin(), result.end(), result.begin(), ::toupper);
  return result;
}

std::string DataCleansingTransformation::lowerCase(const std::string& str) {
  std::string result = str;
  std::transform(result.begin(), result.end(), result.begin(), ::tolower);
  return result;
}

std::string DataCleansingTransformation::removeSpecialChars(
  const std::string& str,
  const std::string& keepChars
) {
  std::string result;
  result.reserve(str.length());
  
  for (char c : str) {
    if (std::isalnum(c) || std::isspace(c) || 
        keepChars.find(c) != std::string::npos) {
      result += c;
    }
  }
  
  return result;
}

std::string DataCleansingTransformation::removeWhitespace(const std::string& str) {
  std::string result;
  result.reserve(str.length());
  
  for (char c : str) {
    if (!std::isspace(c)) {
      result += c;
    }
  }
  
  return result;
}

std::string DataCleansingTransformation::removeLeadingZeros(const std::string& str) {
  if (str.empty()) return str;
  
  size_t start = 0;
  while (start < str.length() && str[start] == '0') {
    start++;
  }
  
  if (start == str.length()) {
    return "0"; // All zeros
  }
  
  return str.substr(start);
}

std::string DataCleansingTransformation::normalizeWhitespace(const std::string& str) {
  std::string result;
  result.reserve(str.length());
  bool lastWasSpace = false;
  
  for (char c : str) {
    if (std::isspace(c)) {
      if (!lastWasSpace) {
        result += ' ';
        lastWasSpace = true;
      }
    } else {
      result += c;
      lastWasSpace = false;
    }
  }
  
  return trim(result);
}
