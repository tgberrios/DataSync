#include "transformations/expression_transformation.h"
#include "core/logger.h"
#include <algorithm>
#include <sstream>
#include <cmath>
#include <regex>
#include <ctime>
#include <iomanip>

ExpressionTransformation::ExpressionTransformation() = default;

bool ExpressionTransformation::validateConfig(const json& config) const {
  if (!config.contains("expressions") || !config["expressions"].is_array()) {
    Logger::error(LogCategory::TRANSFER, "ExpressionTransformation",
                  "Missing or invalid expressions in config");
    return false;
  }
  
  auto expressions = config["expressions"];
  if (expressions.empty()) {
    Logger::error(LogCategory::TRANSFER, "ExpressionTransformation",
                  "expressions array cannot be empty");
    return false;
  }
  
  for (const auto& expr : expressions) {
    if (!expr.is_object()) {
      Logger::error(LogCategory::TRANSFER, "ExpressionTransformation",
                    "Expression must be an object");
      return false;
    }
    
    if (!expr.contains("target_column") || !expr["target_column"].is_string()) {
      Logger::error(LogCategory::TRANSFER, "ExpressionTransformation",
                    "Missing or invalid target_column in expression");
      return false;
    }
    
    if (!expr.contains("expression") || !expr["expression"].is_string()) {
      Logger::error(LogCategory::TRANSFER, "ExpressionTransformation",
                    "Missing or invalid expression");
      return false;
    }
  }
  
  return true;
}

std::vector<json> ExpressionTransformation::execute(
  const std::vector<json>& inputData,
  const json& config
) {
  if (inputData.empty()) {
    return inputData;
  }
  
  if (!validateConfig(config)) {
    Logger::error(LogCategory::TRANSFER, "ExpressionTransformation",
                  "Invalid config, returning input data unchanged");
    return inputData;
  }
  
  std::vector<json> result;
  result.reserve(inputData.size());
  
  for (const auto& row : inputData) {
    json outputRow = row;
    
    // Evaluate each expression
    for (const auto& expr : config["expressions"]) {
      std::string targetColumn = expr["target_column"];
      json value = evaluateExpression(row, expr);
      outputRow[targetColumn] = value;
    }
    
    result.push_back(outputRow);
  }
  
  Logger::info(LogCategory::TRANSFER, "ExpressionTransformation",
               "Evaluated expressions on " + std::to_string(inputData.size()) + " rows");
  
  return result;
}

json ExpressionTransformation::evaluateExpression(
  const json& row,
  const json& expressionConfig
) {
  std::string expression = expressionConfig["expression"];
  std::string type = expressionConfig.value("type", "auto");
  
  // Try to determine type from expression
  if (type == "auto") {
    // Check if it's a math expression (contains operators)
    if (expression.find('+') != std::string::npos ||
        expression.find('-') != std::string::npos ||
        expression.find('*') != std::string::npos ||
        expression.find('/') != std::string::npos ||
        expression.find("SUM") != std::string::npos ||
        expression.find("AVG") != std::string::npos) {
      type = "math";
    } else if (expression.find("UPPER") != std::string::npos ||
               expression.find("LOWER") != std::string::npos ||
               expression.find("TRIM") != std::string::npos ||
               expression.find("CONCAT") != std::string::npos) {
      type = "string";
    } else if (expression.find("DATEADD") != std::string::npos ||
               expression.find("DATEDIFF") != std::string::npos ||
               expression.find("DATEPART") != std::string::npos) {
      type = "date";
    } else {
      type = "string";
    }
  }
  
  if (type == "math") {
    double result = evaluateMathExpression(expression, row);
    return result;
  } else if (type == "string") {
    std::string result = evaluateStringExpression(expression, row);
    return result;
  } else if (type == "date") {
    std::string result = evaluateDateExpression(expression, row);
    return result;
  }
  
  // Default: try to evaluate as string
  return evaluateStringExpression(expression, row);
}

double ExpressionTransformation::evaluateMathExpression(
  const std::string& expression,
  const json& row
) {
  // Simple math expression evaluator
  // Supports: column references, +, -, *, /, parentheses, basic functions
  
  std::string expr = replaceColumnReferences(expression, row);
  
  // Extract variables from expression and create map
  std::map<std::string, double> variables;
  
  // Try to parse as simple arithmetic
  try {
    // Very simple evaluator - replace column names with values
    std::regex colRegex(R"(\{(\w+)\})");
    std::smatch matches;
    std::string evalExpr = expression;
    
    while (std::regex_search(evalExpr, matches, colRegex)) {
      std::string colName = matches[1].str();
      json colValue = getColumnValue(row, colName);
      
      double numValue = 0.0;
      if (colValue.is_number()) {
        numValue = colValue.get<double>();
      } else if (colValue.is_string()) {
        try {
          numValue = std::stod(colValue.get<std::string>());
        } catch (...) {
          numValue = 0.0;
        }
      }
      
      evalExpr.replace(matches.position(), matches.length(), std::to_string(numValue));
    }
    
    // Evaluate simple expressions (this is a simplified version)
    // For production, use a proper expression parser library
    return evaluateSimpleMath(evalExpr, variables);
  } catch (...) {
    Logger::warning(LogCategory::TRANSFER, "ExpressionTransformation",
                    "Error evaluating math expression: " + expression);
    return 0.0;
  }
}

std::string ExpressionTransformation::evaluateStringExpression(
  const std::string& expression,
  const json& row
) {
  std::string result = expression;
  
  // Handle UPPER(column)
  std::regex upperRegex(R"(UPPER\((\w+)\))", std::regex_constants::icase);
  std::smatch matches;
  if (std::regex_search(result, matches, upperRegex)) {
    std::string colName = matches[1].str();
    json colValue = getColumnValue(row, colName);
    std::string strValue = colValue.is_string() ? colValue.get<std::string>() : colValue.dump();
    result = std::regex_replace(result, upperRegex, upperCase(strValue));
  }
  
  // Handle LOWER(column)
  std::regex lowerRegex(R"(LOWER\((\w+)\))", std::regex_constants::icase);
  if (std::regex_search(result, matches, lowerRegex)) {
    std::string colName = matches[1].str();
    json colValue = getColumnValue(row, colName);
    std::string strValue = colValue.is_string() ? colValue.get<std::string>() : colValue.dump();
    result = std::regex_replace(result, lowerRegex, lowerCase(strValue));
  }
  
  // Handle TRIM(column)
  std::regex trimRegex(R"(TRIM\((\w+)\))", std::regex_constants::icase);
  if (std::regex_search(result, matches, trimRegex)) {
    std::string colName = matches[1].str();
    json colValue = getColumnValue(row, colName);
    std::string strValue = colValue.is_string() ? colValue.get<std::string>() : colValue.dump();
    result = std::regex_replace(result, trimRegex, trim(strValue));
  }
  
  // Handle CONCAT(column1, column2, ...)
  std::regex concatRegex(R"(CONCAT\(([^)]+)\))", std::regex_constants::icase);
  if (std::regex_search(result, matches, concatRegex)) {
    std::string args = matches[1].str();
    std::vector<std::string> parts;
    std::istringstream iss(args);
    std::string part;
    while (std::getline(iss, part, ',')) {
      part.erase(0, part.find_first_not_of(" \t"));
      part.erase(part.find_last_not_of(" \t") + 1);
      json colValue = getColumnValue(row, part);
      parts.push_back(colValue.is_string() ? colValue.get<std::string>() : colValue.dump());
    }
    result = std::regex_replace(result, concatRegex, concat(parts, ""));
  }
  
  // Handle REGEX_REPLACE(column, pattern, replacement)
  std::regex regexReplaceRegex(R"(REGEX_REPLACE\((\w+),\s*'([^']+)',\s*'([^']+)'\))", std::regex_constants::icase);
  if (std::regex_search(result, matches, regexReplaceRegex)) {
    std::string colName = matches[1].str();
    std::string pattern = matches[2].str();
    std::string replacement = matches[3].str();
    json colValue = getColumnValue(row, colName);
    std::string strValue = colValue.is_string() ? colValue.get<std::string>() : colValue.dump();
    try {
      std::regex regexPattern(pattern);
      strValue = std::regex_replace(strValue, regexPattern, replacement);
    } catch (...) {
      Logger::warning(LogCategory::TRANSFER, "ExpressionTransformation",
                      "Invalid regex pattern: " + pattern);
    }
    result = std::regex_replace(result, regexReplaceRegex, strValue);
  }
  
  // Handle SPLIT(column, delimiter)
  std::regex splitRegex(R"(SPLIT\((\w+),\s*'([^']+)'\))", std::regex_constants::icase);
  if (std::regex_search(result, matches, splitRegex)) {
    std::string colName = matches[1].str();
    std::string delimiter = matches[2].str();
    json colValue = getColumnValue(row, colName);
    std::string strValue = colValue.is_string() ? colValue.get<std::string>() : colValue.dump();
    std::vector<std::string> parts;
    std::istringstream iss(strValue);
    std::string part;
    while (std::getline(iss, part, delimiter[0])) {
      parts.push_back(part);
    }
    result = std::regex_replace(result, splitRegex, concat(parts, ","));
  }
  
  // Replace column references {column_name}
  result = replaceColumnReferences(result, row);
  
  return result;
}

std::string ExpressionTransformation::evaluateDateExpression(
  const std::string& expression,
  const json& row
) {
  std::string result = expression;
  
  // Handle DATEADD(date_column, days)
  std::regex dateAddRegex(R"(DATEADD\((\w+),\s*(-?\d+)\))", std::regex_constants::icase);
  std::smatch matches;
  if (std::regex_search(result, matches, dateAddRegex)) {
    std::string colName = matches[1].str();
    int days = std::stoi(matches[2].str());
    json colValue = getColumnValue(row, colName);
    std::string dateStr = colValue.is_string() ? colValue.get<std::string>() : colValue.dump();
    std::string newDate = dateAdd(dateStr, days);
    result = std::regex_replace(result, dateAddRegex, newDate);
  }
  
  // Handle DATEDIFF(date1, date2)
  std::regex dateDiffRegex(R"(DATEDIFF\((\w+),\s*(\w+)\))", std::regex_constants::icase);
  if (std::regex_search(result, matches, dateDiffRegex)) {
    std::string col1 = matches[1].str();
    std::string col2 = matches[2].str();
    json val1 = getColumnValue(row, col1);
    json val2 = getColumnValue(row, col2);
    std::string date1 = val1.is_string() ? val1.get<std::string>() : val1.dump();
    std::string date2 = val2.is_string() ? val2.get<std::string>() : val2.dump();
    int diff = dateDiff(date1, date2);
    result = std::regex_replace(result, dateDiffRegex, std::to_string(diff));
  }
  
  // Handle DATEPART(date_column, part)
  std::regex datePartRegex(R"(DATEPART\((\w+),\s*'(\w+)'\))", std::regex_constants::icase);
  if (std::regex_search(result, matches, datePartRegex)) {
    std::string colName = matches[1].str();
    std::string part = matches[2].str();
    json colValue = getColumnValue(row, colName);
    std::string dateStr = colValue.is_string() ? colValue.get<std::string>() : colValue.dump();
    int partValue = datePart(dateStr, part);
    result = std::regex_replace(result, datePartRegex, std::to_string(partValue));
  }
  
  return result;
}

json ExpressionTransformation::getColumnValue(const json& row, const std::string& columnPath) {
  if (row.contains(columnPath)) {
    return row[columnPath];
  }
  return json(nullptr);
}

std::string ExpressionTransformation::replaceColumnReferences(
  const std::string& expression,
  const json& row
) {
  std::string result = expression;
  std::regex colRegex(R"(\{(\w+)\})");
  std::smatch matches;
  
  while (std::regex_search(result, matches, colRegex)) {
    std::string colName = matches[1].str();
    json colValue = getColumnValue(row, colName);
    
    std::string replacement;
    if (colValue.is_string()) {
      replacement = colValue.get<std::string>();
    } else if (colValue.is_number()) {
      replacement = std::to_string(colValue.get<double>());
    } else {
      replacement = colValue.dump();
    }
    
    result.replace(matches.position(), matches.length(), replacement);
  }
  
  return result;
}

double ExpressionTransformation::evaluateSimpleMath(
  const std::string& expr,
  const std::map<std::string, double>& variables
) {
  // Very simple math evaluator - handles basic arithmetic
  // For production, use a proper expression parser
  
  try {
    // Try to evaluate as a simple arithmetic expression
    // This is a placeholder - would need a proper parser
    std::istringstream iss(expr);
    double result = 0.0;
    char op = '+';
    double num;
    
    while (iss >> num) {
      if (op == '+') result += num;
      else if (op == '-') result -= num;
      else if (op == '*') result *= num;
      else if (op == '/') result /= num;
      iss >> op;
    }
    
    return result;
  } catch (...) {
    return 0.0;
  }
}

std::string ExpressionTransformation::upperCase(const std::string& str) {
  std::string result = str;
  std::transform(result.begin(), result.end(), result.begin(), ::toupper);
  return result;
}

std::string ExpressionTransformation::lowerCase(const std::string& str) {
  std::string result = str;
  std::transform(result.begin(), result.end(), result.begin(), ::tolower);
  return result;
}

std::string ExpressionTransformation::trim(const std::string& str) {
  size_t first = str.find_first_not_of(" \t\n\r");
  if (first == std::string::npos) return "";
  size_t last = str.find_last_not_of(" \t\n\r");
  return str.substr(first, (last - first + 1));
}

std::string ExpressionTransformation::substring(
  const std::string& str,
  int start,
  int length
) {
  if (start < 0 || start >= static_cast<int>(str.length())) return "";
  if (length < 0) length = str.length() - start;
  return str.substr(start, length);
}

std::string ExpressionTransformation::concat(
  const std::vector<std::string>& strings,
  const std::string& separator
) {
  std::ostringstream oss;
  for (size_t i = 0; i < strings.size(); ++i) {
    if (i > 0) oss << separator;
    oss << strings[i];
  }
  return oss.str();
}

std::string ExpressionTransformation::dateAdd(const std::string& dateStr, int days) {
  try {
    std::tm tm = {};
    std::istringstream ss(dateStr);
    ss >> std::get_time(&tm, "%Y-%m-%d");
    if (ss.fail()) {
      ss.clear();
      ss.str(dateStr);
      ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
    }
    if (!ss.fail()) {
      std::time_t time = std::mktime(&tm);
      time += days * 24 * 60 * 60;
      std::tm* newTm = std::localtime(&time);
      std::ostringstream oss;
      oss << std::put_time(newTm, "%Y-%m-%d");
      return oss.str();
    }
  } catch (...) {
    Logger::warning(LogCategory::TRANSFER, "ExpressionTransformation",
                    "Error parsing date: " + dateStr);
  }
  return dateStr;
}

int ExpressionTransformation::dateDiff(const std::string& date1, const std::string& date2) {
  try {
    std::tm tm1 = {}, tm2 = {};
    std::istringstream ss1(date1), ss2(date2);
    ss1 >> std::get_time(&tm1, "%Y-%m-%d");
    ss2 >> std::get_time(&tm2, "%Y-%m-%d");
    if (ss1.fail() || ss2.fail()) {
      ss1.clear();
      ss2.clear();
      ss1.str(date1);
      ss2.str(date2);
      ss1 >> std::get_time(&tm1, "%Y-%m-%d %H:%M:%S");
      ss2 >> std::get_time(&tm2, "%Y-%m-%d %H:%M:%S");
    }
    if (!ss1.fail() && !ss2.fail()) {
      std::time_t time1 = std::mktime(&tm1);
      std::time_t time2 = std::mktime(&tm2);
      return static_cast<int>(std::difftime(time2, time1) / (24 * 60 * 60));
    }
  } catch (...) {
    Logger::warning(LogCategory::TRANSFER, "ExpressionTransformation",
                    "Error calculating date difference");
  }
  return 0;
}

int ExpressionTransformation::datePart(const std::string& dateStr, const std::string& part) {
  try {
    std::tm tm = {};
    std::istringstream ss(dateStr);
    ss >> std::get_time(&tm, "%Y-%m-%d");
    if (ss.fail()) {
      ss.clear();
      ss.str(dateStr);
      ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
    }
    if (!ss.fail()) {
      std::string lowerPart = part;
      std::transform(lowerPart.begin(), lowerPart.end(), lowerPart.begin(), ::tolower);
      if (lowerPart == "year") return tm.tm_year + 1900;
      if (lowerPart == "month") return tm.tm_mon + 1;
      if (lowerPart == "day") return tm.tm_mday;
      if (lowerPart == "hour") return tm.tm_hour;
      if (lowerPart == "minute") return tm.tm_min;
      if (lowerPart == "second") return tm.tm_sec;
      if (lowerPart == "weekday" || lowerPart == "dow") return tm.tm_wday;
    }
  } catch (...) {
    Logger::warning(LogCategory::TRANSFER, "ExpressionTransformation",
                    "Error extracting date part from: " + dateStr);
  }
  return 0;
}
