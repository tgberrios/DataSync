#include "transformations/router_transformation.h"
#include "core/logger.h"
#include <algorithm>
#include <sstream>

RouterTransformation::RouterTransformation() = default;

bool RouterTransformation::validateConfig(const json& config) const {
  if (!config.contains("routes") || !config["routes"].is_array()) {
    Logger::error(LogCategory::TRANSFER, "RouterTransformation",
                  "Missing or invalid routes in config");
    return false;
  }
  
  auto routes = config["routes"];
  if (routes.empty()) {
    Logger::error(LogCategory::TRANSFER, "RouterTransformation",
                  "routes array cannot be empty");
    return false;
  }
  
  for (const auto& route : routes) {
    if (!route.contains("name") || !route["name"].is_string()) {
      Logger::error(LogCategory::TRANSFER, "RouterTransformation",
                    "Missing or invalid route name");
      return false;
    }
    
    if (!route.contains("condition") || !route["condition"].is_object()) {
      Logger::error(LogCategory::TRANSFER, "RouterTransformation",
                    "Missing or invalid route condition");
      return false;
    }
    
    auto condition = route["condition"];
    if (!condition.contains("column") || !condition["column"].is_string()) {
      Logger::error(LogCategory::TRANSFER, "RouterTransformation",
                    "Missing or invalid condition column");
      return false;
    }
    
    if (!condition.contains("op") || !condition["op"].is_string()) {
      Logger::error(LogCategory::TRANSFER, "RouterTransformation",
                    "Missing or invalid condition operator");
      return false;
    }
    
    std::string op = condition["op"];
    std::vector<std::string> validOps = {
      "=", "!=", ">", "<", ">=", "<=", "LIKE", "IN", "NOT IN", "IS NULL", "IS NOT NULL"
    };
    
    if (std::find(validOps.begin(), validOps.end(), op) == validOps.end()) {
      Logger::error(LogCategory::TRANSFER, "RouterTransformation",
                    "Invalid condition operator: " + op);
      return false;
    }
  }
  
  return true;
}

std::vector<json> RouterTransformation::execute(
  const std::vector<json>& inputData,
  const json& config
) {
  if (inputData.empty()) {
    return inputData;
  }
  
  if (!validateConfig(config)) {
    Logger::error(LogCategory::TRANSFER, "RouterTransformation",
                  "Invalid config, returning input data unchanged");
    return inputData;
  }
  
  // Get default route name (if specified)
  std::string defaultRoute = config.value("default_route", "");
  
  // Group data by routes
  std::map<std::string, std::vector<json>> routeGroups;
  
  for (const auto& row : inputData) {
    bool matched = false;
    
    for (const auto& route : config["routes"]) {
      std::string routeName = route["name"];
      json condition = route["condition"];
      
      if (evaluateCondition(row, condition)) {
        routeGroups[routeName].push_back(row);
        matched = true;
        break; // First matching route wins
      }
    }
    
    if (!matched && !defaultRoute.empty()) {
      routeGroups[defaultRoute].push_back(row);
    }
  }
  
  // For router, we return all rows but with a route_name column added
  // In a real implementation, this would be handled differently (multiple outputs)
  std::vector<json> result;
  
  for (const auto& [routeName, rows] : routeGroups) {
    for (const auto& row : rows) {
      json routedRow = row;
      routedRow["_route_name"] = routeName;
      result.push_back(routedRow);
    }
  }
  
  Logger::info(LogCategory::TRANSFER, "RouterTransformation",
               "Routed " + std::to_string(inputData.size()) + " rows into " +
               std::to_string(routeGroups.size()) + " routes");
  
  return result;
}

bool RouterTransformation::evaluateCondition(const json& row, const json& condition) {
  std::string column = condition["column"];
  std::string op = condition["op"];
  json value = condition.value("value", json(nullptr));
  
  json columnValue = getValueByPath(row, column);
  
  return evaluateOperator(columnValue, op, value);
}

bool RouterTransformation::evaluateOperator(
  const json& leftValue,
  const std::string& op,
  const json& rightValue
) {
  if (op == "IS NULL") {
    return leftValue.is_null();
  }
  
  if (op == "IS NOT NULL") {
    return !leftValue.is_null();
  }
  
  if (leftValue.is_null() || rightValue.is_null()) {
    return false;
  }
  
  if (op == "=") {
    return leftValue == rightValue;
  }
  
  if (op == "!=") {
    return leftValue != rightValue;
  }
  
  // Numeric comparisons
  if (op == ">" || op == "<" || op == ">=" || op == "<=") {
    double leftNum = 0.0, rightNum = 0.0;
    
    if (leftValue.is_number()) {
      leftNum = leftValue.get<double>();
    } else if (leftValue.is_string()) {
      try {
        leftNum = std::stod(leftValue.get<std::string>());
      } catch (...) {
        return false;
      }
    } else {
      return false;
    }
    
    if (rightValue.is_number()) {
      rightNum = rightValue.get<double>();
    } else if (rightValue.is_string()) {
      try {
        rightNum = std::stod(rightValue.get<std::string>());
      } catch (...) {
        return false;
      }
    } else {
      return false;
    }
    
    if (op == ">") return leftNum > rightNum;
    if (op == "<") return leftNum < rightNum;
    if (op == ">=") return leftNum >= rightNum;
    if (op == "<=") return leftNum <= rightNum;
  }
  
  if (op == "LIKE") {
    if (!leftValue.is_string() || !rightValue.is_string()) {
      return false;
    }
    std::string leftStr = leftValue.get<std::string>();
    std::string rightStr = rightValue.get<std::string>();
    
    // Simple LIKE implementation (convert SQL LIKE to regex)
    std::string pattern = rightStr;
    // Replace % with .* and _ with .
    size_t pos = 0;
    while ((pos = pattern.find("%", pos)) != std::string::npos) {
      pattern.replace(pos, 1, ".*");
      pos += 2;
    }
    pos = 0;
    while ((pos = pattern.find("_", pos)) != std::string::npos) {
      pattern.replace(pos, 1, ".");
      pos += 1;
    }
    
    // Simple substring match for now (full regex would require <regex>)
    return leftStr.find(rightStr) != std::string::npos;
  }
  
  if (op == "IN" || op == "NOT IN") {
    if (!rightValue.is_array()) {
      return false;
    }
    
    bool found = false;
    for (const auto& val : rightValue) {
      if (leftValue == val) {
        found = true;
        break;
      }
    }
    
    return (op == "IN") ? found : !found;
  }
  
  return false;
}

json RouterTransformation::getValueByPath(const json& row, const std::string& columnPath) {
  // Simple implementation - just get by key
  // Could be extended to support nested paths like "user.address.city"
  if (row.contains(columnPath)) {
    return row[columnPath];
  }
  return json(nullptr);
}

std::string RouterTransformation::generateRouterSQL(
  const std::string& sourceQuery,
  const std::vector<json>& routes
) {
  // Router in SQL would typically use CASE statements or UNION ALL
  std::ostringstream sql;
  
  sql << "SELECT *, CASE ";
  for (const auto& route : routes) {
    std::string routeName = route["name"];
    json condition = route["condition"];
    std::string column = condition["column"];
    std::string op = condition["op"];
    json value = condition.value("value", json(nullptr));
    
    sql << "WHEN \"" << column << "\" " << op;
    if (!value.is_null()) {
      if (value.is_string()) {
        sql << " '" << value.get<std::string>() << "'";
      } else {
        sql << " " << value.dump();
      }
    }
    sql << " THEN '" << routeName << "' ";
  }
  sql << "ELSE 'default' END AS route_name FROM (" << sourceQuery << ") AS source";
  
  return sql.str();
}
