#include "transformations/sorter_transformation.h"
#include "core/logger.h"
#include <algorithm>
#include <sstream>

SorterTransformation::SorterTransformation() = default;

bool SorterTransformation::validateConfig(const json& config) const {
  if (!config.contains("sort_columns") || !config["sort_columns"].is_array()) {
    Logger::error(LogCategory::TRANSFER, "SorterTransformation",
                  "Missing or invalid sort_columns in config");
    return false;
  }
  
  auto sortColumns = config["sort_columns"];
  if (sortColumns.empty()) {
    Logger::error(LogCategory::TRANSFER, "SorterTransformation",
                  "sort_columns array cannot be empty");
    return false;
  }
  
  for (const auto& col : sortColumns) {
    if (!col.is_object()) {
      Logger::error(LogCategory::TRANSFER, "SorterTransformation",
                    "Sort column must be an object");
      return false;
    }
    
    if (!col.contains("column") || !col["column"].is_string()) {
      Logger::error(LogCategory::TRANSFER, "SorterTransformation",
                    "Missing or invalid column in sort column");
      return false;
    }
    
    if (col.contains("order") && !col["order"].is_string()) {
      Logger::error(LogCategory::TRANSFER, "SorterTransformation",
                    "order must be a string if provided");
      return false;
    }
  }
  
  return true;
}

std::vector<json> SorterTransformation::execute(
  const std::vector<json>& inputData,
  const json& config
) {
  if (inputData.empty()) {
    return inputData;
  }
  
  if (!validateConfig(config)) {
    Logger::error(LogCategory::TRANSFER, "SorterTransformation",
                  "Invalid config, returning input data unchanged");
    return inputData;
  }
  
  // Parse sort columns
  std::vector<SortColumn> sortColumns;
  for (const auto& col : config["sort_columns"]) {
    SortColumn sortCol;
    sortCol.column = col["column"];
    std::string orderStr = col.value("order", "asc");
    sortCol.order = parseSortOrder(orderStr);
    sortColumns.push_back(sortCol);
  }
  
  // Sort the data
  std::vector<json> result = inputData;
  
  std::sort(result.begin(), result.end(), 
    [&sortColumns, this](const json& a, const json& b) {
      return compareRows(a, b, sortColumns);
    }
  );
  
  Logger::info(LogCategory::TRANSFER, "SorterTransformation",
               "Sorted " + std::to_string(inputData.size()) + " rows");
  
  return result;
}

bool SorterTransformation::compareRows(
  const json& row1,
  const json& row2,
  const std::vector<SortColumn>& sortColumns
) {
  for (const auto& sortCol : sortColumns) {
    json val1 = row1.value(sortCol.column, json(nullptr));
    json val2 = row2.value(sortCol.column, json(nullptr));
    
    int comparison = compareValues(val1, val2);
    
    if (comparison != 0) {
      if (sortCol.order == SortOrder::ASC) {
        return comparison < 0;
      } else {
        return comparison > 0;
      }
    }
  }
  
  return false; // Equal
}

int SorterTransformation::compareValues(const json& val1, const json& val2) {
  // Handle nulls
  if (val1.is_null() && val2.is_null()) return 0;
  if (val1.is_null()) return -1;
  if (val2.is_null()) return 1;
  
  // Compare numbers
  if (val1.is_number() && val2.is_number()) {
    double d1 = val1.get<double>();
    double d2 = val2.get<double>();
    if (d1 < d2) return -1;
    if (d1 > d2) return 1;
    return 0;
  }
  
  // Compare strings
  if (val1.is_string() && val2.is_string()) {
    std::string s1 = val1.get<std::string>();
    std::string s2 = val2.get<std::string>();
    return s1.compare(s2);
  }
  
  // Convert to string and compare
  std::string s1 = val1.dump();
  std::string s2 = val2.dump();
  return s1.compare(s2);
}

SorterTransformation::SortOrder SorterTransformation::parseSortOrder(const std::string& orderStr) {
  std::string lower = orderStr;
  std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);
  
  if (lower == "desc" || lower == "descending") {
    return SortOrder::DESC;
  }
  return SortOrder::ASC; // default
}

std::string SorterTransformation::generateSortSQL(
  const std::string& sourceQuery,
  const std::vector<SortColumn>& sortColumns
) {
  std::ostringstream sql;
  
  sql << "SELECT * FROM (" << sourceQuery << ") AS source ORDER BY ";
  
  for (size_t i = 0; i < sortColumns.size(); ++i) {
    if (i > 0) sql << ", ";
    sql << "\"" << sortColumns[i].column << "\"";
    if (sortColumns[i].order == SortOrder::DESC) {
      sql << " DESC";
    } else {
      sql << " ASC";
    }
  }
  
  return sql.str();
}
