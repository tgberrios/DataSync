#include "transformations/lookup_transformation.h"
#include "core/logger.h"
#include <pqxx/pqxx>
#include <algorithm>
#include <sstream>
#include <map>

LookupTransformation::LookupTransformation() = default;

bool LookupTransformation::validateConfig(const json& config) const {
  if (!config.contains("lookup_table") || !config["lookup_table"].is_string()) {
    Logger::error(LogCategory::TRANSFER, "LookupTransformation",
                  "Missing or invalid lookup_table in config");
    return false;
  }
  
  if (!config.contains("source_columns") || !config["source_columns"].is_array()) {
    Logger::error(LogCategory::TRANSFER, "LookupTransformation",
                  "Missing or invalid source_columns in config");
    return false;
  }
  
  if (!config.contains("lookup_columns") || !config["lookup_columns"].is_array()) {
    Logger::error(LogCategory::TRANSFER, "LookupTransformation",
                  "Missing or invalid lookup_columns in config");
    return false;
  }
  
  if (!config.contains("return_columns") || !config["return_columns"].is_array()) {
    Logger::error(LogCategory::TRANSFER, "LookupTransformation",
                  "Missing or invalid return_columns in config");
    return false;
  }
  
  auto sourceCols = config["source_columns"];
  auto lookupCols = config["lookup_columns"];
  
  if (sourceCols.size() != lookupCols.size()) {
    Logger::error(LogCategory::TRANSFER, "LookupTransformation",
                  "source_columns and lookup_columns must have same size");
    return false;
  }
  
  return true;
}

std::vector<json> LookupTransformation::execute(
  const std::vector<json>& inputData,
  const json& config
) {
  if (inputData.empty()) {
    return inputData;
  }
  
  if (!validateConfig(config)) {
    Logger::error(LogCategory::TRANSFER, "LookupTransformation",
                  "Invalid config, returning input data unchanged");
    return inputData;
  }
  
  std::string lookupTable = config["lookup_table"];
  std::string lookupSchema = config.value("lookup_schema", "");
  std::string connectionString = config.value("connection_string", "");
  std::string dbEngine = config.value("db_engine", "PostgreSQL");
  
  std::vector<std::string> sourceColumns;
  for (const auto& col : config["source_columns"]) {
    sourceColumns.push_back(col.get<std::string>());
  }
  
  std::vector<std::string> lookupColumns;
  for (const auto& col : config["lookup_columns"]) {
    lookupColumns.push_back(col.get<std::string>());
  }
  
  std::vector<std::string> returnColumns;
  for (const auto& col : config["return_columns"]) {
    returnColumns.push_back(col.get<std::string>());
  }
  
  // Load lookup table data
  std::vector<json> lookupData = loadLookupTable(
    connectionString,
    dbEngine,
    lookupSchema,
    lookupTable,
    lookupColumns,
    returnColumns
  );
  
  if (lookupData.empty()) {
    Logger::warning(LogCategory::TRANSFER, "LookupTransformation",
                    "Lookup table is empty, returning input data unchanged");
    return inputData;
  }
  
  // Perform lookup
  return performLookup(
    inputData,
    lookupData,
    sourceColumns,
    lookupColumns,
    returnColumns
  );
}

std::vector<json> LookupTransformation::loadLookupTable(
  const std::string& connectionString,
  const std::string& dbEngine,
  const std::string& schema,
  const std::string& table,
  const std::vector<std::string>& lookupColumns,
  const std::vector<std::string>& returnColumns
) {
  // Create cache key
  std::string cacheKey = connectionString + ":" + dbEngine + ":" + 
                         schema + ":" + table;
  
  // Check cache
  auto it = lookupCache_.find(cacheKey);
  if (it != lookupCache_.end()) {
    Logger::info(LogCategory::TRANSFER, "LookupTransformation",
                 "Using cached lookup table: " + cacheKey);
    return it->second;
  }
  
  // Build SELECT query
  std::vector<std::string> allColumns = lookupColumns;
  allColumns.insert(allColumns.end(), returnColumns.begin(), returnColumns.end());
  
  std::ostringstream query;
  query << "SELECT ";
  for (size_t i = 0; i < allColumns.size(); ++i) {
    if (i > 0) query << ", ";
    query << "\"" << allColumns[i] << "\"";
  }
  query << " FROM ";
  if (!schema.empty()) {
    query << "\"" << schema << "\".";
  }
  query << "\"" << table << "\"";
  
  std::string queryStr = query.str();
  Logger::info(LogCategory::TRANSFER, "LookupTransformation",
               "Loading lookup table with query: " + queryStr);
  
  std::vector<json> lookupData;
  
  try {
    if (dbEngine == "PostgreSQL") {
      pqxx::connection conn(connectionString);
      pqxx::work txn(conn);
      auto result = txn.exec(queryStr);
      
      for (const auto& row : result) {
        json record;
        for (size_t i = 0; i < allColumns.size(); ++i) {
          if (!row[i].is_null()) {
            record[allColumns[i]] = row[i].as<std::string>();
          } else {
            record[allColumns[i]] = nullptr;
          }
        }
        lookupData.push_back(record);
      }
    } else if (dbEngine == "MariaDB") {
      // Use MariaDB engine to load data
      // Similar implementation for other engines
      Logger::warning(LogCategory::TRANSFER, "LookupTransformation",
                      "MariaDB lookup not fully implemented yet");
    } else {
      Logger::warning(LogCategory::TRANSFER, "LookupTransformation",
                      "DB engine " + dbEngine + " not fully supported for lookup");
    }
  } catch (const std::exception& e) {
    Logger::error(LogCategory::TRANSFER, "LookupTransformation",
                  "Error loading lookup table: " + std::string(e.what()));
    return {};
  }
  
  // Cache the result
  lookupCache_[cacheKey] = lookupData;
  
  Logger::info(LogCategory::TRANSFER, "LookupTransformation",
               "Loaded " + std::to_string(lookupData.size()) + 
               " rows from lookup table");
  
  return lookupData;
}

std::vector<json> LookupTransformation::performLookup(
  const std::vector<json>& inputData,
  const std::vector<json>& lookupData,
  const std::vector<std::string>& sourceColumns,
  const std::vector<std::string>& lookupColumns,
  const std::vector<std::string>& returnColumns
) {
  // Create a map for fast lookup
  // Key: concatenated lookup column values, Value: return columns
  std::map<std::string, json> lookupMap;
  
  for (const auto& lookupRow : lookupData) {
    std::ostringstream keyStream;
    for (size_t i = 0; i < lookupColumns.size(); ++i) {
      if (i > 0) keyStream << "|||";
      if (lookupRow.contains(lookupColumns[i])) {
        keyStream << lookupRow[lookupColumns[i]].dump();
      }
    }
    std::string key = keyStream.str();
    
    json returnData;
    for (const auto& returnCol : returnColumns) {
      if (lookupRow.contains(returnCol)) {
        returnData[returnCol] = lookupRow[returnCol];
      }
    }
    
    lookupMap[key] = returnData;
  }
  
  // Perform lookup on input data
  std::vector<json> result;
  result.reserve(inputData.size());
  
  for (const auto& inputRow : inputData) {
    json outputRow = inputRow;
    
    // Build lookup key from source columns
    std::ostringstream keyStream;
    for (size_t i = 0; i < sourceColumns.size(); ++i) {
      if (i > 0) keyStream << "|||";
      if (inputRow.contains(sourceColumns[i])) {
        keyStream << inputRow[sourceColumns[i]].dump();
      }
    }
    std::string key = keyStream.str();
    
    // Find matching lookup row
    auto it = lookupMap.find(key);
    if (it != lookupMap.end()) {
      // Merge return columns into output row
      for (const auto& returnCol : returnColumns) {
        if (it->second.contains(returnCol)) {
          outputRow[returnCol] = it->second[returnCol];
        }
      }
    } else {
      // No match found - set return columns to null
      for (const auto& returnCol : returnColumns) {
        outputRow[returnCol] = nullptr;
      }
    }
    
    result.push_back(outputRow);
  }
  
  Logger::info(LogCategory::TRANSFER, "LookupTransformation",
               "Performed lookup on " + std::to_string(inputData.size()) + 
               " rows, found " + std::to_string(result.size()) + " matches");
  
  return result;
}

std::string LookupTransformation::generateLookupSQL(
  const std::string& sourceQuery,
  const std::string& lookupSchema,
  const std::string& lookupTable,
  const std::vector<std::string>& sourceColumns,
  const std::vector<std::string>& lookupColumns,
  const std::vector<std::string>& returnColumns
) {
  // Generate SQL with LEFT JOIN for lookup
  // This is an alternative approach that can be more efficient for large datasets
  std::ostringstream sql;
  
  sql << "SELECT s.*";
  for (const auto& returnCol : returnColumns) {
    sql << ", l.\"" << returnCol << "\" AS \"lookup_" << returnCol << "\"";
  }
  sql << " FROM (" << sourceQuery << ") AS s";
  sql << " LEFT JOIN ";
  if (!lookupSchema.empty()) {
    sql << "\"" << lookupSchema << "\".";
  }
  sql << "\"" << lookupTable << "\" AS l ON ";
  
  for (size_t i = 0; i < sourceColumns.size(); ++i) {
    if (i > 0) sql << " AND ";
    sql << "s.\"" << sourceColumns[i] << " = l.\"" << lookupColumns[i] << "\"";
  }
  
  return sql.str();
}
