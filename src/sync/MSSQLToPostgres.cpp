#include "sync/MSSQLToPostgres.h"
#include "core/Config.h"
#include "core/database_config.h"
#include "engines/database_engine.h"
#include "engines/mssql_engine.h"
#include "third_party/json.hpp"
#include <algorithm>
#include <cctype>
#include <mutex>

using json = nlohmann::json;

std::unordered_map<std::string, std::string> MSSQLToPostgres::dataTypeMap = {
    {"int", "INTEGER"},
    {"bigint", "BIGINT"},
    {"smallint", "SMALLINT"},
    {"tinyint", "SMALLINT"},
    {"bit", "BOOLEAN"},
    {"decimal", "NUMERIC"},
    {"numeric", "NUMERIC"},
    {"float", "REAL"},
    {"real", "REAL"},
    {"money", "NUMERIC(19,4)"},
    {"smallmoney", "NUMERIC(10,4)"},
    {"varchar", "VARCHAR"},
    {"nvarchar", "VARCHAR"},
    {"char", "TEXT"},
    {"nchar", "TEXT"},
    {"text", "TEXT"},
    {"ntext", "TEXT"},
    {"datetime", "TIMESTAMP"},
    {"datetime2", "TIMESTAMP"},
    {"smalldatetime", "TIMESTAMP"},
    {"date", "DATE"},
    {"time", "TIME"},
    {"datetimeoffset", "TIMESTAMP WITH TIME ZONE"},
    {"uniqueidentifier", "UUID"},
    {"varbinary", "BYTEA"},
    {"image", "BYTEA"},
    {"binary", "BYTEA"},
    {"xml", "TEXT"},
    {"sql_variant", "TEXT"}};

std::unordered_map<std::string, std::string> MSSQLToPostgres::collationMap = {
    {"SQL_Latin1_General_CP1_CI_AS", "en_US.utf8"},
    {"Latin1_General_CI_AS", "en_US.utf8"},
    {"SQL_Latin1_General_CP1_CS_AS", "C"},
    {"Latin1_General_CS_AS", "C"}};

// Cleans and normalizes a value from MSSQL for insertion into PostgreSQL.
// Handles null detection (empty strings, "NULL", invalid dates like
// "0000-00-00", "1900-01-01", "1970-01-01"), invalid binary characters
// (non-ASCII), and invalid date formats. For null values, returns appropriate
// defaults based on column type (0 for integers, 0.0 for floats, "DEFAULT" for
// strings, "1970-01-01 00:00:00" for timestamps, "false" for booleans). For
// boolean types, normalizes values ("N"/"0"/"false" -> "false", "Y"/"1"/"true"
// -> "true"). Removes control characters (except tab, newline, carriage return)
// from all values. For date/timestamp types, validates format and detects
// invalid dates containing "-00". Returns the cleaned value ready for SQL
// insertion.
std::string
MSSQLToPostgres::cleanValueForPostgres(const std::string &value,
                                       const std::string &columnType) {
  std::string cleanValue = value;
  std::string upperType = columnType;
  std::transform(upperType.begin(), upperType.end(), upperType.begin(),
                 ::toupper);

  bool isNull =
      (cleanValue.empty() || cleanValue == "NULL" || cleanValue == "null" ||
       cleanValue == "\\N" || cleanValue == "\\0" ||
       cleanValue.find("0000-") != std::string::npos ||
       cleanValue.find("1900-01-01") != std::string::npos ||
       cleanValue.find("1970-01-01") != std::string::npos);

  for (char &c : cleanValue) {
    if (static_cast<unsigned char>(c) > 127 || c < 32) {
      isNull = true;
      break;
    }
  }

  if (upperType.find("TIMESTAMP") != std::string::npos ||
      upperType.find("DATETIME") != std::string::npos ||
      upperType.find("DATE") != std::string::npos) {
    if (cleanValue.length() < 10 || cleanValue.find("-") == std::string::npos ||
        cleanValue.find("0000") != std::string::npos) {
      isNull = true;
    } else {
      if (cleanValue.find("-00") != std::string::npos ||
          cleanValue.find("-00 ") != std::string::npos ||
          cleanValue.find(" 00:00:00") != std::string::npos) {
        isNull = true;
      }
    }
  }

  if (isNull) {
    if (upperType.find("INTEGER") != std::string::npos ||
        upperType.find("BIGINT") != std::string::npos ||
        upperType.find("SMALLINT") != std::string::npos) {
      return "0";
    } else if (upperType.find("REAL") != std::string::npos ||
               upperType.find("FLOAT") != std::string::npos ||
               upperType.find("DOUBLE") != std::string::npos ||
               upperType.find("NUMERIC") != std::string::npos) {
      return "0.0";
    } else if (upperType.find("VARCHAR") != std::string::npos ||
               upperType.find("TEXT") != std::string::npos ||
               upperType.find("CHAR") != std::string::npos) {
      // Return empty string instead of NULL for text types to avoid NOT NULL
      // constraint violations
      return "";
    } else if (upperType.find("TIMESTAMP") != std::string::npos ||
               upperType.find("DATETIME") != std::string::npos) {
      return "1970-01-01 00:00:00";
    } else if (upperType.find("DATE") != std::string::npos) {
      return "1970-01-01";
    } else if (upperType.find("TIME") != std::string::npos) {
      return "00:00:00";
    } else if (upperType.find("BOOLEAN") != std::string::npos ||
               upperType.find("BOOL") != std::string::npos) {
      return "false";
    } else {
      return "DEFAULT";
    }
  }

  cleanValue.erase(std::remove_if(cleanValue.begin(), cleanValue.end(),
                                  [](unsigned char c) {
                                    return c < 32 && c != 9 && c != 10 &&
                                           c != 13;
                                  }),
                   cleanValue.end());

  if (upperType.find("BOOLEAN") != std::string::npos ||
      upperType.find("BOOL") != std::string::npos) {
    if (cleanValue == "N" || cleanValue == "0" || cleanValue == "false" ||
        cleanValue == "FALSE") {
      cleanValue = "false";
    } else if (cleanValue == "Y" || cleanValue == "1" || cleanValue == "true" ||
               cleanValue == "TRUE") {
      cleanValue = "true";
    }
  } else if (upperType.find("BIT") != std::string::npos) {
    if (cleanValue == "0" || cleanValue == "false" || cleanValue == "FALSE") {
      cleanValue = "false";
    } else if (cleanValue == "1" || cleanValue == "true" ||
               cleanValue == "TRUE") {
      cleanValue = "true";
    }
  }

  return cleanValue;
}

void MSSQLToPostgres::processTableCDC(
    const std::string &tableKey, SQLHDBC mssqlConn, const TableInfo &table,
    pqxx::connection &pgConn, const std::vector<std::string> &columnNames,
    const std::vector<std::string> &columnTypes) {
  try {
    Logger::error(LogCategory::TRANSFER, "processTableCDC",
                  "[FLOW] Starting CDC processing for " + tableKey);
    const size_t CHUNK_SIZE = SyncConfig::getChunkSize();
    long long lastChangeId = 0;

    try {
      pqxx::work txn(pgConn);
      std::string query =
          "SELECT sync_metadata->>'last_change_id' FROM metadata.catalog "
          "WHERE schema_name=" +
          txn.quote(table.schema_name) +
          " AND table_name=" + txn.quote(table.table_name) +
          " AND db_engine='MSSQL'";
      auto res = txn.exec(query);
      txn.commit();

      if (!res.empty() && !res[0][0].is_null()) {
        std::string value = res[0][0].as<std::string>();
        if (!value.empty() && value.size() <= 20) {
          try {
            lastChangeId = std::stoll(value);
            Logger::error(LogCategory::TRANSFER, "processTableCDC",
                          "[FLOW] Retrieved last_change_id=" + std::to_string(lastChangeId) + " for " + tableKey);
          } catch (const std::exception &e) {
            Logger::error(LogCategory::TRANSFER, "processTableCDC",
                          "Failed to parse last_change_id for " + tableKey +
                              ": " + std::string(e.what()));
            lastChangeId = 0;
          }
        } else {
          Logger::error(LogCategory::TRANSFER, "processTableCDC",
                        "[FLOW] No last_change_id found, starting from 0 for " + tableKey);
        }
      } else {
        Logger::error(LogCategory::TRANSFER, "processTableCDC",
                      "[FLOW] No last_change_id found, starting from 0 for " + tableKey);
      }
    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER, "processTableCDC",
                    "Error getting last_change_id for " + tableKey + ": " +
                        std::string(e.what()));
      lastChangeId = 0;
    }

    std::vector<std::string> pkColumns =
        getPKColumnsFromCatalog(pgConn, table.schema_name, table.table_name);
    bool hasPK = !pkColumns.empty();
    Logger::error(LogCategory::TRANSFER, "processTableCDC",
                  "[FLOW] Table " + tableKey + " hasPK=" + std::string(hasPK ? "true" : "false") + 
                  ", pkColumns.size()=" + std::to_string(pkColumns.size()));

    std::string databaseName = extractDatabaseName(table.connection_string);
    std::string useQuery = "USE [" + databaseName + "];";
    executeQueryMSSQL(mssqlConn, useQuery);

    std::string createSchemaQuery =
        "IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = "
        "'datasync_metadata') BEGIN EXEC('CREATE SCHEMA "
        "datasync_metadata') "
        "END;";
    executeQueryMSSQL(mssqlConn, createSchemaQuery);

    std::string createTableQuery =
        "IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = "
        "OBJECT_ID(N'datasync_metadata.ds_change_log') AND type in "
        "(N'U')) BEGIN CREATE TABLE datasync_metadata.ds_change_log ("
        "change_id BIGINT IDENTITY(1,1) PRIMARY KEY, "
        "change_time DATETIME NOT NULL DEFAULT GETDATE(), "
        "operation CHAR(1) NOT NULL, "
        "schema_name NVARCHAR(255) NOT NULL, "
        "table_name NVARCHAR(255) NOT NULL, "
        "pk_values NVARCHAR(MAX) NOT NULL, "
        "row_data NVARCHAR(MAX) NOT NULL); "
        "CREATE INDEX idx_ds_change_log_table_time ON "
        "datasync_metadata.ds_change_log (schema_name, table_name, "
        "change_time); "
        "CREATE INDEX idx_ds_change_log_table_change ON "
        "datasync_metadata.ds_change_log (schema_name, table_name, "
        "change_id); END;";
    executeQueryMSSQL(mssqlConn, createTableQuery);

    bool hasMore = true;
    size_t batchNumber = 0;

    while (hasMore) {
      batchNumber++;
      Logger::error(LogCategory::TRANSFER, "processTableCDC",
                    "[FLOW] Processing batch " + std::to_string(batchNumber) + " for " + tableKey + 
                    ", lastChangeId=" + std::to_string(lastChangeId));
      std::string query = "SELECT change_id, operation, pk_values, row_data "
                          "FROM datasync_metadata.ds_change_log WHERE "
                          "schema_name='" +
                          escapeSQL(table.schema_name) + "' AND table_name='" +
                          escapeSQL(table.table_name) + "' AND change_id > " +
                          std::to_string(lastChangeId) +
                          " ORDER BY change_id OFFSET 0 ROWS "
                          "FETCH NEXT " +
                          std::to_string(CHUNK_SIZE) + " ROWS ONLY";

      std::vector<std::vector<std::string>> rows =
          executeQueryMSSQL(mssqlConn, query);

      Logger::error(LogCategory::TRANSFER, "processTableCDC",
                    "[FLOW] Batch " + std::to_string(batchNumber) + " retrieved " + 
                    std::to_string(rows.size()) + " rows for " + tableKey);

      if (rows.empty()) {
        Logger::error(LogCategory::TRANSFER, "processTableCDC",
                      "[FLOW] No more rows, ending batch processing for " + tableKey);
        hasMore = false;
        break;
      }

      long long maxChangeId = lastChangeId;
      std::vector<std::vector<std::string>> deletedPKs;
      std::vector<std::vector<std::string>> recordsToUpsert;

      for (const auto &row : rows) {
        if (row.size() < 3) {
          Logger::error(LogCategory::TRANSFER, "processTableCDC",
                        "[FLOW] Skipping row with insufficient columns (size=" + 
                        std::to_string(row.size()) + ") for " + tableKey);
          continue;
        }

        std::string changeIdStr = row[0];
        std::string op = row[1];
        std::string pkJson = row[2];
        Logger::error(LogCategory::TRANSFER, "processTableCDC",
                      "[FLOW] Processing change: change_id=" + changeIdStr + 
                      ", operation=" + op + ", table=" + tableKey);

        try {
          if (!changeIdStr.empty()) {
            long long cid = std::stoll(changeIdStr);
            if (cid > maxChangeId) {
              maxChangeId = cid;
            }
          }
        } catch (const std::exception &e) {
          Logger::error(LogCategory::TRANSFER, "processTableCDC",
                        "Failed to parse change_id for " + tableKey + ": " +
                            std::string(e.what()));
        }

        try {
          json pkObject = json::parse(pkJson);
          bool isNoPKTable = !hasPK && pkObject.contains("_hash");
          Logger::error(LogCategory::TRANSFER, "processTableCDC",
                        "[FLOW] Parsed pk_values JSON, isNoPKTable=" + 
                        std::string(isNoPKTable ? "true" : "false") + " for " + tableKey);
          if (op == "D") {
            Logger::error(LogCategory::TRANSFER, "processTableCDC",
                          "[FLOW] DELETE pk_values JSON: " + pkJson);
            Logger::error(LogCategory::TRANSFER, "processTableCDC",
                          "[FLOW] DELETE pkColumns: " + 
                          (pkColumns.empty() ? "empty" : pkColumns[0]));
          }

          if (isNoPKTable) {
            std::string hashValue = pkObject["_hash"].get<std::string>();

            if (op == "D") {
              Logger::error(LogCategory::TRANSFER, "processTableCDC",
                            "[FLOW] Processing DELETE operation for " + tableKey + 
                            ", hash=" + hashValue);
              if (row.size() >= 4 && !row[3].empty() && row[3] != "NULL") {
                try {
                  json rowData = json::parse(row[3]);
                  std::vector<std::string> record;
                  record.reserve(columnNames.size());
                  Logger::error(LogCategory::TRANSFER, "processTableCDC",
                                "[FLOW] DELETE has row_data, parsing for " + tableKey);

                  for (const auto &colName : columnNames) {
                    if (rowData.contains(colName) &&
                        !rowData[colName].is_null()) {
                      if (rowData[colName].is_string()) {
                        record.push_back(rowData[colName].get<std::string>());
                      } else {
                        record.push_back(rowData[colName].dump());
                      }
                    } else {
                      record.push_back("");
                    }
                  }

                  if (record.size() == columnNames.size()) {
                    std::vector<std::string> deleteRecord;
                    deleteRecord.push_back(hashValue);
                    deleteRecord.insert(deleteRecord.end(), record.begin(),
                                        record.end());
                    deletedPKs.push_back(deleteRecord);
                    Logger::error(LogCategory::TRANSFER, "processTableCDC",
                                  "[FLOW] DELETE record prepared, total deletes=" + 
                                  std::to_string(deletedPKs.size()) + " for " + tableKey);
                  } else {
                    Logger::error(LogCategory::TRANSFER, "processTableCDC",
                                  "[FLOW] DELETE record size mismatch: " + 
                                  std::to_string(record.size()) + " != " + 
                                  std::to_string(columnNames.size()) + " for " + tableKey);
                  }
                } catch (const std::exception &e) {
                  Logger::warning(LogCategory::TRANSFER, "processTableCDC",
                                  "Failed to parse row_data for DELETE: " +
                                      std::string(e.what()));
                }
              } else {
                std::vector<std::string> deleteRecord;
                deleteRecord.push_back(hashValue);
                deletedPKs.push_back(deleteRecord);
                Logger::error(LogCategory::TRANSFER, "processTableCDC",
                              "[FLOW] DELETE without row_data, using hash only, total deletes=" + 
                              std::to_string(deletedPKs.size()) + " for " + tableKey);
              }
            } else if (op == "I" || op == "U") {
              Logger::error(LogCategory::TRANSFER, "processTableCDC",
                            "[FLOW] Processing " + op + " operation for " + tableKey + 
                            " (no PK table), hash=" + hashValue);
              bool useRowData = false;
              std::vector<std::string> record;

              if (row.size() >= 4 && !row[3].empty() && row[3] != "NULL") {
                try {
                  json rowData = json::parse(row[3]);
                  record.reserve(columnNames.size());

                  for (const auto &colName : columnNames) {
                    std::string jsonKey = colName;
                    bool found = false;
                    
                    for (auto it = rowData.begin(); it != rowData.end(); ++it) {
                      std::string key = it.key();
                      std::string keyLower = key;
                      std::transform(keyLower.begin(), keyLower.end(), keyLower.begin(), ::tolower);
                      if (keyLower == colName) {
                        jsonKey = key;
                        found = true;
                        break;
                      }
                    }
                    
                    if (found && rowData.contains(jsonKey) && !rowData[jsonKey].is_null()) {
                      if (rowData[jsonKey].is_string()) {
                        record.push_back(rowData[jsonKey].get<std::string>());
                      } else {
                        record.push_back(rowData[jsonKey].dump());
                      }
                    } else {
                      record.push_back("");
                    }
                  }

                  if (record.size() == columnNames.size()) {
                    recordsToUpsert.push_back(record);
                    useRowData = true;
                    Logger::error(LogCategory::TRANSFER, "processTableCDC",
                                  "[FLOW] " + op + " record prepared from row_data, total upserts=" + 
                                  std::to_string(recordsToUpsert.size()) + " for " + tableKey);
                  } else {
                    Logger::error(LogCategory::TRANSFER, "processTableCDC",
                                  "[FLOW] " + op + " record size mismatch: " + 
                                  std::to_string(record.size()) + " != " + 
                                  std::to_string(columnNames.size()) + " for " + tableKey);
                  }
                } catch (const std::exception &e) {
                  Logger::error(LogCategory::TRANSFER, "processTableCDC",
                                "[FLOW] Failed to parse row_data for " + op + " in " + tableKey + 
                                ": " + std::string(e.what()));
                }
              } else {
                Logger::error(LogCategory::TRANSFER, "processTableCDC",
                              "[FLOW] " + op + " operation has no row_data for " + tableKey);
              }

              if (!useRowData) {
                Logger::error(LogCategory::TRANSFER, "processTableCDC",
                              "[FLOW] row_data not available for table without PK: " + tableKey);
              }
            }
          } else {
            if (!hasPK) {
              Logger::warning(
                  LogCategory::TRANSFER, "processTableCDC",
                  "Table " + tableKey +
                      " has no PK but pk_values doesn't contain _hash");
              continue;
            }

            std::vector<std::string> pkValues;
            for (const auto &pkCol : pkColumns) {
              std::string jsonKey = pkCol;
              bool found = false;
              std::string pkColLower = pkCol;
              std::transform(pkColLower.begin(), pkColLower.end(), pkColLower.begin(), ::tolower);
              
              Logger::error(LogCategory::TRANSFER, "processTableCDC",
                            "[FLOW] Looking for PK column: " + pkCol + " (lower: " + pkColLower + ") in JSON");
              
              for (auto it = pkObject.begin(); it != pkObject.end(); ++it) {
                std::string key = it.key();
                std::string keyLower = key;
                std::transform(keyLower.begin(), keyLower.end(), keyLower.begin(), ::tolower);
                Logger::error(LogCategory::TRANSFER, "processTableCDC",
                              "[FLOW] JSON key: " + key + " (lower: " + keyLower + ") vs pkCol: " + pkColLower);
                if (keyLower == pkColLower) {
                  jsonKey = key;
                  found = true;
                  Logger::error(LogCategory::TRANSFER, "processTableCDC",
                                "[FLOW] Found matching key: " + jsonKey);
                  break;
                }
              }
              
              if (found && pkObject.contains(jsonKey) && !pkObject[jsonKey].is_null()) {
                Logger::error(LogCategory::TRANSFER, "processTableCDC",
                              "[FLOW] Extracting PK value from key: " + jsonKey);
                if (pkObject[jsonKey].is_string()) {
                  pkValues.push_back(pkObject[jsonKey].get<std::string>());
                } else if (pkObject[jsonKey].is_number_integer()) {
                  pkValues.push_back(std::to_string(pkObject[jsonKey].get<long long>()));
                } else if (pkObject[jsonKey].is_number_float()) {
                  pkValues.push_back(std::to_string(pkObject[jsonKey].get<double>()));
                } else {
                  std::string dumped = pkObject[jsonKey].dump();
                  if (dumped.size() >= 2 && dumped[0] == '"' && dumped[dumped.size()-1] == '"') {
                    pkValues.push_back(dumped.substr(1, dumped.size()-2));
                  } else {
                    pkValues.push_back(dumped);
                  }
                }
                Logger::error(LogCategory::TRANSFER, "processTableCDC",
                              "[FLOW] PK value extracted: " + pkValues.back());
              } else {
                Logger::error(LogCategory::TRANSFER, "processTableCDC",
                              "[FLOW] PK value NOT found, using NULL. found=" + 
                              std::string(found ? "true" : "false") + 
                              ", contains=" + std::string(pkObject.contains(jsonKey) ? "true" : "false") +
                              ", is_null=" + std::string((found && pkObject.contains(jsonKey) && pkObject[jsonKey].is_null()) ? "true" : "false"));
                pkValues.push_back("NULL");
              }
            }

            if (pkValues.size() != pkColumns.size()) {
              continue;
            }

            if (op == "D") {
              Logger::error(LogCategory::TRANSFER, "processTableCDC",
                            "[FLOW] Processing DELETE operation for " + tableKey + 
                            ", pkValues.size()=" + std::to_string(pkValues.size()));
              deletedPKs.push_back(pkValues);
              Logger::error(LogCategory::TRANSFER, "processTableCDC",
                            "[FLOW] DELETE record added, total deletes=" + 
                            std::to_string(deletedPKs.size()) + " for " + tableKey);
            } else if (op == "I" || op == "U") {
              Logger::error(LogCategory::TRANSFER, "processTableCDC",
                            "[FLOW] Processing " + op + " operation for " + tableKey + 
                            ", pkValues.size()=" + std::to_string(pkValues.size()));
              bool useRowData = false;
              std::vector<std::string> record;

              if (row.size() >= 4 && !row[3].empty() && row[3] != "NULL") {
                try {
                  json rowData = json::parse(row[3]);
                  record.reserve(columnNames.size());

                  for (const auto &colName : columnNames) {
                    std::string jsonKey = colName;
                    bool found = false;
                    
                    for (auto it = rowData.begin(); it != rowData.end(); ++it) {
                      std::string key = it.key();
                      std::string keyLower = key;
                      std::transform(keyLower.begin(), keyLower.end(), keyLower.begin(), ::tolower);
                      if (keyLower == colName) {
                        jsonKey = key;
                        found = true;
                        break;
                      }
                    }
                    
                    if (found && rowData.contains(jsonKey) && !rowData[jsonKey].is_null()) {
                      if (rowData[jsonKey].is_string()) {
                        record.push_back(rowData[jsonKey].get<std::string>());
                      } else {
                        record.push_back(rowData[jsonKey].dump());
                      }
                    } else {
                      record.push_back("");
                    }
                  }

                  if (record.size() == columnNames.size()) {
                    recordsToUpsert.push_back(record);
                    useRowData = true;
                    Logger::error(LogCategory::TRANSFER, "processTableCDC",
                                  "[FLOW] " + op + " record prepared from row_data, total upserts=" + 
                                  std::to_string(recordsToUpsert.size()) + " for " + tableKey);
                  } else {
                    Logger::error(LogCategory::TRANSFER, "processTableCDC",
                                  "[FLOW] " + op + " record size mismatch: " + 
                                  std::to_string(record.size()) + " != " + 
                                  std::to_string(columnNames.size()) + " for " + tableKey);
                  }
                } catch (const std::exception &e) {
                  Logger::error(LogCategory::TRANSFER, "processTableCDC",
                                "[FLOW] Failed to parse row_data for " + op + " in " + tableKey +
                                    ": " + std::string(e.what()) +
                                    ", falling back to SELECT");
                }
              }

              if (!useRowData) {
                std::string whereClause = "";
                for (size_t i = 0; i < pkColumns.size(); ++i) {
                  if (i > 0) {
                    whereClause += " AND ";
                  }
                  std::string pkValue = pkValues[i];
                  if (pkValue == "NULL") {
                    whereClause += "[" + pkColumns[i] + "] IS NULL";
                  } else {
                    whereClause +=
                        "[" + pkColumns[i] + "] = '" + escapeSQL(pkValue) + "'";
                  }
                }

                std::string selectQuery = "SELECT ";
                for (size_t i = 0; i < columnNames.size(); ++i) {
                  if (i > 0) {
                    selectQuery += ", ";
                  }
                  selectQuery += "[" + columnNames[i] + "]";
                }
                selectQuery += " FROM [" + table.schema_name + "].[" +
                               table.table_name + "] WHERE " + whereClause;

                std::vector<std::vector<std::string>> recordResult =
                    executeQueryMSSQL(mssqlConn, selectQuery);

                if (!recordResult.empty() &&
                    recordResult[0].size() == columnNames.size()) {
                  recordsToUpsert.push_back(recordResult[0]);
                  Logger::error(LogCategory::TRANSFER, "processTableCDC",
                                "[FLOW] " + op + " record retrieved from SELECT, total upserts=" + 
                                std::to_string(recordsToUpsert.size()) + " for " + tableKey);
                } else {
                  Logger::error(LogCategory::TRANSFER, "processTableCDC",
                                "[FLOW] Record not found in source for " + tableKey +
                                    " operation " + op +
                                    " with PK: " + pkJson);
                }
              }
            }
          }
        } catch (const std::exception &e) {
          Logger::error(LogCategory::TRANSFER, "processTableCDC",
                        "Failed to process change for " + tableKey + ": " +
                            std::string(e.what()));
        }
      }

      size_t upsertedCount = 0;
      if (!recordsToUpsert.empty()) {
        Logger::error(LogCategory::TRANSFER, "processTableCDC",
                      "[FLOW] Executing " + std::to_string(recordsToUpsert.size()) + 
                      " UPSERT operations for " + tableKey + " (before DELETE)");
        std::string lowerSchemaName = table.schema_name;
        std::transform(lowerSchemaName.begin(), lowerSchemaName.end(),
                       lowerSchemaName.begin(), ::tolower);
        std::string lowerTableName = table.table_name;
        std::transform(lowerTableName.begin(), lowerTableName.end(),
                       lowerTableName.begin(), ::tolower);
        try {
          bool isNoPKTable = !hasPK;
          if (isNoPKTable) {
            Logger::error(LogCategory::TRANSFER, "processTableCDC",
                          "[FLOW] Using performBulkUpsertNoPK for " + tableKey);
            performBulkUpsertNoPK(pgConn, recordsToUpsert, columnNames,
                                  columnTypes, lowerSchemaName, lowerTableName,
                                  table.schema_name);
          } else {
            Logger::error(LogCategory::TRANSFER, "processTableCDC",
                          "[FLOW] Using performBulkUpsert for " + tableKey);
            performBulkUpsert(pgConn, recordsToUpsert, columnNames, columnTypes,
                              lowerSchemaName, lowerTableName,
                              table.schema_name);
          }
          upsertedCount = recordsToUpsert.size();
          Logger::error(LogCategory::TRANSFER, "processTableCDC",
                        "[FLOW] UPSERT operations completed: " + std::to_string(upsertedCount) + 
                        " records upserted for " + tableKey);
        } catch (const std::exception &e) {
          Logger::error(LogCategory::TRANSFER, "processTableCDC",
                        "[FLOW] Failed to upsert records for " + tableKey + ": " +
                            std::string(e.what()));
        }
      } else {
        Logger::error(LogCategory::TRANSFER, "processTableCDC",
                      "[FLOW] No UPSERT operations to execute for " + tableKey);
      }

      size_t deletedCount = 0;
      if (!deletedPKs.empty()) {
        Logger::error(LogCategory::TRANSFER, "processTableCDC",
                      "[FLOW] Executing " + std::to_string(deletedPKs.size()) + 
                      " DELETE operations for " + tableKey + " (after UPSERT)");
        std::string lowerSchemaName = table.schema_name;
        std::transform(lowerSchemaName.begin(), lowerSchemaName.end(),
                       lowerSchemaName.begin(), ::tolower);
        std::string lowerTableName = table.table_name;
        std::transform(lowerTableName.begin(), lowerTableName.end(),
                       lowerTableName.begin(), ::tolower);

        bool isNoPKTable = !hasPK;
        if (isNoPKTable && !deletedPKs.empty()) {
          Logger::error(LogCategory::TRANSFER, "processTableCDC",
                        "[FLOW] Using deleteRecordsByHash for " + tableKey);
          deletedCount = deleteRecordsByHash(
              pgConn, lowerSchemaName, lowerTableName, deletedPKs, columnNames);
        } else if (hasPK && !deletedPKs.empty()) {
          Logger::error(LogCategory::TRANSFER, "processTableCDC",
                        "[FLOW] Using deleteRecordsByPrimaryKey for " + tableKey);
          deletedCount = deleteRecordsByPrimaryKey(
              pgConn, lowerSchemaName, lowerTableName, deletedPKs, pkColumns);
        }
        Logger::error(LogCategory::TRANSFER, "processTableCDC",
                      "[FLOW] DELETE operations completed: " + std::to_string(deletedCount) + 
                      " records deleted for " + tableKey);
      } else {
        Logger::error(LogCategory::TRANSFER, "processTableCDC",
                      "[FLOW] No DELETE operations to execute for " + tableKey);
      }

      if (maxChangeId > lastChangeId) {
        Logger::error(LogCategory::TRANSFER, "processTableCDC",
                      "[FLOW] Updating last_change_id from " + std::to_string(lastChangeId) + 
                      " to " + std::to_string(maxChangeId) + " for " + tableKey);
        try {
          std::lock_guard<std::mutex> lock(metadataUpdateMutex);
          pqxx::work txn(pgConn);
          std::string updateQuery =
              "UPDATE metadata.catalog SET sync_metadata = "
              "COALESCE(sync_metadata, '{}'::jsonb) || "
              "jsonb_build_object('last_change_id', " +
              std::to_string(maxChangeId) +
              ") WHERE schema_name=" + txn.quote(table.schema_name) +
              " AND table_name=" + txn.quote(table.table_name) +
              " AND db_engine='MSSQL'";
          txn.exec(updateQuery);
          txn.commit();
          lastChangeId = maxChangeId;
          Logger::error(LogCategory::TRANSFER, "processTableCDC",
                        "[FLOW] last_change_id updated successfully to " + 
                        std::to_string(maxChangeId) + " for " + tableKey);
        } catch (const std::exception &e) {
          Logger::error(LogCategory::TRANSFER, "processTableCDC",
                        "[FLOW] Error updating last_change_id for " + tableKey + ": " +
                            std::string(e.what()));
        }
      } else {
        Logger::error(LogCategory::TRANSFER, "processTableCDC",
                      "[FLOW] No need to update last_change_id (maxChangeId=" + 
                      std::to_string(maxChangeId) + " <= lastChangeId=" + 
                      std::to_string(lastChangeId) + ") for " + tableKey);
      }

      Logger::info(
          LogCategory::TRANSFER, "processTableCDC",
          "Processed CDC batch " + std::to_string(batchNumber) + " for " +
              tableKey + " with " + std::to_string(rows.size()) +
              " changes: " + std::to_string(upsertedCount) + " upserts, " +
              std::to_string(deletedCount) +
              " deletes; last_change_id=" + std::to_string(lastChangeId));

      if (rows.size() < CHUNK_SIZE) {
        hasMore = false;
      }
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "processTableCDC",
                  "Error in CDC processing for " + tableKey + ": " +
                      std::string(e.what()));
  }
}

void MSSQLToPostgres::processTableCDC(const TableInfo &table,
                                      pqxx::connection &pgConn) {
  std::string tableKey = table.schema_name + "." + table.table_name;
  SQLHDBC mssqlConn = getMSSQLConnection(table.connection_string);
  if (!mssqlConn) {
    Logger::error(LogCategory::TRANSFER, "processTableCDC",
                  "Failed to get MSSQL connection for " + tableKey);
    return;
  }

  try {
    MSSQLEngine engine(table.connection_string);
    std::vector<ColumnInfo> sourceColumns =
        engine.getTableColumns(table.schema_name, table.table_name);

    std::vector<std::string> columnNames;
    std::vector<std::string> columnTypes;
    for (const auto &col : sourceColumns) {
      columnNames.push_back(col.name);
      columnTypes.push_back(col.pgType);
    }

    if (columnNames.empty()) {
      Logger::error(LogCategory::TRANSFER, "processTableCDC",
                    "No columns found for " + tableKey);
      closeMSSQLConnection(mssqlConn);
      return;
    }

    processTableCDC(tableKey, mssqlConn, table, pgConn, columnNames,
                    columnTypes);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "processTableCDC",
                  "Error in processTableCDC for " + tableKey + ": " +
                      std::string(e.what()));
  }

  if (mssqlConn) {
    closeMSSQLConnection(mssqlConn);
  }
}
