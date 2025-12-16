#include "sync/MariaDBToPostgres.h"
#include "core/Config.h"
#include "core/database_config.h"
#include "engines/database_engine.h"
#include "engines/mariadb_engine.h"
#include "third_party/json.hpp"
#include <algorithm>
#include <cctype>
#include <mutex>

using json = nlohmann::json;

std::unordered_map<std::string, std::string> MariaDBToPostgres::dataTypeMap = {
    {"int", "INTEGER"},
    {"bigint", "BIGINT"},
    {"smallint", "SMALLINT"},
    {"tinyint", "SMALLINT"},
    {"decimal", "NUMERIC"},
    {"float", "REAL"},
    {"double", "DOUBLE PRECISION"},
    {"varchar", "VARCHAR"},
    {"char", "TEXT"},
    {"text", "TEXT"},
    {"longtext", "TEXT"},
    {"mediumtext", "TEXT"},
    {"tinytext", "TEXT"},
    {"blob", "BYTEA"},
    {"longblob", "BYTEA"},
    {"mediumblob", "BYTEA"},
    {"tinyblob", "BYTEA"},
    {"json", "JSON"},
    {"boolean", "BOOLEAN"},
    {"bit", "BIT"},
    {"timestamp", "TIMESTAMP"},
    {"datetime", "TIMESTAMP"},
    {"date", "DATE"},
    {"time", "TIME"}};

std::unordered_map<std::string, std::string> MariaDBToPostgres::collationMap = {
    {"utf8_general_ci", "en_US.utf8"},
    {"utf8mb4_general_ci", "en_US.utf8"},
    {"latin1_swedish_ci", "C"},
    {"ascii_general_ci", "C"}};

// Cleans and normalizes a value from MariaDB for insertion into PostgreSQL.
// Handles null detection (empty strings, "NULL", invalid dates like
// "0000-00-00", "1900-01-01", "1970-01-01"), invalid binary characters
// (non-ASCII), and invalid date formats. For VARCHAR/CHAR types, truncates
// values that exceed the maximum length specified in the column type. For
// BYTEA/BLOB/BIT types, validates hexadecimal format and truncates large binary
// data (>1000 bytes). For null values, returns appropriate defaults based on
// column type (0 for integers, 0.0 for floats, "DEFAULT" for strings,
// "1970-01-01 00:00:00" for timestamps). For date/timestamp types, validates
// format and detects invalid dates containing "-00". Returns the cleaned value
// ready for SQL insertion.
std::string
MariaDBToPostgres::cleanValueForPostgres(const std::string &value,
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

  // No truncate CHAR/VARCHAR values - use TEXT type instead
  // This allows values of any length without truncation

  if (upperType.find("BYTEA") != std::string::npos ||
      upperType.find("BLOB") != std::string::npos ||
      upperType.find("BIT") != std::string::npos) {

    bool hasInvalidBinaryChars = false;
    for (char c : cleanValue) {
      if (!std::isxdigit(c) && c != ' ' && c != '\\' && c != 'x') {
        hasInvalidBinaryChars = true;
        break;
      }
    }

    if (hasInvalidBinaryChars) {
      Logger::warning(LogCategory::TRANSFER, "cleanValueForPostgres",
                      "Invalid binary data detected, converting to NULL: " +
                          cleanValue.substr(0, 50) + "...");
      isNull = true;
    } else if (!cleanValue.empty() && cleanValue.length() > 1000) {
      Logger::warning(LogCategory::TRANSFER, "cleanValueForPostgres",
                      "Large binary data detected, truncating: " +
                          std::to_string(cleanValue.length()) + " bytes");
      cleanValue = cleanValue.substr(0, 1000);
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
      return "DEFAULT";
    } else if (upperType.find("TIMESTAMP") != std::string::npos ||
               upperType.find("DATETIME") != std::string::npos) {
      return "1970-01-01 00:00:00";
    } else if (upperType.find("DATE") != std::string::npos) {
      return "1970-01-01";
    } else if (upperType.find("TIME") != std::string::npos) {
      return "00:00:00";
    } else {
      return "DEFAULT";
    }
  }

  return cleanValue;
}

void MariaDBToPostgres::processTableCDC(
    const std::string &tableKey, MYSQL *mariadbConn, const TableInfo &table,
    pqxx::connection &pgConn, const std::vector<std::string> &columnNames,
    const std::vector<std::string> &columnTypes) {
  try {
    const size_t CHUNK_SIZE = SyncConfig::getChunkSize();
    long long lastChangeId = 0;

    try {
      pqxx::work txn(pgConn);
      std::string query =
          "SELECT sync_metadata->>'last_change_id' FROM metadata.catalog "
          "WHERE schema_name=" +
          txn.quote(table.schema_name) +
          " AND table_name=" + txn.quote(table.table_name) +
          " AND db_engine='MariaDB'";
      auto res = txn.exec(query);
      txn.commit();

      if (!res.empty() && !res[0][0].is_null()) {
        std::string value = res[0][0].as<std::string>();
        if (!value.empty() && value.size() <= 20) {
          try {
            lastChangeId = std::stoll(value);
          } catch (const std::exception &e) {
            Logger::error(LogCategory::TRANSFER, "processTableCDC",
                          "Failed to parse last_change_id for " + tableKey +
                              ": " + std::string(e.what()));
            lastChangeId = 0;
          }
        }
      }
    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER, "processTableCDC",
                    "Error getting last_change_id for " + tableKey + ": " +
                        std::string(e.what()));
      lastChangeId = 0;
    }

    std::vector<std::string> pkColumns =
        getPKColumnsFromCatalog(pgConn, table.schema_name, table.table_name);
    if (pkColumns.empty()) {
      Logger::warning(LogCategory::TRANSFER, "processTableCDC",
                      "No PK columns found for " + tableKey +
                          " in catalog; skipping CDC processing");
      return;
    }

    bool hasMore = true;
    size_t batchNumber = 0;

    while (hasMore) {
      batchNumber++;
      std::string query = "SELECT change_id, operation, pk_values, row_data "
                          "FROM datasync_metadata.ds_change_log WHERE "
                          "schema_name='" +
                          escapeSQL(table.schema_name) + "' AND table_name='" +
                          escapeSQL(table.table_name) + "' AND change_id > " +
                          std::to_string(lastChangeId) +
                          " ORDER BY change_id LIMIT " +
                          std::to_string(CHUNK_SIZE);

      std::vector<std::vector<std::string>> rows =
          executeQueryMariaDB(mariadbConn, query);

      if (rows.empty()) {
        hasMore = false;
        break;
      }

      long long maxChangeId = lastChangeId;
      std::vector<std::vector<std::string>> deletedPKs;
      std::vector<std::vector<std::string>> recordsToUpsert;

      for (const auto &row : rows) {
        if (row.size() < 3) {
          continue;
        }

        std::string changeIdStr = row[0];
        std::string op = row[1];
        std::string pkJson = row[2];

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
          std::vector<std::string> pkValues;
          json pkObject = json::parse(pkJson);
          for (const auto &pkCol : pkColumns) {
            if (pkObject.contains(pkCol) && !pkObject[pkCol].is_null()) {
              if (pkObject[pkCol].is_string()) {
                pkValues.push_back(pkObject[pkCol].get<std::string>());
              } else {
                pkValues.push_back(pkObject[pkCol].dump());
              }
            } else {
              pkValues.push_back("NULL");
            }
          }

          if (pkValues.size() != pkColumns.size()) {
            continue;
          }

          if (op == "D") {
            deletedPKs.push_back(pkValues);
          } else if (op == "I" || op == "U") {
            bool useRowData = false;
            std::vector<std::string> record;

            if (row.size() >= 4 && !row[3].empty() && row[3] != "NULL") {
              try {
                json rowData = json::parse(row[3]);
                record.reserve(columnNames.size());
                bool allColumnsFound = true;

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
                    allColumnsFound = false;
                  }
                }

                if (allColumnsFound && record.size() == columnNames.size()) {
                  recordsToUpsert.push_back(record);
                  useRowData = true;
                }
              } catch (const std::exception &e) {
                Logger::warning(LogCategory::TRANSFER, "processTableCDC",
                                "Failed to parse row_data for " + tableKey +
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
                  whereClause += "`" + pkColumns[i] + "` IS NULL";
                } else {
                  whereClause +=
                      "`" + pkColumns[i] + "` = '" + escapeSQL(pkValue) + "'";
                }
              }

              std::string selectQuery = "SELECT * FROM `" + table.schema_name +
                                        "`.`" + table.table_name + "` WHERE " +
                                        whereClause + " LIMIT 1";

              std::vector<std::vector<std::string>> recordResult =
                  executeQueryMariaDB(mariadbConn, selectQuery);

              if (!recordResult.empty() &&
                  recordResult[0].size() == columnNames.size()) {
                recordsToUpsert.push_back(recordResult[0]);
              } else {
                Logger::warning(LogCategory::TRANSFER, "processTableCDC",
                                "Record not found in source for " + tableKey +
                                    " operation " + op + " with PK: " + pkJson);
              }
            }
          }
        } catch (const std::exception &e) {
          Logger::error(LogCategory::TRANSFER, "processTableCDC",
                        "Failed to process change for " + tableKey + ": " +
                            std::string(e.what()));
        }
      }

      size_t deletedCount = 0;
      if (!deletedPKs.empty()) {
        std::string lowerSchemaName = table.schema_name;
        std::transform(lowerSchemaName.begin(), lowerSchemaName.end(),
                       lowerSchemaName.begin(), ::tolower);
        deletedCount = deleteRecordsByPrimaryKey(
            pgConn, lowerSchemaName, table.table_name, deletedPKs, pkColumns);
      }

      size_t upsertedCount = 0;
      if (!recordsToUpsert.empty()) {
        std::string lowerSchemaName = table.schema_name;
        std::transform(lowerSchemaName.begin(), lowerSchemaName.end(),
                       lowerSchemaName.begin(), ::tolower);
        try {
          performBulkUpsert(pgConn, recordsToUpsert, columnNames, columnTypes,
                            lowerSchemaName, table.table_name,
                            table.schema_name);
          upsertedCount = recordsToUpsert.size();
        } catch (const std::exception &e) {
          Logger::error(LogCategory::TRANSFER, "processTableCDC",
                        "Failed to upsert records for " + tableKey + ": " +
                            std::string(e.what()));
        }
      }

      if (maxChangeId > lastChangeId) {
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
              " AND db_engine='MariaDB'";
          txn.exec(updateQuery);
          txn.commit();
          lastChangeId = maxChangeId;
        } catch (const std::exception &e) {
          Logger::error(LogCategory::TRANSFER, "processTableCDC",
                        "Error updating last_change_id for " + tableKey + ": " +
                            std::string(e.what()));
        }
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

void MariaDBToPostgres::processTableCDC(const TableInfo &table,
                                        pqxx::connection &pgConn) {
  std::string tableKey = table.schema_name + "." + table.table_name;
  MYSQL *mariadbConn = getMariaDBConnection(table.connection_string);
  if (!mariadbConn) {
    Logger::error(LogCategory::TRANSFER, "processTableCDC",
                  "Failed to get MariaDB connection for " + tableKey);
    return;
  }

  try {
    MariaDBEngine engine(table.connection_string);
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
      mysql_close(mariadbConn);
      return;
    }

    processTableCDC(tableKey, mariadbConn, table, pgConn, columnNames,
                    columnTypes);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "processTableCDC",
                  "Error in processTableCDC for " + tableKey + ": " +
                      std::string(e.what()));
  }

  if (mariadbConn) {
    mysql_close(mariadbConn);
  }
}
