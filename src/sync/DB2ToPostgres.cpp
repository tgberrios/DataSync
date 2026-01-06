#include "sync/DB2ToPostgres.h"
#include "core/Config.h"
#include "core/database_config.h"
#include "engines/database_engine.h"
#include "engines/db2_engine.h"
#include "third_party/json.hpp"
#include <algorithm>
#include <cctype>
#include <mutex>

using json = nlohmann::json;

std::unordered_map<std::string, std::string> DB2ToPostgres::dataTypeMap = {
    {"INTEGER", "INTEGER"},
    {"BIGINT", "BIGINT"},
    {"SMALLINT", "SMALLINT"},
    {"DECIMAL", "NUMERIC"},
    {"NUMERIC", "NUMERIC"},
    {"REAL", "REAL"},
    {"DOUBLE", "DOUBLE PRECISION"},
    {"FLOAT", "REAL"},
    {"CHAR", "TEXT"},
    {"VARCHAR", "VARCHAR"},
    {"CHARACTER VARYING", "VARCHAR"},
    {"CLOB", "TEXT"},
    {"DATE", "DATE"},
    {"TIME", "TIME"},
    {"TIMESTAMP", "TIMESTAMP"},
    {"BLOB", "BYTEA"},
    {"BINARY", "BYTEA"},
    {"VARBINARY", "BYTEA"},
    {"GRAPHIC", "TEXT"},
    {"VARGRAPHIC", "TEXT"},
    {"DBCLOB", "TEXT"}};

std::string
DB2ToPostgres::cleanValueForPostgres(const std::string &value,
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

  return cleanValue;
}

SQLHDBC DB2ToPostgres::getDB2Connection(const std::string &connectionString) {
  if (connectionString.empty()) {
    Logger::error(LogCategory::TRANSFER, "getDB2Connection",
                  "Empty connection string provided");
    return nullptr;
  }

  std::string database, uid, pwd, host, port;
  std::istringstream ss(connectionString);
  std::string token;
  while (std::getline(ss, token, ';')) {
    auto pos = token.find('=');
    if (pos == std::string::npos)
      continue;
    std::string key = token.substr(0, pos);
    std::string value = token.substr(pos + 1);
    key.erase(0, key.find_first_not_of(" \t\r\n"));
    key.erase(key.find_last_not_of(" \t\r\n") + 1);
    value.erase(0, value.find_first_not_of(" \t\r\n"));
    value.erase(value.find_last_not_of(" \t\r\n") + 1);
    if (key == "DATABASE")
      database = value;
    else if (key == "UID")
      uid = value;
    else if (key == "PWD")
      pwd = value;
    else if (key == "HOSTNAME" || key == "HOST")
      host = value;
    else if (key == "PORT")
      port = value;
  }

  if (database.empty() || uid.empty()) {
    Logger::error(LogCategory::TRANSFER, "getDB2Connection",
                  "Missing required connection parameters (DATABASE or UID)");
    return nullptr;
  }

  SQLHENV tempEnv = nullptr;
  SQLHDBC tempConn = nullptr;
  SQLRETURN ret;

  ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &tempEnv);
  if (!SQL_SUCCEEDED(ret)) {
    Logger::error(LogCategory::TRANSFER, "getDB2Connection",
                  "Failed to allocate ODBC environment handle");
    return nullptr;
  }

  ret = SQLSetEnvAttr(tempEnv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3,
                      0);
  if (!SQL_SUCCEEDED(ret)) {
    SQLFreeHandle(SQL_HANDLE_ENV, tempEnv);
    Logger::error(LogCategory::TRANSFER, "getDB2Connection",
                  "Failed to set ODBC version");
    return nullptr;
  }

  ret = SQLAllocHandle(SQL_HANDLE_DBC, tempEnv, &tempConn);
  if (!SQL_SUCCEEDED(ret)) {
    SQLFreeHandle(SQL_HANDLE_ENV, tempEnv);
    Logger::error(LogCategory::TRANSFER, "getDB2Connection",
                  "Failed to allocate ODBC connection handle");
    return nullptr;
  }

  SQLSetConnectAttr(tempConn, SQL_ATTR_CONNECTION_TIMEOUT, (SQLPOINTER)30, 0);
  SQLSetConnectAttr(tempConn, SQL_ATTR_LOGIN_TIMEOUT, (SQLPOINTER)30, 0);

  SQLCHAR outConnStr[1024];
  SQLSMALLINT outConnStrLen;
  ret = SQLDriverConnect(tempConn, nullptr, (SQLCHAR *)connectionString.c_str(),
                         SQL_NTS, outConnStr, sizeof(outConnStr),
                         &outConnStrLen, SQL_DRIVER_NOPROMPT);
  if (!SQL_SUCCEEDED(ret)) {
    SQLCHAR sqlState[6], msg[SQL_MAX_MESSAGE_LENGTH];
    SQLINTEGER nativeError;
    SQLSMALLINT msgLen;
    SQLGetDiagRec(SQL_HANDLE_DBC, tempConn, 1, sqlState, &nativeError, msg,
                  sizeof(msg), &msgLen);
    SQLFreeHandle(SQL_HANDLE_DBC, tempConn);
    SQLFreeHandle(SQL_HANDLE_ENV, tempEnv);
    Logger::error(LogCategory::TRANSFER, "getDB2Connection",
                  "Failed to connect to DB2: " + std::string((char *)msg) +
                      " (database: " + database + ", uid: " + uid + ")");
    return nullptr;
  }

  SQLHSTMT testStmt;
  ret = SQLAllocHandle(SQL_HANDLE_STMT, tempConn, &testStmt);
  if (ret == SQL_SUCCESS) {
    ret = SQLExecDirect(testStmt, (SQLCHAR *)"SELECT 1 FROM SYSIBM.SYSDUMMY1",
                        SQL_NTS);
    if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
      SQLFreeHandle(SQL_HANDLE_STMT, testStmt);
    } else {
      SQLFreeHandle(SQL_HANDLE_STMT, testStmt);
      SQLFreeHandle(SQL_HANDLE_DBC, tempConn);
      SQLFreeHandle(SQL_HANDLE_ENV, tempEnv);
      Logger::error(LogCategory::TRANSFER, "getDB2Connection",
                    "Connection test failed");
      return nullptr;
    }
  }

  return tempConn;
}

void DB2ToPostgres::closeDB2Connection(SQLHDBC conn) {
  if (conn) {
    SQLDisconnect(conn);
    SQLFreeHandle(SQL_HANDLE_DBC, conn);
  }
}

std::vector<std::vector<std::string>>
DB2ToPostgres::executeQueryDB2(SQLHDBC conn, const std::string &query) {
  std::vector<std::vector<std::string>> results;
  if (!conn) {
    Logger::error(LogCategory::TRANSFER, "executeQueryDB2",
                  "No valid DB2 connection");
    return results;
  }

  SQLHSTMT stmt;
  SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, conn, &stmt);
  if (ret != SQL_SUCCESS) {
    Logger::error(LogCategory::TRANSFER, "executeQueryDB2",
                  "SQLAllocHandle(STMT) failed");
    return results;
  }

  ret = SQLExecDirect(stmt, (SQLCHAR *)query.c_str(), SQL_NTS);
  if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
    SQLCHAR sqlState[6];
    SQLCHAR errorMsg[SQL_MAX_MESSAGE_LENGTH];
    SQLINTEGER nativeError;
    SQLSMALLINT msgLen;

    SQLGetDiagRec(SQL_HANDLE_STMT, stmt, 1, sqlState, &nativeError, errorMsg,
                  sizeof(errorMsg), &msgLen);

    Logger::error(
        LogCategory::TRANSFER, "executeQueryDB2",
        "SQLExecDirect failed - SQLState: " + std::string((char *)sqlState) +
            ", NativeError: " + std::to_string(nativeError) +
            ", Error: " + std::string((char *)errorMsg) + ", Query: " + query);
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    return results;
  }

  SQLSMALLINT numCols;
  SQLNumResultCols(stmt, &numCols);

  while (SQLFetch(stmt) == SQL_SUCCESS) {
    std::vector<std::string> row;
    for (SQLSMALLINT i = 1; i <= numCols; i++) {
      std::string value;
      char buffer[1024];
      SQLLEN len;
      ret = SQLGetData(stmt, i, SQL_C_CHAR, buffer, sizeof(buffer) - 1, &len);
      if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
        if (len == SQL_NULL_DATA) {
          row.push_back("NULL");
          continue;
        }
        if (len > 0 && len < static_cast<SQLLEN>(sizeof(buffer) - 1)) {
          buffer[len] = '\0';
          value = std::string(buffer, len);
        } else if (len >= static_cast<SQLLEN>(sizeof(buffer) - 1)) {
          buffer[sizeof(buffer) - 1] = '\0';
          value = std::string(buffer, sizeof(buffer) - 1);
          SQLLEN remainingLen = len - (sizeof(buffer) - 1);
          while (remainingLen > 0 &&
                 (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO)) {
            SQLLEN chunkRead;
            ret = SQLGetData(stmt, i, SQL_C_CHAR, buffer, sizeof(buffer) - 1,
                             &chunkRead);
            if (chunkRead > 0 &&
                chunkRead < static_cast<SQLLEN>(sizeof(buffer) - 1)) {
              buffer[chunkRead] = '\0';
              value += std::string(buffer, chunkRead);
              remainingLen -= chunkRead;
            } else if (chunkRead >= static_cast<SQLLEN>(sizeof(buffer) - 1)) {
              buffer[sizeof(buffer) - 1] = '\0';
              value += std::string(buffer, sizeof(buffer) - 1);
              remainingLen -= (sizeof(buffer) - 1);
            } else {
              break;
            }
          }
        }
        row.push_back(value);
      } else {
        row.push_back("NULL");
      }
    }
    results.push_back(row);
  }

  SQLFreeHandle(SQL_HANDLE_STMT, stmt);
  return results;
}

std::string
DB2ToPostgres::extractDatabaseName(const std::string &connectionString) {
  std::istringstream ss(connectionString);
  std::string token;
  while (std::getline(ss, token, ';')) {
    auto pos = token.find('=');
    if (pos == std::string::npos)
      continue;
    std::string key = token.substr(0, pos);
    std::string value = token.substr(pos + 1);
    if (key == "DATABASE") {
      return value;
    }
  }
  return "";
}

void DB2ToPostgres::processTableCDC(
    const DatabaseToPostgresSync::TableInfo &table, pqxx::connection &pgConn) {
  std::string tableKey = table.schema_name + "." + table.table_name;
  SQLHDBC db2Conn = getDB2Connection(table.connection_string);
  if (!db2Conn) {
    Logger::error(LogCategory::TRANSFER, "processTableCDC",
                  "Failed to get DB2 connection for " + tableKey);
    return;
  }

  try {
    processTableCDCInternal(table, pgConn, db2Conn);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "processTableCDC",
                  "Error in processTableCDC for " + tableKey + ": " +
                      std::string(e.what()));
  }

  if (db2Conn) {
    closeDB2Connection(db2Conn);
  }
}

void DB2ToPostgres::processTableCDCInternal(
    const DatabaseToPostgresSync::TableInfo &table, pqxx::connection &pgConn,
    SQLHDBC db2Conn) {
  std::string tableKey = table.schema_name + "." + table.table_name;
  const size_t CHUNK_SIZE = SyncConfig::getChunkSize();
  long long lastChangeId = 0;

  DB2Engine engine(table.connection_string);
  auto sourceColumns =
      engine.getTableColumns(table.schema_name, table.table_name);
  std::vector<std::string> columnNames;
  std::vector<std::string> columnTypes;
  for (const auto &col : sourceColumns) {
    columnNames.push_back(col.name);
    columnTypes.push_back(col.pgType);
  }

  if (columnNames.empty()) {
    Logger::error(LogCategory::TRANSFER, "processTableCDCInternal",
                  "No columns found for " + tableKey);
    return;
  }

  try {
    pqxx::work txn(pgConn);
    std::string query =
        "SELECT sync_metadata->>'last_change_id' FROM metadata.catalog "
        "WHERE schema_name=" +
        txn.quote(table.schema_name) +
        " AND table_name=" + txn.quote(table.table_name) +
        " AND db_engine='DB2'";
    auto res = txn.exec(query);
    txn.commit();

    if (!res.empty() && !res[0][0].is_null()) {
      std::string value = res[0][0].as<std::string>();
      if (!value.empty() && value.size() <= 20) {
        try {
          lastChangeId = std::stoll(value);
        } catch (const std::exception &e) {
          Logger::error(LogCategory::TRANSFER, "processTableCDCInternal",
                        "Failed to parse last_change_id for " + tableKey +
                            ": " + std::string(e.what()));
          lastChangeId = 0;
        }
      }
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "processTableCDCInternal",
                  "Error getting last_change_id for " + tableKey + ": " +
                      std::string(e.what()));
    lastChangeId = 0;
  }

  std::vector<std::string> pkColumns =
      getPKColumnsFromCatalog(pgConn, table.schema_name, table.table_name);
  bool hasPK = !pkColumns.empty();

  bool hasMore = true;
  size_t batchNumber = 0;

  while (hasMore) {
    batchNumber++;
    std::string query = "SELECT change_id, operation, pk_values, row_data "
                        "FROM DATASYNC_METADATA.DS_CHANGE_LOG WHERE "
                        "SCHEMA_NAME='" +
                        escapeSQL(table.schema_name) + "' AND TABLE_NAME='" +
                        escapeSQL(table.table_name) + "' AND change_id > " +
                        std::to_string(lastChangeId) +
                        " ORDER BY change_id FETCH FIRST " +
                        std::to_string(CHUNK_SIZE) + " ROWS ONLY";

    std::vector<std::vector<std::string>> rows =
        executeQueryDB2(db2Conn, query);

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
        Logger::error(LogCategory::TRANSFER, "processTableCDCInternal",
                      "Failed to parse change_id for " + tableKey + ": " +
                          std::string(e.what()));
      }

      try {
        json pkObject = json::parse(pkJson);
        bool isNoPKTable = !hasPK && pkObject.contains("_hash");

        if (isNoPKTable) {
          std::string hashValue = pkObject["_hash"].get<std::string>();

          if (op == "D") {
            if (row.size() >= 4 && !row[3].empty() && row[3] != "NULL") {
              try {
                json rowData = json::parse(row[3]);
                std::vector<std::string> record;
                record.reserve(pkColumns.size() + 1);
                record.push_back(hashValue);

                for (const auto &colName : pkColumns) {
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

                if (record.size() == pkColumns.size() + 1) {
                  deletedPKs.push_back(record);
                }
              } catch (const std::exception &e) {
                Logger::warning(LogCategory::TRANSFER,
                                "processTableCDCInternal",
                                "Failed to parse row_data for DELETE: " +
                                    std::string(e.what()));
              }
            } else {
              std::vector<std::string> deleteRecord;
              deleteRecord.push_back(hashValue);
              deletedPKs.push_back(deleteRecord);
            }
          } else if (op == "I" || op == "U") {
            bool useRowData = false;
            std::vector<std::string> record;

            if (row.size() >= 4 && !row[3].empty() && row[3] != "NULL") {
              try {
                json rowData = json::parse(row[3]);
                record.reserve(pkColumns.size());
                bool allColumnsFound = true;

                for (const auto &colName : pkColumns) {
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

                if (allColumnsFound && record.size() == pkColumns.size()) {
                  recordsToUpsert.push_back(record);
                  useRowData = true;
                }
              } catch (const std::exception &e) {
                Logger::warning(
                    LogCategory::TRANSFER, "processTableCDCInternal",
                    "Failed to parse row_data: " + std::string(e.what()));
              }
            }

            if (!useRowData) {
              Logger::warning(LogCategory::TRANSFER, "processTableCDCInternal",
                              "row_data not available for table without PK: " +
                                  tableKey);
            }
          }
        } else {
          if (!hasPK) {
            Logger::warning(
                LogCategory::TRANSFER, "processTableCDCInternal",
                "Table " + tableKey +
                    " has no PK but pk_values doesn't contain _hash");
            continue;
          }

          std::vector<std::string> pkValues;
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
                record.reserve(pkColumns.size());
                bool allColumnsFound = true;

                for (const auto &colName : pkColumns) {
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

                if (allColumnsFound && record.size() == pkColumns.size()) {
                  recordsToUpsert.push_back(record);
                  useRowData = true;
                }
              } catch (const std::exception &e) {
                Logger::warning(
                    LogCategory::TRANSFER, "processTableCDCInternal",
                    "Failed to parse row_data for " + tableKey + ": " +
                        std::string(e.what()) + ", falling back to SELECT");
              }
            }

            if (!useRowData) {
              Logger::warning(LogCategory::TRANSFER, "processTableCDCInternal",
                              "row_data not available for " + tableKey +
                                  ", falling back to SELECT");
            }
          }
        }
      } catch (const std::exception &e) {
        Logger::warning(LogCategory::TRANSFER, "processTableCDCInternal",
                        "Failed to parse pk_values JSON for " + tableKey +
                            ": " + std::string(e.what()));
        continue;
      }
    }

    size_t deletedCount = 0;
    if (!deletedPKs.empty()) {
      std::string lowerSchemaName = table.schema_name;
      std::transform(lowerSchemaName.begin(), lowerSchemaName.end(),
                     lowerSchemaName.begin(), ::tolower);
      std::string lowerTableName = table.table_name;
      std::transform(lowerTableName.begin(), lowerTableName.end(),
                     lowerTableName.begin(), ::tolower);

      bool isNoPKTable = !hasPK;
      if (isNoPKTable && !deletedPKs.empty()) {
        deletedCount = deleteRecordsByHash(
            pgConn, lowerSchemaName, lowerTableName, deletedPKs, columnNames);
      } else if (hasPK && !deletedPKs.empty()) {
        deletedCount = deleteRecordsByPrimaryKey(
            pgConn, lowerSchemaName, lowerTableName, deletedPKs, pkColumns);
      }
    }

    size_t upsertedCount = 0;
    if (!recordsToUpsert.empty()) {
      std::string lowerSchemaName = table.schema_name;
      std::transform(lowerSchemaName.begin(), lowerSchemaName.end(),
                     lowerSchemaName.begin(), ::tolower);
      std::string lowerTableName = table.table_name;
      std::transform(lowerTableName.begin(), lowerTableName.end(),
                     lowerTableName.begin(), ::tolower);
      try {

        bool isNoPKTable = !hasPK;
        if (isNoPKTable) {
          performBulkUpsertNoPK(pgConn, recordsToUpsert, columnNames,
                                columnTypes, lowerSchemaName, lowerTableName,
                                table.schema_name);
        } else {
          performBulkUpsert(pgConn, recordsToUpsert, columnNames, columnTypes,
                            lowerSchemaName, lowerTableName, table.schema_name);
        }
        upsertedCount = recordsToUpsert.size();
      } catch (const std::exception &e) {
        Logger::error(LogCategory::TRANSFER, "processTableCDCInternal",
                      "Failed to upsert records for " + tableKey + ": " +
                          std::string(e.what()));
      }
    }

    if (maxChangeId > lastChangeId) {
      try {
        pqxx::work txn(pgConn);
        json metadata;
        metadata["last_change_id"] = std::to_string(maxChangeId);
        pqxx::params params;
        params.append(metadata.dump());
        params.append(table.schema_name);
        params.append(table.table_name);
        txn.exec(
            pqxx::zview("UPDATE metadata.catalog SET sync_metadata = $1 WHERE "
            "schema_name = $2 AND table_name = $3 AND db_engine = 'DB2'"),
            params);
        txn.commit();
      } catch (const std::exception &e) {
        Logger::error(LogCategory::TRANSFER, "processTableCDCInternal",
                      "Failed to update last_change_id for " + tableKey + ": " +
                          std::string(e.what()));
      }
    }

    lastChangeId = maxChangeId;
  }
}

std::string
DB2ToPostgres::mapDB2DataTypeToPostgres(const std::string &db2Type) {
  std::string upperType = db2Type;
  std::transform(upperType.begin(), upperType.end(), upperType.begin(),
                 ::toupper);
  if (dataTypeMap.count(upperType)) {
    return dataTypeMap[upperType];
  }
  return "TEXT";
}

void DB2ToPostgres::setupTableTargetDB2ToPostgres() {
  Logger::info(LogCategory::TRANSFER, "Starting DB2 table target setup");
}

void DB2ToPostgres::transferDataDB2ToPostgres() {
  Logger::info(LogCategory::TRANSFER, "Starting DB2 data transfer");
}

void DB2ToPostgres::transferDataDB2ToPostgresParallel() {
  Logger::info(LogCategory::TRANSFER, "Starting DB2 parallel data transfer");
}

void DB2ToPostgres::processTableParallelWithConnection(const TableInfo &table) {
  try {
    pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());
    processTableParallel(table, pgConn);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "processTableParallelWithConnection",
                  "Error in processTableParallelWithConnection: " +
                      std::string(e.what()));
  }
}

void DB2ToPostgres::processTableParallel(const TableInfo &table,
                                         pqxx::connection &pgConn) {
  Logger::info(LogCategory::TRANSFER, "processTableParallel",
               "Processing table " + table.schema_name + "." +
                   table.table_name);
}

void DB2ToPostgres::processTableFullLoad(const TableInfo &table,
                                         pqxx::connection &pgConn,
                                         SQLHDBC db2Conn) {
  (void)pgConn;
  (void)db2Conn;
  Logger::info(LogCategory::TRANSFER, "processTableFullLoad",
               "Full load for " + table.schema_name + "." + table.table_name);
}

void DB2ToPostgres::createChangeLogTable(SQLHDBC db2Conn,
                                         const std::string &schema,
                                         const std::string &table) {
  (void)db2Conn;
  Logger::info(LogCategory::TRANSFER, "createChangeLogTable",
               "Creating change log table for " + schema + "." + table);
}

void DB2ToPostgres::createChangeLogTriggers(SQLHDBC db2Conn,
                                            const std::string &schema,
                                            const std::string &table) {
  (void)db2Conn;
  Logger::info(LogCategory::TRANSFER, "createChangeLogTriggers",
               "Creating change log triggers for " + schema + "." + table);
}

std::string DB2ToPostgres::escapeSQL(const std::string &value) {
  if (value.empty()) {
    return value;
  }
  std::string escaped = value;
  size_t pos = 0;
  while ((pos = escaped.find('\'', pos)) != std::string::npos) {
    escaped.replace(pos, 1, "''");
    pos += 2;
  }
  return escaped;
}
