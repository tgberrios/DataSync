#include "sync/OracleToPostgres.h"
#include "core/database_config.h"
#include "core/logger.h"
#include "engines/database_engine.h"
#include <algorithm>
#include <cctype>
#include <pqxx/pqxx>
#include <sstream>

std::unordered_map<std::string, std::string> OracleToPostgres::dataTypeMap = {
    {"NUMBER", "NUMERIC"},
    {"INTEGER", "INTEGER"},
    {"BINARY_INTEGER", "INTEGER"},
    {"BIGINT", "BIGINT"},
    {"SMALLINT", "SMALLINT"},
    {"FLOAT", "DOUBLE PRECISION"},
    {"BINARY_FLOAT", "REAL"},
    {"BINARY_DOUBLE", "DOUBLE PRECISION"},
    {"VARCHAR2", "VARCHAR"},
    {"VARCHAR", "VARCHAR"},
    {"CHAR", "CHAR"},
    {"NCHAR", "CHAR"},
    {"NVARCHAR2", "VARCHAR"},
    {"CLOB", "TEXT"},
    {"NCLOB", "TEXT"},
    {"LONG", "TEXT"},
    {"BLOB", "BYTEA"},
    {"RAW", "BYTEA"},
    {"LONG RAW", "BYTEA"},
    {"BFILE", "BYTEA"},
    {"DATE", "TIMESTAMP"},
    {"TIMESTAMP", "TIMESTAMP"},
    {"TIMESTAMP WITH TIME ZONE", "TIMESTAMP WITH TIME ZONE"},
    {"TIMESTAMP WITH LOCAL TIME ZONE", "TIMESTAMP"},
    {"INTERVAL YEAR TO MONTH", "INTERVAL"},
    {"INTERVAL DAY TO SECOND", "INTERVAL"},
    {"XMLTYPE", "XML"},
    {"JSON", "JSONB"}};

std::string
OracleToPostgres::cleanValueForPostgres(const std::string &value,
                                        const std::string &columnType) {
  std::string cleanValue = value;
  std::string upperType = columnType;
  std::transform(upperType.begin(), upperType.end(), upperType.begin(),
                 ::toupper);

  bool isNull =
      (cleanValue.empty() || cleanValue == "NULL" || cleanValue == "null");

  if (upperType.find("TIMESTAMP") != std::string::npos ||
      upperType.find("DATE") != std::string::npos) {
    if (cleanValue.find("0000-") != std::string::npos ||
        cleanValue.find("1900-01-01") != std::string::npos ||
        cleanValue.find("1970-01-01") != std::string::npos) {
      isNull = true;
    }
  }

  if (isNull) {
    if (upperType.find("INTEGER") != std::string::npos ||
        upperType.find("NUMERIC") != std::string::npos ||
        upperType.find("REAL") != std::string::npos ||
        upperType.find("DOUBLE") != std::string::npos) {
      return "0";
    } else if (upperType.find("TIMESTAMP") != std::string::npos ||
               upperType.find("DATE") != std::string::npos) {
      return "1970-01-01 00:00:00";
    } else if (upperType.find("BOOLEAN") != std::string::npos) {
      return "false";
    } else {
      return "";
    }
  }

  size_t pos = 0;
  while ((pos = cleanValue.find("'", pos)) != std::string::npos) {
    cleanValue.replace(pos, 1, "''");
    pos += 2;
  }

  return cleanValue;
}

std::unique_ptr<OCIConnection>
OracleToPostgres::getOracleConnection(const std::string &connectionString) {
  if (connectionString.empty()) {
    Logger::error(LogCategory::TRANSFER, "getOracleConnection",
                  "Empty connection string provided");
    return nullptr;
  }

  auto conn = std::make_unique<OCIConnection>(connectionString);
  if (!conn->isValid()) {
    Logger::error(LogCategory::TRANSFER, "getOracleConnection",
                  "Failed to create Oracle connection");
    return nullptr;
  }

  return conn;
}

std::vector<std::vector<std::string>>
OracleToPostgres::executeQueryOracle(OCIConnection *conn,
                                     const std::string &query) {
  std::vector<std::vector<std::string>> results;
  if (!conn || !conn->isValid()) {
    Logger::error(LogCategory::TRANSFER, "executeQueryOracle",
                  "Invalid Oracle connection");
    return results;
  }

  if (query.empty()) {
    Logger::error(LogCategory::TRANSFER, "executeQueryOracle",
                  "Empty query provided");
    return results;
  }

  OCIStmt *stmt = nullptr;
  OCIError *err = conn->getErr();
  OCISvcCtx *svc = conn->getSvc();
  OCIEnv *env = conn->getEnv();

  struct StmtGuard {
    OCIStmt *stmt_;
    OCIError *err_;
    StmtGuard(OCIStmt *s, OCIError *e) : stmt_(s), err_(e) {}
    ~StmtGuard() {
      if (stmt_) {
        OCIHandleFree(stmt_, OCI_HTYPE_STMT);
      }
    }
  };

  sword status =
      OCIHandleAlloc((dvoid *)env, (dvoid **)&stmt, OCI_HTYPE_STMT, 0, nullptr);
  if (status != OCI_SUCCESS) {
    Logger::error(LogCategory::TRANSFER, "executeQueryOracle",
                  "OCIHandleAlloc(STMT) failed");
    return results;
  }

  StmtGuard guard(stmt, err);

  status = OCIStmtPrepare(stmt, err, (OraText *)query.c_str(), query.length(),
                          OCI_NTV_SYNTAX, OCI_DEFAULT);
  if (status != OCI_SUCCESS) {
    Logger::error(LogCategory::TRANSFER, "executeQueryOracle",
                  "OCIStmtPrepare failed for query: " + query);
    return results;
  }

  status = OCIStmtExecute(svc, stmt, err, 0, 0, nullptr, nullptr, OCI_DEFAULT);
  if (status != OCI_SUCCESS && status != OCI_SUCCESS_WITH_INFO) {
    Logger::error(LogCategory::TRANSFER, "executeQueryOracle",
                  "OCIStmtExecute failed for query: " + query);
    return results;
  }

  ub4 numCols = 0;
  OCIAttrGet(stmt, OCI_HTYPE_STMT, &numCols, nullptr, OCI_ATTR_PARAM_COUNT,
             err);

  if (numCols == 0) {
    return results;
  }

  std::vector<OCIDefine *> defines(numCols);
  std::vector<std::vector<char>> buffers(numCols);
  std::vector<ub2> lengths(numCols, 0);
  std::vector<sb2> inds(numCols, -1);

  for (ub4 i = 0; i < numCols; ++i) {
    buffers[i].resize(4000, 0);
    status =
        OCIDefineByPos(stmt, &defines[i], err, i + 1, buffers[i].data(), 4000,
                       SQLT_STR, &inds[i], &lengths[i], nullptr, OCI_DEFAULT);
    if (status != OCI_SUCCESS) {
      Logger::error(LogCategory::TRANSFER, "executeQueryOracle",
                    "OCIDefineByPos failed for column " +
                        std::to_string(i + 1));
      return results;
    }
  }

  while (OCIStmtFetch(stmt, err, 1, OCI_FETCH_NEXT, OCI_DEFAULT) ==
         OCI_SUCCESS) {
    std::vector<std::string> row;
    row.reserve(numCols);
    for (ub4 i = 0; i < numCols; ++i) {
      if (inds[i] == -1) {
        row.push_back("NULL");
      } else if (lengths[i] > 0 && lengths[i] <= 4000) {
        row.push_back(std::string(buffers[i].data(), lengths[i]));
      } else {
        row.push_back("");
      }
    }
    results.push_back(row);
  }

  guard.stmt_ = nullptr;
  return results;
}

std::vector<DatabaseToPostgresSync::TableInfo>
OracleToPostgres::getActiveTables(pqxx::connection &pgConn) {
  std::vector<TableInfo> data;

  try {
    pqxx::work txn(pgConn);
    auto results = txn.exec(
        "SELECT schema_name, table_name, cluster_name, db_engine, "
        "connection_string, last_sync_time, last_sync_column, "
        "status, last_processed_pk, pk_strategy, "
        "pk_columns, has_pk "
        "FROM metadata.catalog "
        "WHERE active=true AND db_engine='Oracle' AND status != 'NO_DATA' "
        "ORDER BY schema_name, table_name;");
    txn.commit();

    for (const auto &row : results) {
      if (row.size() < 12)
        continue;

      TableInfo t;
      t.schema_name = row[0].is_null() ? "" : row[0].as<std::string>();
      t.table_name = row[1].is_null() ? "" : row[1].as<std::string>();
      t.cluster_name = row[2].is_null() ? "" : row[2].as<std::string>();
      t.db_engine = row[3].is_null() ? "" : row[3].as<std::string>();
      t.connection_string = row[4].is_null() ? "" : row[4].as<std::string>();
      t.last_sync_time = row[5].is_null() ? "" : row[5].as<std::string>();
      t.last_sync_column = row[6].is_null() ? "" : row[6].as<std::string>();
      t.status = row[7].is_null() ? "" : row[7].as<std::string>();
      t.last_processed_pk = row[8].is_null() ? "" : row[8].as<std::string>();
      t.pk_strategy = row[9].is_null() ? "" : row[9].as<std::string>();
      t.pk_columns = row[10].is_null() ? "" : row[10].as<std::string>();
      t.has_pk = row[11].is_null() ? false : row[11].as<bool>();
      data.push_back(t);
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "getActiveTables",
                  "ERROR getting active tables: " + std::string(e.what()));
  }

  return data;
}

void OracleToPostgres::updateLastOffset(pqxx::connection &pgConn,
                                        const std::string &schema_name,
                                        const std::string &table_name,
                                        long long offset) {
  try {
    std::lock_guard<std::mutex> lock(metadataUpdateMutex);

    pqxx::work txn(pgConn);
    std::string updateQuery = "UPDATE metadata.catalog SET sync_metadata = "
                              "COALESCE(sync_metadata, '{}'::jsonb) || "
                              "jsonb_build_object('last_offset', " +
                              std::to_string(offset) +
                              ") "
                              "WHERE schema_name=" +
                              txn.quote(schema_name) +
                              " AND table_name=" + txn.quote(table_name);

    txn.exec(updateQuery);
    txn.commit();

    Logger::info(LogCategory::TRANSFER, "updateLastOffset",
                 "Updated last_offset to " + std::to_string(offset) + " for " +
                     schema_name + "." + table_name);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "updateLastOffset",
                  "Error updating last_offset: " + std::string(e.what()));
  }
}

long long OracleToPostgres::getLastOffset(pqxx::connection &pgConn,
                                          const std::string &schema_name,
                                          const std::string &table_name) {
  try {
    pqxx::work txn(pgConn);
    std::string query =
        "SELECT sync_metadata->>'last_offset' FROM metadata.catalog "
        "WHERE schema_name=" +
        txn.quote(schema_name) + " AND table_name=" + txn.quote(table_name);

    auto result = txn.exec(query);
    txn.commit();

    if (!result.empty() && !result[0][0].is_null()) {
      std::string offsetStr = result[0][0].as<std::string>();
      if (!offsetStr.empty() && offsetStr.length() <= 20) {
        try {
          return std::stoll(offsetStr);
        } catch (const std::exception &e) {
          Logger::error(LogCategory::TRANSFER, "getLastOffset",
                        "Failed to parse offset value '" + offsetStr +
                            "': " + std::string(e.what()));
        }
      }
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "getLastOffset",
                  "Error getting last_offset: " + std::string(e.what()));
  }
  return 0;
}

void OracleToPostgres::updateStatus(pqxx::connection &pgConn,
                                    const std::string &schema_name,
                                    const std::string &table_name,
                                    const std::string &status,
                                    size_t rowCount) {
  try {
    pqxx::work txn(pgConn);
    try {
      std::string updateQuery =
          "UPDATE metadata.catalog SET status=" + txn.quote(status) +
          " WHERE schema_name=" + txn.quote(schema_name) +
          " AND table_name=" + txn.quote(table_name);
      txn.exec(updateQuery);
      txn.commit();
    } catch (...) {
      try {
        txn.abort();
      } catch (...) {
      }
      throw;
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "updateStatus",
                  "Error updating status: " + std::string(e.what()));
  }
}

std::vector<std::string>
OracleToPostgres::getPrimaryKeyColumns(OCIConnection *conn,
                                       const std::string &schema_name,
                                       const std::string &table_name) {
  std::vector<std::string> pkColumns;

  if (schema_name.empty() || table_name.empty()) {
    Logger::error(LogCategory::TRANSFER, "getPrimaryKeyColumns",
                  "Empty schema or table name");
    return pkColumns;
  }

  if (!isValidOracleIdentifier(schema_name) ||
      !isValidOracleIdentifier(table_name)) {
    Logger::error(
        LogCategory::TRANSFER, "getPrimaryKeyColumns",
        "Invalid Oracle identifier characters in schema or table name");
    return pkColumns;
  }

  std::string upperSchema = schema_name;
  std::transform(upperSchema.begin(), upperSchema.end(), upperSchema.begin(),
                 ::toupper);
  std::string upperTable = table_name;
  std::transform(upperTable.begin(), upperTable.end(), upperTable.begin(),
                 ::toupper);

  std::string query =
      "SELECT column_name FROM all_cons_columns WHERE constraint_name = ("
      "SELECT constraint_name FROM all_constraints "
      "WHERE UPPER(owner) = '" +
      escapeOracleValue(upperSchema) + "' AND UPPER(table_name) = '" +
      escapeOracleValue(upperTable) +
      "' AND constraint_type = 'P') ORDER BY position";

  auto results = executeQueryOracle(conn, query);
  for (const auto &row : results) {
    if (!row.empty()) {
      std::string colName = row[0];
      std::transform(colName.begin(), colName.end(), colName.begin(),
                     ::tolower);
      pkColumns.push_back(colName);
    }
  }

  return pkColumns;
}

// Helper function to safely escape Oracle string values
// Escapes single quotes and validates for SQL injection patterns
std::string OracleToPostgres::escapeOracleValue(const std::string &value) {
  if (value.empty()) {
    return value;
  }

  // Check for SQL injection patterns
  std::string upperValue = value;
  std::transform(upperValue.begin(), upperValue.end(), upperValue.begin(),
                 ::toupper);

  // Reject obvious SQL injection attempts
  if (upperValue.find("--") != std::string::npos ||
      upperValue.find("/*") != std::string::npos ||
      upperValue.find("*/") != std::string::npos ||
      upperValue.find(";") != std::string::npos ||
      upperValue.find("DROP") != std::string::npos ||
      upperValue.find("DELETE") != std::string::npos ||
      upperValue.find("UPDATE") != std::string::npos ||
      upperValue.find("INSERT") != std::string::npos ||
      upperValue.find("EXEC") != std::string::npos ||
      upperValue.find("EXECUTE") != std::string::npos) {
    Logger::warning(LogCategory::TRANSFER, "escapeOracleValue",
                    "Potentially dangerous value detected, rejecting: " +
                        value);
    throw std::invalid_argument(
        "Invalid value contains SQL keywords or special characters");
  }

  // Escape single quotes (Oracle standard: ' -> '')
  std::string escaped = value;
  size_t pos = 0;
  while ((pos = escaped.find("'", pos)) != std::string::npos) {
    escaped.replace(pos, 1, "''");
    pos += 2;
  }

  return escaped;
}

// Validates that a string is a valid Oracle identifier
// Oracle identifiers can contain letters, digits, _, $, #, and must start with
// a letter
bool OracleToPostgres::isValidOracleIdentifier(const std::string &identifier) {
  if (identifier.empty() || identifier.length() > 128) {
    return false;
  }

  // Oracle identifiers must start with a letter
  if (!std::isalpha(static_cast<unsigned char>(identifier[0]))) {
    return false;
  }

  // Check remaining characters: letters, digits, _, $, #
  for (size_t i = 1; i < identifier.length(); ++i) {
    unsigned char c = static_cast<unsigned char>(identifier[i]);
    if (!std::isalnum(c) && c != '_' && c != '$' && c != '#') {
      return false;
    }
  }

  return true;
}

void OracleToPostgres::setupTableTargetOracleToPostgres() {
  Logger::info(LogCategory::TRANSFER, "Starting Oracle table target setup");

  try {
    pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());
    if (!pgConn.is_open()) {
      Logger::error(LogCategory::TRANSFER, "setupTableTargetOracleToPostgres",
                    "CRITICAL ERROR: Cannot establish PostgreSQL connection");
      return;
    }

    auto tables = getActiveTables(pgConn);
    if (tables.empty()) {
      Logger::info(LogCategory::TRANSFER,
                   "No active Oracle tables found to setup");
      return;
    }

    for (const auto &table : tables) {
      if (table.db_engine != "Oracle") {
        continue;
      }

      auto oracleConn = getOracleConnection(table.connection_string);
      if (!oracleConn || !oracleConn->isValid()) {
        Logger::error(LogCategory::TRANSFER, "setupTableTargetOracleToPostgres",
                      "Failed to get Oracle connection for table " +
                          table.schema_name + "." + table.table_name);
        continue;
      }

      std::string upperSchema = table.schema_name;
      std::transform(upperSchema.begin(), upperSchema.end(),
                     upperSchema.begin(), ::toupper);
      std::string upperTable = table.table_name;
      std::transform(upperTable.begin(), upperTable.end(), upperTable.begin(),
                     ::toupper);

      if (!isValidOracleIdentifier(table.schema_name) ||
          !isValidOracleIdentifier(table.table_name)) {
        Logger::error(LogCategory::TRANSFER, "setupTableTargetOracleToPostgres",
                      "Invalid Oracle identifier characters for " +
                          table.schema_name + "." + table.table_name);
        continue;
      }

      std::string query =
          "SELECT column_name, data_type, data_length, data_precision, "
          "data_scale, nullable, data_default "
          "FROM all_tab_columns WHERE UPPER(owner) = '" +
          escapeOracleValue(upperSchema) + "' AND UPPER(table_name) = '" +
          escapeOracleValue(upperTable) + "' ORDER BY column_id";

      auto columns = executeQueryOracle(oracleConn.get(), query);
      if (columns.empty()) {
        Logger::error(LogCategory::TRANSFER, "setupTableTargetOracleToPostgres",
                      "No columns found for table " + table.schema_name + "." +
                          table.table_name);
        continue;
      }

      std::string lowerSchema = table.schema_name;
      std::transform(lowerSchema.begin(), lowerSchema.end(),
                     lowerSchema.begin(), ::tolower);
      std::string lowerTable = table.table_name;
      std::transform(lowerTable.begin(), lowerTable.end(), lowerTable.begin(),
                     ::tolower);

      {
        pqxx::work txn(pgConn);
        txn.exec("CREATE SCHEMA IF NOT EXISTS \"" + lowerSchema + "\";");
        txn.commit();
      }

      std::vector<std::string> primaryKeys = getPrimaryKeyColumns(
          oracleConn.get(), table.schema_name, table.table_name);

      std::string createQuery = "CREATE TABLE IF NOT EXISTS \"" + lowerSchema +
                                "\".\"" + lowerTable + "\" (";

      for (size_t i = 0; i < columns.size(); ++i) {
        if (columns[i].size() < 6)
          continue;

        std::string colName = columns[i][0];
        std::transform(colName.begin(), colName.end(), colName.begin(),
                       ::tolower);
        std::string dataType = columns[i][1];
        std::string dataLength = columns[i].size() > 2 ? columns[i][2] : "";
        std::string dataPrecision = columns[i].size() > 3 ? columns[i][3] : "";
        std::string dataScale = columns[i].size() > 4 ? columns[i][4] : "";

        std::string pgType = "TEXT";
        if (dataTypeMap.count(dataType)) {
          pgType = dataTypeMap[dataType];
          if (pgType == "VARCHAR" && !dataLength.empty() &&
              dataLength != "NULL") {
            try {
              if (!dataLength.empty() && dataLength.length() <= 10) {
                int len = std::stoi(dataLength);
                if (len > 0 && len <= 10485760) {
                  pgType = "VARCHAR(" + std::to_string(len) + ")";
                }
              }
            } catch (const std::exception &e) {
              Logger::warning(LogCategory::TRANSFER,
                              "setupTableTargetOracleToPostgres",
                              "Failed to parse dataLength '" + dataLength +
                                  "': " + std::string(e.what()));
            }
          } else if (pgType == "NUMERIC" && !dataPrecision.empty() &&
                     dataPrecision != "NULL") {
            try {
              if (!dataPrecision.empty() && dataPrecision.length() <= 10) {
                int prec = std::stoi(dataPrecision);
                int scale = 0;
                if (!dataScale.empty() && dataScale != "NULL" &&
                    dataScale.length() <= 10) {
                  scale = std::stoi(dataScale);
                }
                if (prec > 0 && prec <= 1000 && scale >= 0 && scale <= prec) {
                  pgType = "NUMERIC(" + std::to_string(prec) + "," +
                           std::to_string(scale) + ")";
                }
              }
            } catch (const std::exception &e) {
              Logger::warning(LogCategory::TRANSFER,
                              "setupTableTargetOracleToPostgres",
                              "Failed to parse numeric precision/scale: " +
                                  std::string(e.what()));
            }
          }
        }

        if (i > 0)
          createQuery += ", ";
        createQuery += "\"" + colName + "\" " + pgType;
      }

      if (!primaryKeys.empty()) {
        createQuery += ", PRIMARY KEY (";
        for (size_t i = 0; i < primaryKeys.size(); ++i) {
          if (i > 0)
            createQuery += ", ";
          createQuery += "\"" + primaryKeys[i] + "\"";
        }
        createQuery += ")";
      }
      createQuery += ");";

      {
        pqxx::work txn(pgConn);
        txn.exec(createQuery);
        txn.commit();
      }

      Logger::info(LogCategory::TRANSFER, "setupTableTargetOracleToPostgres",
                   "Created table " + lowerSchema + "." + lowerTable);
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "setupTableTargetOracleToPostgres",
                  "Error in setupTableTargetOracleToPostgres: " +
                      std::string(e.what()));
  }
}

void OracleToPostgres::transferDataOracleToPostgres() {
  Logger::info(LogCategory::TRANSFER,
               "Starting Oracle to PostgreSQL data transfer");

  try {
    pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());
    if (!pgConn.is_open()) {
      Logger::error(LogCategory::TRANSFER, "transferDataOracleToPostgres",
                    "CRITICAL ERROR: Cannot establish PostgreSQL connection");
      return;
    }

    auto tables = getActiveTables(pgConn);
    if (tables.empty()) {
      Logger::info(LogCategory::TRANSFER,
                   "No active Oracle tables found for data transfer");
      return;
    }

    for (const auto &table : tables) {
      if (table.db_engine != "Oracle") {
        continue;
      }

      Logger::info(LogCategory::TRANSFER,
                   "Processing table: " + table.schema_name + "." +
                       table.table_name + " (status: " + table.status + ")");

      std::string originalStatus = table.status;
      updateStatus(pgConn, table.schema_name, table.table_name, "IN_PROGRESS");

      auto oracleConn = getOracleConnection(table.connection_string);
      if (!oracleConn || !oracleConn->isValid()) {
        Logger::error(LogCategory::TRANSFER, "transferDataOracleToPostgres",
                      "Failed to get Oracle connection for table " +
                          table.schema_name + "." + table.table_name);
        updateStatus(pgConn, table.schema_name, table.table_name, "ERROR");
        continue;
      }

      std::string schema_name = table.schema_name;
      std::string table_name = table.table_name;
      std::string upperSchema = schema_name;
      std::transform(upperSchema.begin(), upperSchema.end(),
                     upperSchema.begin(), ::toupper);
      std::string upperTable = table_name;
      std::transform(upperTable.begin(), upperTable.end(), upperTable.begin(),
                     ::toupper);

      std::string lowerSchema = schema_name;
      std::transform(lowerSchema.begin(), lowerSchema.end(),
                     lowerSchema.begin(), ::tolower);
      std::string lowerTable = table_name;
      std::transform(lowerTable.begin(), lowerTable.end(), lowerTable.begin(),
                     ::tolower);

      // Get row counts (same as MariaDB/MSSQL)
      std::string countQuery =
          "SELECT COUNT(*) FROM " + upperSchema + "." + upperTable;
      auto countResults = executeQueryOracle(oracleConn.get(), countQuery);
      size_t sourceCount = 0;
      if (!countResults.empty() && !countResults[0].empty()) {
        try {
          const std::string &countStr = countResults[0][0];
          if (!countStr.empty() && countStr.length() <= 20) {
            sourceCount = std::stoul(countStr);
          }
        } catch (const std::exception &e) {
          Logger::warning(LogCategory::TRANSFER,
                          "Could not parse source count for table " +
                              schema_name + "." + table_name + ": " +
                              std::string(e.what()) + " - using 0");
          sourceCount = 0;
        }
      }

      std::string targetCountQuery =
          "SELECT COUNT(*) FROM \"" + lowerSchema + "\".\"" + lowerTable + "\"";
      size_t targetCount = 0;
      try {
        pqxx::work txn(pgConn);
        auto targetResult = txn.exec(targetCountQuery);
        if (!targetResult.empty()) {
          targetCount = targetResult[0][0].as<size_t>();
        }
        txn.commit();
      } catch (const std::exception &e) {
        Logger::error(LogCategory::TRANSFER, "transferDataOracleToPostgres",
                      "ERROR getting target count for table " + lowerSchema +
                          "." + lowerTable + ": " + std::string(e.what()));
      }

      // Handle FULL_LOAD status - truncate and reset
      if (table.status == "FULL_LOAD" || table.status == "RESET") {
        try {
          pqxx::work truncateTxn(pgConn);
          truncateTxn.exec("TRUNCATE TABLE \"" + lowerSchema + "\".\"" +
                           lowerTable + "\" CASCADE;");
          truncateTxn.exec(
              "UPDATE metadata.catalog SET last_processed_pk=NULL, "
              "sync_metadata='{}'::jsonb WHERE schema_name=" +
              truncateTxn.quote(schema_name) +
              " AND table_name=" + truncateTxn.quote(table_name));
          truncateTxn.commit();
          targetCount = 0;
          Logger::info(LogCategory::TRANSFER, "transferDataOracleToPostgres",
                       "Truncated table for FULL_LOAD/RESET: " + schema_name +
                           "." + table_name);
        } catch (const std::exception &e) {
          Logger::error(LogCategory::TRANSFER, "transferDataOracleToPostgres",
                        "Error truncating table: " + std::string(e.what()));
        }
      }

      // Handle NO_DATA case
      if (sourceCount == 0) {
        if (targetCount == 0) {
          updateStatus(pgConn, schema_name, table_name, "NO_DATA", 0);
        } else {
          updateStatus(pgConn, schema_name, table_name, "LISTENING_CHANGES", 0);
        }
        continue;
      }

      // If sourceCount == targetCount, check if FULL_LOAD completed
      if (sourceCount == targetCount) {
        if (table.status == "FULL_LOAD") {
          Logger::info(LogCategory::TRANSFER, "transferDataOracleToPostgres",
                       "FULL_LOAD completed for " + schema_name + "." +
                           table_name + ", transitioning to LISTENING_CHANGES");
          updateStatus(pgConn, schema_name, table_name, "LISTENING_CHANGES",
                       targetCount);
          continue;
        }
        // For non-FULL_LOAD tables with matching counts, just mark as
        // LISTENING_CHANGES
        updateStatus(pgConn, schema_name, table_name, "LISTENING_CHANGES",
                     targetCount);
        continue;
      }

      // Get column names once
      if (!isValidOracleIdentifier(schema_name) ||
          !isValidOracleIdentifier(table_name)) {
        Logger::error(LogCategory::TRANSFER, "transferDataOracleToPostgres",
                      "Invalid Oracle identifier characters for " +
                          schema_name + "." + table_name);
        continue;
      }

      std::string columnQuery =
          "SELECT column_name FROM all_tab_columns WHERE owner = '" +
          escapeOracleValue(upperSchema) + "' AND table_name = '" +
          escapeOracleValue(upperTable) + "' ORDER BY column_id";
      auto columnResults = executeQueryOracle(oracleConn.get(), columnQuery);

      std::vector<std::string> columnNames;
      for (const auto &row : columnResults) {
        if (!row.empty()) {
          std::string colName = row[0];
          std::transform(colName.begin(), colName.end(), colName.begin(),
                         ::tolower);
          columnNames.push_back(colName);
        }
      }

      if (columnNames.empty()) {
        Logger::error(LogCategory::TRANSFER, "transferDataOracleToPostgres",
                      "No column names found for table " + schema_name + "." +
                          table_name);
        continue;
      }

      // Get PK strategy and columns
      std::vector<std::string> pkColumns =
          getPKColumnsFromCatalog(pgConn, schema_name, table_name);
      std::string pkStrategy =
          getPKStrategyFromCatalog(pgConn, schema_name, table_name);
      std::string lastProcessedPK =
          getLastProcessedPKFromCatalog(pgConn, schema_name, table_name);
      long long lastOffset = getLastOffset(pgConn, schema_name, table_name);

      // Process in chunks (same as MariaDB/MSSQL)
      bool hasMoreData = sourceCount > targetCount;
      size_t chunkNumber = 0;
      size_t rawChunkSize = SyncConfig::getChunkSize();
      const size_t CHUNK_SIZE =
          (rawChunkSize == 0 || rawChunkSize > 10000) ? 1000 : rawChunkSize;

      while (hasMoreData) {
        chunkNumber++;
        std::string selectQuery =
            "SELECT * FROM " + upperSchema + "." + upperTable;

        // Build query based on PK strategy (same as MariaDB/MSSQL)
        if (pkStrategy == "PK" && !pkColumns.empty()) {
          std::string upperPkCol = pkColumns[0];
          std::transform(upperPkCol.begin(), upperPkCol.end(),
                         upperPkCol.begin(), ::toupper);
          if (!lastProcessedPK.empty()) {
            std::vector<std::string> lastPKValues =
                parseLastPK(lastProcessedPK);
            if (!lastPKValues.empty() && pkColumns.size() == 1) {
              std::string escapedPK = escapeOracleValue(lastPKValues[0]);
              selectQuery += " WHERE " + upperPkCol + " > '" + escapedPK + "'";
            }
          }
          selectQuery += " ORDER BY " + upperPkCol;
          selectQuery += " OFFSET 0 ROWS FETCH NEXT " +
                         std::to_string(CHUNK_SIZE) + " ROWS ONLY";
        } else if (pkStrategy == "OFFSET") {
          selectQuery += " ORDER BY ROWID";
          selectQuery += " OFFSET " + std::to_string(lastOffset) +
                         " ROWS FETCH NEXT " + std::to_string(CHUNK_SIZE) +
                         " ROWS ONLY";
        } else {
          selectQuery += " ORDER BY ROWID";
          selectQuery += " OFFSET 0 ROWS FETCH NEXT " +
                         std::to_string(CHUNK_SIZE) + " ROWS ONLY";
        }

        auto results = executeQueryOracle(oracleConn.get(), selectQuery);
        if (results.empty()) {
          hasMoreData = false;
          break;
        }

        try {
          pqxx::work txn(pgConn);

          std::ostringstream insertQuery;
          insertQuery << "INSERT INTO " << txn.quote_name(lowerSchema) << "."
                      << txn.quote_name(lowerTable) << " (";

          for (size_t i = 0; i < columnNames.size(); ++i) {
            if (i > 0)
              insertQuery << ", ";
            insertQuery << txn.quote_name(columnNames[i]);
          }
          insertQuery << ") VALUES ";

          for (size_t i = 0; i < results.size(); ++i) {
            if (i > 0)
              insertQuery << ", ";
            insertQuery << "(";
            for (size_t j = 0; j < columnNames.size() && j < results[i].size();
                 ++j) {
              if (j > 0)
                insertQuery << ", ";
              if (results[i][j] == "NULL" || results[i][j].empty()) {
                insertQuery << "NULL";
              } else {
                std::string cleanValue =
                    cleanValueForPostgres(results[i][j], "TEXT");
                insertQuery << txn.quote(cleanValue);
              }
            }
            insertQuery << ")";
          }

          try {
            txn.exec(insertQuery.str());
            txn.commit();
            targetCount += results.size();
          } catch (...) {
            try {
              txn.abort();
            } catch (...) {
            }
            throw;
          }

          // Update last_processed_pk or last_offset after each chunk
          if (pkStrategy == "PK" && !pkColumns.empty() && !results.empty()) {
            std::string lastPK =
                getLastPKFromResults(results, pkColumns, columnNames);
            if (!lastPK.empty()) {
              updateLastProcessedPK(pgConn, schema_name, table_name, lastPK);
              lastProcessedPK = lastPK; // Update for next iteration
            }
          } else if (pkStrategy == "OFFSET") {
            lastOffset += results.size();
            updateLastOffset(pgConn, schema_name, table_name, lastOffset);
          }

          // Check if we're done
          if (results.size() < CHUNK_SIZE || targetCount >= sourceCount) {
            hasMoreData = false;
          }
        } catch (const std::exception &e) {
          Logger::error(LogCategory::TRANSFER, "transferDataOracleToPostgres",
                        "Error inserting data: " + std::string(e.what()));
          updateStatus(pgConn, schema_name, table_name, "ERROR");
          hasMoreData = false;
          break;
        }
      }

      // Final status update
      if (table.status == "FULL_LOAD" && targetCount >= sourceCount) {
        updateStatus(pgConn, schema_name, table_name, "LISTENING_CHANGES",
                     targetCount);
      } else {
        updateStatus(pgConn, schema_name, table_name, "LISTENING_CHANGES",
                     targetCount);
      }
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "transferDataOracleToPostgres",
                  "Error in transferDataOracleToPostgres: " +
                      std::string(e.what()));
  }
}

void OracleToPostgres::transferDataOracleToPostgresParallel() {
  transferDataOracleToPostgres();
}

void OracleToPostgres::processTableParallel(const TableInfo &table,
                                            pqxx::connection &pgConn) {
  transferDataOracleToPostgres();
}
