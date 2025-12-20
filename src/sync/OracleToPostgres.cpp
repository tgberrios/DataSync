#include "sync/OracleToPostgres.h"
#include "core/Config.h"
#include "core/database_config.h"
#include "core/logger.h"
#include "engines/database_engine.h"
#include "sync/SchemaSync.h"
#include "third_party/json.hpp"
#include <algorithm>
#include <cctype>
#include <pqxx/pqxx>
#include <sstream>

using json = nlohmann::json;

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

  sword fetchStatus;
  while ((fetchStatus = OCIStmtFetch(stmt, err, 1, OCI_FETCH_NEXT,
                                     OCI_DEFAULT)) == OCI_SUCCESS) {
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

  if (fetchStatus != OCI_NO_DATA) {
    char errbuf[512];
    sb4 errcode = 0;
    OCIErrorGet(err, 1, nullptr, &errcode, (OraText *)errbuf, sizeof(errbuf),
                OCI_HTYPE_ERROR);
    Logger::error(LogCategory::TRANSFER, "executeQueryOracle",
                  "OCIStmtFetch error: " + std::string(errbuf));
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
        "connection_string, "
        "status, pk_strategy, "
        "pk_columns "
        "FROM metadata.catalog "
        "WHERE active=true AND db_engine='Oracle' AND status != 'NO_DATA' "
        "ORDER BY schema_name, table_name;");
    txn.commit();

    for (const auto &row : results) {
      if (row.size() < 8)
        continue;

      TableInfo t;
      t.schema_name = row[0].is_null() ? "" : row[0].as<std::string>();
      t.table_name = row[1].is_null() ? "" : row[1].as<std::string>();
      t.cluster_name = row[2].is_null() ? "" : row[2].as<std::string>();
      t.db_engine = row[3].is_null() ? "" : row[3].as<std::string>();
      t.connection_string = row[4].is_null() ? "" : row[4].as<std::string>();
      t.status = row[5].is_null() ? "" : row[5].as<std::string>();
      t.pk_strategy = row[6].is_null() ? "" : row[6].as<std::string>();
      t.pk_columns = row[7].is_null() ? "" : row[7].as<std::string>();
      std::vector<std::string> pkCols = parseJSONArray(t.pk_columns);
      t.has_pk = !pkCols.empty();
      data.push_back(t);
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "getActiveTables",
                  "ERROR getting active tables: " + std::string(e.what()));
  }

  return data;
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

  if (upperValue.find("--") != std::string::npos ||
      upperValue.find("/*") != std::string::npos ||
      upperValue.find("*/") != std::string::npos ||
      upperValue.find(";") != std::string::npos) {
    Logger::warning(LogCategory::TRANSFER, "escapeOracleValue",
                    "Potentially dangerous value detected, rejecting: " +
                        value);
    throw std::invalid_argument(
        "Invalid value contains SQL comment characters");
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

    std::set<std::string> processedSchemas;

    for (const auto &table : tables) {
      if (table.db_engine != "Oracle") {
        continue;
      }

      std::string upperSchema = table.schema_name;
      std::transform(upperSchema.begin(), upperSchema.end(),
                     upperSchema.begin(), ::toupper);

      if (processedSchemas.find(upperSchema) == processedSchemas.end()) {
        auto setupConn = getOracleConnection(table.connection_string);
        if (!setupConn || !setupConn->isValid()) {
          Logger::error(LogCategory::TRANSFER,
                        "setupTableTargetOracleToPostgres",
                        "Failed to get Oracle connection for schema " +
                            table.schema_name);
          continue;
        }

        std::string createSchemaQuery =
            "BEGIN "
            "EXECUTE IMMEDIATE 'CREATE USER datasync_metadata IDENTIFIED BY "
            "datasync_metadata DEFAULT TABLESPACE USERS'; "
            "EXCEPTION WHEN OTHERS THEN NULL; "
            "END;";
        executeQueryOracle(setupConn.get(), createSchemaQuery);

        std::string grantQuery =
            "BEGIN "
            "EXECUTE IMMEDIATE 'GRANT CONNECT, RESOURCE TO datasync_metadata'; "
            "EXECUTE IMMEDIATE 'GRANT EXECUTE ON DBMS_CRYPTO TO "
            "datasync_metadata'; "
            "EXCEPTION WHEN OTHERS THEN NULL; "
            "END;";
        executeQueryOracle(setupConn.get(), grantQuery);

        std::string createTableQuery =
            "BEGIN "
            "EXECUTE IMMEDIATE 'CREATE TABLE datasync_metadata.ds_change_log ("
            "change_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY, "
            "change_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
            "operation CHAR(1) NOT NULL, "
            "schema_name VARCHAR2(255) NOT NULL, "
            "table_name VARCHAR2(255) NOT NULL, "
            "pk_values CLOB NOT NULL, "
            "row_data CLOB NOT NULL)'; "
            "EXCEPTION WHEN OTHERS THEN NULL; "
            "END;";
        executeQueryOracle(setupConn.get(), createTableQuery);

        std::string createIndex1Query =
            "BEGIN "
            "EXECUTE IMMEDIATE 'CREATE INDEX idx_ds_change_log_table_time ON "
            "datasync_metadata.ds_change_log (schema_name, table_name, "
            "change_time)'; "
            "EXCEPTION WHEN OTHERS THEN NULL; "
            "END;";
        executeQueryOracle(setupConn.get(), createIndex1Query);

        std::string createIndex2Query =
            "BEGIN "
            "EXECUTE IMMEDIATE 'CREATE INDEX idx_ds_change_log_table_change ON "
            "datasync_metadata.ds_change_log (schema_name, table_name, "
            "change_id)'; "
            "EXCEPTION WHEN OTHERS THEN NULL; "
            "END;";
        executeQueryOracle(setupConn.get(), createIndex2Query);

        Logger::info(LogCategory::TRANSFER, "setupTableTargetOracleToPostgres",
                     "Ensured datasync_metadata user and ds_change_log table "
                     "exist");

        processedSchemas.insert(upperSchema);
      }

      auto oracleConn = getOracleConnection(table.connection_string);
      if (!oracleConn || !oracleConn->isValid()) {
        Logger::error(LogCategory::TRANSFER, "setupTableTargetOracleToPostgres",
                      "Failed to get Oracle connection for table " +
                          table.schema_name + "." + table.table_name);
        continue;
      }

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

      std::string allColumnsQuery =
          "SELECT column_name FROM all_tab_columns WHERE UPPER(owner) = '" +
          escapeOracleValue(upperSchema) + "' AND UPPER(table_name) = '" +
          escapeOracleValue(upperTable) + "' ORDER BY column_id";
      auto allColumns = executeQueryOracle(oracleConn.get(), allColumnsQuery);

      if (allColumns.empty()) {
        Logger::warning(LogCategory::TRANSFER,
                        "setupTableTargetOracleToPostgres",
                        "No columns found for " + table.schema_name + "." +
                            table.table_name + " - skipping trigger creation");
        continue;
      }

      bool hasPK = !primaryKeys.empty();
      std::string jsonObjectNew;
      std::string jsonObjectOld;

      if (hasPK) {
        jsonObjectNew = "'{' || ";
        jsonObjectOld = "'{' || ";
        for (size_t i = 0; i < primaryKeys.size(); ++i) {
          if (i > 0) {
            jsonObjectNew += "',' || ";
            jsonObjectOld += "',' || ";
          }
          std::string upperPk = primaryKeys[i];
          std::transform(upperPk.begin(), upperPk.end(), upperPk.begin(),
                         ::toupper);
          jsonObjectNew +=
              "'\"" + primaryKeys[i] +
              "\":\"' || "
              "REPLACE(REPLACE(REPLACE(TO_CHAR(:NEW." +
              upperPk +
              "), '\\', '\\\\'), '\"', '\\\"'), CHR(10), '\\n') || "
              "'\"'";
          jsonObjectOld +=
              "'\"" + primaryKeys[i] +
              "\":\"' || "
              "REPLACE(REPLACE(REPLACE(TO_CHAR(:OLD." +
              upperPk +
              "), '\\', '\\\\'), '\"', '\\\"'), CHR(10), '\\n') || "
              "'\"'";
        }
        jsonObjectNew += " || '}'";
        jsonObjectOld += " || '}'";
      } else {
        std::string upperColFirst = allColumns[0][0];
        std::transform(upperColFirst.begin(), upperColFirst.end(),
                       upperColFirst.begin(), ::toupper);
        std::string concatFieldsNew =
            "NVL(TO_CHAR(:NEW." + upperColFirst + "), '')";
        std::string concatFieldsOld =
            "NVL(TO_CHAR(:OLD." + upperColFirst + "), '')";
        for (size_t i = 1; i < allColumns.size(); ++i) {
          std::string upperCol = allColumns[i][0];
          std::transform(upperCol.begin(), upperCol.end(), upperCol.begin(),
                         ::toupper);
          concatFieldsNew +=
              " || '|' || NVL(TO_CHAR(:NEW." + upperCol + "), '')";
          concatFieldsOld +=
              " || '|' || NVL(TO_CHAR(:OLD." + upperCol + "), '')";
        }
        jsonObjectNew = "'{\"_hash\":\"' || "
                        "RAWTOHEX(DBMS_CRYPTO.HASH(UTL_RAW.CAST_TO_RAW(" +
                        concatFieldsNew + "), 2)) || '\"}'";
        jsonObjectOld = "'{\"_hash\":\"' || "
                        "RAWTOHEX(DBMS_CRYPTO.HASH(UTL_RAW.CAST_TO_RAW(" +
                        concatFieldsOld + "), 2)) || '\"}'";
      }

      std::string rowDataNew = "'{' || ";
      std::string rowDataOld = "'{' || ";
      for (size_t i = 0; i < allColumns.size(); ++i) {
        if (i > 0) {
          rowDataNew += "',' || ";
          rowDataOld += "',' || ";
        }
        std::string colName = allColumns[i][0];
        std::string upperCol = colName;
        std::transform(upperCol.begin(), upperCol.end(), upperCol.begin(),
                       ::toupper);
        rowDataNew += "'\"" + colName +
                      "\":\"' || "
                      "REPLACE(REPLACE(REPLACE(TO_CHAR(:NEW." +
                      upperCol +
                      "), '\\', '\\\\'), '\"', '\\\"'), CHR(10), '\\n') || "
                      "'\"'";
        rowDataOld += "'\"" + colName +
                      "\":\"' || "
                      "REPLACE(REPLACE(REPLACE(TO_CHAR(:OLD." +
                      upperCol +
                      "), '\\', '\\\\'), '\"', '\\\"'), CHR(10), '\\n') || "
                      "'\"'";
      }
      rowDataNew += " || '}'";
      rowDataOld += " || '}'";

      std::string triggerInsert =
          "ds_tr_" + upperSchema + "_" + upperTable + "_ai";
      std::string triggerUpdate =
          "ds_tr_" + upperSchema + "_" + upperTable + "_au";
      std::string triggerDelete =
          "ds_tr_" + upperSchema + "_" + upperTable + "_ad";

      std::string dropInsert =
          "DROP TRIGGER " + upperSchema + "." + triggerInsert;
      std::string dropUpdate =
          "DROP TRIGGER " + upperSchema + "." + triggerUpdate;
      std::string dropDelete =
          "DROP TRIGGER " + upperSchema + "." + triggerDelete;

      try {
        executeQueryOracle(oracleConn.get(), dropInsert);
      } catch (...) {
      }
      try {
        executeQueryOracle(oracleConn.get(), dropUpdate);
      } catch (...) {
      }
      try {
        executeQueryOracle(oracleConn.get(), dropDelete);
      } catch (...) {
      }

      std::string createInsertTrigger =
          "CREATE OR REPLACE TRIGGER " + upperSchema + "." + triggerInsert +
          " AFTER INSERT ON " + upperSchema + "." + upperTable +
          " FOR EACH ROW "
          "BEGIN "
          "INSERT INTO datasync_metadata.ds_change_log "
          "(operation, schema_name, table_name, pk_values, row_data) "
          "VALUES ('I', '" +
          escapeOracleValue(table.schema_name) + "', '" +
          escapeOracleValue(table.table_name) + "', " + jsonObjectNew + ", " +
          rowDataNew +
          "); "
          "END;";

      std::string createUpdateTrigger =
          "CREATE OR REPLACE TRIGGER " + upperSchema + "." + triggerUpdate +
          " AFTER UPDATE ON " + upperSchema + "." + upperTable +
          " FOR EACH ROW "
          "BEGIN "
          "INSERT INTO datasync_metadata.ds_change_log "
          "(operation, schema_name, table_name, pk_values, row_data) "
          "VALUES ('U', '" +
          escapeOracleValue(table.schema_name) + "', '" +
          escapeOracleValue(table.table_name) + "', " + jsonObjectNew + ", " +
          rowDataNew +
          "); "
          "END;";

      std::string createDeleteTrigger =
          "CREATE OR REPLACE TRIGGER " + upperSchema + "." + triggerDelete +
          " AFTER DELETE ON " + upperSchema + "." + upperTable +
          " FOR EACH ROW "
          "BEGIN "
          "INSERT INTO datasync_metadata.ds_change_log "
          "(operation, schema_name, table_name, pk_values, row_data) "
          "VALUES ('D', '" +
          escapeOracleValue(table.schema_name) + "', '" +
          escapeOracleValue(table.table_name) + "', " + jsonObjectOld + ", " +
          rowDataOld +
          "); "
          "END;";

      executeQueryOracle(oracleConn.get(), createInsertTrigger);
      executeQueryOracle(oracleConn.get(), createUpdateTrigger);
      executeQueryOracle(oracleConn.get(), createDeleteTrigger);

      Logger::info(LogCategory::TRANSFER, "setupTableTargetOracleToPostgres",
                   "Created CDC triggers for " + table.schema_name + "." +
                       table.table_name +
                       (hasPK ? " (with PK)" : " (no PK, using hash)"));

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

      try {
        OracleEngine engine(table.connection_string);
        std::vector<ColumnInfo> sourceColumns =
            engine.getTableColumns(table.schema_name, table.table_name);

        if (!sourceColumns.empty()) {
          SchemaSync::syncSchema(pgConn, table.schema_name, table.table_name,
                                 sourceColumns, "Oracle");
        }
      } catch (const std::exception &e) {
        Logger::warning(LogCategory::TRANSFER, "transferDataOracleToPostgres",
                        "Error syncing schema for " + table.schema_name + "." +
                            table.table_name + ": " + std::string(e.what()) +
                            " - continuing with sync");
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
          truncateTxn.exec("UPDATE metadata.catalog SET "
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

      std::string pkStrategy =
          getPKStrategyFromCatalog(pgConn, schema_name, table_name);

      Logger::info(LogCategory::TRANSFER, "transferDataOracleToPostgres",
                   "Starting data transfer for " + schema_name + "." +
                       table_name + " - strategy=" + pkStrategy +
                       ", status=" + table.status);

      if (pkStrategy == "CDC" && table.status != "FULL_LOAD") {
        Logger::info(LogCategory::TRANSFER, "transferDataOracleToPostgres",
                     "CDC strategy detected - using processTableCDC for " +
                         schema_name + "." + table_name);
        processTableCDC(table, pgConn);
        return;
      }

      std::vector<std::string> pkColumns =
          getPKColumnsFromCatalog(pgConn, schema_name, table_name);

      bool hasMoreData = sourceCount > targetCount;
      size_t chunkNumber = 0;
      size_t lastProcessedOffset = 0;
      size_t rawChunkSize = SyncConfig::getChunkSize();
      const size_t CHUNK_SIZE =
          (rawChunkSize == 0 || rawChunkSize > 10000) ? 1000 : rawChunkSize;

      while (hasMoreData) {
        chunkNumber++;
        std::string selectQuery =
            "SELECT * FROM " + upperSchema + "." + upperTable;

        if (!pkColumns.empty()) {
          std::string upperPkCol = pkColumns[0];
          std::transform(upperPkCol.begin(), upperPkCol.end(),
                         upperPkCol.begin(), ::toupper);
          selectQuery += " ORDER BY " + upperPkCol;
        } else {
          selectQuery += " ORDER BY ROWID";
        }
        selectQuery += " OFFSET " + std::to_string(lastProcessedOffset) +
                       " ROWS FETCH NEXT " + std::to_string(CHUNK_SIZE) +
                       " ROWS ONLY";

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

          targetCount += results.size();
          lastProcessedOffset += results.size();

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

      if (targetCount >= sourceCount) {
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

void OracleToPostgres::processTableCDC(const TableInfo &table,
                                       pqxx::connection &pgConn) {
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
          " AND db_engine='Oracle'";
      auto res = txn.exec(query);
      txn.commit();

      if (!res.empty() && !res[0][0].is_null()) {
        std::string value = res[0][0].as<std::string>();
        if (!value.empty() && value.size() <= 20) {
          try {
            lastChangeId = std::stoll(value);
          } catch (const std::exception &e) {
            Logger::error(LogCategory::TRANSFER, "processTableCDC",
                          "Failed to parse last_change_id for " +
                              table.schema_name + "." + table.table_name +
                              ": " + std::string(e.what()));
            lastChangeId = 0;
          }
        }
      }
    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER, "processTableCDC",
                    "Error getting last_change_id for " + table.schema_name +
                        "." + table.table_name + ": " + std::string(e.what()));
      lastChangeId = 0;
    }

    std::vector<std::string> pkColumns =
        getPKColumnsFromCatalog(pgConn, table.schema_name, table.table_name);
    bool hasPK = !pkColumns.empty();

    auto oracleConn = getOracleConnection(table.connection_string);
    if (!oracleConn || !oracleConn->isValid()) {
      Logger::error(LogCategory::TRANSFER, "processTableCDC",
                    "Failed to get Oracle connection for " + table.schema_name +
                        "." + table.table_name);
      return;
    }

    std::string upperSchema = table.schema_name;
    std::transform(upperSchema.begin(), upperSchema.end(), upperSchema.begin(),
                   ::toupper);
    std::string upperTable = table.table_name;
    std::transform(upperTable.begin(), upperTable.end(), upperTable.begin(),
                   ::toupper);

    std::string columnQuery =
        "SELECT column_name FROM all_tab_columns WHERE UPPER(owner) = '" +
        escapeOracleValue(upperSchema) + "' AND UPPER(table_name) = '" +
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
      Logger::error(LogCategory::TRANSFER, "processTableCDC",
                    "No column names found for " + table.schema_name + "." +
                        table.table_name);
      return;
    }

    std::vector<std::string> columnTypes;
    columnTypes.reserve(columnNames.size());
    for (const auto &colName : columnNames) {
      columnTypes.push_back("TEXT");
    }

    bool hasMore = true;
    size_t batchNumber = 0;

    while (hasMore) {
      batchNumber++;
      std::string query = "SELECT change_id, operation, pk_values, row_data "
                          "FROM datasync_metadata.ds_change_log WHERE "
                          "schema_name='" +
                          escapeOracleValue(table.schema_name) +
                          "' AND table_name='" +
                          escapeOracleValue(table.table_name) +
                          "' AND change_id > " + std::to_string(lastChangeId) +
                          " ORDER BY change_id OFFSET 0 ROWS "
                          "FETCH NEXT " +
                          std::to_string(CHUNK_SIZE) + " ROWS ONLY";

      std::vector<std::vector<std::string>> rows =
          executeQueryOracle(oracleConn.get(), query);

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
                        "Failed to parse change_id for " + table.schema_name +
                            "." + table.table_name + ": " +
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
                  record.reserve(columnNames.size());

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
              }
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
                                  "Failed to parse row_data: " +
                                      std::string(e.what()));
                }
              }

              if (!useRowData) {
                Logger::warning(
                    LogCategory::TRANSFER, "processTableCDC",
                    "row_data not available for table without PK: " +
                        table.schema_name + "." + table.table_name);
              }
            }
          } else {
            if (!hasPK) {
              Logger::warning(
                  LogCategory::TRANSFER, "processTableCDC",
                  "Table " + table.schema_name + "." + table.table_name +
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
                  Logger::warning(
                      LogCategory::TRANSFER, "processTableCDC",
                      "Failed to parse row_data for " + table.schema_name +
                          "." + table.table_name + ": " +
                          std::string(e.what()) + ", falling back to SELECT");
                }
              }

              if (!useRowData) {
                std::string whereClause = "";
                for (size_t i = 0; i < pkColumns.size(); ++i) {
                  if (i > 0) {
                    whereClause += " AND ";
                  }
                  std::string pkValue = pkValues[i];
                  std::string upperPk = pkColumns[i];
                  std::transform(upperPk.begin(), upperPk.end(),
                                 upperPk.begin(), ::toupper);
                  if (pkValue == "NULL") {
                    whereClause += upperPk + " IS NULL";
                  } else {
                    whereClause +=
                        upperPk + " = '" + escapeOracleValue(pkValue) + "'";
                  }
                }

                std::string selectQuery = "SELECT * FROM " + upperSchema + "." +
                                          upperTable + " WHERE " + whereClause +
                                          " AND ROWNUM <= 1";

                std::vector<std::vector<std::string>> recordResult =
                    executeQueryOracle(oracleConn.get(), selectQuery);

                if (!recordResult.empty() &&
                    recordResult[0].size() == columnNames.size()) {
                  recordsToUpsert.push_back(recordResult[0]);
                } else {
                  Logger::warning(LogCategory::TRANSFER, "processTableCDC",
                                  "Record not found in source for " +
                                      table.schema_name + "." +
                                      table.table_name + " operation " + op +
                                      " with PK: " + pkJson);
                }
              }
            }
          }
        } catch (const std::exception &e) {
          Logger::error(LogCategory::TRANSFER, "processTableCDC",
                        "Failed to process change for " + table.schema_name +
                            "." + table.table_name + ": " +
                            std::string(e.what()));
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
                              lowerSchemaName, lowerTableName,
                              table.schema_name);
          }
          upsertedCount = recordsToUpsert.size();
        } catch (const std::exception &e) {
          Logger::error(LogCategory::TRANSFER, "processTableCDC",
                        "Failed to upsert records for " + table.schema_name +
                            "." + table.table_name + ": " +
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
              " AND db_engine='Oracle'";
          txn.exec(updateQuery);
          txn.commit();
          lastChangeId = maxChangeId;
        } catch (const std::exception &e) {
          Logger::error(LogCategory::TRANSFER, "processTableCDC",
                        "Error updating last_change_id for " +
                            table.schema_name + "." + table.table_name + ": " +
                            std::string(e.what()));
        }
      }

      Logger::info(
          LogCategory::TRANSFER, "processTableCDC",
          "Processed CDC batch " + std::to_string(batchNumber) + " for " +
              table.schema_name + "." + table.table_name + " with " +
              std::to_string(rows.size()) +
              " changes: " + std::to_string(upsertedCount) + " upserts, " +
              std::to_string(deletedCount) +
              " deletes; last_change_id=" + std::to_string(lastChangeId));

      if (rows.size() < CHUNK_SIZE) {
        hasMore = false;
      }
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "processTableCDC",
                  "Error in CDC processing for " + table.schema_name + "." +
                      table.table_name + ": " + std::string(e.what()));
  }
}
