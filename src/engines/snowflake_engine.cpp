#include "engines/snowflake_engine.h"
#include "third_party/json.hpp"
#include <algorithm>
#include <sstream>

SnowflakeEngine::SnowflakeEngine(std::string connectionString)
    : connectionString_(std::move(connectionString)) {}

SnowflakeEngine::~SnowflakeEngine() = default;

std::unique_ptr<SnowflakeODBCHandles> SnowflakeEngine::createConnection() {
  auto handles = std::make_unique<SnowflakeODBCHandles>();
  handles->env = SQL_NULL_HANDLE;
  handles->dbc = SQL_NULL_HANDLE;

  SQLRETURN ret =
      SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &handles->env);
  if (!SQL_SUCCEEDED(ret)) {
    Logger::error(LogCategory::TRANSFER, "SnowflakeEngine::createConnection",
                  "Failed to allocate environment handle");
    return nullptr;
  }

  ret = SQLSetEnvAttr(handles->env, SQL_ATTR_ODBC_VERSION,
                      (SQLPOINTER)SQL_OV_ODBC3, 0);
  if (!SQL_SUCCEEDED(ret)) {
    SQLFreeHandle(SQL_HANDLE_ENV, handles->env);
    Logger::error(LogCategory::TRANSFER, "SnowflakeEngine::createConnection",
                  "Failed to set ODBC version");
    return nullptr;
  }

  ret = SQLAllocHandle(SQL_HANDLE_DBC, handles->env, &handles->dbc);
  if (!SQL_SUCCEEDED(ret)) {
    SQLFreeHandle(SQL_HANDLE_ENV, handles->env);
    Logger::error(LogCategory::TRANSFER, "SnowflakeEngine::createConnection",
                  "Failed to allocate connection handle");
    return nullptr;
  }

  SQLCHAR outConnStr[1024];
  SQLSMALLINT outConnStrLen;
  ret = SQLDriverConnect(
      handles->dbc, nullptr, (SQLCHAR *)connectionString_.c_str(), SQL_NTS,
      outConnStr, sizeof(outConnStr), &outConnStrLen, SQL_DRIVER_NOPROMPT);
  if (!SQL_SUCCEEDED(ret)) {
    SQLCHAR sqlState[6], msg[SQL_MAX_MESSAGE_LENGTH];
    SQLINTEGER nativeError;
    SQLSMALLINT msgLen;
    SQLGetDiagRec(SQL_HANDLE_DBC, handles->dbc, 1, sqlState, &nativeError, msg,
                  sizeof(msg), &msgLen);
    Logger::error(LogCategory::TRANSFER, "SnowflakeEngine::createConnection",
                  "Connection failed: " + std::string((char *)msg));
    cleanupConnection(handles->env, handles->dbc);
    return nullptr;
  }

  return handles;
}

void SnowflakeEngine::cleanupConnection(SQLHENV env, SQLHDBC dbc) {
  if (dbc != SQL_NULL_HANDLE) {
    SQLDisconnect(dbc);
    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
  }
  if (env != SQL_NULL_HANDLE) {
    SQLFreeHandle(SQL_HANDLE_ENV, env);
  }
}

bool SnowflakeEngine::testConnection() {
  try {
    auto handles = createConnection();
    if (!handles) {
      return false;
    }
    executeODBCQuery(handles->dbc, "SELECT 1");
    cleanupConnection(handles->env, handles->dbc);
    return true;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "SnowflakeEngine::testConnection",
                  "Connection test failed: " + std::string(e.what()));
    return false;
  }
}

std::string SnowflakeEngine::mapDataType(const std::string &dataType) {
  std::string upperType = dataType;
  std::transform(upperType.begin(), upperType.end(), upperType.begin(),
                 ::toupper);

  if (upperType.find("VARCHAR") != std::string::npos ||
      upperType.find("CHAR") != std::string::npos ||
      upperType.find("TEXT") != std::string::npos) {
    return "VARCHAR";
  }
  if (upperType.find("INTEGER") != std::string::npos ||
      upperType.find("INT") != std::string::npos) {
    return "INTEGER";
  }
  if (upperType.find("BIGINT") != std::string::npos) {
    return "BIGINT";
  }
  if (upperType.find("DECIMAL") != std::string::npos ||
      upperType.find("NUMERIC") != std::string::npos) {
    return "NUMBER(38,2)";
  }
  if (upperType.find("DOUBLE") != std::string::npos ||
      upperType.find("FLOAT") != std::string::npos ||
      upperType.find("REAL") != std::string::npos) {
    return "FLOAT";
  }
  if (upperType.find("BOOLEAN") != std::string::npos ||
      upperType.find("BOOL") != std::string::npos) {
    return "BOOLEAN";
  }
  if (upperType.find("DATE") != std::string::npos) {
    return "DATE";
  }
  if (upperType.find("TIMESTAMP") != std::string::npos ||
      upperType.find("DATETIME") != std::string::npos) {
    return "TIMESTAMP_NTZ";
  }
  if (upperType.find("JSON") != std::string::npos ||
      upperType.find("JSONB") != std::string::npos) {
    return "VARIANT";
  }

  return "VARCHAR";
}

void SnowflakeEngine::executeODBCQuery(SQLHDBC dbc, const std::string &query) {
  SQLHSTMT stmt;
  SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
  if (!SQL_SUCCEEDED(ret)) {
    throw std::runtime_error("Failed to allocate statement handle");
  }

  ret = SQLExecDirect(stmt, (SQLCHAR *)query.c_str(), SQL_NTS);
  if (!SQL_SUCCEEDED(ret)) {
    SQLCHAR sqlState[6], msg[SQL_MAX_MESSAGE_LENGTH];
    SQLINTEGER nativeError;
    SQLSMALLINT msgLen;
    SQLGetDiagRec(SQL_HANDLE_STMT, stmt, 1, sqlState, &nativeError, msg,
                  sizeof(msg), &msgLen);
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    throw std::runtime_error("Query execution failed: " +
                             std::string((char *)msg));
  }

  SQLFreeHandle(SQL_HANDLE_STMT, stmt);
}

std::vector<std::vector<std::string>>
SnowflakeEngine::executeODBCSelect(SQLHDBC dbc, const std::string &query) {
  std::vector<std::vector<std::string>> results;
  SQLHSTMT stmt;
  SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
  if (!SQL_SUCCEEDED(ret)) {
    return results;
  }

  ret = SQLExecDirect(stmt, (SQLCHAR *)query.c_str(), SQL_NTS);
  if (!SQL_SUCCEEDED(ret)) {
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    return results;
  }

  SQLSMALLINT numCols;
  SQLNumResultCols(stmt, &numCols);

  while (SQL_SUCCEEDED(SQLFetch(stmt))) {
    std::vector<std::string> row;
    for (SQLSMALLINT i = 1; i <= numCols; ++i) {
      char buffer[4096];
      SQLLEN indicator;
      ret = SQLGetData(stmt, i, SQL_C_CHAR, buffer, sizeof(buffer), &indicator);
      if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
        if (indicator == SQL_NULL_DATA) {
          row.push_back("");
        } else {
          row.push_back(std::string(buffer));
        }
      } else {
        row.push_back("");
      }
    }
    results.push_back(row);
  }

  SQLFreeHandle(SQL_HANDLE_STMT, stmt);
  return results;
}

void SnowflakeEngine::createSchema(const std::string &schemaName) {
  try {
    auto handles = createConnection();
    if (!handles) {
      throw std::runtime_error("Failed to create connection");
    }

    std::string upperSchema = schemaName;
    std::transform(upperSchema.begin(), upperSchema.end(), upperSchema.begin(),
                   ::toupper);
    std::string query =
        "CREATE SCHEMA IF NOT EXISTS " + quoteIdentifier(upperSchema);
    executeODBCQuery(handles->dbc, query);
    cleanupConnection(handles->env, handles->dbc);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "SnowflakeEngine::createSchema",
                  "Error creating schema: " + std::string(e.what()));
    throw;
  }
}

void SnowflakeEngine::createTable(
    const std::string &schemaName, const std::string &tableName,
    const std::vector<WarehouseColumnInfo> &columns,
    const std::vector<std::string> &primaryKeys) {
  try {
    auto handles = createConnection();
    if (!handles) {
      throw std::runtime_error("Failed to create connection");
    }

    std::string upperSchema = schemaName;
    std::transform(upperSchema.begin(), upperSchema.end(), upperSchema.begin(),
                   ::toupper);
    std::string upperTable = tableName;
    std::transform(upperTable.begin(), upperTable.end(), upperTable.begin(),
                   ::toupper);

    std::string createSQL = "CREATE TABLE IF NOT EXISTS " +
                            quoteIdentifier(upperSchema) + "." +
                            quoteIdentifier(upperTable) + " (";

    bool first = true;
    for (const auto &col : columns) {
      if (!first)
        createSQL += ", ";
      std::string upperCol = col.name;
      std::transform(upperCol.begin(), upperCol.end(), upperCol.begin(),
                     ::toupper);
      std::string dataType = mapDataType(col.data_type);
      std::string nullable = col.is_nullable ? "" : " NOT NULL";
      createSQL += quoteIdentifier(upperCol) + " " + dataType + nullable;
      first = false;
    }

    if (!primaryKeys.empty()) {
      createSQL += ", PRIMARY KEY (";
      for (size_t i = 0; i < primaryKeys.size(); ++i) {
        if (i > 0)
          createSQL += ", ";
        std::string upperPK = primaryKeys[i];
        std::transform(upperPK.begin(), upperPK.end(), upperPK.begin(),
                       ::toupper);
        createSQL += quoteIdentifier(upperPK);
      }
      createSQL += ")";
    }

    createSQL += ")";

    executeODBCQuery(handles->dbc, createSQL);
    cleanupConnection(handles->env, handles->dbc);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "SnowflakeEngine::createTable",
                  "Error creating table: " + std::string(e.what()));
    throw;
  }
}

void SnowflakeEngine::insertData(
    const std::string &schemaName, const std::string &tableName,
    const std::vector<std::string> &columns,
    const std::vector<std::vector<std::string>> &rows) {
  if (rows.empty())
    return;

  try {
    auto handles = createConnection();
    if (!handles) {
      throw std::runtime_error("Failed to create connection");
    }

    std::string upperSchema = schemaName;
    std::transform(upperSchema.begin(), upperSchema.end(), upperSchema.begin(),
                   ::toupper);
    std::string upperTable = tableName;
    std::transform(upperTable.begin(), upperTable.end(), upperTable.begin(),
                   ::toupper);

    const size_t BATCH_SIZE = 1000;
    for (size_t batchStart = 0; batchStart < rows.size();
         batchStart += BATCH_SIZE) {
      size_t batchEnd = std::min(batchStart + BATCH_SIZE, rows.size());

      std::string insertSQL = "INSERT INTO " + quoteIdentifier(upperSchema) +
                              "." + quoteIdentifier(upperTable) + " (";

      for (size_t i = 0; i < columns.size(); ++i) {
        if (i > 0)
          insertSQL += ", ";
        std::string upperCol = columns[i];
        std::transform(upperCol.begin(), upperCol.end(), upperCol.begin(),
                       ::toupper);
        insertSQL += quoteIdentifier(upperCol);
      }
      insertSQL += ") VALUES ";

      for (size_t rowIdx = batchStart; rowIdx < batchEnd; ++rowIdx) {
        if (rowIdx > batchStart)
          insertSQL += ", ";
        insertSQL += "(";
        for (size_t colIdx = 0; colIdx < columns.size(); ++colIdx) {
          if (colIdx > 0)
            insertSQL += ", ";
          if (colIdx < rows[rowIdx].size() && !rows[rowIdx][colIdx].empty()) {
            insertSQL += quoteValue(rows[rowIdx][colIdx]);
          } else {
            insertSQL += "NULL";
          }
        }
        insertSQL += ")";
      }

      executeODBCQuery(handles->dbc, insertSQL);
    }

    cleanupConnection(handles->env, handles->dbc);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "SnowflakeEngine::insertData",
                  "Error inserting data: " + std::string(e.what()));
    throw;
  }
}

void SnowflakeEngine::upsertData(
    const std::string &schemaName, const std::string &tableName,
    const std::vector<std::string> &columns,
    const std::vector<std::string> &primaryKeys,
    const std::vector<std::vector<std::string>> &rows) {
  if (rows.empty())
    return;

  try {
    auto handles = createConnection();
    if (!handles) {
      throw std::runtime_error("Failed to create connection");
    }

    std::string upperSchema = schemaName;
    std::transform(upperSchema.begin(), upperSchema.end(), upperSchema.begin(),
                   ::toupper);
    std::string upperTable = tableName;
    std::transform(upperTable.begin(), upperTable.end(), upperTable.begin(),
                   ::toupper);

    for (const auto &row : rows) {
      std::string mergeSQL = "MERGE INTO " + quoteIdentifier(upperSchema) +
                             "." + quoteIdentifier(upperTable) +
                             " AS target "
                             "USING (SELECT ";

      for (size_t i = 0; i < columns.size(); ++i) {
        if (i > 0)
          mergeSQL += ", ";
        if (i < row.size() && !row[i].empty()) {
          mergeSQL += quoteValue(row[i]) + " AS " + quoteIdentifier(columns[i]);
        } else {
          mergeSQL += "NULL AS " + quoteIdentifier(columns[i]);
        }
      }
      mergeSQL += ") AS source ON ";

      for (size_t i = 0; i < primaryKeys.size(); ++i) {
        if (i > 0)
          mergeSQL += " AND ";
        std::string upperPK = primaryKeys[i];
        std::transform(upperPK.begin(), upperPK.end(), upperPK.begin(),
                       ::toupper);
        mergeSQL += "target." + quoteIdentifier(upperPK) + " = source." +
                    quoteIdentifier(upperPK);
      }

      mergeSQL += " WHEN MATCHED THEN UPDATE SET ";
      for (size_t i = 0; i < columns.size(); ++i) {
        if (i > 0)
          mergeSQL += ", ";
        std::string upperCol = columns[i];
        std::transform(upperCol.begin(), upperCol.end(), upperCol.begin(),
                       ::toupper);
        mergeSQL += quoteIdentifier(upperCol) + " = source." +
                    quoteIdentifier(upperCol);
      }

      mergeSQL += " WHEN NOT MATCHED THEN INSERT (";
      for (size_t i = 0; i < columns.size(); ++i) {
        if (i > 0)
          mergeSQL += ", ";
        std::string upperCol = columns[i];
        std::transform(upperCol.begin(), upperCol.end(), upperCol.begin(),
                       ::toupper);
        mergeSQL += quoteIdentifier(upperCol);
      }
      mergeSQL += ") VALUES (";
      for (size_t i = 0; i < columns.size(); ++i) {
        if (i > 0)
          mergeSQL += ", ";
        mergeSQL += "source." + quoteIdentifier(columns[i]);
      }
      mergeSQL += ")";

      executeODBCQuery(handles->dbc, mergeSQL);
    }

    cleanupConnection(handles->env, handles->dbc);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "SnowflakeEngine::upsertData",
                  "Error upserting data: " + std::string(e.what()));
    throw;
  }
}

void SnowflakeEngine::createIndex(const std::string &schemaName,
                                  const std::string &tableName,
                                  const std::vector<std::string> &indexColumns,
                                  const std::string &indexName) {
  Logger::warning(LogCategory::TRANSFER, "SnowflakeEngine::createIndex",
                  "Snowflake uses automatic clustering. "
                  "Explicit indexes are not supported.");
}

void SnowflakeEngine::createPartition(const std::string &schemaName,
                                      const std::string &tableName,
                                      const std::string &partitionColumn) {
  Logger::warning(LogCategory::TRANSFER, "SnowflakeEngine::createPartition",
                  "Snowflake uses automatic partitioning. "
                  "Explicit partitioning is not needed.");
}

void SnowflakeEngine::executeStatement(const std::string &statement) {
  try {
    auto handles = createConnection();
    if (!handles) {
      throw std::runtime_error("Failed to create connection");
    }
    executeODBCQuery(handles->dbc, statement);
    cleanupConnection(handles->env, handles->dbc);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "SnowflakeEngine::executeStatement",
                  "Error executing statement: " + std::string(e.what()));
    throw;
  }
}

std::vector<json> SnowflakeEngine::executeQuery(const std::string &query) {
  std::vector<json> results;
  try {
    auto handles = createConnection();
    if (!handles) {
      throw std::runtime_error("Failed to create connection");
    }

    auto rows = executeODBCSelect(handles->dbc, query);
    cleanupConnection(handles->env, handles->dbc);

    if (rows.empty()) {
      return results;
    }

    for (const auto &row : rows) {
      json rowObj = json::object();
      for (size_t i = 0; i < row.size(); ++i) {
        std::string colName = "COL" + std::to_string(i);
        rowObj[colName] = row[i];
      }
      results.push_back(rowObj);
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "SnowflakeEngine::executeQuery",
                  "Error executing query: " + std::string(e.what()));
    throw;
  }
  return results;
}

std::string SnowflakeEngine::quoteIdentifier(const std::string &identifier) {
  return "\"" + identifier + "\"";
}

std::string SnowflakeEngine::quoteValue(const std::string &value) {
  std::string escaped = value;
  size_t pos = 0;
  while ((pos = escaped.find('\'', pos)) != std::string::npos) {
    escaped.replace(pos, 1, "''");
    pos += 2;
  }
  return "'" + escaped + "'";
}
