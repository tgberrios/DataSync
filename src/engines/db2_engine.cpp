#include "engines/db2_engine.h"
#include "sync/DB2ToPostgres.h"
#include <algorithm>
#include <chrono>
#include <pqxx/pqxx>
#include <thread>
#include <unordered_set>

DB2ODBCConnection::DB2ODBCConnection(const std::string &connectionString) {
  SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env_);
  if (!SQL_SUCCEEDED(ret)) {
    Logger::error(LogCategory::DATABASE, "DB2ODBCConnection",
                  "Failed to allocate environment handle");
    return;
  }

  ret = SQLSetEnvAttr(env_, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
  if (!SQL_SUCCEEDED(ret)) {
    SQLFreeHandle(SQL_HANDLE_ENV, env_);
    env_ = SQL_NULL_HANDLE;
    Logger::error(LogCategory::DATABASE, "DB2ODBCConnection",
                  "Failed to set ODBC version");
    return;
  }

  ret = SQLAllocHandle(SQL_HANDLE_DBC, env_, &dbc_);
  if (!SQL_SUCCEEDED(ret)) {
    SQLFreeHandle(SQL_HANDLE_ENV, env_);
    env_ = SQL_NULL_HANDLE;
    Logger::error(LogCategory::DATABASE, "DB2ODBCConnection",
                  "Failed to allocate connection handle");
    return;
  }

  SQLCHAR outConnStr[DatabaseDefaults::BUFFER_SIZE];
  SQLSMALLINT outConnStrLen;
  ret = SQLDriverConnect(dbc_, nullptr, (SQLCHAR *)connectionString.c_str(),
                         SQL_NTS, outConnStr, sizeof(outConnStr),
                         &outConnStrLen, SQL_DRIVER_NOPROMPT);
  if (!SQL_SUCCEEDED(ret)) {
    SQLCHAR sqlState[6], msg[SQL_MAX_MESSAGE_LENGTH];
    SQLINTEGER nativeError;
    SQLSMALLINT msgLen;
    SQLGetDiagRec(SQL_HANDLE_DBC, dbc_, 1, sqlState, &nativeError, msg,
                  sizeof(msg), &msgLen);
    Logger::error(LogCategory::DATABASE, "DB2ODBCConnection",
                  "Connection failed: " + std::string((char *)msg));
    SQLFreeHandle(SQL_HANDLE_DBC, dbc_);
    SQLFreeHandle(SQL_HANDLE_ENV, env_);
    dbc_ = SQL_NULL_HANDLE;
    env_ = SQL_NULL_HANDLE;
    return;
  }

  valid_ = true;
}

DB2ODBCConnection::~DB2ODBCConnection() {
  if (dbc_ != SQL_NULL_HANDLE) {
    SQLDisconnect(dbc_);
    SQLFreeHandle(SQL_HANDLE_DBC, dbc_);
  }
  if (env_ != SQL_NULL_HANDLE) {
    SQLFreeHandle(SQL_HANDLE_ENV, env_);
  }
}

DB2ODBCConnection::DB2ODBCConnection(DB2ODBCConnection &&other) noexcept
    : env_(other.env_), dbc_(other.dbc_), valid_(other.valid_) {
  other.env_ = SQL_NULL_HANDLE;
  other.dbc_ = SQL_NULL_HANDLE;
  other.valid_ = false;
}

DB2ODBCConnection &
DB2ODBCConnection::operator=(DB2ODBCConnection &&other) noexcept {
  if (this != &other) {
    if (dbc_ != SQL_NULL_HANDLE) {
      SQLDisconnect(dbc_);
      SQLFreeHandle(SQL_HANDLE_DBC, dbc_);
    }
    if (env_ != SQL_NULL_HANDLE) {
      SQLFreeHandle(SQL_HANDLE_ENV, env_);
    }

    env_ = other.env_;
    dbc_ = other.dbc_;
    valid_ = other.valid_;

    other.env_ = SQL_NULL_HANDLE;
    other.dbc_ = SQL_NULL_HANDLE;
    other.valid_ = false;
  }
  return *this;
}

DB2Engine::DB2Engine(std::string connectionString)
    : connectionString_(std::move(connectionString)) {}

std::unique_ptr<DB2ODBCConnection> DB2Engine::createConnection() {
  const int MAX_RETRIES = 3;
  const int INITIAL_BACKOFF_MS = 100;

  for (int attempt = 1; attempt <= MAX_RETRIES; ++attempt) {
    auto conn = std::make_unique<DB2ODBCConnection>(connectionString_);
    if (conn->isValid()) {
      if (attempt > 1) {
        Logger::info(LogCategory::DATABASE, "DB2Engine",
                     "Connection successful on attempt " +
                         std::to_string(attempt));
      }
      return conn;
    }

    if (attempt < MAX_RETRIES) {
      int backoffMs = INITIAL_BACKOFF_MS * (1 << (attempt - 1));
      Logger::warning(LogCategory::DATABASE, "DB2Engine",
                      "Connection attempt " + std::to_string(attempt) +
                          " failed, retrying in " + std::to_string(backoffMs) +
                          "ms...");
      std::this_thread::sleep_for(std::chrono::milliseconds(backoffMs));
    }
  }

  Logger::error(LogCategory::DATABASE, "DB2Engine",
                "Failed to connect after " + std::to_string(MAX_RETRIES) +
                    " attempts");
  return nullptr;
}

std::vector<std::vector<std::string>>
DB2Engine::executeQuery(SQLHDBC dbc, const std::string &query) {
  std::vector<std::vector<std::string>> results;
  if (!dbc)
    return results;

  SQLHSTMT stmt;
  SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
  if (!SQL_SUCCEEDED(ret)) {
    Logger::error(LogCategory::DATABASE, "DB2Engine",
                  "Failed to allocate statement handle");
    return results;
  }

  ret = SQLExecDirect(stmt, (SQLCHAR *)query.c_str(), SQL_NTS);
  if (!SQL_SUCCEEDED(ret)) {
    Logger::error(LogCategory::DATABASE, "DB2Engine", "Query execution failed");
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    return results;
  }

  SQLSMALLINT numCols = 0;
  ret = SQLNumResultCols(stmt, &numCols);
  if (!SQL_SUCCEEDED(ret) || numCols <= 0) {
    Logger::error(LogCategory::DATABASE, "DB2Engine",
                  "SQLNumResultCols failed or no columns");
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    return results;
  }

  SQLRETURN fetchRet;
  while ((fetchRet = SQLFetch(stmt)) == SQL_SUCCESS ||
         fetchRet == SQL_SUCCESS_WITH_INFO) {
    std::vector<std::string> row;
    for (SQLSMALLINT i = 1; i <= numCols; i++) {
      std::string cellValue;
      SQLLEN totalLen = 0;
      SQLLEN len = 0;
      constexpr SQLLEN CHUNK_SIZE = DatabaseDefaults::BUFFER_SIZE - 1;
      char buffer[DatabaseDefaults::BUFFER_SIZE];

      do {
        ret = SQLGetData(stmt, i, SQL_C_CHAR, buffer, sizeof(buffer), &len);
        if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
          if (len == SQL_NULL_DATA) {
            cellValue = "NULL";
            break;
          } else if (len > 0) {
            SQLLEN copyLen = (len < CHUNK_SIZE) ? len : CHUNK_SIZE;
            cellValue.append(buffer, copyLen);
            totalLen += copyLen;
            if (len < CHUNK_SIZE) {
              break;
            }
          } else {
            break;
          }
        } else if (ret == SQL_NO_DATA) {
          break;
        } else {
          cellValue = "NULL";
          break;
        }
      } while (ret == SQL_SUCCESS_WITH_INFO);

      if (cellValue.empty() && totalLen == 0) {
        cellValue = "NULL";
      }
      row.push_back(cellValue);
    }
    results.push_back(std::move(row));
  }

  if (fetchRet != SQL_NO_DATA && !SQL_SUCCEEDED(fetchRet)) {
    SQLCHAR sqlState[6], msg[SQL_MAX_MESSAGE_LENGTH];
    SQLINTEGER nativeError;
    SQLSMALLINT msgLen;
    SQLGetDiagRec(SQL_HANDLE_STMT, stmt, 1, sqlState, &nativeError, msg,
                  sizeof(msg), &msgLen);
    Logger::error(LogCategory::DATABASE, "DB2Engine",
                  "SQLFetch failed: " + std::string((char *)msg));
  }

  SQLFreeHandle(SQL_HANDLE_STMT, stmt);
  return results;
}

std::vector<CatalogTableInfo> DB2Engine::discoverTables() {
  std::vector<CatalogTableInfo> tables;
  auto conn = createConnection();
  if (!conn)
    return tables;

  std::string query =
      "SELECT TRIM(TABSCHEMA) AS table_schema, TRIM(TABNAME) AS table_name "
      "FROM SYSCAT.TABLES "
      "WHERE TYPE = 'T' "
      "AND TABSCHEMA NOT IN ('SYSIBM', 'SYSCAT', 'SYSFUN', 'SYSIBMADM', "
      "'SYSPROC', 'SYSPUBLIC', 'SYSTOOLS') "
      "AND TABSCHEMA NOT LIKE 'SYS%' "
      "ORDER BY TABSCHEMA, TABNAME";

  auto results = executeQuery(conn->getDbc(), query);
  for (const auto &row : results) {
    if (row.size() >= 2)
      tables.push_back({row[0], row[1], connectionString_});
  }
  return tables;
}

std::vector<std::string> DB2Engine::detectPrimaryKey(const std::string &schema,
                                                     const std::string &table) {
  if (schema.empty() || table.empty()) {
    Logger::error(LogCategory::DATABASE, "DB2Engine",
                  "detectPrimaryKey: schema and table must not be empty");
    return {};
  }

  std::vector<std::string> pkColumns;
  auto conn = createConnection();
  if (!conn) {
    Logger::error(LogCategory::DATABASE, "DB2Engine",
                  "detectPrimaryKey: Failed to connect for " + schema + "." +
                      table);
    return pkColumns;
  }

  std::string safeSchema;
  for (char c : schema) {
    if (c == '\'') {
      safeSchema += "''";
    } else if (c >= 32 && c <= 126 && c != ';' && c != '\\' && c != '/') {
      safeSchema += c;
    }
  }
  if (safeSchema.empty()) {
    Logger::error(LogCategory::DATABASE, "DB2Engine",
                  "detectPrimaryKey: sanitized schema is empty");
    return {};
  }

  std::string safeTable;
  for (char c : table) {
    if (c == '\'') {
      safeTable += "''";
    } else if (c >= 32 && c <= 126 && c != ';' && c != '\\' && c != '/') {
      safeTable += c;
    }
  }
  if (safeTable.empty()) {
    Logger::error(LogCategory::DATABASE, "DB2Engine",
                  "detectPrimaryKey: sanitized table is empty");
    return {};
  }

  std::string query = "SELECT TRIM(COLNAME) AS COLUMN_NAME "
                      "FROM SYSCAT.KEYCOLUSE kc "
                      "INNER JOIN SYSCAT.TABLES t ON kc.TABSCHEMA = "
                      "t.TABSCHEMA AND kc.TABNAME = t.TABNAME "
                      "INNER JOIN SYSCAT.INDEXES i ON kc.INDSCHEMA = "
                      "i.INDSCHEMA AND kc.INDNAME = i.INDNAME "
                      "WHERE t.TABSCHEMA = '" +
                      safeSchema + "' AND t.TABNAME = '" + safeTable +
                      "' AND i.UNIQUERULE = 'P' "
                      "ORDER BY kc.COLSEQ";

  auto results = executeQuery(conn->getDbc(), query);
  for (const auto &row : results) {
    if (!row.empty() && !row[0].empty() && row[0] != "NULL")
      pkColumns.push_back(row[0]);
  }
  return pkColumns;
}

std::string DB2Engine::detectTimeColumn(const std::string &schema,
                                        const std::string &table) {
  if (schema.empty() || table.empty()) {
    Logger::error(LogCategory::DATABASE, "DB2Engine",
                  "detectTimeColumn: schema and table must not be empty");
    return "";
  }

  auto conn = createConnection();
  if (!conn) {
    Logger::error(LogCategory::DATABASE, "DB2Engine",
                  "detectTimeColumn: Failed to connect for " + schema + "." +
                      table);
    return "";
  }

  std::string safeSchema;
  for (char c : schema) {
    if (c == '\'') {
      safeSchema += "''";
    } else if (c >= 32 && c <= 126 && c != ';' && c != '\\' && c != '/') {
      safeSchema += c;
    }
  }
  if (safeSchema.empty()) {
    Logger::error(LogCategory::DATABASE, "DB2Engine",
                  "detectTimeColumn: sanitized schema is empty");
    return "";
  }

  std::string safeTable;
  for (char c : table) {
    if (c == '\'') {
      safeTable += "''";
    } else if (c >= 32 && c <= 126 && c != ';' && c != '\\' && c != '/') {
      safeTable += c;
    }
  }
  if (safeTable.empty()) {
    Logger::error(LogCategory::DATABASE, "DB2Engine",
                  "detectTimeColumn: sanitized table is empty");
    return "";
  }

  std::string candidates;
  for (size_t i = 0; i < DatabaseDefaults::TIME_COLUMN_COUNT; ++i) {
    if (i > 0)
      candidates += ", ";
    candidates +=
        std::string("'") + DatabaseDefaults::TIME_COLUMN_CANDIDATES[i] + "'";
  }

  std::string query = "SELECT TRIM(COLNAME) AS COLUMN_NAME "
                      "FROM SYSCAT.COLUMNS "
                      "WHERE TABSCHEMA = '" +
                      safeSchema + "' AND TABNAME = '" + safeTable +
                      "' AND COLNAME IN (" + candidates +
                      ") ORDER BY CASE COLNAME ";

  for (size_t i = 0; i < DatabaseDefaults::TIME_COLUMN_COUNT; ++i) {
    query += "WHEN '" +
             std::string(DatabaseDefaults::TIME_COLUMN_CANDIDATES[i]) +
             "' THEN " + std::to_string(i + 1) + " ";
  }
  query += "ELSE " + std::to_string(DatabaseDefaults::TIME_COLUMN_COUNT + 1) +
           " END";

  auto results = executeQuery(conn->getDbc(), query);
  if (!results.empty() && !results[0].empty() && results[0][0] != "NULL")
    return results[0][0];

  return "";
}

std::pair<int, int>
DB2Engine::getColumnCounts(const std::string &schema, const std::string &table,
                           const std::string &targetConnStr) {
  if (schema.empty() || table.empty()) {
    Logger::error(LogCategory::DATABASE, "DB2Engine",
                  "getColumnCounts: schema and table must not be empty");
    return {0, 0};
  }

  auto conn = createConnection();
  if (!conn) {
    Logger::error(LogCategory::DATABASE, "DB2Engine",
                  "getColumnCounts: Failed to connect for " + schema + "." +
                      table);
    return {0, 0};
  }

  std::string safeSchema;
  for (char c : schema) {
    if (c == '\'') {
      safeSchema += "''";
    } else if (c >= 32 && c <= 126 && c != ';' && c != '\\' && c != '/') {
      safeSchema += c;
    }
  }
  if (safeSchema.empty()) {
    Logger::error(LogCategory::DATABASE, "DB2Engine",
                  "getColumnCounts: sanitized schema is empty");
    return {0, 0};
  }

  std::string safeTable;
  for (char c : table) {
    if (c == '\'') {
      safeTable += "''";
    } else if (c >= 32 && c <= 126 && c != ';' && c != '\\' && c != '/') {
      safeTable += c;
    }
  }
  if (safeTable.empty()) {
    Logger::error(LogCategory::DATABASE, "DB2Engine",
                  "getColumnCounts: sanitized table is empty");
    return {0, 0};
  }

  std::string sourceQuery = "SELECT COUNT(*) FROM SYSCAT.COLUMNS "
                            "WHERE TABSCHEMA = '" +
                            safeSchema + "' AND TABNAME = '" + safeTable + "'";

  auto sourceResults = executeQuery(conn->getDbc(), sourceQuery);
  int sourceCount = 0;
  if (!sourceResults.empty() && !sourceResults[0].empty() &&
      sourceResults[0][0] != "NULL") {
    try {
      sourceCount = std::stoi(sourceResults[0][0]);
    } catch (const std::exception &e) {
      Logger::error(LogCategory::DATABASE, "DB2Engine",
                    "Failed to parse source column count: " +
                        std::string(e.what()));
      sourceCount = 0;
    } catch (...) {
      Logger::error(LogCategory::DATABASE, "DB2Engine",
                    "Failed to parse source column count: unknown error");
      sourceCount = 0;
    }
  }

  try {
    pqxx::connection pgConn(targetConnStr);
    pqxx::work txn(pgConn);

    std::string lowerSchema = schema;
    std::transform(lowerSchema.begin(), lowerSchema.end(), lowerSchema.begin(),
                   ::tolower);
    std::string lowerTable = table;
    std::transform(lowerTable.begin(), lowerTable.end(), lowerTable.begin(),
                   ::tolower);

    auto targetResults =
        txn.exec_params("SELECT COUNT(*) FROM information_schema.columns "
                        "WHERE table_schema = $1 AND table_name = $2",
                        lowerSchema, lowerTable);
    txn.commit();

    int targetCount = 0;
    if (!targetResults.empty() && !targetResults[0][0].is_null())
      targetCount = targetResults[0][0].as<int>();

    return {sourceCount, targetCount};
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "DB2Engine",
                  "Error getting target column count for " + schema + "." +
                      table + ": " + std::string(e.what()));
    return {sourceCount, 0};
  }
}

std::vector<ColumnInfo> DB2Engine::getTableColumns(const std::string &schema,
                                                   const std::string &table) {
  std::vector<ColumnInfo> columns;
  if (schema.empty() || table.empty()) {
    Logger::error(LogCategory::DATABASE, "DB2Engine",
                  "getTableColumns: schema and table must not be empty");
    return columns;
  }

  auto conn = createConnection();
  if (!conn || !conn->isValid()) {
    Logger::error(LogCategory::DATABASE, "DB2Engine",
                  "getTableColumns: connection is invalid");
    return columns;
  }

  std::string safeSchema;
  for (char c : schema) {
    if (c == '\'') {
      safeSchema += "''";
    } else if (c >= 32 && c <= 126 && c != ';' && c != '\\' && c != '/') {
      safeSchema += c;
    }
  }
  if (safeSchema.empty()) {
    Logger::error(LogCategory::DATABASE, "DB2Engine",
                  "getTableColumns: sanitized schema is empty");
    return columns;
  }

  std::string safeTable;
  for (char c : table) {
    if (c == '\'') {
      safeTable += "''";
    } else if (c >= 32 && c <= 126 && c != ';' && c != '\\' && c != '/') {
      safeTable += c;
    }
  }
  if (safeTable.empty()) {
    Logger::error(LogCategory::DATABASE, "DB2Engine",
                  "getTableColumns: sanitized table is empty");
    return columns;
  }

  std::string query =
      "SELECT TRIM(COLNAME) AS COLUMN_NAME, TRIM(TYPENAME) AS DATA_TYPE, "
      "CASE WHEN NULLS = 'Y' THEN 'YES' ELSE 'NO' END as IS_NULLABLE, "
      "CASE WHEN KEYSEQ IS NOT NULL THEN 'YES' ELSE 'NO' END as "
      "IS_PRIMARY_KEY, "
      "LENGTH AS CHARACTER_MAXIMUM_LENGTH, "
      "SCALE AS NUMERIC_SCALE, "
      "COLSIZE AS NUMERIC_PRECISION, "
      "COLNO AS ORDINAL_POSITION "
      "FROM SYSCAT.COLUMNS c "
      "LEFT JOIN ( "
      "  SELECT COLNAME, TABSCHEMA, TABNAME, KEYSEQ "
      "  FROM SYSCAT.KEYCOLUSE kc "
      "  INNER JOIN SYSCAT.INDEXES i ON kc.INDSCHEMA = i.INDSCHEMA AND "
      "kc.INDNAME = i.INDNAME "
      "  WHERE i.UNIQUERULE = 'P' "
      ") pk ON c.COLNAME = pk.COLNAME AND c.TABSCHEMA = pk.TABSCHEMA AND "
      "c.TABNAME = pk.TABNAME "
      "WHERE c.TABSCHEMA = '" +
      safeSchema + "' AND c.TABNAME = '" + safeTable + "' ORDER BY c.COLNO";

  auto results = executeQuery(conn->getDbc(), query);

  for (const auto &row : results) {
    if (row.size() < 8)
      continue;

    ColumnInfo col;
    col.name = row[0];
    std::transform(col.name.begin(), col.name.end(), col.name.begin(),
                   ::tolower);
    col.dataType = row[1];
    col.isNullable = (row[2] == "YES");
    col.isPrimaryKey = (row[3] == "YES");
    col.maxLength = row.size() > 4 ? row[4] : "";
    col.numericScale = row.size() > 5 ? row[5] : "";
    col.numericPrecision = row.size() > 6 ? row[6] : "";
    try {
      col.ordinalPosition = std::stoi(row[7]);
    } catch (...) {
      col.ordinalPosition = 0;
    }

    std::string pgType = "TEXT";
    if (col.dataType == "DECIMAL" || col.dataType == "NUMERIC") {
      if (!col.numericPrecision.empty() && col.numericPrecision != "NULL" &&
          !col.numericScale.empty() && col.numericScale != "NULL") {
        pgType =
            "NUMERIC(" + col.numericPrecision + "," + col.numericScale + ")";
      } else {
        pgType = "NUMERIC(18,4)";
      }
    } else if (col.dataType == "VARCHAR" ||
               col.dataType == "CHARACTER VARYING") {
      if (!col.maxLength.empty() && col.maxLength != "NULL" &&
          col.maxLength != "-1") {
        pgType = "VARCHAR(" + col.maxLength + ")";
      } else {
        pgType = "VARCHAR";
      }
    } else if (col.dataType == "CHAR" || col.dataType == "CHARACTER") {
      pgType = "TEXT";
    } else if (DB2ToPostgres::dataTypeMap.count(col.dataType)) {
      pgType = DB2ToPostgres::dataTypeMap[col.dataType];
    }

    col.pgType = pgType;
    columns.push_back(col);
  }

  return columns;
}
