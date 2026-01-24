#include "engines/as400_engine.h"
#include "core/logger.h"
#include "core/Config.h"
#include "utils/connection_utils.h"
#include <thread>
#include <chrono>

AS400ODBCConnection::AS400ODBCConnection(const std::string &connectionString) {
  SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env_);
  if (!SQL_SUCCEEDED(ret)) {
    Logger::error(LogCategory::DATABASE, "AS400ODBCConnection",
                  "Failed to allocate environment handle");
    return;
  }

  ret = SQLSetEnvAttr(env_, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
  if (!SQL_SUCCEEDED(ret)) {
    SQLFreeHandle(SQL_HANDLE_ENV, env_);
    env_ = SQL_NULL_HANDLE;
    Logger::error(LogCategory::DATABASE, "AS400ODBCConnection",
                  "Failed to set ODBC version");
    return;
  }

  ret = SQLAllocHandle(SQL_HANDLE_DBC, env_, &dbc_);
  if (!SQL_SUCCEEDED(ret)) {
    SQLFreeHandle(SQL_HANDLE_ENV, env_);
    env_ = SQL_NULL_HANDLE;
    Logger::error(LogCategory::DATABASE, "AS400ODBCConnection",
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
    Logger::error(LogCategory::DATABASE, "AS400ODBCConnection",
                  "Connection failed: " + std::string((char *)msg));
    SQLFreeHandle(SQL_HANDLE_DBC, dbc_);
    SQLFreeHandle(SQL_HANDLE_ENV, env_);
    dbc_ = SQL_NULL_HANDLE;
    env_ = SQL_NULL_HANDLE;
    return;
  }

  valid_ = true;
}

AS400ODBCConnection::~AS400ODBCConnection() {
  if (dbc_ != SQL_NULL_HANDLE) {
    SQLDisconnect(dbc_);
    SQLFreeHandle(SQL_HANDLE_DBC, dbc_);
  }
  if (env_ != SQL_NULL_HANDLE) {
    SQLFreeHandle(SQL_HANDLE_ENV, env_);
  }
}

AS400ODBCConnection::AS400ODBCConnection(AS400ODBCConnection &&other) noexcept
    : env_(other.env_), dbc_(other.dbc_), valid_(other.valid_) {
  other.env_ = SQL_NULL_HANDLE;
  other.dbc_ = SQL_NULL_HANDLE;
  other.valid_ = false;
}

AS400ODBCConnection &
AS400ODBCConnection::operator=(AS400ODBCConnection &&other) noexcept {
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

AS400Engine::AS400Engine(std::string connectionString)
    : connectionString_(std::move(connectionString)) {}

std::unique_ptr<AS400ODBCConnection> AS400Engine::createConnection() {
  const int MAX_RETRIES = 3;
  const int INITIAL_BACKOFF_MS = 100;

  for (int attempt = 1; attempt <= MAX_RETRIES; ++attempt) {
    auto conn = std::make_unique<AS400ODBCConnection>(connectionString_);
    if (conn->isValid()) {
      if (attempt > 1) {
        Logger::info(LogCategory::DATABASE, "AS400Engine",
                     "Connection successful on attempt " +
                         std::to_string(attempt));
      }
      return conn;
    }

    if (attempt < MAX_RETRIES) {
      int backoffMs = INITIAL_BACKOFF_MS * (1 << (attempt - 1));
      Logger::warning(LogCategory::DATABASE, "AS400Engine",
                      "Connection attempt " + std::to_string(attempt) +
                          " failed, retrying in " + std::to_string(backoffMs) +
                          "ms...");
      std::this_thread::sleep_for(std::chrono::milliseconds(backoffMs));
    }
  }

  Logger::error(LogCategory::DATABASE, "AS400Engine",
                "Failed to connect after " + std::to_string(MAX_RETRIES) +
                    " attempts");
  return nullptr;
}

std::vector<std::vector<std::string>>
AS400Engine::executeQuery(SQLHDBC dbc, const std::string &query) {
  std::vector<std::vector<std::string>> results;
  if (!dbc)
    return results;

  SQLHSTMT stmt;
  SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
  if (!SQL_SUCCEEDED(ret)) {
    Logger::error(LogCategory::DATABASE, "AS400Engine",
                  "Failed to allocate statement handle");
    return results;
  }

  ret = SQLExecDirect(stmt, (SQLCHAR *)query.c_str(), SQL_NTS);
  if (!SQL_SUCCEEDED(ret)) {
    Logger::error(LogCategory::DATABASE, "AS400Engine", "Query execution failed");
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    return results;
  }

  SQLSMALLINT numCols = 0;
  ret = SQLNumResultCols(stmt, &numCols);
  if (!SQL_SUCCEEDED(ret) || numCols <= 0) {
    Logger::error(LogCategory::DATABASE, "AS400Engine",
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
    Logger::error(LogCategory::DATABASE, "AS400Engine",
                  "SQLFetch failed: " + std::string((char *)msg));
  }

  SQLFreeHandle(SQL_HANDLE_STMT, stmt);
  return results;
}

std::vector<CatalogTableInfo> AS400Engine::discoverTables() {
  std::vector<CatalogTableInfo> tables;
  auto conn = createConnection();
  if (!conn)
    return tables;

  std::string query =
      "SELECT TABLE_SCHEMA, TABLE_NAME "
      "FROM QSYS2.SYSTABLES "
      "WHERE TABLE_TYPE = 'T' "
      "ORDER BY TABLE_SCHEMA, TABLE_NAME";

  auto results = executeQuery(conn->getDbc(), query);
  for (const auto &row : results) {
    if (row.size() >= 2)
      tables.push_back({row[0], row[1], connectionString_});
  }
  return tables;
}

std::vector<std::string> AS400Engine::detectPrimaryKey(const std::string &schema,
                                                      const std::string &table) {
  if (schema.empty() || table.empty()) {
    Logger::error(LogCategory::DATABASE, "AS400Engine",
                  "detectPrimaryKey: schema and table must not be empty");
    return {};
  }

  std::vector<std::string> pkColumns;
  auto conn = createConnection();
  if (!conn)
    return pkColumns;

  std::string query =
      "SELECT COLUMN_NAME "
      "FROM QSYS2.SYSKEYCST "
      "WHERE TABLE_SCHEMA = '" + schema + "' "
      "AND TABLE_NAME = '" + table + "' "
      "ORDER BY ORDINAL_POSITION";

  auto results = executeQuery(conn->getDbc(), query);
  for (const auto &row : results) {
    if (!row.empty())
      pkColumns.push_back(row[0]);
  }
  return pkColumns;
}

std::string AS400Engine::detectTimeColumn(const std::string &schema,
                                         const std::string &table) {
  if (schema.empty() || table.empty()) {
    Logger::error(LogCategory::DATABASE, "AS400Engine",
                  "detectTimeColumn: schema and table must not be empty");
    return "";
  }

  auto conn = createConnection();
  if (!conn)
    return "";

  std::string query =
      "SELECT COLUMN_NAME "
      "FROM QSYS2.SYSCOLUMNS "
      "WHERE TABLE_SCHEMA = '" + schema + "' "
      "AND TABLE_NAME = '" + table + "' "
      "AND (COLUMN_NAME LIKE '%TIME%' OR COLUMN_NAME LIKE '%DATE%' "
      "OR COLUMN_NAME LIKE '%STAMP%' OR DATA_TYPE LIKE '%TIMESTAMP%') "
      "ORDER BY COLUMN_NAME "
      "FETCH FIRST 1 ROW ONLY";

  auto results = executeQuery(conn->getDbc(), query);
  if (!results.empty() && !results[0].empty())
    return results[0][0];
  return "";
}

std::pair<int, int> AS400Engine::getColumnCounts(const std::string &schema,
                                                 const std::string &table,
                                                 const std::string &targetConnStr) {
  (void)targetConnStr; // Unused parameter
  if (schema.empty() || table.empty()) {
    Logger::error(LogCategory::DATABASE, "AS400Engine",
                  "getColumnCounts: schema and table must not be empty");
    return {0, 0};
  }

  auto conn = createConnection();
  if (!conn)
    return {0, 0};

  std::string query =
      "SELECT COUNT(*) "
      "FROM QSYS2.SYSCOLUMNS "
      "WHERE TABLE_SCHEMA = '" + schema + "' "
      "AND TABLE_NAME = '" + table + "'";

  auto results = executeQuery(conn->getDbc(), query);
  if (!results.empty() && !results[0].empty()) {
    int count = std::stoi(results[0][0]);
    return {count, count};
  }
  return {0, 0};
}
