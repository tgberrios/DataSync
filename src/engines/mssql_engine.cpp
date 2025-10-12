#include "engines/mssql_engine.h"
#include <algorithm>
#include <pqxx/pqxx>

ODBCConnection::ODBCConnection(const std::string &connectionString) {
  SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env_);
  if (!SQL_SUCCEEDED(ret)) {
    Logger::error(LogCategory::DATABASE, "ODBCConnection",
                  "Failed to allocate environment handle");
    return;
  }

  ret = SQLSetEnvAttr(env_, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
  if (!SQL_SUCCEEDED(ret)) {
    SQLFreeHandle(SQL_HANDLE_ENV, env_);
    env_ = SQL_NULL_HANDLE;
    Logger::error(LogCategory::DATABASE, "ODBCConnection",
                  "Failed to set ODBC version");
    return;
  }

  ret = SQLAllocHandle(SQL_HANDLE_DBC, env_, &dbc_);
  if (!SQL_SUCCEEDED(ret)) {
    SQLFreeHandle(SQL_HANDLE_ENV, env_);
    env_ = SQL_NULL_HANDLE;
    Logger::error(LogCategory::DATABASE, "ODBCConnection",
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
    Logger::error(LogCategory::DATABASE, "ODBCConnection",
                  "Connection failed: " + std::string((char *)msg));
    SQLFreeHandle(SQL_HANDLE_DBC, dbc_);
    SQLFreeHandle(SQL_HANDLE_ENV, env_);
    dbc_ = SQL_NULL_HANDLE;
    env_ = SQL_NULL_HANDLE;
    return;
  }

  valid_ = true;
}

ODBCConnection::~ODBCConnection() {
  if (dbc_ != SQL_NULL_HANDLE) {
    SQLDisconnect(dbc_);
    SQLFreeHandle(SQL_HANDLE_DBC, dbc_);
  }
  if (env_ != SQL_NULL_HANDLE) {
    SQLFreeHandle(SQL_HANDLE_ENV, env_);
  }
}

ODBCConnection::ODBCConnection(ODBCConnection &&other) noexcept
    : env_(other.env_), dbc_(other.dbc_), valid_(other.valid_) {
  other.env_ = SQL_NULL_HANDLE;
  other.dbc_ = SQL_NULL_HANDLE;
  other.valid_ = false;
}

ODBCConnection &ODBCConnection::operator=(ODBCConnection &&other) noexcept {
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

MSSQLEngine::MSSQLEngine(std::string connectionString)
    : connectionString_(std::move(connectionString)) {}

std::unique_ptr<ODBCConnection> MSSQLEngine::createConnection() {
  auto conn = std::make_unique<ODBCConnection>(connectionString_);
  if (!conn->isValid())
    return nullptr;
  return conn;
}

std::vector<std::vector<std::string>>
MSSQLEngine::executeQuery(SQLHDBC dbc, const std::string &query) {
  std::vector<std::vector<std::string>> results;
  if (!dbc)
    return results;

  SQLHSTMT stmt;
  SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
  if (!SQL_SUCCEEDED(ret)) {
    Logger::error(LogCategory::DATABASE, "MSSQLEngine",
                  "Failed to allocate statement handle");
    return results;
  }

  ret = SQLExecDirect(stmt, (SQLCHAR *)query.c_str(), SQL_NTS);
  if (!SQL_SUCCEEDED(ret)) {
    Logger::error(LogCategory::DATABASE, "MSSQLEngine",
                  "Query execution failed");
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    return results;
  }

  SQLSMALLINT numCols;
  SQLNumResultCols(stmt, &numCols);

  while (SQLFetch(stmt) == SQL_SUCCESS) {
    std::vector<std::string> row;
    for (SQLSMALLINT i = 1; i <= numCols; i++) {
      char buffer[DatabaseDefaults::BUFFER_SIZE];
      SQLLEN len;
      ret = SQLGetData(stmt, i, SQL_C_CHAR, buffer, sizeof(buffer), &len);
      if (SQL_SUCCEEDED(ret)) {
        if (len == SQL_NULL_DATA)
          row.push_back("NULL");
        else
          row.push_back(std::string(buffer, len));
      } else {
        row.push_back("NULL");
      }
    }
    results.push_back(std::move(row));
  }

  SQLFreeHandle(SQL_HANDLE_STMT, stmt);
  return results;
}

std::string
MSSQLEngine::extractDatabaseName(const std::string &connectionString) {
  auto params = ConnectionStringParser::parse(connectionString);
  return params ? params->db : "master";
}

std::vector<CatalogTableInfo> MSSQLEngine::discoverTables() {
  std::vector<CatalogTableInfo> tables;
  auto conn = createConnection();
  if (!conn)
    return tables;

  std::string query =
      "SELECT s.name AS table_schema, t.name AS table_name "
      "FROM sys.tables t "
      "INNER JOIN sys.schemas s ON t.schema_id = s.schema_id "
      "WHERE s.name NOT IN ('INFORMATION_SCHEMA', 'sys', 'guest') "
      "AND t.name NOT LIKE 'spt_%' AND t.name NOT LIKE 'MS%' "
      "AND t.name NOT LIKE 'sp_%' AND t.name NOT LIKE 'fn_%' "
      "AND t.name NOT LIKE 'xp_%' AND t.name NOT LIKE 'dt_%' "
      "ORDER BY s.name, t.name";

  auto results = executeQuery(conn->getDbc(), query);
  for (const auto &row : results) {
    if (row.size() >= 2)
      tables.push_back({row[0], row[1], connectionString_});
  }
  return tables;
}

std::vector<std::string>
MSSQLEngine::detectPrimaryKey(const std::string &schema,
                              const std::string &table) {
  std::vector<std::string> pkColumns;
  auto conn = createConnection();
  if (!conn)
    return pkColumns;

  std::string safeSchema = schema;
  std::string safeTable = table;
  safeSchema.erase(
      std::remove_if(safeSchema.begin(), safeSchema.end(),
                     [](char c) { return c == '\'' || c == ';' || c == '-'; }),
      safeSchema.end());
  safeTable.erase(
      std::remove_if(safeTable.begin(), safeTable.end(),
                     [](char c) { return c == '\'' || c == ';' || c == '-'; }),
      safeTable.end());

  std::string query =
      "SELECT c.name AS COLUMN_NAME "
      "FROM sys.columns c "
      "INNER JOIN sys.tables t ON c.object_id = t.object_id "
      "INNER JOIN sys.schemas s ON t.schema_id = s.schema_id "
      "INNER JOIN sys.index_columns ic ON c.object_id = ic.object_id AND "
      "c.column_id = ic.column_id "
      "INNER JOIN sys.indexes i ON ic.object_id = i.object_id AND "
      "ic.index_id = i.index_id "
      "WHERE s.name = '" +
      safeSchema + "' AND t.name = '" + safeTable +
      "' AND i.is_primary_key = 1 "
      "ORDER BY ic.key_ordinal";

  auto results = executeQuery(conn->getDbc(), query);
  for (const auto &row : results) {
    if (!row.empty() && !row[0].empty() && row[0] != "NULL")
      pkColumns.push_back(row[0]);
  }
  return pkColumns;
}

std::string MSSQLEngine::detectTimeColumn(const std::string &schema,
                                          const std::string &table) {
  auto conn = createConnection();
  if (!conn)
    return "";

  std::string safeSchema = schema;
  std::string safeTable = table;
  safeSchema.erase(
      std::remove_if(safeSchema.begin(), safeSchema.end(),
                     [](char c) { return c == '\'' || c == ';' || c == '-'; }),
      safeSchema.end());
  safeTable.erase(
      std::remove_if(safeTable.begin(), safeTable.end(),
                     [](char c) { return c == '\'' || c == ';' || c == '-'; }),
      safeTable.end());

  std::string candidates;
  for (size_t i = 0; i < DatabaseDefaults::TIME_COLUMN_COUNT; ++i) {
    if (i > 0)
      candidates += ", ";
    candidates +=
        std::string("'") + DatabaseDefaults::TIME_COLUMN_CANDIDATES[i] + "'";
  }

  std::string query = "SELECT c.name AS COLUMN_NAME "
                      "FROM sys.columns c "
                      "INNER JOIN sys.tables t ON c.object_id = t.object_id "
                      "INNER JOIN sys.schemas s ON t.schema_id = s.schema_id "
                      "WHERE s.name = '" +
                      safeSchema + "' AND t.name = '" + safeTable +
                      "' AND c.name IN (" + candidates +
                      ") ORDER BY CASE c.name ";

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
MSSQLEngine::getColumnCounts(const std::string &schema,
                             const std::string &table,
                             const std::string &targetConnStr) {
  auto conn = createConnection();
  if (!conn)
    return {0, 0};

  std::string safeSchema = schema;
  std::string safeTable = table;
  safeSchema.erase(
      std::remove_if(safeSchema.begin(), safeSchema.end(),
                     [](char c) { return c == '\'' || c == ';' || c == '-'; }),
      safeSchema.end());
  safeTable.erase(
      std::remove_if(safeTable.begin(), safeTable.end(),
                     [](char c) { return c == '\'' || c == ';' || c == '-'; }),
      safeTable.end());

  std::string sourceQuery =
      "SELECT COUNT(*) FROM sys.columns c "
      "INNER JOIN sys.tables t ON c.object_id = t.object_id "
      "INNER JOIN sys.schemas s ON t.schema_id = s.schema_id "
      "WHERE s.name = '" +
      safeSchema + "' AND t.name = '" + safeTable + "'";

  auto sourceResults = executeQuery(conn->getDbc(), sourceQuery);
  int sourceCount = 0;
  if (!sourceResults.empty() && !sourceResults[0].empty() &&
      sourceResults[0][0] != "NULL") {
    try {
      sourceCount = std::stoi(sourceResults[0][0]);
    } catch (...) {
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
    Logger::error(LogCategory::DATABASE, "MSSQLEngine",
                  "Error getting target column count: " +
                      std::string(e.what()));
    return {sourceCount, 0};
  }
}
