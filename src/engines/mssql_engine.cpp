#include "engines/mssql_engine.h"
#include <algorithm>
#include <chrono>
#include <pqxx/pqxx>
#include <thread>

// Constructor for ODBCConnection. Initializes an ODBC connection to a SQL
// Server database using the provided connection string. Allocates environment
// and connection handles, sets ODBC version to 3, and attempts to connect.
// If any step fails, logs an error, cleans up allocated handles, and sets
// valid_ to false. The connection must be checked with isValid() before use.
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

// Destructor for ODBCConnection. Disconnects from the database and frees all
// allocated ODBC handles (connection and environment). Safe to call even if
// the connection was never established or is already closed.
ODBCConnection::~ODBCConnection() {
  if (dbc_ != SQL_NULL_HANDLE) {
    SQLDisconnect(dbc_);
    SQLFreeHandle(SQL_HANDLE_DBC, dbc_);
  }
  if (env_ != SQL_NULL_HANDLE) {
    SQLFreeHandle(SQL_HANDLE_ENV, env_);
  }
}

// Move constructor for ODBCConnection. Transfers ownership of ODBC handles
// (environment and connection) from the source object. The source object's
// handles are set to SQL_NULL_HANDLE and valid_ is set to false to prevent
// double-free. This allows efficient transfer of connection objects without
// copying.
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

// Constructor for MSSQLEngine. Stores the connection string for later use when
// creating database connections. The connection string should be in ODBC
// format (e.g., "DRIVER={ODBC Driver 17 for SQL Server};SERVER=...;DATABASE=...;UID=...;PWD=...").
MSSQLEngine::MSSQLEngine(std::string connectionString)
    : connectionString_(std::move(connectionString)) {}

// Creates a new ODBC connection with retry logic. Attempts to establish a
// connection up to MAX_RETRIES times with exponential backoff (100ms, 200ms,
// 400ms). On successful connection, returns a unique_ptr to the connection.
// If all attempts fail, returns nullptr. This function is thread-safe and can
// be called multiple times to create independent connections.
std::unique_ptr<ODBCConnection> MSSQLEngine::createConnection() {
  const int MAX_RETRIES = 3;
  const int INITIAL_BACKOFF_MS = 100;

  for (int attempt = 1; attempt <= MAX_RETRIES; ++attempt) {
    auto conn = std::make_unique<ODBCConnection>(connectionString_);
    if (conn->isValid()) {
      if (attempt > 1) {
        Logger::info(LogCategory::DATABASE, "MSSQLEngine",
                     "Connection successful on attempt " +
                         std::to_string(attempt));
      }
      return conn;
    }

    if (attempt < MAX_RETRIES) {
      int backoffMs = INITIAL_BACKOFF_MS * (1 << (attempt - 1));
      Logger::warning(LogCategory::DATABASE, "MSSQLEngine",
                      "Connection attempt " + std::to_string(attempt) +
                          " failed, retrying in " + std::to_string(backoffMs) +
                          "ms...");
      std::this_thread::sleep_for(std::chrono::milliseconds(backoffMs));
    }
  }

  Logger::error(LogCategory::DATABASE, "MSSQLEngine",
                "Failed to connect after " + std::to_string(MAX_RETRIES) +
                    " attempts");
  return nullptr;
}

// Executes a SQL query on the provided ODBC connection and returns the
// results as a vector of rows, where each row is a vector of strings. Handles
// NULL values by converting them to the string "NULL". If the query fails or
// the connection handle is invalid, returns an empty vector and logs an
// error. This function stores the entire result set in memory, so it should
// not be used for very large result sets. The caller is responsible for
// ensuring the connection handle is valid.
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

// Extracts the database name from a connection string. Parses the connection
// string and returns the database name, or "master" if parsing fails or no
// Discovers all user tables in the SQL Server database. Queries sys.tables and
// sys.schemas to find all user tables, excluding system schemas
// (INFORMATION_SCHEMA, sys, guest) and system tables (spt_%, MS%, sp_%, fn_%,
// xp_%, dt_%). Returns a vector of CatalogTableInfo objects containing schema
// name, table name, and the connection string. If connection fails, returns
// an empty vector. This function is used during catalog synchronization to
// identify tables that should be synced.
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

// Detects the primary key columns for a given table. Queries sys.columns,
// sys.tables, sys.schemas, sys.index_columns, and sys.indexes to find all
// columns that are part of the PRIMARY KEY constraint, ordered by their key
// ordinal position. Sanitizes schema and table names by removing potentially
// dangerous characters (single quotes, semicolons, hyphens) to prevent SQL
// injection. Returns a vector of column names in the order they appear in the
// primary key. If no primary key exists or connection fails, returns an empty
// vector. If schema or table is empty, the behavior is undefined.
std::vector<std::string>
MSSQLEngine::detectPrimaryKey(const std::string &schema,
                              const std::string &table) {
  if (schema.empty() || table.empty()) {
    Logger::error(LogCategory::DATABASE, "MSSQLEngine",
                  "detectPrimaryKey: schema and table must not be empty");
    return {};
  }

  std::vector<std::string> pkColumns;
  auto conn = createConnection();
  if (!conn) {
    Logger::error(LogCategory::DATABASE, "MSSQLEngine",
                  "detectPrimaryKey: Failed to connect for " + schema + "." +
                      table);
    return pkColumns;
  }

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

// Detects a time-based column (e.g., updated_at, created_at) in the specified
// table. Searches for columns matching TIME_COLUMN_CANDIDATES in sys.columns,
// ordered by preference using a CASE statement. Sanitizes schema and table
// names to prevent SQL injection. Returns the first matching column name, or
// an empty string if no time column is found or connection fails. This column
// is used for incremental sync strategies. If schema or table is empty, the
// behavior is undefined.
std::string MSSQLEngine::detectTimeColumn(const std::string &schema,
                                          const std::string &table) {
  if (schema.empty() || table.empty()) {
    Logger::error(LogCategory::DATABASE, "MSSQLEngine",
                  "detectTimeColumn: schema and table must not be empty");
    return "";
  }

  auto conn = createConnection();
  if (!conn) {
    Logger::error(LogCategory::DATABASE, "MSSQLEngine",
                  "detectTimeColumn: Failed to connect for " + schema + "." +
                      table);
    return "";
  }

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

// Gets the column count for a table in both the source SQL Server database and
// the target PostgreSQL database. Queries sys.columns in SQL Server and
// information_schema.columns in PostgreSQL, and returns a pair where first is
// the source count and second is the target count. This is used to detect
// schema mismatches between source and target. Schema and table names are
// sanitized for SQL Server queries and converted to lowercase for PostgreSQL
// queries. If connection fails or query fails, returns {0, 0} for source and
// {sourceCount, 0} for target respectively. If schema or table is empty, the
// behavior is undefined.
std::pair<int, int>
MSSQLEngine::getColumnCounts(const std::string &schema,
                             const std::string &table,
                             const std::string &targetConnStr) {
  if (schema.empty() || table.empty()) {
    Logger::error(LogCategory::DATABASE, "MSSQLEngine",
                  "getColumnCounts: schema and table must not be empty");
    return {0, 0};
  }

  auto conn = createConnection();
  if (!conn) {
    Logger::error(LogCategory::DATABASE, "MSSQLEngine",
                  "getColumnCounts: Failed to connect for " + schema + "." +
                      table);
    return {0, 0};
  }

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
                  "Error getting target column count for " + schema + "." +
                      table + ": " + std::string(e.what()));
    return {sourceCount, 0};
  }
}
