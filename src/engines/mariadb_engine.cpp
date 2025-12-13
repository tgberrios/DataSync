#include "engines/mariadb_engine.h"
#include <algorithm>
#include <chrono>
#include <pqxx/pqxx>
#include <thread>

// Constructor for MySQLConnection. Initializes a MySQL connection using the
// provided connection parameters. Parses the port from the params, defaulting
// to DEFAULT_MYSQL_PORT if parsing fails or port is empty. Attempts to
// establish a connection to the MySQL/MariaDB server. If connection fails, logs
// an error and sets conn_ to nullptr. The connection must be checked with
// isValid() before use.
MySQLConnection::MySQLConnection(const ConnectionParams &params) {
  conn_ = mysql_init(nullptr);
  if (!conn_) {
    Logger::error(LogCategory::DATABASE, "MySQLConnection",
                  "mysql_init() failed");
    return;
  }

  unsigned int port = DatabaseDefaults::DEFAULT_MYSQL_PORT;
  if (!params.port.empty()) {
    try {
      port = std::stoul(params.port);
    } catch (...) {
      port = DatabaseDefaults::DEFAULT_MYSQL_PORT;
    }
  }

  if (mysql_real_connect(conn_, params.host.c_str(), params.user.c_str(),
                         params.password.c_str(), params.db.c_str(), port,
                         nullptr, 0) == nullptr) {
    Logger::error(LogCategory::DATABASE, "MySQLConnection",
                  "Connection failed: " + std::string(mysql_error(conn_)));
    mysql_close(conn_);
    conn_ = nullptr;
  }
}

// Destructor for MySQLConnection. Closes the MySQL connection if it is open.
// Safe to call even if the connection was never established or is already
// closed.
MySQLConnection::~MySQLConnection() {
  if (conn_)
    mysql_close(conn_);
}

// Move constructor for MySQLConnection. Transfers ownership of the MySQL
// connection handle from the source object. The source object's connection
// handle is set to nullptr to prevent double-free. This allows efficient
// transfer of connection objects without copying.
MySQLConnection::MySQLConnection(MySQLConnection &&other) noexcept
    : conn_(other.conn_) {
  other.conn_ = nullptr;
}

// Move assignment operator for MySQLConnection. Transfers ownership of the
// MySQL connection handle from the source object. If this object already has an
// open connection, it is closed before the transfer. The source object's
// connection handle is set to nullptr. Returns a reference to this object.
MySQLConnection &MySQLConnection::operator=(MySQLConnection &&other) noexcept {
  if (this != &other) {
    if (conn_)
      mysql_close(conn_);
    conn_ = other.conn_;
    other.conn_ = nullptr;
  }
  return *this;
}

// Constructor for MariaDBEngine. Stores the connection string for later use
// when creating database connections. The connection string should be in a
// format parseable by ConnectionStringParser (e.g.,
// "mysql://user:pass@host:port/db").
MariaDBEngine::MariaDBEngine(std::string connectionString)
    : connectionString_(std::move(connectionString)) {}

// Creates a new MySQL connection with retry logic. Parses the connection string
// and attempts to establish a connection up to MAX_RETRIES times with
// exponential backoff (100ms, 200ms, 400ms). On successful connection, sets
// connection timeouts and returns a unique_ptr to the connection. If all
// attempts fail, returns nullptr. This function is thread-safe and can be
// called multiple times to create independent connections.
std::unique_ptr<MySQLConnection> MariaDBEngine::createConnection() {
  auto params = ConnectionStringParser::parse(connectionString_);
  if (!params) {
    Logger::error(LogCategory::DATABASE, "MariaDBEngine",
                  "Invalid connection string");
    return nullptr;
  }

  const int MAX_RETRIES = 3;
  const int INITIAL_BACKOFF_MS = 100;

  for (int attempt = 1; attempt <= MAX_RETRIES; ++attempt) {
    auto conn = std::make_unique<MySQLConnection>(*params);
    if (conn->isValid()) {
      setConnectionTimeouts(conn->get());
      if (attempt > 1) {
        Logger::info(LogCategory::DATABASE, "MariaDBEngine",
                     "Connection successful on attempt " +
                         std::to_string(attempt));
      }
      return conn;
    }

    if (attempt < MAX_RETRIES) {
      int backoffMs = INITIAL_BACKOFF_MS * (1 << (attempt - 1));
      Logger::warning(LogCategory::DATABASE, "MariaDBEngine",
                      "Connection attempt " + std::to_string(attempt) +
                          " failed, retrying in " + std::to_string(backoffMs) +
                          "ms...");
      std::this_thread::sleep_for(std::chrono::milliseconds(backoffMs));
    }
  }

  Logger::error(LogCategory::DATABASE, "MariaDBEngine",
                "Failed to connect after " + std::to_string(MAX_RETRIES) +
                    " attempts");
  return nullptr;
}

// Sets various timeout parameters on a MySQL connection to prevent long-running
// queries from hanging. Configures wait_timeout, interactive_timeout,
// net_read_timeout, net_write_timeout, innodb_lock_wait_timeout, and
// lock_wait_timeout to MARIADB_TIMEOUT_SECONDS. If setting timeouts fails, logs
// a warning but does not fail the connection. This function should be called
// immediately after establishing a connection.
void MariaDBEngine::setConnectionTimeouts(MYSQL *conn) {
  const int timeout = DatabaseDefaults::MARIADB_TIMEOUT_SECONDS;
  std::string query =
      "SET SESSION wait_timeout = " + std::to_string(timeout) +
      ", interactive_timeout = " + std::to_string(timeout) +
      ", net_read_timeout = " + std::to_string(timeout) +
      ", net_write_timeout = " + std::to_string(timeout) +
      ", innodb_lock_wait_timeout = " + std::to_string(timeout) +
      ", lock_wait_timeout = " + std::to_string(timeout);

  if (mysql_query(conn, query.c_str())) {
    Logger::warning(LogCategory::DATABASE, "MariaDBEngine",
                    "Failed to set connection timeouts: " +
                        std::string(mysql_error(conn)));
  }
}

// Executes a SQL query on the provided MySQL connection and returns the results
// as a vector of rows, where each row is a vector of strings. Handles NULL
// values by converting them to the string "NULL". If the query fails or the
// connection is invalid, returns an empty vector and logs an error. This
// function stores the entire result set in memory, so it should not be used
// for very large result sets. The caller is responsible for ensuring the
// connection is valid.
std::vector<std::vector<std::string>>
MariaDBEngine::executeQuery(MYSQL *conn, const std::string &query) {
  std::vector<std::vector<std::string>> results;
  if (!conn)
    return results;

  if (mysql_query(conn, query.c_str())) {
    Logger::error(LogCategory::DATABASE, "MariaDBEngine",
                  "Query failed: " + std::string(mysql_error(conn)));
    return results;
  }

  MYSQL_RES *res = mysql_store_result(conn);
  if (!res) {
    unsigned int fieldCount = mysql_field_count(conn);
    if (fieldCount > 0) {
      unsigned int errNo = mysql_errno(conn);
      if (errNo != 0) {
        Logger::error(LogCategory::DATABASE, "MariaDBEngine",
                      "Result fetch failed: " + std::string(mysql_error(conn)) +
                          " (error code: " + std::to_string(errNo) + ")");
      } else {
        Logger::error(LogCategory::DATABASE, "MariaDBEngine",
                      "Result fetch failed: " + std::string(mysql_error(conn)));
      }
    }
    return results;
  }

  unsigned int numFields = mysql_num_fields(res);
  MYSQL_ROW row;
  while ((row = mysql_fetch_row(res))) {
    std::vector<std::string> rowData;
    rowData.reserve(numFields);
    for (unsigned int i = 0; i < numFields; ++i) {
      rowData.push_back(row[i] ? row[i] : "NULL");
    }
    results.push_back(std::move(rowData));
  }
  mysql_free_result(res);
  return results;
}

// Discovers all user tables in the MariaDB database. Queries
// information_schema.tables to find all BASE TABLE types, excluding system
// schemas (information_schema, mysql, performance_schema, sys). Returns a
// vector of CatalogTableInfo objects containing schema name, table name, and
// the connection string. If connection fails, returns an empty vector. This
// function is used during catalog synchronization to identify tables that
// should be synced.
std::vector<CatalogTableInfo> MariaDBEngine::discoverTables() {
  std::vector<CatalogTableInfo> tables;
  auto conn = createConnection();
  if (!conn)
    return tables;

  std::string query =
      "SELECT table_schema, table_name "
      "FROM information_schema.tables "
      "WHERE table_schema NOT IN ('information_schema', 'mysql', "
      "'performance_schema', 'sys') "
      "AND table_type = 'BASE TABLE' "
      "ORDER BY table_schema, table_name";

  auto results = executeQuery(conn->get(), query);
  for (const auto &row : results) {
    if (row.size() >= 2) {
      tables.push_back({row[0], row[1], connectionString_});
    }
  }
  return tables;
}

// Detects the primary key columns for a given table. Queries
// information_schema.KEY_COLUMN_USAGE to find all columns that are part of the
// PRIMARY KEY constraint, ordered by their ordinal position. Escapes the
// schema and table names using mysql_real_escape_string to prevent SQL
// injection. Returns a vector of column names in the order they appear in the
// primary key. If no primary key exists or connection fails, returns an empty
// vector. If schema or table is empty, the behavior is undefined.
std::vector<std::string>
MariaDBEngine::detectPrimaryKey(const std::string &schema,
                                const std::string &table) {
  if (schema.empty() || table.empty()) {
    Logger::error(LogCategory::DATABASE, "MariaDBEngine",
                  "detectPrimaryKey: schema and table must not be empty");
    return {};
  }

  std::vector<std::string> pkColumns;
  auto conn = createConnection();
  if (!conn)
    return pkColumns;

  if (!conn->isValid()) {
    Logger::error(LogCategory::DATABASE, "MariaDBEngine",
                  "detectPrimaryKey: connection is invalid");
    return pkColumns;
  }

  MYSQL *mysqlConn = conn->get();
  if (!mysqlConn) {
    Logger::error(LogCategory::DATABASE, "MariaDBEngine",
                  "detectPrimaryKey: MySQL connection is null");
    return pkColumns;
  }

  size_t schemaEscapedLen = schema.length() * 2 + 1;
  std::vector<char> escapedSchemaBuf(schemaEscapedLen);
  size_t tableEscapedLen = table.length() * 2 + 1;
  std::vector<char> escapedTableBuf(tableEscapedLen);

  unsigned long schemaLen = mysql_real_escape_string(
      mysqlConn, escapedSchemaBuf.data(), schema.c_str(), schema.length());
  unsigned long tableLen = mysql_real_escape_string(
      mysqlConn, escapedTableBuf.data(), table.c_str(), table.length());

  if (schemaLen == (unsigned long)-1 || tableLen == (unsigned long)-1) {
    Logger::error(LogCategory::DATABASE, "MariaDBEngine",
                  "detectPrimaryKey: mysql_real_escape_string failed");
    return pkColumns;
  }

  std::string escapedSchema(escapedSchemaBuf.data(), schemaLen);
  std::string escapedTable(escapedTableBuf.data(), tableLen);

  std::string query = "SELECT COLUMN_NAME "
                      "FROM information_schema.KEY_COLUMN_USAGE "
                      "WHERE TABLE_SCHEMA = '" +
                      escapedSchema + "' AND TABLE_NAME = '" + escapedTable +
                      "' AND CONSTRAINT_NAME = 'PRIMARY' "
                      "ORDER BY ORDINAL_POSITION";

  auto results = executeQuery(conn->get(), query);
  for (const auto &row : results) {
    if (!row.empty() && !row[0].empty() && row[0] != "NULL")
      pkColumns.push_back(row[0]);
  }
  return pkColumns;
}

// Detects a time-based column (e.g., updated_at, created_at) in the specified
// table. Searches for columns matching TIME_COLUMN_CANDIDATES in
// information_schema.columns, ordered by preference using MySQL's FIELD()
// function. Escapes schema and table names to prevent SQL injection. Returns
// the first matching column name, or an empty string if no time column is
// found or connection fails. This column is used for incremental sync
// strategies. If schema or table is empty, the behavior is undefined.
std::string MariaDBEngine::detectTimeColumn(const std::string &schema,
                                            const std::string &table) {
  if (schema.empty() || table.empty()) {
    Logger::error(LogCategory::DATABASE, "MariaDBEngine",
                  "detectTimeColumn: schema and table must not be empty");
    return "";
  }

  auto conn = createConnection();
  if (!conn)
    return "";

  if (!conn->isValid()) {
    Logger::error(LogCategory::DATABASE, "MariaDBEngine",
                  "detectTimeColumn: connection is invalid");
    return "";
  }

  MYSQL *mysqlConn = conn->get();
  if (!mysqlConn) {
    Logger::error(LogCategory::DATABASE, "MariaDBEngine",
                  "detectTimeColumn: MySQL connection is null");
    return "";
  }

  size_t schemaEscapedLen = schema.length() * 2 + 1;
  std::vector<char> escapedSchemaBuf(schemaEscapedLen);
  size_t tableEscapedLen = table.length() * 2 + 1;
  std::vector<char> escapedTableBuf(tableEscapedLen);

  unsigned long schemaLen = mysql_real_escape_string(
      mysqlConn, escapedSchemaBuf.data(), schema.c_str(), schema.length());
  unsigned long tableLen = mysql_real_escape_string(
      mysqlConn, escapedTableBuf.data(), table.c_str(), table.length());

  if (schemaLen == (unsigned long)-1 || tableLen == (unsigned long)-1) {
    Logger::error(LogCategory::DATABASE, "MariaDBEngine",
                  "detectTimeColumn: mysql_real_escape_string failed");
    return "";
  }

  std::string escapedSchema(escapedSchemaBuf.data(), schemaLen);
  std::string escapedTable(escapedTableBuf.data(), tableLen);

  std::string candidates;
  for (size_t i = 0; i < DatabaseDefaults::TIME_COLUMN_COUNT; ++i) {
    if (i > 0)
      candidates += ", ";
    candidates +=
        std::string("'") + DatabaseDefaults::TIME_COLUMN_CANDIDATES[i] + "'";
  }

  std::string query = "SELECT COLUMN_NAME FROM information_schema.columns "
                      "WHERE table_schema = '" +
                      escapedSchema + "' AND table_name = '" + escapedTable +
                      "' AND COLUMN_NAME IN (" + candidates +
                      ") ORDER BY FIELD(COLUMN_NAME, ";

  for (size_t i = 0; i < DatabaseDefaults::TIME_COLUMN_COUNT; ++i) {
    if (i > 0)
      query += ", ";
    query +=
        std::string("'") + DatabaseDefaults::TIME_COLUMN_CANDIDATES[i] + "'";
  }
  query += ")";

  auto results = executeQuery(conn->get(), query);
  if (!results.empty() && !results[0].empty() && results[0][0] != "NULL")
    return results[0][0];

  return "";
}

// Gets the column count for a table in both the source MariaDB database and
// the target PostgreSQL database. Queries information_schema.columns in both
// databases and returns a pair where first is the source count and second is
// the target count. This is used to detect schema mismatches between source
// and target. Schema and table names are converted to lowercase for the
// PostgreSQL query. If connection fails or query fails, returns {0, 0} for
// source and {sourceCount, 0} for target respectively. If schema or table is
// empty, the behavior is undefined.
std::pair<int, int>
MariaDBEngine::getColumnCounts(const std::string &schema,
                               const std::string &table,
                               const std::string &targetConnStr) {
  if (schema.empty() || table.empty()) {
    Logger::error(LogCategory::DATABASE, "MariaDBEngine",
                  "getColumnCounts: schema and table must not be empty");
    return {0, 0};
  }

  auto conn = createConnection();
  if (!conn)
    return {0, 0};

  if (!conn->isValid()) {
    Logger::error(LogCategory::DATABASE, "MariaDBEngine",
                  "getColumnCounts: connection is invalid");
    return {0, 0};
  }

  MYSQL *mysqlConn = conn->get();
  if (!mysqlConn) {
    Logger::error(LogCategory::DATABASE, "MariaDBEngine",
                  "getColumnCounts: MySQL connection is null");
    return {0, 0};
  }

  size_t schemaEscapedLen = schema.length() * 2 + 1;
  std::vector<char> escapedSchemaBuf(schemaEscapedLen);
  size_t tableEscapedLen = table.length() * 2 + 1;
  std::vector<char> escapedTableBuf(tableEscapedLen);

  unsigned long schemaLen = mysql_real_escape_string(
      mysqlConn, escapedSchemaBuf.data(), schema.c_str(), schema.length());
  unsigned long tableLen = mysql_real_escape_string(
      mysqlConn, escapedTableBuf.data(), table.c_str(), table.length());

  if (schemaLen == (unsigned long)-1 || tableLen == (unsigned long)-1) {
    Logger::error(LogCategory::DATABASE, "MariaDBEngine",
                  "getColumnCounts: mysql_real_escape_string failed");
    return {0, 0};
  }

  std::string escapedSchema(escapedSchemaBuf.data(), schemaLen);
  std::string escapedTable(escapedTableBuf.data(), tableLen);

  std::string sourceQuery = "SELECT COUNT(*) FROM information_schema.columns "
                            "WHERE table_schema = '" +
                            escapedSchema + "' AND table_name = '" +
                            escapedTable + "'";

  auto sourceResults = executeQuery(conn->get(), sourceQuery);
  int sourceCount = 0;
  if (!sourceResults.empty() && !sourceResults[0].empty() &&
      sourceResults[0][0] != "NULL") {
    try {
      sourceCount = std::stoi(sourceResults[0][0]);
    } catch (const std::exception &e) {
      Logger::error(LogCategory::DATABASE, "MariaDBEngine",
                    "Failed to parse source column count: " +
                        std::string(e.what()));
      sourceCount = 0;
    } catch (...) {
      Logger::error(LogCategory::DATABASE, "MariaDBEngine",
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
    Logger::error(LogCategory::DATABASE, "MariaDBEngine",
                  "Error getting target column count for " + schema + "." +
                      table + ": " + std::string(e.what()));
    return {sourceCount, 0};
  }
}
