#include "engines/mariadb_engine.h"
#include "sync/MariaDBToPostgres.h"
#include <algorithm>
#include <chrono>
#include <pqxx/pqxx>
#include <thread>
#include <unordered_set>

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

MySQLConnection::~MySQLConnection() {
  if (conn_)
    mysql_close(conn_);
}

MySQLConnection::MySQLConnection(MySQLConnection &&other) noexcept
    : conn_(other.conn_) {
  other.conn_ = nullptr;
}

MySQLConnection &MySQLConnection::operator=(MySQLConnection &&other) noexcept {
  if (this != &other) {
    if (conn_)
      mysql_close(conn_);
    conn_ = other.conn_;
    other.conn_ = nullptr;
  }
  return *this;
}

MariaDBEngine::MariaDBEngine(std::string connectionString)
    : connectionString_(std::move(connectionString)) {}

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

std::vector<CatalogTableInfo> MariaDBEngine::discoverTables() {
  std::vector<CatalogTableInfo> tables;
  auto conn = createConnection();
  if (!conn)
    return tables;

  std::string query =
      "SELECT table_schema, table_name "
      "FROM information_schema.tables "
      "WHERE table_schema NOT IN ('information_schema', 'mysql', "
      "'performance_schema', 'sys', 'datasync_metadata') "
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

std::vector<ColumnInfo>
MariaDBEngine::getTableColumns(const std::string &schema,
                               const std::string &table) {
  std::vector<ColumnInfo> columns;
  if (schema.empty() || table.empty()) {
    Logger::error(LogCategory::DATABASE, "MariaDBEngine",
                  "getTableColumns: schema and table must not be empty");
    return columns;
  }

  auto conn = createConnection();
  if (!conn || !conn->isValid()) {
    Logger::error(LogCategory::DATABASE, "MariaDBEngine",
                  "getTableColumns: connection is invalid");
    return columns;
  }

  MYSQL *mysqlConn = conn->get();
  if (!mysqlConn) {
    Logger::error(LogCategory::DATABASE, "MariaDBEngine",
                  "getTableColumns: MySQL connection is null");
    return columns;
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
                  "getTableColumns: mysql_real_escape_string failed");
    return columns;
  }

  std::string escapedSchema(escapedSchemaBuf.data(), schemaLen);
  std::string escapedTable(escapedTableBuf.data(), tableLen);

  std::string query =
      "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_KEY, EXTRA, "
      "CHARACTER_MAXIMUM_LENGTH, COLUMN_DEFAULT, ORDINAL_POSITION "
      "FROM information_schema.columns "
      "WHERE TABLE_SCHEMA = '" +
      escapedSchema + "' AND TABLE_NAME = '" + escapedTable +
      "' ORDER BY ORDINAL_POSITION";

  auto results = executeQuery(conn->get(), query);

  std::vector<std::string> pkColumns = detectPrimaryKey(schema, table);
  std::unordered_set<std::string> pkSet(pkColumns.begin(), pkColumns.end());

  for (const auto &row : results) {
    if (row.size() < 8)
      continue;

    ColumnInfo col;
    col.name = row[0];
    std::transform(col.name.begin(), col.name.end(), col.name.begin(),
                   ::tolower);
    col.dataType = row[1];
    col.isNullable = (row[2] == "YES");
    std::string columnKey = row[3];
    std::string extra = row[4];
    col.maxLength = row.size() > 5 ? row[5] : "";
    col.defaultValue = row.size() > 6 ? row[6] : "";
    try {
      col.ordinalPosition = std::stoi(row[7]);
    } catch (...) {
      col.ordinalPosition = 0;
    }
    col.isPrimaryKey = pkSet.find(row[0]) != pkSet.end();

    std::string pgType = "TEXT";
    if (extra == "auto_increment") {
      pgType = (col.dataType == "bigint") ? "BIGINT" : "INTEGER";
    } else if (col.dataType == "timestamp" || col.dataType == "datetime") {
      pgType = "TIMESTAMP";
    } else if (col.dataType == "date") {
      pgType = "DATE";
    } else if (col.dataType == "time") {
      pgType = "TIME";
    } else if (col.dataType == "char") {
      pgType = "TEXT";
    } else if (col.dataType == "varchar") {
      if (!col.maxLength.empty() && col.maxLength != "NULL") {
        try {
          size_t length = std::stoul(col.maxLength);
          if (length >= 1 && length <= 65535) {
            pgType = "VARCHAR(" + col.maxLength + ")";
          } else {
            pgType = "VARCHAR";
          }
        } catch (...) {
          pgType = "VARCHAR";
        }
      } else {
        pgType = "VARCHAR";
      }
    } else if (MariaDBToPostgres::dataTypeMap.count(col.dataType)) {
      pgType = MariaDBToPostgres::dataTypeMap[col.dataType];
    }

    col.pgType = pgType;
    columns.push_back(col);
  }

  return columns;
}
