#include "engines/mariadb_engine.h"
#include <algorithm>
#include <pqxx/pqxx>

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

  auto conn = std::make_unique<MySQLConnection>(*params);
  if (!conn->isValid())
    return nullptr;

  setConnectionTimeouts(conn->get());
  return conn;
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
    if (mysql_field_count(conn) > 0) {
      Logger::error(LogCategory::DATABASE, "MariaDBEngine",
                    "Result fetch failed: " + std::string(mysql_error(conn)));
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

std::vector<std::string>
MariaDBEngine::detectPrimaryKey(const std::string &schema,
                                const std::string &table) {
  std::vector<std::string> pkColumns;
  auto conn = createConnection();
  if (!conn)
    return pkColumns;

  char escapedSchema[schema.length() * 2 + 1];
  char escapedTable[table.length() * 2 + 1];
  mysql_real_escape_string(conn->get(), escapedSchema, schema.c_str(),
                           schema.length());
  mysql_real_escape_string(conn->get(), escapedTable, table.c_str(),
                           table.length());

  std::string query = "SELECT COLUMN_NAME "
                      "FROM information_schema.KEY_COLUMN_USAGE "
                      "WHERE TABLE_SCHEMA = '" +
                      std::string(escapedSchema) + "' AND TABLE_NAME = '" +
                      std::string(escapedTable) +
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
  auto conn = createConnection();
  if (!conn)
    return "";

  char escapedSchema[schema.length() * 2 + 1];
  char escapedTable[table.length() * 2 + 1];
  mysql_real_escape_string(conn->get(), escapedSchema, schema.c_str(),
                           schema.length());
  mysql_real_escape_string(conn->get(), escapedTable, table.c_str(),
                           table.length());

  std::string candidates;
  for (size_t i = 0; i < DatabaseDefaults::TIME_COLUMN_COUNT; ++i) {
    if (i > 0)
      candidates += ", ";
    candidates +=
        std::string("'") + DatabaseDefaults::TIME_COLUMN_CANDIDATES[i] + "'";
  }

  std::string query = "SELECT COLUMN_NAME FROM information_schema.columns "
                      "WHERE table_schema = '" +
                      std::string(escapedSchema) + "' AND table_name = '" +
                      std::string(escapedTable) + "' AND COLUMN_NAME IN (" +
                      candidates + ") ORDER BY FIELD(COLUMN_NAME, ";

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
  auto conn = createConnection();
  if (!conn)
    return {0, 0};

  char escapedSchema[schema.length() * 2 + 1];
  char escapedTable[table.length() * 2 + 1];
  mysql_real_escape_string(conn->get(), escapedSchema, schema.c_str(),
                           schema.length());
  mysql_real_escape_string(conn->get(), escapedTable, table.c_str(),
                           table.length());

  std::string sourceQuery = "SELECT COUNT(*) FROM information_schema.columns "
                            "WHERE table_schema = '" +
                            std::string(escapedSchema) +
                            "' AND table_name = '" + std::string(escapedTable) +
                            "'";

  auto sourceResults = executeQuery(conn->get(), sourceQuery);
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
    Logger::error(LogCategory::DATABASE, "MariaDBEngine",
                  "Error getting target column count: " +
                      std::string(e.what()));
    return {sourceCount, 0};
  }
}
