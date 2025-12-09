#include "engines/postgres_engine.h"

// Constructor for PostgreSQLEngine. Stores the connection string for later use
// when creating database connections. The connection string should be in
// PostgreSQL libpq format (e.g., "postgresql://user:pass@host:port/db" or
// "host=... port=... dbname=... user=... password=...").
PostgreSQLEngine::PostgreSQLEngine(std::string connectionString)
    : connectionString_(std::move(connectionString)) {}

// Discovers all user tables in the PostgreSQL database. Queries
// information_schema.tables to find all BASE TABLE types, excluding system
// schemas (information_schema, pg_catalog, pg_toast, pg_temp_1,
// pg_toast_temp_1, metadata). Returns a vector of CatalogTableInfo objects
// containing schema name, table name, and the connection string. If
// connection fails, returns an empty vector and logs an error. This function
// is used during catalog synchronization to identify tables that should be
// synced.
std::vector<CatalogTableInfo> PostgreSQLEngine::discoverTables() {
  std::vector<CatalogTableInfo> tables;
  try {
    pqxx::connection conn(connectionString_);
    if (!conn.is_open())
      return tables;

    pqxx::work txn(conn);
    auto results = txn.exec(
        "SELECT table_schema, table_name "
        "FROM information_schema.tables "
        "WHERE table_schema NOT IN ('information_schema', 'pg_catalog', "
        "'pg_toast', 'pg_temp_1', 'pg_toast_temp_1', 'metadata') "
        "AND table_type = 'BASE TABLE' "
        "ORDER BY table_schema, table_name");
    txn.commit();

    for (const auto &row : results) {
      if (row.size() >= 2) {
        tables.push_back({row[0].as<std::string>(), row[1].as<std::string>(),
                          connectionString_});
      }
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "PostgreSQLEngine",
                  "Error discovering tables: " + std::string(e.what()));
  }
  return tables;
}

// Detects the primary key columns for a given table. Queries
// information_schema.table_constraints and information_schema.key_column_usage
// to find all columns that are part of the PRIMARY KEY constraint, ordered by
// their ordinal position. Uses parameterized queries to prevent SQL injection.
// Returns a vector of column names in the order they appear in the primary
// key. If no primary key exists or connection fails, returns an empty vector
// and logs an error. If schema or table is empty, the behavior is undefined.
std::vector<std::string>
PostgreSQLEngine::detectPrimaryKey(const std::string &schema,
                                   const std::string &table) {
  if (schema.empty() || table.empty()) {
    Logger::error(LogCategory::DATABASE, "PostgreSQLEngine",
                  "detectPrimaryKey: schema and table must not be empty");
    return {};
  }

  std::vector<std::string> pkColumns;
  try {
    pqxx::connection conn(connectionString_);
    if (!conn.is_open())
      return pkColumns;

    pqxx::work txn(conn);
    auto results =
        txn.exec_params("SELECT kcu.column_name "
                        "FROM information_schema.table_constraints tc "
                        "INNER JOIN information_schema.key_column_usage kcu "
                        "ON tc.constraint_name = kcu.constraint_name "
                        "WHERE tc.table_schema = $1 AND tc.table_name = $2 "
                        "AND tc.constraint_type = 'PRIMARY KEY' "
                        "ORDER BY kcu.ordinal_position",
                        schema, table);
    txn.commit();

    for (const auto &row : results) {
      if (!row[0].is_null())
        pkColumns.push_back(row[0].as<std::string>());
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "PostgreSQLEngine",
                  "Error detecting primary key for " + schema + "." + table +
                      ": " + std::string(e.what()));
  }
  return pkColumns;
}

// Detects a time-based column (e.g., updated_at, created_at) in the specified
// table. Searches for columns matching TIME_COLUMN_CANDIDATES in
// information_schema.columns, ordered by preference using a CASE statement.
// Uses parameterized queries to prevent SQL injection. Returns the first
// matching column name, or an empty string if no time column is found or
// connection fails. This column is used for incremental sync strategies. If
// schema or table is empty, the behavior is undefined.
std::string PostgreSQLEngine::detectTimeColumn(const std::string &schema,
                                               const std::string &table) {
  if (schema.empty() || table.empty()) {
    Logger::error(LogCategory::DATABASE, "PostgreSQLEngine",
                  "detectTimeColumn: schema and table must not be empty");
    return "";
  }

  try {
    pqxx::connection conn(connectionString_);
    if (!conn.is_open())
      return "";

    std::string caseClause;
    for (size_t i = 0; i < DatabaseDefaults::TIME_COLUMN_COUNT; ++i) {
      caseClause += "WHEN '" +
                    std::string(DatabaseDefaults::TIME_COLUMN_CANDIDATES[i]) +
                    "' THEN " + std::to_string(i + 1) + " ";
    }

    pqxx::work txn(conn);
    auto results = txn.exec_params(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = $1 AND table_name = $2 "
        "AND column_name IN ('updated_at', 'created_at', 'modified_at', "
        "'timestamp', 'last_modified', 'updated_time', 'created_time') "
        "ORDER BY CASE column_name " +
            caseClause + "ELSE " +
            std::to_string(DatabaseDefaults::TIME_COLUMN_COUNT + 1) + " END",
        schema, table);
    txn.commit();

    if (!results.empty() && !results[0][0].is_null())
      return results[0][0].as<std::string>();

  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "PostgreSQLEngine",
                  "Error detecting time column for " + schema + "." + table +
                      ": " + std::string(e.what()));
  }
  return "";
}

// Gets the column count for a table in both the source PostgreSQL database and
// the target PostgreSQL database. Queries information_schema.columns in both
// databases using parameterized queries and returns a pair where first is the
// source count and second is the target count. This is used to detect schema
// mismatches between source and target. If connection fails or query fails,
// returns {0, 0} and logs an error. If schema or table is empty, the behavior
// is undefined.
std::pair<int, int>
PostgreSQLEngine::getColumnCounts(const std::string &schema,
                                  const std::string &table,
                                  const std::string &targetConnStr) {
  if (schema.empty() || table.empty()) {
    Logger::error(LogCategory::DATABASE, "PostgreSQLEngine",
                  "getColumnCounts: schema and table must not be empty");
    return {0, 0};
  }

  try {
    pqxx::connection sourceConn(connectionString_);
    if (!sourceConn.is_open())
      return {0, 0};

    pqxx::work sourceTxn(sourceConn);
    auto sourceResults =
        sourceTxn.exec_params("SELECT COUNT(*) FROM information_schema.columns "
                              "WHERE table_schema = $1 AND table_name = $2",
                              schema, table);
    sourceTxn.commit();

    int sourceCount = 0;
    if (!sourceResults.empty() && !sourceResults[0][0].is_null())
      sourceCount = sourceResults[0][0].as<int>();

    pqxx::connection targetConn(targetConnStr);
    pqxx::work targetTxn(targetConn);
    auto targetResults =
        targetTxn.exec_params("SELECT COUNT(*) FROM information_schema.columns "
                              "WHERE table_schema = $1 AND table_name = $2",
                              schema, table);
    targetTxn.commit();

    int targetCount = 0;
    if (!targetResults.empty() && !targetResults[0][0].is_null())
      targetCount = targetResults[0][0].as<int>();

    return {sourceCount, targetCount};
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "PostgreSQLEngine",
                  "Error getting column counts for " + schema + "." + table +
                      ": " + std::string(e.what()));
    return {0, 0};
  }
}
