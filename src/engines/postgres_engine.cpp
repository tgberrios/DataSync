#include "engines/postgres_engine.h"
#include "core/logger.h"

PostgreSQLEngine::PostgreSQLEngine(std::string connectionString)
    : connectionString_(std::move(connectionString)) {}

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
        "'pg_toast', 'pg_temp_1', 'pg_toast_temp_1', 'metadata', "
        "'datasync_metadata') "
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
    std::string inClause;
    for (size_t i = 0; i < DatabaseDefaults::TIME_COLUMN_COUNT; ++i) {
      if (i > 0) {
        inClause += ", ";
        caseClause += " ";
      }
      inClause +=
          "'" + std::string(DatabaseDefaults::TIME_COLUMN_CANDIDATES[i]) + "'";
      caseClause += "WHEN '" +
                    std::string(DatabaseDefaults::TIME_COLUMN_CANDIDATES[i]) +
                    "' THEN " + std::to_string(i + 1);
    }

    pqxx::work txn(conn);
    auto results = txn.exec_params(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = $1 AND table_name = $2 "
        "AND column_name IN (" +
            inClause +
            ") "
            "ORDER BY CASE column_name " +
            caseClause + " ELSE " +
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
