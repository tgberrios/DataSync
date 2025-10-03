#include "MariaDBToPostgres.h"
#include <algorithm>
#include <sstream>

// Static member definitions
std::unordered_map<std::string, std::string> MariaDBToPostgres::dataTypeMap = {
    {"int", "INTEGER"},
    {"bigint", "BIGINT"},
    {"smallint", "SMALLINT"},
    {"tinyint", "SMALLINT"},
    {"decimal", "NUMERIC"},
    {"float", "REAL"},
    {"double", "DOUBLE PRECISION"},
    {"varchar", "VARCHAR"},
    {"char", "CHAR"},
    {"text", "TEXT"},
    {"longtext", "TEXT"},
    {"mediumtext", "TEXT"},
    {"tinytext", "TEXT"},
    {"blob", "BYTEA"},
    {"longblob", "BYTEA"},
    {"mediumblob", "BYTEA"},
    {"tinyblob", "BYTEA"},
    {"json", "JSON"},
    {"boolean", "BOOLEAN"},
    {"bit", "BIT"},
    {"timestamp", "TIMESTAMP"},
    {"datetime", "TIMESTAMP"},
    {"date", "DATE"},
    {"time", "TIME"}};

MYSQL *
MariaDBToPostgres::getMariaDBConnection(const std::string &connectionString) {
  return connectionManager.getConnection(connectionString);
}

void MariaDBToPostgres::setupTableTargetMariaDBToPostgres() {
  Logger::getInstance().info(LogCategory::TRANSFER,
                             "Starting MariaDB table target setup");

  try {
    pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());
    if (!pgConn.is_open()) {
      Logger::getInstance().error(LogCategory::TRANSFER,
                                  "CRITICAL ERROR: Cannot establish PostgreSQL "
                                  "connection for MariaDB table setup");
      return;
    }

    auto tables = getActiveTables(pgConn);
    if (tables.empty()) {
      Logger::getInstance().info(LogCategory::TRANSFER,
                                 "No active MariaDB tables found to setup");
      return;
    }

    sortTablesByPriority(tables);
    Logger::getInstance().info(LogCategory::TRANSFER,
                               "Processing " + std::to_string(tables.size()) +
                                   " MariaDB tables in priority order");

    for (const auto &table : tables) {
      if (table.db_engine != "MariaDB")
        continue;
      processTableSetup(table, pgConn);
    }

    Logger::getInstance().info(LogCategory::TRANSFER,
                               "MariaDB table target setup completed");
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::TRANSFER,
                                "Error in setupTableTargetMariaDBToPostgres: " +
                                    std::string(e.what()));
  }
}

void MariaDBToPostgres::transferDataMariaDBToPostgres() {
  Logger::getInstance().info(LogCategory::TRANSFER,
                             "Starting MariaDB to PostgreSQL data transfer");

  try {
    pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());
    if (!pgConn.is_open()) {
      Logger::getInstance().error(LogCategory::TRANSFER,
                                  "CRITICAL ERROR: Cannot establish PostgreSQL "
                                  "connection for MariaDB data transfer");
      return;
    }

    auto tables = getActiveTables(pgConn);
    if (tables.empty()) {
      Logger::getInstance().info(
          LogCategory::TRANSFER,
          "No active MariaDB tables found for data transfer");
      return;
    }

    sortTablesByPriority(tables);
    Logger::getInstance().info(LogCategory::TRANSFER,
                               "Processing " + std::to_string(tables.size()) +
                                   " MariaDB tables in priority order");

    for (const auto &table : tables) {
      if (table.db_engine != "MariaDB")
        continue;

      MYSQL *mariadbConn = getMariaDBConnection(table.connection_string);
      if (!mariadbConn) {
        Logger::getInstance().error(
            LogCategory::TRANSFER,
            "CRITICAL ERROR: Failed to get MariaDB connection for table " +
                table.schema_name + "." + table.table_name +
                " - marking as ERROR and skipping");
        continue;
      }

      dataTransfer.transferData(mariadbConn, pgConn, table);
      connectionManager.closeConnection(mariadbConn);
    }

    Logger::getInstance().info(
        LogCategory::TRANSFER,
        "MariaDB to PostgreSQL data transfer completed successfully");
  } catch (const std::exception &e) {
    Logger::getInstance().error(
        LogCategory::TRANSFER,
        "CRITICAL ERROR in transferDataMariaDBToPostgres: " +
            std::string(e.what()) +
            " - MariaDB data transfer completely failed");
  }
}

std::vector<TableInfo>
MariaDBToPostgres::getActiveTables(pqxx::connection &pgConn) const {
  std::vector<TableInfo> data;

  try {
    pqxx::work txn(pgConn);
    auto results = txn.exec(
        "SELECT schema_name, table_name, cluster_name, db_engine, "
        "connection_string, last_sync_time, last_sync_column, "
        "status, last_offset, last_processed_pk, pk_strategy, "
        "pk_columns, candidate_columns, has_pk, table_size "
        "FROM metadata.catalog "
        "WHERE active=true AND db_engine='MariaDB' AND status != 'NO_DATA' "
        "ORDER BY table_size ASC, schema_name, table_name;");
    txn.commit();

    for (const auto &row : results) {
      if (row.size() < 15)
        continue;

      TableInfo t;
      t.schema_name = row[0].is_null() ? "" : row[0].as<std::string>();
      t.table_name = row[1].is_null() ? "" : row[1].as<std::string>();
      t.cluster_name = row[2].is_null() ? "" : row[2].as<std::string>();
      t.db_engine = row[3].is_null() ? "" : row[3].as<std::string>();
      t.connection_string = row[4].is_null() ? "" : row[4].as<std::string>();
      t.last_sync_time = row[5].is_null() ? "" : row[5].as<std::string>();
      t.last_sync_column = row[6].is_null() ? "" : row[6].as<std::string>();
      t.status = row[7].is_null() ? "" : row[7].as<std::string>();
      t.last_offset = row[8].is_null() ? "" : row[8].as<std::string>();
      t.last_processed_pk = row[9].is_null() ? "" : row[9].as<std::string>();
      t.pk_strategy = row[10].is_null() ? "" : row[10].as<std::string>();
      t.pk_columns = row[11].is_null() ? "" : row[11].as<std::string>();
      t.candidate_columns = row[12].is_null() ? "" : row[12].as<std::string>();
      t.has_pk = row[13].is_null() ? false : row[13].as<bool>();
      data.push_back(t);
    }
  } catch (const pqxx::sql_error &e) {
    Logger::getInstance().error(
        LogCategory::TRANSFER, "getActiveTables",
        "SQL ERROR getting active tables: " + std::string(e.what()) +
            " [SQL State: " + e.sqlstate() + "]");
  } catch (const pqxx::broken_connection &e) {
    Logger::getInstance().error(LogCategory::TRANSFER, "getActiveTables",
                                "CONNECTION ERROR getting active tables: " +
                                    std::string(e.what()));
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::TRANSFER, "getActiveTables",
                                "ERROR getting active tables: " +
                                    std::string(e.what()));
  }

  return data;
}

void MariaDBToPostgres::syncIndexesAndConstraints(
    const std::string &schema_name, const std::string &table_name,
    pqxx::connection &pgConn, const std::string &lowerSchemaName,
    const std::string &connection_string) {
  if (schema_name.empty() || table_name.empty() || lowerSchemaName.empty() ||
      connection_string.empty()) {
    Logger::getInstance().error(
        LogCategory::TRANSFER, "syncIndexesAndConstraints",
        "Invalid parameters: schema_name, table_name, lowerSchemaName, or "
        "connection_string is empty");
    return;
  }

  MYSQL *mariadbConn = getMariaDBConnection(connection_string);
  if (!mariadbConn) {
    Logger::getInstance().error(LogCategory::TRANSFER,
                                "syncIndexesAndConstraints",
                                "Failed to get MariaDB connection");
    return;
  }

  auto indexes =
      queryExecutor.getTableIndexes(mariadbConn, schema_name, table_name);

  for (const auto &row : indexes) {
    if (row.size() < 3)
      continue;

    std::string indexName = row[0];
    std::string columnName = row[2];
    std::transform(columnName.begin(), columnName.end(), columnName.begin(),
                   ::tolower);

    std::string createQuery = "CREATE INDEX IF NOT EXISTS \"" + indexName +
                              "\" ON \"" + lowerSchemaName + "\".\"" +
                              table_name + "\" (\"" + columnName + "\");";

    try {
      pqxx::work txn(pgConn);
      txn.exec(createQuery);
      txn.commit();
    } catch (const std::exception &e) {
      Logger::getInstance().error(
          LogCategory::TRANSFER, "syncIndexesAndConstraints",
          "ERROR creating index '" + indexName + "': " + std::string(e.what()));
    }
  }

  connectionManager.closeConnection(mariadbConn);
}

void MariaDBToPostgres::processTableSetup(const TableInfo &table,
                                          pqxx::connection &pgConn) {
  MYSQL *mariadbConn = getMariaDBConnection(table.connection_string);
  if (!mariadbConn) {
    Logger::getInstance().error(
        LogCategory::TRANSFER,
        "Failed to get MariaDB connection for table setup");
    return;
  }

  createTableSchema(table, pgConn, mariadbConn);
  connectionManager.closeConnection(mariadbConn);
}

void MariaDBToPostgres::createTableSchema(const TableInfo &table,
                                          pqxx::connection &pgConn,
                                          MYSQL *mariadbConn) {
  auto columns = queryExecutor.getTableColumns(mariadbConn, table.schema_name,
                                               table.table_name);
  if (columns.empty()) {
    Logger::getInstance().error(LogCategory::TRANSFER,
                                "No columns found for table " +
                                    table.schema_name + "." + table.table_name +
                                    " - skipping");
    return;
  }

  std::string lowerSchema = table.schema_name;
  std::transform(lowerSchema.begin(), lowerSchema.end(), lowerSchema.begin(),
                 ::tolower);

  // Create schema
  {
    pqxx::work txn(pgConn);
    txn.exec("CREATE SCHEMA IF NOT EXISTS \"" + lowerSchema + "\";");
    txn.commit();
  }

  // Build CREATE TABLE query
  std::string createQuery = "CREATE TABLE IF NOT EXISTS \"" + lowerSchema +
                            "\".\"" + table.table_name + "\" (";
  std::vector<std::string> primaryKeys;
  std::vector<std::string> columnDefinitions;

  for (const auto &col : columns) {
    if (col.size() < 6)
      continue;

    std::string colName = col[0];
    std::transform(colName.begin(), colName.end(), colName.begin(), ::tolower);

    std::string dataType = col[1];
    std::string maxLength = col[5];
    std::string columnKey = col[3];

    std::string pgType = mapDataType(dataType, maxLength);
    std::string columnDef = "\"" + colName + "\" " + pgType;
    columnDefinitions.push_back(columnDef);

    if (columnKey == "PRI") {
      primaryKeys.push_back(colName);
    }
  }

  if (columnDefinitions.empty()) {
    Logger::getInstance().error(LogCategory::TRANSFER,
                                "No valid columns found for table " +
                                    table.schema_name + "." + table.table_name +
                                    " - skipping");
    return;
  }

  // Add columns to CREATE query
  for (size_t i = 0; i < columnDefinitions.size(); ++i) {
    if (i > 0)
      createQuery += ", ";
    createQuery += columnDefinitions[i];
  }

  // Add primary key
  if (!primaryKeys.empty()) {
    createQuery += ", PRIMARY KEY (";
    for (size_t i = 0; i < primaryKeys.size(); ++i) {
      if (i > 0)
        createQuery += ", ";
      createQuery += "\"" + primaryKeys[i] + "\"";
    }
    createQuery += ")";
  }
  createQuery += ");";

  // Execute CREATE TABLE
  {
    pqxx::work txn(pgConn);
    txn.exec(createQuery);
    txn.commit();
  }
}

std::string MariaDBToPostgres::mapDataType(const std::string &mariaType,
                                           const std::string &maxLength) {
  if (mariaType == "char" || mariaType == "varchar") {
    if (!maxLength.empty() && maxLength != "NULL") {
      try {
        size_t length = std::stoul(maxLength);
        if (length >= 1 && length <= 65535) {
          return mariaType + "(" + maxLength + ")";
        }
      } catch (const std::exception &) {
        // Fall through to default
      }
    }
    return "VARCHAR";
  } else if (dataTypeMap.count(mariaType)) {
    return dataTypeMap[mariaType];
  }
  return "TEXT";
}

void MariaDBToPostgres::sortTablesByPriority(std::vector<TableInfo> &tables) {
  std::sort(
      tables.begin(), tables.end(), [](const TableInfo &a, const TableInfo &b) {
        if (a.status == "FULL_LOAD" && b.status != "FULL_LOAD")
          return true;
        if (a.status != "FULL_LOAD" && b.status == "FULL_LOAD")
          return false;
        if (a.status == "RESET" && b.status != "RESET")
          return true;
        if (a.status != "RESET" && b.status == "RESET")
          return false;
        if (a.status == "LISTENING_CHANGES" && b.status != "LISTENING_CHANGES")
          return true;
        if (a.status != "LISTENING_CHANGES" && b.status == "LISTENING_CHANGES")
          return false;
        return false;
      });
}
