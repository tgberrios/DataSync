#include "../include/catalog_manager.h"

// Main catalog operations
void CatalogManager::cleanCatalog() {
  try {
    pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());

    cleanNonExistentPostgresTables(pgConn);
    cleanNonExistentMariaDBTables(pgConn);
    cleanNonExistentMSSQLTables(pgConn);
    cleanOrphanedTables(pgConn);
    cleanInconsistentPaginationFields();
    updateClusterNames();

    Logger::info(LogCategory::DATABASE, "Catalog cleanup completed");
  } catch (const pqxx::sql_error &e) {
    Logger::error(LogCategory::DATABASE, "cleanCatalog",
                  "SQL ERROR cleaning catalog: " + std::string(e.what()) +
                      " [SQL State: " + e.sqlstate() + "]");
  } catch (const pqxx::broken_connection &e) {
    Logger::error(LogCategory::DATABASE, "cleanCatalog",
                  "CONNECTION ERROR cleaning catalog: " +
                      std::string(e.what()));
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "cleanCatalog",
                  "ERROR cleaning catalog: " + std::string(e.what()));
  }
}

void CatalogManager::deactivateNoDataTables() {
  try {
    pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());
    pqxx::work txn(pgConn);

    auto countResult = txn.exec("SELECT COUNT(*) FROM metadata.catalog WHERE "
                                "status = 'NO_DATA' AND active = true");
    int noDataCount = countResult[0][0].as<int>();

    if (noDataCount == 0) {
      txn.commit();
      return;
    }

    auto updateResult =
        txn.exec("UPDATE metadata.catalog SET active = false WHERE status = "
                 "'NO_DATA' AND active = true");

    txn.commit();

    Logger::info(LogCategory::DATABASE,
                 "Deactivated " + std::to_string(updateResult.affected_rows()) +
                     " NO_DATA tables");

  } catch (const pqxx::sql_error &e) {
    Logger::error(
        LogCategory::DATABASE, "deactivateNoDataTables",
        "SQL ERROR deactivating NO_DATA tables: " + std::string(e.what()) +
            " [SQL State: " + e.sqlstate() + "]");
  } catch (const pqxx::broken_connection &e) {
    Logger::error(LogCategory::DATABASE, "deactivateNoDataTables",
                  "CONNECTION ERROR deactivating NO_DATA tables: " +
                      std::string(e.what()));
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "deactivateNoDataTables",
                  "ERROR deactivating NO_DATA tables: " +
                      std::string(e.what()));
  }
}

void CatalogManager::updateClusterNames() {
  try {
    pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());

    pqxx::work txn(pgConn);
    auto results = txn.exec(
        "SELECT DISTINCT connection_string, db_engine FROM metadata.catalog "
        "WHERE (cluster_name IS NULL OR cluster_name = '') AND active = "
        "true");
    txn.commit();

    for (const auto &row : results) {
      std::string connectionString = row[0].as<std::string>();
      std::string dbEngine = row[1].as<std::string>();

      DBEngine engine = (dbEngine == "MariaDB") ? DBEngine::MARIADB
                        : (dbEngine == "MSSQL") ? DBEngine::MSSQL
                                                : DBEngine::POSTGRES;
      std::string clusterName = resolveClusterName(connectionString, engine);
      if (clusterName.empty()) {
        std::string hostname =
            extractHostnameFromConnection(connectionString, engine);
        clusterName = getClusterNameFromHostname(hostname);
      }

      if (!clusterName.empty()) {
        pqxx::work updateTxn(pgConn);
        auto updateResult = updateTxn.exec(
            "UPDATE metadata.catalog SET cluster_name = '" +
            escapeSQL(clusterName) + "' WHERE connection_string = '" +
            escapeSQL(connectionString) + "' AND db_engine = '" +
            escapeSQL(dbEngine) + "'");
        updateTxn.commit();

        Logger::info(LogCategory::DATABASE,
                     "Updated cluster_name to '" + clusterName + "' for " +
                         std::to_string(updateResult.affected_rows()) +
                         " tables");
      }
    }

    Logger::info(LogCategory::DATABASE, "Cluster name updates completed");
  } catch (const pqxx::sql_error &e) {
    Logger::error(LogCategory::DATABASE, "updateClusterNames",
                  "SQL ERROR updating cluster names: " + std::string(e.what()) +
                      " [SQL State: " + e.sqlstate() + "]");
  } catch (const pqxx::broken_connection &e) {
    Logger::error(LogCategory::DATABASE, "updateClusterNames",
                  "CONNECTION ERROR updating cluster names: " +
                      std::string(e.what()));
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "updateClusterNames",
                  "ERROR updating cluster names: " + std::string(e.what()));
  }
}

void CatalogManager::validateSchemaConsistency() {
  try {
    pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());
    Logger::info(LogCategory::DATABASE,
                 "Starting schema consistency validation");

    auto tablesToValidate = getTablesForValidation(pgConn);
    Logger::info(LogCategory::DATABASE,
                 "Found " + std::to_string(tablesToValidate.size()) +
                     " tables to validate");

    ValidationResults results = validateAllTables(pgConn, tablesToValidate);
    logValidationResults(results);

  } catch (const pqxx::sql_error &e) {
    Logger::error(LogCategory::DATABASE, "validateSchemaConsistency",
                  "SQL ERROR in schema validation: " + std::string(e.what()) +
                      " [SQL State: " + e.sqlstate() + "]");
  } catch (const pqxx::broken_connection &e) {
    Logger::error(LogCategory::DATABASE, "validateSchemaConsistency",
                  "CONNECTION ERROR in schema validation: " +
                      std::string(e.what()));
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "validateSchemaConsistency",
                  "ERROR in schema validation: " + std::string(e.what()));
  }
}

// Database-specific sync operations
void CatalogManager::syncCatalogMariaDBToPostgres() {
  try {
    pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());

    auto connections = getMariaDBConnections(pgConn);
    Logger::info(LogCategory::DATABASE, "Found " +
                                            std::to_string(connections.size()) +
                                            " MariaDB connection(s)");

    if (connections.empty()) {
      Logger::warning(LogCategory::DATABASE,
                      "No MariaDB connections found in catalog");
      return;
    }

    SyncResults totalResults;
    totalResults.totalConnections = connections.size();

    for (const auto &connectionString : connections) {
      auto results = processMariaDBConnection(pgConn, connectionString);
      totalResults.processedConnections += results.processedConnections;
      totalResults.totalTables += results.totalTables;
      totalResults.updatedTables += results.updatedTables;
      totalResults.newTables += results.newTables;
    }

    logSyncResults(totalResults);
    updateClusterNames();

  } catch (const pqxx::sql_error &e) {
    Logger::error(
        LogCategory::DATABASE, "syncCatalogMariaDBToPostgres",
        "SQL ERROR in syncCatalogMariaDBToPostgres: " + std::string(e.what()) +
            " [SQL State: " + e.sqlstate() + "]");
  } catch (const pqxx::broken_connection &e) {
    Logger::error(LogCategory::DATABASE, "syncCatalogMariaDBToPostgres",
                  "CONNECTION ERROR in syncCatalogMariaDBToPostgres: " +
                      std::string(e.what()));
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "syncCatalogMariaDBToPostgres",
                  "ERROR in syncCatalogMariaDBToPostgres: " +
                      std::string(e.what()));
  }
}

// Schema validation helper functions
std::vector<CatalogTableInfo>
CatalogManager::getTablesForValidation(pqxx::connection &pgConn) {
  std::vector<CatalogTableInfo> tables;

  pqxx::work txn(pgConn);
  auto results = txn.exec("SELECT schema_name, table_name, db_engine, "
                          "connection_string, status "
                          "FROM metadata.catalog "
                          "WHERE active = true AND status IN "
                          "('LISTENING_CHANGES', 'FULL_LOAD') "
                          "ORDER BY db_engine, schema_name, table_name");
  txn.commit();

  for (const auto &row : results) {
    CatalogTableInfo table;
    table.schemaName = row[0].as<std::string>();
    table.tableName = row[1].as<std::string>();
    table.dbEngine = row[2].as<std::string>();
    table.connectionString = row[3].as<std::string>();
    table.status = row[4].as<std::string>();
    tables.push_back(table);
  }

  return tables;
}

ValidationResults
CatalogManager::validateAllTables(pqxx::connection &pgConn,
                                  const std::vector<CatalogTableInfo> &tables) {
  ValidationResults results;
  results.totalTables = tables.size();

  for (const auto &table : tables) {
    Logger::info(LogCategory::DATABASE,
                 "Validating schema: " + table.schemaName + "." +
                     table.tableName + " [" + table.dbEngine + "]");

    if (validateTableSchema(pgConn, table)) {
      results.validatedTables++;
    } else {
      resetTableSchema(pgConn, table);
      results.resetTables++;
    }
  }

  return results;
}

void CatalogManager::logValidationResults(const ValidationResults &results) {
  Logger::info(LogCategory::DATABASE,
               "Schema validation completed - Validated: " +
                   std::to_string(results.validatedTables) +
                   ", Reset: " + std::to_string(results.resetTables) +
                   ", Total: " + std::to_string(results.totalTables));
}

bool CatalogManager::validateTableSchema(pqxx::connection &pgConn,
                                         const CatalogTableInfo &table) {
  auto counts = getColumnCountsForEngine(table.dbEngine, table.connectionString,
                                         table.schemaName, table.tableName);

  int sourceColumnCount = counts.first;
  int targetColumnCount = counts.second;

  if (sourceColumnCount != targetColumnCount) {
    Logger::warning(
        LogCategory::DATABASE,
        "SCHEMA MISMATCH: " + table.schemaName + "." + table.tableName +
            " - Source columns: " + std::to_string(sourceColumnCount) +
            ", Target columns: " + std::to_string(targetColumnCount) +
            " - Dropping and resetting table");
    return false;
  } else {
    Logger::info(LogCategory::DATABASE,
                 "SCHEMA VALID: " + table.schemaName + "." + table.tableName +
                     " - Columns match: " + std::to_string(sourceColumnCount));
    return true;
  }
}

void CatalogManager::resetTableSchema(pqxx::connection &pgConn,
                                      const CatalogTableInfo &table) {
  pqxx::work resetTxn(pgConn);

  resetTxn.exec("DROP TABLE IF EXISTS \"" + escapeSQL(table.schemaName) +
                "\".\"" + escapeSQL(table.tableName) + "\"");

  resetTxn.exec("UPDATE metadata.catalog SET "
                "status = 'FULL_LOAD', "
                "last_offset = 0, "
                "last_processed_pk = 0 "
                "WHERE schema_name = '" +
                escapeSQL(table.schemaName) + "' AND table_name = '" +
                escapeSQL(table.tableName) + "' AND db_engine = '" +
                escapeSQL(table.dbEngine) + "'");
  resetTxn.commit();
}

std::pair<int, int> CatalogManager::getColumnCountsForEngine(
    const std::string &dbEngine, const std::string &connectionString,
    const std::string &schema, const std::string &table) {
  if (dbEngine == "MariaDB") {
    return getColumnCountsMariaDB(connectionString, schema, table);
  } else if (dbEngine == "MSSQL") {
    return getColumnCountsMSSQL(connectionString, schema, table);
  } else if (dbEngine == "PostgreSQL") {
    return getColumnCountsPostgres(connectionString, schema, table);
  }
  return {0, 0};
}

// MariaDB sync helper functions
std::vector<std::string>
CatalogManager::getMariaDBConnections(pqxx::connection &pgConn) {
  std::vector<std::string> connections;

  pqxx::work txn(pgConn);
  auto results = txn.exec("SELECT connection_string FROM metadata.catalog "
                          "WHERE db_engine='MariaDB' AND active=true;");
  txn.commit();

  for (const auto &row : results) {
    if (row.size() >= 1) {
      connections.push_back(row[0].as<std::string>());
    }
  }

  return connections;
}

MariaDBConnectionInfo CatalogManager::parseMariaDBConnectionString(
    const std::string &connectionString) {
  MariaDBConnectionInfo connInfo;

  std::istringstream ss(connectionString);
  std::string token;
  while (std::getline(ss, token, ';')) {
    auto pos = token.find('=');
    if (pos == std::string::npos)
      continue;

    std::string key = token.substr(0, pos);
    std::string value = token.substr(pos + 1);

    // Trim whitespace
    key.erase(0, key.find_first_not_of(" \t\r\n"));
    key.erase(key.find_last_not_of(" \t\r\n") + 1);
    value.erase(0, value.find_first_not_of(" \t\r\n"));
    value.erase(value.find_last_not_of(" \t\r\n") + 1);

    if (key == "host")
      connInfo.host = value;
    else if (key == "user")
      connInfo.user = value;
    else if (key == "password")
      connInfo.password = value;
    else if (key == "db")
      connInfo.database = value;
    else if (key == "port")
      connInfo.port = value;
  }

  // Parse port number
  if (!connInfo.port.empty()) {
    try {
      connInfo.portNumber = std::stoul(connInfo.port);
    } catch (...) {
      connInfo.portNumber = 3306;
    }
  }

  return connInfo;
}

MYSQL *CatalogManager::establishMariaDBConnection(
    const std::string &connectionString) {
  // Parse connection string to validate required parameters (like
  // MariaDBToPostgres.h)
  std::string host, user, password, db, port;
  std::istringstream ss(connectionString);
  std::string token;
  while (std::getline(ss, token, ';')) {
    auto pos = token.find('=');
    if (pos == std::string::npos)
      continue;
    std::string key = token.substr(0, pos);
    std::string value = token.substr(pos + 1);
    key.erase(0, key.find_first_not_of(" \t\r\n"));
    key.erase(key.find_last_not_of(" \t\r\n") + 1);
    value.erase(0, value.find_first_not_of(" \t\r\n"));
    value.erase(value.find_last_not_of(" \t\r\n") + 1);
    if (key == "host")
      host = value;
    else if (key == "user")
      user = value;
    else if (key == "password")
      password = value;
    else if (key == "db")
      db = value;
    else if (key == "port")
      port = value;
  }

  // Validate required parameters
  if (host.empty() || user.empty() || db.empty()) {
    Logger::error(LogCategory::DATABASE, "establishMariaDBConnection",
                  "Missing required connection parameters (host, user, or db)");
    return nullptr;
  }

  MYSQL *conn = mysql_init(nullptr);
  if (!conn) {
    Logger::error(LogCategory::DATABASE, "establishMariaDBConnection",
                  "mysql_init() failed");
    return nullptr;
  }

  unsigned int portNum = 3306;
  if (!port.empty()) {
    try {
      portNum = std::stoul(port);
      if (portNum == 0 || portNum > 65535) {
        Logger::warning(LogCategory::DATABASE, "establishMariaDBConnection",
                        "Invalid port number " + port + ", using default 3306");
        portNum = 3306;
      }
    } catch (const std::exception &e) {
      Logger::warning(LogCategory::DATABASE, "establishMariaDBConnection",
                      "Could not parse port " + port + ": " +
                          std::string(e.what()) + ", using default 3306");
      portNum = 3306;
    }
  }

  if (mysql_real_connect(conn, host.c_str(), user.c_str(), password.c_str(),
                         db.c_str(), portNum, nullptr, 0) == nullptr) {
    std::string errorMsg = mysql_error(conn);
    Logger::error(LogCategory::DATABASE, "establishMariaDBConnection",
                  "MariaDB connection failed: " + errorMsg + " (host: " + host +
                      ", user: " + user + ", db: " + db +
                      ", port: " + std::to_string(portNum) + ")");
    mysql_close(conn);
    return nullptr;
  }

  // Test connection with a simple query
  if (mysql_query(conn, "SELECT 1")) {
    std::string errorMsg = mysql_error(conn);
    Logger::error(LogCategory::DATABASE, "establishMariaDBConnection",
                  "Connection test failed: " + errorMsg);
    mysql_close(conn);
    return nullptr;
  }

  // Free the test result
  MYSQL_RES *testResult = mysql_store_result(conn);
  if (testResult) {
    mysql_free_result(testResult);
  }

  return conn;
}

void CatalogManager::configureMariaDBTimeouts(MYSQL *conn) {
  std::string timeoutQuery = "SET SESSION wait_timeout = 600, "
                             "interactive_timeout = 600, "
                             "net_read_timeout = 600, "
                             "net_write_timeout = 600, "
                             "innodb_lock_wait_timeout = 600, "
                             "lock_wait_timeout = 600";
  mysql_query(conn, timeoutQuery.c_str());
}

std::vector<std::vector<std::string>>
CatalogManager::discoverMariaDBTables(MYSQL *conn) {
  std::string query = "SELECT table_schema, table_name "
                      "FROM information_schema.tables "
                      "WHERE table_schema NOT IN ('information_schema', "
                      "'mysql', 'performance_schema', 'sys') "
                      "AND table_type = 'BASE TABLE' "
                      "ORDER BY table_schema, table_name";

  return executeQueryMariaDB(conn, query);
}

CatalogTableMetadata
CatalogManager::analyzeTableMetadata(MYSQL *conn, const std::string &schemaName,
                                     const std::string &tableName) {
  CatalogTableMetadata metadata;
  metadata.schemaName = schemaName;
  metadata.tableName = tableName;

  metadata.timeColumn = detectTimeColumnMariaDB(conn, schemaName, tableName);
  metadata.pkColumns = detectPrimaryKeyColumns(conn, schemaName, tableName);
  metadata.candidateColumns =
      detectCandidateColumns(conn, schemaName, tableName);
  metadata.pkStrategy =
      determinePKStrategy(metadata.pkColumns, metadata.candidateColumns);
  metadata.hasPK = !metadata.pkColumns.empty();

  Logger::info(LogCategory::DATABASE,
               "Detecting PK for table: " + schemaName + "." + tableName);
  Logger::info(LogCategory::DATABASE,
               "PK Detection Results for " + schemaName + "." + tableName +
                   ": hasPK=" + (metadata.hasPK ? "true" : "false") +
                   ", pkStrategy=" + metadata.pkStrategy + ", pkColumns=" +
                   columnsToJSON(metadata.pkColumns) + ", candidateColumns=" +
                   columnsToJSON(metadata.candidateColumns));

  return metadata;
}

int64_t CatalogManager::getTableSize(pqxx::connection &pgConn,
                                     const std::string &schemaName,
                                     const std::string &tableName) {
  try {
    pqxx::work txn(pgConn);
    std::string query =
        "SELECT COALESCE(reltuples::bigint, 0) FROM pg_class "
        "WHERE relname = '" +
        escapeSQL(tableName) +
        "' AND "
        "relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = '" +
        escapeSQL(schemaName) + "')";

    auto result = txn.exec(query);
    txn.commit();

    if (!result.empty() && !result[0][0].is_null()) {
      return result[0][0].as<int64_t>();
    }
  } catch (const std::exception &e) {
    // Silently continue with default size
  }

  return 0;
}

void CatalogManager::updateOrInsertTableMetadata(
    pqxx::connection &pgConn, const std::string &connectionString,
    const CatalogTableMetadata &metadata) {
  pqxx::work txn(pgConn);

  auto existingCheck =
      txn.exec("SELECT last_sync_column, pk_columns, pk_strategy, has_pk, "
               "candidate_columns, table_size "
               "FROM metadata.catalog "
               "WHERE schema_name='" +
               escapeSQL(metadata.schemaName) + "' AND table_name='" +
               escapeSQL(metadata.tableName) + "' AND db_engine='MariaDB'");

  if (!existingCheck.empty()) {
    // Update existing table
    std::string currentTimeColumn = existingCheck[0][0].is_null()
                                        ? ""
                                        : existingCheck[0][0].as<std::string>();
    std::string currentPKColumns = existingCheck[0][1].is_null()
                                       ? ""
                                       : existingCheck[0][1].as<std::string>();
    std::string currentPKStrategy = existingCheck[0][2].is_null()
                                        ? ""
                                        : existingCheck[0][2].as<std::string>();
    bool currentHasPK =
        existingCheck[0][3].is_null() ? false : existingCheck[0][3].as<bool>();
    std::string currentCandidateColumns =
        existingCheck[0][4].is_null() ? ""
                                      : existingCheck[0][4].as<std::string>();

    bool needsUpdate = false;
    std::string updateQuery = "UPDATE metadata.catalog SET ";

    if (currentTimeColumn != metadata.timeColumn) {
      updateQuery +=
          "last_sync_column = '" + escapeSQL(metadata.timeColumn) + "'";
      needsUpdate = true;
    }

    if (currentPKColumns != columnsToJSON(metadata.pkColumns)) {
      if (needsUpdate)
        updateQuery += ", ";
      updateQuery +=
          "pk_columns = '" + escapeSQL(columnsToJSON(metadata.pkColumns)) + "'";
      needsUpdate = true;
    }

    if (currentPKStrategy != metadata.pkStrategy) {
      if (needsUpdate)
        updateQuery += ", ";
      updateQuery += "pk_strategy = '" + escapeSQL(metadata.pkStrategy) + "'";
      needsUpdate = true;
    }

    if (currentHasPK != metadata.hasPK) {
      if (needsUpdate)
        updateQuery += ", ";
      updateQuery +=
          "has_pk = " + std::string(metadata.hasPK ? "true" : "false");
      needsUpdate = true;
    }

    if (currentCandidateColumns != columnsToJSON(metadata.candidateColumns)) {
      if (needsUpdate)
        updateQuery += ", ";
      updateQuery += "candidate_columns = '" +
                     escapeSQL(columnsToJSON(metadata.candidateColumns)) + "'";
      needsUpdate = true;
    }

    if (needsUpdate)
      updateQuery += ", ";
    updateQuery += "table_size = " + std::to_string(metadata.tableSize);
    needsUpdate = true;

    if (needsUpdate) {
      updateQuery += " WHERE schema_name='" + escapeSQL(metadata.schemaName) +
                     "' AND table_name='" + escapeSQL(metadata.tableName) +
                     "' AND db_engine='MariaDB'";
      txn.exec(updateQuery);
    }
  } else {
    // Insert new table
    txn.exec(
        "INSERT INTO metadata.catalog "
        "(schema_name, table_name, cluster_name, db_engine, connection_string, "
        "last_sync_time, last_sync_column, status, last_offset, active, "
        "pk_columns, pk_strategy, has_pk, candidate_columns, table_size) "
        "VALUES ('" +
        escapeSQL(metadata.schemaName) + "', '" +
        escapeSQL(metadata.tableName) + "', '', 'MariaDB', '" +
        escapeSQL(connectionString) + "', NOW(), '" +
        escapeSQL(metadata.timeColumn) + "', 'PENDING', '0', false, '" +
        escapeSQL(columnsToJSON(metadata.pkColumns)) + "', '" +
        escapeSQL(metadata.pkStrategy) + "', " +
        std::string(metadata.hasPK ? "true" : "false") + ", '" +
        escapeSQL(columnsToJSON(metadata.candidateColumns)) + "', " +
        std::to_string(metadata.tableSize) + ")");
  }

  txn.commit();
}

SyncResults
CatalogManager::processMariaDBConnection(pqxx::connection &pgConn,
                                         const std::string &connectionString) {
  SyncResults results;

  MYSQL *conn = establishMariaDBConnection(connectionString);
  if (!conn) {
    return results;
  }

  configureMariaDBTimeouts(conn);
  auto tables = discoverMariaDBTables(conn);

  results.totalTables = tables.size();
  results.processedConnections = 1;

  for (const auto &row : tables) {
    if (row.size() < 2)
      continue;

    std::string schemaName = row[0];
    std::string tableName = row[1];

    auto metadata = analyzeTableMetadata(conn, schemaName, tableName);
    metadata.tableSize = getTableSize(pgConn, schemaName, tableName);

    updateOrInsertTableMetadata(pgConn, connectionString, metadata);
    results.updatedTables++;
  }

  mysql_close(conn);
  return results;
}

void CatalogManager::logSyncResults(const SyncResults &results) {
  Logger::info(LogCategory::DATABASE,
               "MariaDB sync completed - Connections: " +
                   std::to_string(results.processedConnections) + "/" +
                   std::to_string(results.totalConnections) +
                   ", Tables: " + std::to_string(results.totalTables) +
                   ", Updated: " + std::to_string(results.updatedTables) +
                   ", New: " + std::to_string(results.newTables));
}

// MSSQL sync functions
void CatalogManager::syncCatalogMSSQLToPostgres() {
  try {
    pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());

    auto connections = getMSSQLConnections(pgConn);
    Logger::info(LogCategory::DATABASE, "Found " +
                                            std::to_string(connections.size()) +
                                            " MSSQL connection(s)");

    if (connections.empty()) {
      Logger::warning(LogCategory::DATABASE,
                      "No MSSQL connections found in catalog");
      return;
    }

    SyncResults totalResults;
    totalResults.totalConnections = connections.size();

    for (const auto &connectionString : connections) {
      auto results = processMSSQLConnection(pgConn, connectionString);
      totalResults.processedConnections += results.processedConnections;
      totalResults.totalTables += results.totalTables;
      totalResults.updatedTables += results.updatedTables;
      totalResults.newTables += results.newTables;
    }

    Logger::info(
        LogCategory::DATABASE,
        "MSSQL sync completed - Connections: " +
            std::to_string(totalResults.processedConnections) + "/" +
            std::to_string(totalResults.totalConnections) +
            ", Tables: " + std::to_string(totalResults.totalTables) +
            ", Updated: " + std::to_string(totalResults.updatedTables) +
            ", New: " + std::to_string(totalResults.newTables));

    updateClusterNames();

  } catch (const pqxx::sql_error &e) {
    Logger::error(
        LogCategory::DATABASE, "syncCatalogMSSQLToPostgres",
        "SQL ERROR in syncCatalogMSSQLToPostgres: " + std::string(e.what()) +
            " [SQL State: " + e.sqlstate() + "]");
  } catch (const pqxx::broken_connection &e) {
    Logger::error(LogCategory::DATABASE, "syncCatalogMSSQLToPostgres",
                  "CONNECTION ERROR in syncCatalogMSSQLToPostgres: " +
                      std::string(e.what()));
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "syncCatalogMSSQLToPostgres",
                  "ERROR in syncCatalogMSSQLToPostgres: " +
                      std::string(e.what()));
  }
}

// MSSQL sync helper functions
std::vector<std::string>
CatalogManager::getMSSQLConnections(pqxx::connection &pgConn) {
  std::vector<std::string> connections;

  pqxx::work txn(pgConn);
  auto results = txn.exec("SELECT connection_string FROM metadata.catalog "
                          "WHERE db_engine='MSSQL' AND active=true;");
  txn.commit();

  for (const auto &row : results) {
    if (row.size() >= 1) {
      connections.push_back(row[0].as<std::string>());
    }
  }

  return connections;
}

MSSQLConnectionInfo CatalogManager::parseMSSQLConnectionString(
    const std::string &connectionString) {
  MSSQLConnectionInfo connInfo;

  std::istringstream ss(connectionString);
  std::string token;
  while (std::getline(ss, token, ';')) {
    auto pos = token.find('=');
    if (pos == std::string::npos)
      continue;

    std::string key = token.substr(0, pos);
    std::string value = token.substr(pos + 1);

    // Trim whitespace
    key.erase(0, key.find_first_not_of(" \t\r\n"));
    key.erase(key.find_last_not_of(" \t\r\n") + 1);
    value.erase(0, value.find_first_not_of(" \t\r\n"));
    value.erase(value.find_last_not_of(" \t\r\n") + 1);

    if (key == "SERVER")
      connInfo.server = value;
    else if (key == "DATABASE")
      connInfo.database = value;
    else if (key == "UID")
      connInfo.uid = value;
    else if (key == "PWD")
      connInfo.pwd = value;
    else if (key == "DRIVER")
      connInfo.driver = value;
    else if (key == "PORT")
      connInfo.port = value;
    else if (key == "TrustServerCertificate")
      connInfo.trustedConnection = value;
  }

  return connInfo;
}

SQLHDBC
CatalogManager::establishMSSQLConnection(const std::string &connectionString) {
  // Parse connection string to validate required parameters (like
  // MSSQLToPostgres.h)
  std::string server, database, uid, pwd, port;
  std::istringstream ss(connectionString);
  std::string token;
  while (std::getline(ss, token, ';')) {
    auto pos = token.find('=');
    if (pos == std::string::npos)
      continue;
    std::string key = token.substr(0, pos);
    std::string value = token.substr(pos + 1);
    key.erase(0, key.find_first_not_of(" \t\r\n"));
    key.erase(key.find_last_not_of(" \t\r\n") + 1);
    value.erase(0, value.find_first_not_of(" \t\r\n"));
    value.erase(value.find_last_not_of(" \t\r\n") + 1);
    if (key == "SERVER")
      server = value;
    else if (key == "DATABASE")
      database = value;
    else if (key == "UID")
      uid = value;
    else if (key == "PWD")
      pwd = value;
    else if (key == "PORT")
      port = value;
  }

  if (server.empty() || database.empty() || uid.empty()) {
    Logger::error(
        LogCategory::DATABASE, "establishMSSQLConnection",
        "Missing required connection parameters (SERVER, DATABASE, or UID)");
    return SQL_NULL_HDBC;
  }

  SQLHENV env;
  SQLHDBC conn;
  SQLRETURN ret;

  ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
  if (ret != SQL_SUCCESS) {
    Logger::error(LogCategory::DATABASE, "establishMSSQLConnection",
                  "Failed to allocate environment handle");
    return SQL_NULL_HDBC;
  }

  ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void *)SQL_OV_ODBC3, 0);
  if (ret != SQL_SUCCESS) {
    Logger::error(LogCategory::DATABASE, "establishMSSQLConnection",
                  "Failed to set ODBC version");
    SQLFreeHandle(SQL_HANDLE_ENV, env);
    return SQL_NULL_HDBC;
  }

  ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);
  if (ret != SQL_SUCCESS) {
    Logger::error(LogCategory::DATABASE, "establishMSSQLConnection",
                  "Failed to allocate connection handle");
    SQLFreeHandle(SQL_HANDLE_ENV, env);
    return SQL_NULL_HDBC;
  }

  // Use the original connection string directly (like MSSQLToPostgres.h)
  ret = SQLDriverConnect(conn, NULL, (SQLCHAR *)connectionString.c_str(),
                         SQL_NTS, NULL, 0, NULL, SQL_DRIVER_NOPROMPT);
  if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
    Logger::error(LogCategory::DATABASE, "establishMSSQLConnection",
                  "Failed to connect to MSSQL server");
    SQLFreeHandle(SQL_HANDLE_DBC, conn);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
    return SQL_NULL_HDBC;
  }

  SQLFreeHandle(SQL_HANDLE_ENV, env);
  return conn;
}

std::vector<std::vector<std::string>>
CatalogManager::discoverMSSQLTables(SQLHDBC conn) {
  std::string query = "SELECT TABLE_SCHEMA, TABLE_NAME "
                      "FROM INFORMATION_SCHEMA.TABLES "
                      "WHERE TABLE_TYPE = 'BASE TABLE' "
                      "AND TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA', 'SYS') "
                      "ORDER BY TABLE_SCHEMA, TABLE_NAME";

  return executeQueryMSSQL(conn, query);
}

CatalogTableMetadata CatalogManager::analyzeMSSQLTableMetadata(
    SQLHDBC conn, const std::string &schemaName, const std::string &tableName) {
  CatalogTableMetadata metadata;
  metadata.schemaName = schemaName;
  metadata.tableName = tableName;

  metadata.timeColumn = detectTimeColumnMSSQL(conn, schemaName, tableName);
  metadata.pkColumns =
      detectPrimaryKeyColumnsMSSQL(conn, schemaName, tableName);
  metadata.candidateColumns =
      detectCandidateColumnsMSSQL(conn, schemaName, tableName);
  metadata.pkStrategy =
      determinePKStrategy(metadata.pkColumns, metadata.candidateColumns);
  metadata.hasPK = !metadata.pkColumns.empty();

  Logger::info(LogCategory::DATABASE,
               "Detecting PK for table: " + schemaName + "." + tableName);
  Logger::info(LogCategory::DATABASE,
               "PK Detection Results for " + schemaName + "." + tableName +
                   ": hasPK=" + (metadata.hasPK ? "true" : "false") +
                   ", pkStrategy=" + metadata.pkStrategy + ", pkColumns=" +
                   columnsToJSON(metadata.pkColumns) + ", candidateColumns=" +
                   columnsToJSON(metadata.candidateColumns));

  return metadata;
}

SyncResults
CatalogManager::processMSSQLConnection(pqxx::connection &pgConn,
                                       const std::string &connectionString) {
  SyncResults results;

  SQLHDBC conn = establishMSSQLConnection(connectionString);
  if (conn == SQL_NULL_HDBC) {
    return results;
  }

  auto tables = discoverMSSQLTables(conn);

  results.totalTables = tables.size();
  results.processedConnections = 1;

  for (const auto &row : tables) {
    if (row.size() < 2)
      continue;

    std::string schemaName = row[0];
    std::string tableName = row[1];

    auto metadata = analyzeMSSQLTableMetadata(conn, schemaName, tableName);
    metadata.tableSize = getTableSize(pgConn, schemaName, tableName);

    updateOrInsertTableMetadata(pgConn, connectionString, metadata);
    results.updatedTables++;
  }

  SQLDisconnect(conn);
  SQLFreeHandle(SQL_HANDLE_DBC, conn);
  return results;
}

// PostgreSQL sync functions
void CatalogManager::syncCatalogPostgresToPostgres() {
  try {
    pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());

    auto connections = getPostgresConnections(pgConn);
    Logger::info(LogCategory::DATABASE, "Found " +
                                            std::to_string(connections.size()) +
                                            " PostgreSQL connection(s)");

    if (connections.empty()) {
      Logger::warning(LogCategory::DATABASE,
                      "No PostgreSQL connections found in catalog");
      return;
    }

    SyncResults totalResults;
    totalResults.totalConnections = connections.size();

    for (const auto &connectionString : connections) {
      auto results = processPostgresConnection(pgConn, connectionString);
      totalResults.processedConnections += results.processedConnections;
      totalResults.totalTables += results.totalTables;
      totalResults.updatedTables += results.updatedTables;
      totalResults.newTables += results.newTables;
    }

    Logger::info(
        LogCategory::DATABASE,
        "PostgreSQL sync completed - Connections: " +
            std::to_string(totalResults.processedConnections) + "/" +
            std::to_string(totalResults.totalConnections) +
            ", Tables: " + std::to_string(totalResults.totalTables) +
            ", Updated: " + std::to_string(totalResults.updatedTables) +
            ", New: " + std::to_string(totalResults.newTables));

    updateClusterNames();

  } catch (const pqxx::sql_error &e) {
    Logger::error(
        LogCategory::DATABASE, "syncCatalogPostgresToPostgres",
        "SQL ERROR in syncCatalogPostgresToPostgres: " + std::string(e.what()) +
            " [SQL State: " + e.sqlstate() + "]");
  } catch (const pqxx::broken_connection &e) {
    Logger::error(LogCategory::DATABASE, "syncCatalogPostgresToPostgres",
                  "CONNECTION ERROR in syncCatalogPostgresToPostgres: " +
                      std::string(e.what()));
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "syncCatalogPostgresToPostgres",
                  "ERROR in syncCatalogPostgresToPostgres: " +
                      std::string(e.what()));
  }
}

// PostgreSQL sync helper functions
std::vector<std::string>
CatalogManager::getPostgresConnections(pqxx::connection &pgConn) {
  std::vector<std::string> connections;

  pqxx::work txn(pgConn);
  auto results = txn.exec("SELECT connection_string FROM metadata.catalog "
                          "WHERE db_engine='PostgreSQL' AND active=true;");
  txn.commit();

  for (const auto &row : results) {
    if (row.size() >= 1) {
      connections.push_back(row[0].as<std::string>());
    }
  }

  return connections;
}

PostgresConnectionInfo CatalogManager::parsePostgresConnectionString(
    const std::string &connectionString) {
  PostgresConnectionInfo connInfo;

  std::istringstream ss(connectionString);
  std::string token;
  while (std::getline(ss, token, ' ')) {
    auto pos = token.find('=');
    if (pos == std::string::npos)
      continue;

    std::string key = token.substr(0, pos);
    std::string value = token.substr(pos + 1);

    // Remove quotes if present
    if (value.front() == '\'' && value.back() == '\'') {
      value = value.substr(1, value.length() - 2);
    }

    if (key == "host")
      connInfo.host = value;
    else if (key == "port")
      connInfo.port = value;
    else if (key == "dbname")
      connInfo.dbname = value;
    else if (key == "user")
      connInfo.user = value;
    else if (key == "password")
      connInfo.password = value;
    else if (key == "sslmode")
      connInfo.sslmode = value;
  }

  return connInfo;
}

std::unique_ptr<pqxx::connection> CatalogManager::establishPostgresConnection(
    const PostgresConnectionInfo &connInfo) {
  if (connInfo.host.empty() || connInfo.dbname.empty()) {
    Logger::error(LogCategory::DATABASE, "establishPostgresConnection",
                  "Missing required connection parameters (host, dbname)");
    return nullptr;
  }

  try {
    std::string connectionString = "host=" + connInfo.host;
    if (!connInfo.port.empty()) {
      connectionString += " port=" + connInfo.port;
    }
    connectionString += " dbname=" + connInfo.dbname;
    if (!connInfo.user.empty()) {
      connectionString += " user=" + connInfo.user;
    }
    if (!connInfo.password.empty()) {
      connectionString += " password=" + connInfo.password;
    }
    if (!connInfo.sslmode.empty()) {
      connectionString += " sslmode=" + connInfo.sslmode;
    }

    return std::make_unique<pqxx::connection>(connectionString);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "establishPostgresConnection",
                  "Failed to connect to PostgreSQL: " + std::string(e.what()));
    return nullptr;
  }
}

std::vector<std::vector<std::string>>
CatalogManager::discoverPostgresTables(pqxx::connection &conn) {
  std::string query =
      "SELECT schemaname, tablename "
      "FROM pg_tables "
      "WHERE schemaname NOT IN ('information_schema', 'pg_catalog') "
      "ORDER BY schemaname, tablename";

  std::vector<std::vector<std::string>> results;

  try {
    pqxx::work txn(conn);
    auto queryResult = txn.exec(query);
    txn.commit();

    for (const auto &row : queryResult) {
      std::vector<std::string> tableInfo;
      tableInfo.push_back(row[0].as<std::string>());
      tableInfo.push_back(row[1].as<std::string>());
      results.push_back(tableInfo);
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "discoverPostgresTables",
                  "Failed to discover tables: " + std::string(e.what()));
  }

  return results;
}

CatalogTableMetadata
CatalogManager::analyzePostgresTableMetadata(pqxx::connection &conn,
                                             const std::string &schemaName,
                                             const std::string &tableName) {
  CatalogTableMetadata metadata;
  metadata.schemaName = schemaName;
  metadata.tableName = tableName;

  metadata.timeColumn = detectTimeColumnPostgres(conn, schemaName, tableName);
  metadata.pkColumns =
      detectPrimaryKeyColumnsPostgres(conn, schemaName, tableName);
  metadata.candidateColumns =
      detectCandidateColumnsPostgres(conn, schemaName, tableName);
  metadata.pkStrategy =
      determinePKStrategy(metadata.pkColumns, metadata.candidateColumns);
  metadata.hasPK = !metadata.pkColumns.empty();

  Logger::info(LogCategory::DATABASE,
               "Detecting PK for table: " + schemaName + "." + tableName);
  Logger::info(LogCategory::DATABASE,
               "PK Detection Results for " + schemaName + "." + tableName +
                   ": hasPK=" + (metadata.hasPK ? "true" : "false") +
                   ", pkStrategy=" + metadata.pkStrategy + ", pkColumns=" +
                   columnsToJSON(metadata.pkColumns) + ", candidateColumns=" +
                   columnsToJSON(metadata.candidateColumns));

  return metadata;
}

SyncResults
CatalogManager::processPostgresConnection(pqxx::connection &pgConn,
                                          const std::string &connectionString) {
  SyncResults results;

  auto connInfo = parsePostgresConnectionString(connectionString);
  auto conn = establishPostgresConnection(connInfo);
  if (!conn) {
    return results;
  }

  auto tables = discoverPostgresTables(*conn);

  results.totalTables = tables.size();
  results.processedConnections = 1;

  for (const auto &row : tables) {
    if (row.size() < 2)
      continue;

    std::string schemaName = row[0];
    std::string tableName = row[1];

    auto metadata = analyzePostgresTableMetadata(*conn, schemaName, tableName);
    metadata.tableSize = getTableSize(pgConn, schemaName, tableName);

    updateOrInsertTableMetadata(pgConn, connectionString, metadata);
    results.updatedTables++;
  }

  return results;
}

// ============================================================================
// UNIFIED UTILITY FUNCTIONS
// ============================================================================

std::string CatalogManager::escapeSQL(const std::string &input) {
  std::string escaped = input;
  size_t pos = 0;

  while ((pos = escaped.find("'", pos)) != std::string::npos) {
    escaped.replace(pos, 1, "''");
    pos += 2;
  }

  Logger::debug(LogCategory::DATABASE, "escapeSQL",
                "Escaped SQL string: " + input + " -> " + escaped);
  return escaped;
}

std::string
CatalogManager::columnsToJSON(const std::vector<std::string> &columns) {
  if (columns.empty()) {
    return "[]";
  }

  std::string json = "[";
  for (size_t i = 0; i < columns.size(); ++i) {
    if (i > 0)
      json += ",";
    json += "\"" + columns[i] + "\"";
  }
  json += "]";

  Logger::debug(LogCategory::DATABASE, "columnsToJSON",
                "Converted " + std::to_string(columns.size()) +
                    " columns to JSON");
  return json;
}

std::string CatalogManager::determinePKStrategy(
    const std::vector<std::string> &pkColumns,
    const std::vector<std::string> &candidateColumns,
    const std::string &timeColumn) {
  if (!pkColumns.empty()) {
    Logger::debug(LogCategory::DATABASE, "determinePKStrategy",
                  "Using PK strategy - " + std::to_string(pkColumns.size()) +
                      " PK columns");
    return "PK";
  }

  if (!timeColumn.empty()) {
    Logger::debug(LogCategory::DATABASE, "determinePKStrategy",
                  "Using TEMPORAL_PK strategy - time column: " + timeColumn);
    return "TEMPORAL_PK";
  }

  if (!candidateColumns.empty()) {
    Logger::debug(LogCategory::DATABASE, "determinePKStrategy",
                  "Using OFFSET strategy - " +
                      std::to_string(candidateColumns.size()) +
                      " candidate columns");
    return "OFFSET";
  }

  Logger::warning(LogCategory::DATABASE, "determinePKStrategy",
                  "No suitable strategy found, defaulting to OFFSET");
  return "OFFSET";
}

// ============================================================================
// UNIFIED DETECTION FUNCTIONS
// ============================================================================

std::vector<std::string>
CatalogManager::detectPrimaryKeyColumns(DBEngine engine, void *connection,
                                        const std::string &schema,
                                        const std::string &table) {
  std::vector<std::string> pkColumns;

  try {
    std::string query;

    switch (engine) {
    case DBEngine::MARIADB: {
      query = "SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE "
              "WHERE TABLE_SCHEMA = '" +
              escapeSQL(schema) +
              "' "
              "AND TABLE_NAME = '" +
              escapeSQL(table) +
              "' "
              "AND CONSTRAINT_NAME = 'PRIMARY' "
              "ORDER BY ORDINAL_POSITION";

      auto results = executeQuery(engine, connection, query);
      for (const auto &row : results) {
        if (!row.empty()) {
          pkColumns.push_back(row[0]);
        }
      }
      break;
    }

    case DBEngine::MSSQL: {
      query =
          "SELECT c.COLUMN_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc "
          "JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE c ON tc.CONSTRAINT_NAME = "
          "c.CONSTRAINT_NAME "
          "WHERE tc.TABLE_SCHEMA = '" +
          escapeSQL(schema) +
          "' "
          "AND tc.TABLE_NAME = '" +
          escapeSQL(table) +
          "' "
          "AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY' "
          "ORDER BY c.ORDINAL_POSITION";

      auto results = executeQuery(engine, connection, query);
      for (const auto &row : results) {
        if (!row.empty()) {
          pkColumns.push_back(row[0]);
        }
      }
      break;
    }

    case DBEngine::POSTGRES: {
      query = "SELECT a.attname FROM pg_index i "
              "JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = "
              "ANY(i.indkey) "
              "JOIN pg_class c ON c.oid = i.indrelid "
              "JOIN pg_namespace n ON n.oid = c.relnamespace "
              "WHERE i.indisprimary AND n.nspname = '" +
              escapeSQL(schema) +
              "' "
              "AND c.relname = '" +
              escapeSQL(table) +
              "' "
              "ORDER BY a.attnum";

      auto results = executeQuery(engine, connection, query);
      for (const auto &row : results) {
        if (!row.empty()) {
          pkColumns.push_back(row[0]);
        }
      }
      break;
    }
    }

    Logger::info(LogCategory::DATABASE, "detectPrimaryKeyColumns",
                 "Found " + std::to_string(pkColumns.size()) +
                     " PK columns for " + schema + "." + table);

  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "detectPrimaryKeyColumns",
                  "Error detecting PK columns: " + std::string(e.what()));
  }

  return pkColumns;
}

std::vector<std::string>
CatalogManager::detectCandidateColumns(DBEngine engine, void *connection,
                                       const std::string &schema,
                                       const std::string &table) {
  std::vector<std::string> candidateColumns;

  try {
    std::string query;

    switch (engine) {
    case DBEngine::MARIADB: {
      query = "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
              "WHERE TABLE_SCHEMA = '" +
              escapeSQL(schema) +
              "' "
              "AND TABLE_NAME = '" +
              escapeSQL(table) +
              "' "
              "AND COLUMN_KEY != 'PRI' "
              "AND (DATA_TYPE IN ('int', 'bigint', 'varchar', 'char') "
              "OR COLUMN_NAME LIKE '%id%' OR COLUMN_NAME LIKE '%key%') "
              "ORDER BY COLUMN_NAME";
      break;
    }

    case DBEngine::MSSQL: {
      query = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
              "WHERE TABLE_SCHEMA = '" +
              escapeSQL(schema) +
              "' "
              "AND TABLE_NAME = '" +
              escapeSQL(table) +
              "' "
              "AND (DATA_TYPE IN ('int', 'bigint', 'varchar', 'char') "
              "OR COLUMN_NAME LIKE '%id%' OR COLUMN_NAME LIKE '%key%') "
              "ORDER BY COLUMN_NAME";
      break;
    }

    case DBEngine::POSTGRES: {
      query = "SELECT column_name FROM information_schema.columns "
              "WHERE table_schema = '" +
              escapeSQL(schema) +
              "' "
              "AND table_name = '" +
              escapeSQL(table) +
              "' "
              "AND (data_type IN ('integer', 'bigint', 'character varying', "
              "'character') "
              "OR column_name LIKE '%id%' OR column_name LIKE '%key%') "
              "ORDER BY column_name";
      break;
    }
    }

    auto results = executeQuery(engine, connection, query);
    for (const auto &row : results) {
      if (!row.empty()) {
        candidateColumns.push_back(row[0]);
      }
    }

    Logger::info(LogCategory::DATABASE, "detectCandidateColumns",
                 "Found " + std::to_string(candidateColumns.size()) +
                     " candidate columns for " + schema + "." + table);

  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "detectCandidateColumns",
                  "Error detecting candidate columns: " +
                      std::string(e.what()));
  }

  return candidateColumns;
}

std::string CatalogManager::detectTimeColumn(DBEngine engine, void *connection,
                                             const std::string &schema,
                                             const std::string &table) {
  std::string timeColumn;

  try {
    std::string query;

    switch (engine) {
    case DBEngine::MARIADB: {
      query =
          "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
          "WHERE TABLE_SCHEMA = '" +
          escapeSQL(schema) +
          "' "
          "AND TABLE_NAME = '" +
          escapeSQL(table) +
          "' "
          "AND (DATA_TYPE IN ('datetime', 'timestamp', 'date') "
          "OR COLUMN_NAME LIKE '%time%' OR COLUMN_NAME LIKE '%date%' "
          "OR COLUMN_NAME LIKE '%created%' OR COLUMN_NAME LIKE '%updated%') "
          "ORDER BY COLUMN_NAME LIMIT 1";
      break;
    }

    case DBEngine::MSSQL: {
      query =
          "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
          "WHERE TABLE_SCHEMA = '" +
          escapeSQL(schema) +
          "' "
          "AND TABLE_NAME = '" +
          escapeSQL(table) +
          "' "
          "AND (DATA_TYPE IN ('datetime', 'datetime2', 'timestamp', 'date') "
          "OR COLUMN_NAME LIKE '%time%' OR COLUMN_NAME LIKE '%date%' "
          "OR COLUMN_NAME LIKE '%created%' OR COLUMN_NAME LIKE '%updated%') "
          "ORDER BY COLUMN_NAME";
      break;
    }

    case DBEngine::POSTGRES: {
      query =
          "SELECT column_name FROM information_schema.columns "
          "WHERE table_schema = '" +
          escapeSQL(schema) +
          "' "
          "AND table_name = '" +
          escapeSQL(table) +
          "' "
          "AND (data_type IN ('timestamp', 'timestamptz', 'date', 'time') "
          "OR column_name LIKE '%time%' OR column_name LIKE '%date%' "
          "OR column_name LIKE '%created%' OR column_name LIKE '%updated%') "
          "ORDER BY column_name LIMIT 1";
      break;
    }
    }

    auto results = executeQuery(engine, connection, query);
    if (!results.empty() && !results[0].empty()) {
      timeColumn = results[0][0];
    }

    if (!timeColumn.empty()) {
      Logger::info(LogCategory::DATABASE, "detectTimeColumn",
                   "Found time column: " + timeColumn + " for " + schema + "." +
                       table);
    } else {
      Logger::debug(LogCategory::DATABASE, "detectTimeColumn",
                    "No time column found for " + schema + "." + table);
    }

  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "detectTimeColumn",
                  "Error detecting time column: " + std::string(e.what()));
  }

  return timeColumn;
}

// ============================================================================
// UNIFIED UTILITY FUNCTIONS
// ============================================================================

std::pair<int, int> CatalogManager::getColumnCounts(
    DBEngine engine, const std::string &connectionString,
    const std::string &schema, const std::string &table) {
  try {
    switch (engine) {
    case DBEngine::MARIADB: {
      return getColumnCountsMariaDB(connectionString, schema, table);
    }
    case DBEngine::MSSQL: {
      return getColumnCountsMSSQL(connectionString, schema, table);
    }
    case DBEngine::POSTGRES: {
      return getColumnCountsPostgres(connectionString, schema, table);
    }
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "getColumnCounts",
                  "Error getting column counts: " + std::string(e.what()));
  }

  return {0, 0};
}

std::vector<std::vector<std::string>>
CatalogManager::executeQuery(DBEngine engine, void *connection,
                             const std::string &query) {
  std::vector<std::vector<std::string>> results;

  try {
    switch (engine) {
    case DBEngine::MARIADB: {
      return executeQueryMariaDB(static_cast<MYSQL *>(connection), query);
    }
    case DBEngine::MSSQL: {
      return executeQueryMSSQL(static_cast<SQLHDBC>(connection), query);
    }
    case DBEngine::POSTGRES: {
      auto *pgConn = static_cast<pqxx::connection *>(connection);
      pqxx::work txn(*pgConn);
      auto queryResult = txn.exec(query);
      txn.commit();

      for (const auto &row : queryResult) {
        std::vector<std::string> rowData;
        for (size_t i = 0; i < row.size(); ++i) {
          rowData.push_back(row[i].as<std::string>());
        }
        results.push_back(rowData);
      }
      break;
    }
    }

    Logger::debug(LogCategory::DATABASE, "executeQuery",
                  "Executed query, returned " + std::to_string(results.size()) +
                      " rows");

  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "executeQuery",
                  "Error executing query: " + std::string(e.what()));
  }

  return results;
}

// ============================================================================
// UNIFIED CLEANUP FUNCTIONS
// ============================================================================

void CatalogManager::cleanCatalogTables(DBEngine engine,
                                        pqxx::connection &pgConn,
                                        CleanupType type) {
  try {
    std::string query;

    switch (type) {
    case CleanupType::NON_EXISTENT: {
      std::string engineName;
      switch (engine) {
      case DBEngine::MARIADB:
        engineName = "MariaDB";
        break;
      case DBEngine::MSSQL:
        engineName = "MSSQL";
        break;
      case DBEngine::POSTGRES:
        engineName = "PostgreSQL";
        break;
      }

      query = "DELETE FROM metadata.catalog WHERE db_engine = '" +
              escapeSQL(engineName) +
              "' "
              "AND status = 'ACTIVE' AND NOT EXISTS ("
              "SELECT 1 FROM information_schema.tables "
              "WHERE table_schema = metadata.catalog.schema_name "
              "AND table_name = metadata.catalog.table_name)";
      break;
    }

    case CleanupType::ORPHANED: {
      query = "DELETE FROM metadata.catalog WHERE status = 'ACTIVE' "
              "AND connection_string NOT IN ("
              "SELECT DISTINCT connection_string FROM metadata.catalog)";
      break;
    }

    case CleanupType::INCONSISTENT_PAGINATION: {
      query = "UPDATE metadata.catalog SET pk_strategy = 'OFFSET', "
              "pk_columns = '[]', candidate_columns = '[]' "
              "WHERE pk_strategy = 'PK' AND pk_columns = '[]'";
      break;
    }
    }

    pqxx::work txn(pgConn);
    auto result = txn.exec(query);
    txn.commit();

    Logger::info(
        LogCategory::DATABASE, "cleanCatalogTables",
        "Cleaned " + std::to_string(result.affected_rows()) + " records for " +
            (type == CleanupType::NON_EXISTENT ? "non-existent"
             : type == CleanupType::ORPHANED   ? "orphaned"
                                               : "inconsistent pagination") +
            " tables");

  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "cleanCatalogTables",
                  "Error cleaning catalog tables: " + std::string(e.what()));
  }
}

// ============================================================================
// UNIFIED NAMING FUNCTIONS
// ============================================================================

std::string
CatalogManager::resolveClusterName(const std::string &connectionString,
                                   DBEngine engine) {
  try {
    std::string hostname =
        extractHostnameFromConnection(connectionString, engine);
    if (hostname.empty()) {
      Logger::warning(LogCategory::DATABASE, "resolveClusterName",
                      "Could not extract hostname from connection string");
      return "unknown";
    }

    std::string clusterName = getClusterNameFromHostname(hostname);
    if (clusterName.empty()) {
      clusterName = hostname;
    }

    Logger::info(LogCategory::DATABASE, "resolveClusterName",
                 "Resolved cluster name: " + connectionString + " -> " +
                     clusterName);
    return clusterName;

  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "resolveClusterName",
                  "Error resolving cluster name: " + std::string(e.what()));
    return "unknown";
  }
}

std::string CatalogManager::extractHostnameFromConnection(
    const std::string &connectionString, DBEngine engine) {
  try {
    std::string hostname;

    switch (engine) {
    case DBEngine::MARIADB: {
      // Parse connection string to extract hostname (like MariaDBToPostgres.h)
      std::istringstream ss(connectionString);
      std::string token;
      while (std::getline(ss, token, ';')) {
        auto pos = token.find('=');
        if (pos == std::string::npos)
          continue;
        std::string key = token.substr(0, pos);
        std::string value = token.substr(pos + 1);
        key.erase(0, key.find_first_not_of(" \t\r\n"));
        key.erase(key.find_last_not_of(" \t\r\n") + 1);
        value.erase(0, value.find_first_not_of(" \t\r\n"));
        value.erase(value.find_last_not_of(" \t\r\n") + 1);
        if (key == "host") {
          hostname = value;
          break;
        }
      }
      break;
    }
    case DBEngine::MSSQL: {
      auto connInfo = parseMSSQLConnectionString(connectionString);
      hostname = connInfo.server;
      break;
    }
    case DBEngine::POSTGRES: {
      auto connInfo = parsePostgresConnectionString(connectionString);
      hostname = connInfo.host;
      break;
    }
    }

    Logger::debug(LogCategory::DATABASE, "extractHostnameFromConnection",
                  "Extracted hostname: " + hostname + " from " +
                      connectionString);
    return hostname;

  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "extractHostnameFromConnection",
                  "Error extracting hostname: " + std::string(e.what()));
    return "";
  }
}

std::string
CatalogManager::getClusterNameFromHostname(const std::string &hostname) {
  try {
    if (hostname.empty()) {
      return "";
    }

    std::string clusterName = hostname;

    size_t dotPos = hostname.find('.');
    if (dotPos != std::string::npos) {
      clusterName = hostname.substr(0, dotPos);
    }

    std::transform(clusterName.begin(), clusterName.end(), clusterName.begin(),
                   ::tolower);

    Logger::debug(LogCategory::DATABASE, "getClusterNameFromHostname",
                  "Converted hostname: " + hostname + " -> " + clusterName);
    return clusterName;

  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "getClusterNameFromHostname",
                  "Error getting cluster name: " + std::string(e.what()));
    return hostname;
  }
}

// ============================================================================
// LEGACY FUNCTIONS (for backward compatibility)
// ============================================================================

std::vector<std::vector<std::string>>
CatalogManager::executeQueryMariaDB(MYSQL *conn, const std::string &query) {
  std::vector<std::vector<std::string>> results;

  try {
    if (mysql_query(conn, query.c_str()) != 0) {
      Logger::error(LogCategory::DATABASE, "executeQueryMariaDB",
                    "Query failed: " + std::string(mysql_error(conn)));
      return results;
    }

    MYSQL_RES *result = mysql_store_result(conn);
    if (!result) {
      Logger::error(LogCategory::DATABASE, "executeQueryMariaDB",
                    "Failed to store result: " +
                        std::string(mysql_error(conn)));
      return results;
    }

    MYSQL_ROW row;
    while ((row = mysql_fetch_row(result)) != nullptr) {
      std::vector<std::string> rowData;
      int numFields = mysql_num_fields(result);
      for (int i = 0; i < numFields; ++i) {
        rowData.push_back(row[i] ? row[i] : "");
      }
      results.push_back(rowData);
    }

    mysql_free_result(result);
    Logger::debug(LogCategory::DATABASE, "executeQueryMariaDB",
                  "Executed query, returned " + std::to_string(results.size()) +
                      " rows");

  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "executeQueryMariaDB",
                  "Error executing MariaDB query: " + std::string(e.what()));
  }

  return results;
}

std::vector<std::vector<std::string>>
CatalogManager::executeQueryMSSQL(SQLHDBC conn, const std::string &query) {
  std::vector<std::vector<std::string>> results;

  try {
    SQLHSTMT stmt;
    if (SQLAllocHandle(SQL_HANDLE_STMT, conn, &stmt) != SQL_SUCCESS) {
      Logger::error(LogCategory::DATABASE, "executeQueryMSSQL",
                    "Failed to allocate statement handle");
      return results;
    }

    if (SQLExecDirect(stmt, (SQLCHAR *)query.c_str(), SQL_NTS) != SQL_SUCCESS) {
      Logger::error(LogCategory::DATABASE, "executeQueryMSSQL",
                    "Query execution failed");
      SQLFreeHandle(SQL_HANDLE_STMT, stmt);
      return results;
    }

    SQLSMALLINT numColumns;
    SQLNumResultCols(stmt, &numColumns);

    SQLCHAR columnData[1024];
    SQLLEN dataLength;

    while (SQLFetch(stmt) == SQL_SUCCESS) {
      std::vector<std::string> rowData;
      for (int i = 1; i <= numColumns; ++i) {
        if (SQLGetData(stmt, i, SQL_C_CHAR, columnData, sizeof(columnData),
                       &dataLength) == SQL_SUCCESS) {
          rowData.push_back(std::string((char *)columnData));
        } else {
          rowData.push_back("");
        }
      }
      results.push_back(rowData);
    }

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    Logger::debug(LogCategory::DATABASE, "executeQueryMSSQL",
                  "Executed query, returned " + std::to_string(results.size()) +
                      " rows");

  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "executeQueryMSSQL",
                  "Error executing MSSQL query: " + std::string(e.what()));
  }

  return results;
}

std::pair<int, int>
CatalogManager::getColumnCountsMariaDB(const std::string &connectionString,
                                       const std::string &schema,
                                       const std::string &table) {
  try {
    MYSQL *conn = establishMariaDBConnection(connectionString);
    if (!conn) {
      return {0, 0};
    }

    std::string query = "SELECT COUNT(*) FROM information_schema.COLUMNS "
                        "WHERE TABLE_SCHEMA = '" +
                        escapeSQL(schema) +
                        "' "
                        "AND TABLE_NAME = '" +
                        escapeSQL(table) + "'";

    auto results = executeQueryMariaDB(conn, query);
    mysql_close(conn);

    if (!results.empty() && !results[0].empty()) {
      int count = std::stoi(results[0][0]);
      Logger::debug(LogCategory::DATABASE, "getColumnCountsMariaDB",
                    "Found " + std::to_string(count) + " columns for " +
                        schema + "." + table);
      return {count, count};
    }

  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "getColumnCountsMariaDB",
                  "Error getting column counts: " + std::string(e.what()));
  }

  return {0, 0};
}

std::pair<int, int>
CatalogManager::getColumnCountsMSSQL(const std::string &connectionString,
                                     const std::string &schema,
                                     const std::string &table) {
  try {
    SQLHDBC conn = establishMSSQLConnection(connectionString);
    if (conn == SQL_NULL_HDBC) {
      return {0, 0};
    }

    std::string query = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS "
                        "WHERE TABLE_SCHEMA = '" +
                        escapeSQL(schema) +
                        "' "
                        "AND TABLE_NAME = '" +
                        escapeSQL(table) + "'";

    auto results = executeQueryMSSQL(conn, query);
    SQLDisconnect(conn);
    SQLFreeHandle(SQL_HANDLE_DBC, conn);

    if (!results.empty() && !results[0].empty()) {
      int count = std::stoi(results[0][0]);
      Logger::debug(LogCategory::DATABASE, "getColumnCountsMSSQL",
                    "Found " + std::to_string(count) + " columns for " +
                        schema + "." + table);
      return {count, count};
    }

  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "getColumnCountsMSSQL",
                  "Error getting column counts: " + std::string(e.what()));
  }

  return {0, 0};
}

std::pair<int, int>
CatalogManager::getColumnCountsPostgres(const std::string &connectionString,
                                        const std::string &schema,
                                        const std::string &table) {
  try {
    auto connInfo = parsePostgresConnectionString(connectionString);
    auto conn = establishPostgresConnection(connInfo);
    if (!conn) {
      return {0, 0};
    }

    std::string query = "SELECT COUNT(*) FROM information_schema.columns "
                        "WHERE table_schema = '" +
                        escapeSQL(schema) +
                        "' "
                        "AND table_name = '" +
                        escapeSQL(table) + "'";

    pqxx::work txn(*conn);
    auto result = txn.exec(query);
    txn.commit();

    if (!result.empty()) {
      int count = result[0][0].as<int>();
      Logger::debug(LogCategory::DATABASE, "getColumnCountsPostgres",
                    "Found " + std::to_string(count) + " columns for " +
                        schema + "." + table);
      return {count, count};
    }

  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "getColumnCountsPostgres",
                  "Error getting column counts: " + std::string(e.what()));
  }

  return {0, 0};
}

// ============================================================================
// LEGACY DETECTION FUNCTIONS (for backward compatibility)
// ============================================================================

std::string CatalogManager::detectTimeColumnMariaDB(MYSQL *conn,
                                                    const std::string &schema,
                                                    const std::string &table) {
  return detectTimeColumn(DBEngine::MARIADB, conn, schema, table);
}

std::string CatalogManager::detectTimeColumnMSSQL(SQLHDBC conn,
                                                  const std::string &schema,
                                                  const std::string &table) {
  return detectTimeColumn(DBEngine::MSSQL, conn, schema, table);
}

std::string CatalogManager::detectTimeColumnPostgres(pqxx::connection &conn,
                                                     const std::string &schema,
                                                     const std::string &table) {
  return detectTimeColumn(DBEngine::POSTGRES, &conn, schema, table);
}

std::vector<std::string>
CatalogManager::detectPrimaryKeyColumns(MYSQL *conn, const std::string &schema,
                                        const std::string &table) {
  return detectPrimaryKeyColumns(DBEngine::MARIADB, conn, schema, table);
}

std::vector<std::string> CatalogManager::detectPrimaryKeyColumnsMSSQL(
    SQLHDBC conn, const std::string &schema, const std::string &table) {
  return detectPrimaryKeyColumns(DBEngine::MSSQL, conn, schema, table);
}

std::vector<std::string>
CatalogManager::detectPrimaryKeyColumnsPostgres(pqxx::connection &conn,
                                                const std::string &schema,
                                                const std::string &table) {
  return detectPrimaryKeyColumns(DBEngine::POSTGRES, &conn, schema, table);
}

std::vector<std::string>
CatalogManager::detectCandidateColumns(MYSQL *conn, const std::string &schema,
                                       const std::string &table) {
  return detectCandidateColumns(DBEngine::MARIADB, conn, schema, table);
}

std::vector<std::string> CatalogManager::detectCandidateColumnsMSSQL(
    SQLHDBC conn, const std::string &schema, const std::string &table) {
  return detectCandidateColumns(DBEngine::MSSQL, conn, schema, table);
}

std::vector<std::string>
CatalogManager::detectCandidateColumnsPostgres(pqxx::connection &conn,
                                               const std::string &schema,
                                               const std::string &table) {
  return detectCandidateColumns(DBEngine::POSTGRES, &conn, schema, table);
}

std::string CatalogManager::determinePKStrategy(
    const std::vector<std::string> &pkColumns,
    const std::vector<std::string> &candidateColumns) {
  return determinePKStrategy(pkColumns, candidateColumns, "");
}

// ============================================================================
// LEGACY CLEANUP FUNCTIONS (for backward compatibility)
// ============================================================================

void CatalogManager::cleanNonExistentPostgresTables(pqxx::connection &pgConn) {
  cleanCatalogTables(DBEngine::POSTGRES, pgConn, CleanupType::NON_EXISTENT);
}

void CatalogManager::cleanNonExistentMariaDBTables(pqxx::connection &pgConn) {
  cleanCatalogTables(DBEngine::MARIADB, pgConn, CleanupType::NON_EXISTENT);
}

void CatalogManager::cleanNonExistentMSSQLTables(pqxx::connection &pgConn) {
  cleanCatalogTables(DBEngine::MSSQL, pgConn, CleanupType::NON_EXISTENT);
}

void CatalogManager::cleanOrphanedTables(pqxx::connection &pgConn) {
  cleanCatalogTables(DBEngine::MARIADB, pgConn, CleanupType::ORPHANED);
}

void CatalogManager::cleanInconsistentPaginationFields() {
  try {
    pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());
    cleanCatalogTables(DBEngine::MARIADB, pgConn,
                       CleanupType::INCONSISTENT_PAGINATION);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "cleanInconsistentPaginationFields",
                  "Error cleaning inconsistent pagination fields: " +
                      std::string(e.what()));
  }
}
