#include "catalog/metadata_repository.h"
#include "core/logger.h"
#include "utils/string_utils.h"

// Constructor for MetadataRepository. Initializes the repository with a
// connection string to the metadata database. This connection string is used
// for all database operations performed by the repository methods.
MetadataRepository::MetadataRepository(std::string connectionString)
    : connectionString_(std::move(connectionString)) {}

// Creates and returns a new PostgreSQL database connection using the stored
// connection string. This is a private helper method used internally by all
// repository methods to establish database connections for their operations.
pqxx::connection MetadataRepository::getConnection() {
  return pqxx::connection(connectionString_);
}

// Retrieves all distinct connection strings for a specific database engine
// from the metadata catalog. Only returns connection strings for active tables.
// This is useful for discovering all source database connections of a
// particular type (MariaDB, MSSQL, PostgreSQL) that need to be processed.
// Returns an empty vector if no connections are found or if an error occurs.
std::vector<std::string>
MetadataRepository::getConnectionStrings(const std::string &dbEngine) {
  std::vector<std::string> connStrings;
  if (dbEngine.empty()) {
    Logger::error(LogCategory::DATABASE, "MetadataRepository",
                  "Invalid input: dbEngine must not be empty");
    return connStrings;
  }
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);
    auto results = txn.exec_params(
        "SELECT DISTINCT connection_string FROM metadata.catalog "
        "WHERE db_engine = $1 AND active = true",
        dbEngine);
    txn.commit();

    for (const auto &row : results) {
      if (!row[0].is_null())
        connStrings.push_back(row[0].as<std::string>());
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "MetadataRepository",
                  "Error getting connection strings: " + std::string(e.what()));
  }
  return connStrings;
}

// Retrieves all catalog entries for a specific database engine and connection
// string combination. This function returns detailed metadata about each table
// including schema name, table name, status, primary key information, and table
// size. The entries are returned as CatalogEntry objects containing all
// relevant metadata fields. Returns an empty vector if no entries are found
// or if an error occurs.
std::vector<CatalogEntry>
MetadataRepository::getCatalogEntries(const std::string &dbEngine,
                                      const std::string &connectionString) {
  std::vector<CatalogEntry> entries;
  if (dbEngine.empty() || connectionString.empty()) {
    Logger::error(
        LogCategory::DATABASE, "MetadataRepository",
        "Invalid input: dbEngine and connectionString must not be empty");
    return entries;
  }
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);
    auto results = txn.exec_params(
        "SELECT schema_name, table_name, db_engine, connection_string, status, "
        "last_sync_column, pk_columns, pk_strategy, has_pk, table_size "
        "FROM metadata.catalog "
        "WHERE db_engine = $1 AND connection_string = $2",
        dbEngine, connectionString);
    txn.commit();

    for (const auto &row : results) {
      CatalogEntry entry;
      entry.schema = row[0].as<std::string>();
      entry.table = row[1].as<std::string>();
      entry.dbEngine = row[2].as<std::string>();
      entry.connectionString = row[3].as<std::string>();
      entry.status = row[4].as<std::string>();
      entry.lastSyncColumn = row[5].is_null() ? "" : row[5].as<std::string>();
      entry.pkColumns = row[6].is_null() ? "" : row[6].as<std::string>();
      entry.pkStrategy = row[7].is_null() ? "" : row[7].as<std::string>();
      entry.hasPK = row[8].is_null() ? false : row[8].as<bool>();
      entry.tableSize = row[9].is_null() ? 0 : row[9].as<int64_t>();
      entries.push_back(entry);
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "MetadataRepository",
                  "Error getting catalog entries: " + std::string(e.what()));
  }
  return entries;
}

// Inserts a new table entry into the catalog or updates an existing one if it
// already exists. For new entries, the table is inserted with status 'PENDING'
// and active set to false. For existing entries, the function intelligently
// updates only the fields that have changed (time column, primary key columns,
// primary key strategy, has_pk flag) or just the table size if no other changes
// are detected. This ensures the catalog stays synchronized with the actual
// table structure while preserving existing metadata like sync status.
void MetadataRepository::insertOrUpdateTable(
    const CatalogTableInfo &tableInfo, const std::string &timeColumn,
    const std::vector<std::string> &pkColumns, bool hasPK, int64_t tableSize,
    const std::string &dbEngine) {
  if (tableInfo.schema.empty() || tableInfo.table.empty() || dbEngine.empty()) {
    Logger::error(
        LogCategory::DATABASE, "MetadataRepository",
        "Invalid input: schema, table, and dbEngine must not be empty");
    return;
  }
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);

    std::string pkColumnsJSON = columnsToJSON(pkColumns);
    std::string pkStrategy = determinePKStrategy(pkColumns);

    auto existing = txn.exec_params(
        "SELECT last_sync_column, pk_columns, pk_strategy, has_pk, table_size "
        "FROM metadata.catalog "
        "WHERE schema_name = $1 AND table_name = $2 AND db_engine = $3",
        tableInfo.schema, tableInfo.table, dbEngine);

    if (existing.empty()) {
      txn.exec_params(
          "INSERT INTO metadata.catalog "
          "(schema_name, table_name, cluster_name, db_engine, "
          "connection_string, last_sync_time, last_sync_column, "
          "status, active, pk_columns, pk_strategy, "
          "has_pk, table_size) "
          "VALUES ($1, $2, '', $3, $4, NULL, $5, $10, false, $6, $7, "
          "$8, $9)",
          tableInfo.schema, tableInfo.table, dbEngine,
          tableInfo.connectionString, timeColumn, pkColumnsJSON, pkStrategy,
          hasPK, tableSize, std::string(CatalogStatus::PENDING));
    } else {
      std::string currentTimeColumn =
          existing[0][0].is_null() ? "" : existing[0][0].as<std::string>();
      std::string currentPKColumns =
          existing[0][1].is_null() ? "" : existing[0][1].as<std::string>();
      std::string currentPKStrategy =
          existing[0][2].is_null() ? "" : existing[0][2].as<std::string>();
      bool currentHasPK =
          existing[0][3].is_null() ? false : existing[0][3].as<bool>();
      if (currentTimeColumn != timeColumn ||
          currentPKColumns != pkColumnsJSON ||
          currentPKStrategy != pkStrategy || currentHasPK != hasPK) {
        txn.exec_params(
            "UPDATE metadata.catalog SET "
            "last_sync_column = $1, pk_columns = $2, pk_strategy = $3, "
            "has_pk = $4, table_size = $5 "
            "WHERE schema_name = $6 AND table_name = $7 AND db_engine = $8",
            timeColumn, pkColumnsJSON, pkStrategy, hasPK, tableSize,
            tableInfo.schema, tableInfo.table, dbEngine);
      } else {
        txn.exec_params(
            "UPDATE metadata.catalog SET table_size = $1 "
            "WHERE schema_name = $2 AND table_name = $3 AND db_engine = $4",
            tableSize, tableInfo.schema, tableInfo.table, dbEngine);
      }
    }
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "MetadataRepository",
                  "Error inserting/updating table: " + std::string(e.what()));
  }
}

// Updates the cluster name for all catalog entries matching a specific
// connection string and database engine. This is used to group tables from
// the same database cluster together for organizational and monitoring
// purposes. The cluster name is resolved from the connection string using
// the ClusterNameResolver utility.
void MetadataRepository::updateClusterName(const std::string &clusterName,
                                           const std::string &connectionString,
                                           const std::string &dbEngine) {
  if (connectionString.empty() || dbEngine.empty()) {
    Logger::error(
        LogCategory::DATABASE, "MetadataRepository",
        "Invalid input: connectionString and dbEngine must not be empty");
    return;
  }
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);
    txn.exec_params("UPDATE metadata.catalog SET cluster_name = $1 "
                    "WHERE connection_string = $2 AND db_engine = $3",
                    clusterName, connectionString, dbEngine);
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "MetadataRepository",
                  "Error updating cluster name: " + std::string(e.what()));
  }
}

// Deletes a table's metadata entry from the catalog. If a connection string
// is provided, it deletes only entries matching that specific connection.
// If the connection string is empty, it deletes all entries matching the
// schema, table, and database engine combination. This is used during
// catalog cleanup when tables no longer exist in the source database.
// If dropTargetTable is true, the function also drops the corresponding
// table from the target PostgreSQL database before removing the catalog entry.
// The table name in PostgreSQL is constructed as "schema.table" (lowercase).
void MetadataRepository::deleteTable(const std::string &schema,
                                     const std::string &table,
                                     const std::string &dbEngine,
                                     const std::string &connectionString,
                                     bool dropTargetTable) {
  if (schema.empty() || table.empty() || dbEngine.empty()) {
    Logger::error(
        LogCategory::DATABASE, "MetadataRepository",
        "Invalid input: schema, table, and dbEngine must not be empty");
    return;
  }
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);

    if (dropTargetTable) {
      std::string lowerSchema = StringUtils::toLower(schema);
      std::string lowerTable = StringUtils::toLower(table);
      std::string target_full_table =
          txn.quote_name(lowerSchema) + "." + txn.quote_name(lowerTable);
      txn.exec("DROP TABLE IF EXISTS " + target_full_table);
    }

    if (connectionString.empty()) {
      txn.exec_params("DELETE FROM metadata.catalog "
                      "WHERE schema_name = $1 AND table_name = $2 AND "
                      "db_engine = $3",
                      schema, table, dbEngine);
    } else {
      txn.exec_params("DELETE FROM metadata.catalog "
                      "WHERE schema_name = $1 AND table_name = $2 AND "
                      "db_engine = $3 AND connection_string = $4",
                      schema, table, dbEngine, connectionString);
    }
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "MetadataRepository",
                  "Error deleting table: " + std::string(e.what()));
  }
}

// Reactivates tables that have data in the target PostgreSQL database but are
// currently marked as inactive. This function checks if tables have rows in
// the target database by executing COUNT(*) queries and reactivates them by
// setting active = true. This is used before deactivating tables to prevent
// deactivating tables that have received data. Returns the number of tables
// that were reactivated. Returns 0 if an error occurs.
int MetadataRepository::reactivateTablesWithData() {
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);

    auto inactiveTables = txn.exec(
        "SELECT schema_name, table_name, db_engine FROM metadata.catalog "
        "WHERE active = false");

    int reactivatedCount = 0;
    bool transactionAborted = false;
    for (const auto &row : inactiveTables) {
      std::string schema = row[0].as<std::string>();
      std::string table = row[1].as<std::string>();
      std::string dbEngine = row[2].as<std::string>();

      std::string lowerSchema = StringUtils::toLower(schema);
      std::string lowerTable = StringUtils::toLower(table);

      try {
        auto countResult =
            txn.exec("SELECT COUNT(*) FROM " + txn.quote_name(lowerSchema) +
                     "." + txn.quote_name(lowerTable));
        if (!countResult.empty() && countResult[0][0].as<int64_t>() > 0) {
          txn.exec_params(
              "UPDATE metadata.catalog SET active = true "
              "WHERE schema_name = $1 AND table_name = $2 AND db_engine = $3",
              schema, table, dbEngine);
          reactivatedCount++;
        }
      } catch (const std::exception &e) {
        std::string errorMsg = e.what();
        if (errorMsg.find("current transaction is aborted") !=
            std::string::npos) {
          try {
            txn.abort();
          } catch (...) {
          }
          transactionAborted = true;
          Logger::warning(LogCategory::DATABASE, "MetadataRepository",
                          "Transaction aborted while checking " + schema + "." +
                              table + ", skipping remaining tables");
          break;
        } else {
          Logger::error(LogCategory::DATABASE, "MetadataRepository",
                        "Error checking table data for " + schema + "." +
                            table + ": " + std::string(e.what()));
        }
      }
    }

    if (!transactionAborted) {
      txn.commit();
    }
    return reactivatedCount;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "MetadataRepository",
                  "Error reactivating tables with data: " +
                      std::string(e.what()));
    return 0;
  }
}

// Deactivates all tables in the catalog that have a status of 'NO_DATA'.
// This marks tables as inactive when they have no data to sync, preventing
// unnecessary processing attempts. The function should be called periodically
// (recommended: every 24 hours) to maintain catalog consistency. Before
// deactivating, tables that have received data should be reactivated using
// reactivateTablesWithData(). Returns the number of tables that were
// deactivated. Returns 0 if an error occurs.
int MetadataRepository::deactivateNoDataTables() {
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);
    auto result = txn.exec_params("UPDATE metadata.catalog SET active = false "
                                  "WHERE status = $1 AND active = true",
                                  std::string(CatalogStatus::NO_DATA));
    txn.commit();
    return result.affected_rows();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "MetadataRepository",
                  "Error deactivating NO_DATA tables: " +
                      std::string(e.what()));
    return 0;
  }
}

// Marks all inactive tables (where active = false) as 'SKIP' status and
// resets their tracking fields (last_processed_pk).
// This is used to clean up tables that have been deactivated, ensuring they
// are properly marked to be skipped during sync operations. Tables with
// status 'NO_DATA' are excluded from this operation. If truncateTarget is true,
// the function also truncates the corresponding tables in the target PostgreSQL
// database before marking them as SKIP, effectively clearing all data from
// the target tables. Returns the number of tables that were updated.
// Returns 0 if an error occurs.
int MetadataRepository::markInactiveTablesAsSkip(bool truncateTarget) {
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);

    auto inactiveTables =
        txn.exec_params("SELECT schema_name, table_name FROM metadata.catalog "
                        "WHERE active = false AND status != $1",
                        std::string(CatalogStatus::NO_DATA));

    if (truncateTarget) {
      for (const auto &row : inactiveTables) {
        std::string schema = row[0].as<std::string>();
        std::string table = row[1].as<std::string>();

        std::string lowerSchema = StringUtils::toLower(schema);
        std::string lowerTable = StringUtils::toLower(table);
        std::string target_full_table =
            txn.quote_name(lowerSchema) + "." + txn.quote_name(lowerTable);
        try {
          txn.exec("TRUNCATE TABLE " + target_full_table);
        } catch (const std::exception &e) {
          Logger::error(LogCategory::DATABASE, "MetadataRepository",
                        "Error truncating table " + target_full_table + ": " +
                            std::string(e.what()));
          continue;
        }
      }
    }

    auto result = txn.exec_params("UPDATE metadata.catalog SET "
                                  "status = $1, "
                                  "last_processed_pk = '' "
                                  "WHERE active = false AND status != $2",
                                  std::string(CatalogStatus::SKIP),
                                  std::string(CatalogStatus::NO_DATA));
    txn.commit();
    return result.affected_rows();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "MetadataRepository",
                  "Error marking inactive tables: " + std::string(e.what()));
    return 0;
  }
}

// Resets a table in the catalog by dropping the target table in PostgreSQL
// and resetting the catalog entry to 'FULL_LOAD' status with cleared offset
// tracking. This forces a complete reload of the table data from the source
// database. The schema and table names are converted to lowercase and used
// as "schema.table" to match the target table naming convention used in
// deleteTable(). Returns the number of catalog entries updated (typically 1).
// Returns 0 if an error occurs.
int MetadataRepository::resetTable(const std::string &schema,
                                   const std::string &table,
                                   const std::string &dbEngine) {
  if (schema.empty() || table.empty() || dbEngine.empty()) {
    Logger::error(
        LogCategory::DATABASE, "MetadataRepository",
        "Invalid input: schema, table, and dbEngine must not be empty");
    return 0;
  }
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);

    std::string lowerSchema = StringUtils::toLower(schema);
    std::string lowerTable = StringUtils::toLower(table);
    std::string target_full_table =
        txn.quote_name(lowerSchema) + "." + txn.quote_name(lowerTable);

    txn.exec("DROP TABLE IF EXISTS " + target_full_table);

    auto result = txn.exec_params(
        "UPDATE metadata.catalog SET "
        "status = $4, last_processed_pk = '' "
        "WHERE schema_name = $1 AND table_name = $2 AND db_engine = $3",
        schema, table, dbEngine, std::string(CatalogStatus::FULL_LOAD));
    txn.commit();
    return result.affected_rows();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "MetadataRepository",
                  "Error resetting table: " + std::string(e.what()));
    return 0;
  }
}

// Cleans invalid offset tracking data from the catalog. This function removes
// last_processed_pk values for tables using 'OFFSET' strategy (which should be
// NULL for offset-based syncing). This ensures data consistency and prevents
// sync errors. Returns the total number of entries cleaned. Returns 0 if an
// error occurs. DEPRECATED: This function will be removed in a future version.
int MetadataRepository::cleanInvalidOffsets() {
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);

    auto offsetResult = txn.exec(
        "UPDATE metadata.catalog SET last_processed_pk = NULL "
        "WHERE pk_strategy = 'OFFSET' AND last_processed_pk IS NOT NULL");

    txn.commit();
    return offsetResult.affected_rows();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "MetadataRepository",
                  "Error cleaning invalid offsets: " + std::string(e.what()));
    return 0;
  }
}

// Retrieves table sizes (row counts) for all user tables in PostgreSQL in a
// single batch operation. The function queries each table individually using
// COUNT(*) to get accurate row counts for all regular tables, excluding system
// schemas. Returns a map where keys are in the format "schema|table" and values
// are the actual row counts. This is used during catalog synchronization to
// populate table size information. Note: This function gets sizes from the
// target PostgreSQL database, while getTableSize() in CatalogManager gets
// sizes from the source database. Use this function for batch operations during
// catalog sync, and use getTableSize() for individual queries when you need
// the actual source database size.
std::unordered_map<std::string, int64_t>
MetadataRepository::getTableSizesBatch() {
  std::unordered_map<std::string, int64_t> sizes;
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);

    auto result =
        txn.exec("SELECT n.nspname as schema_name, c.relname as table_name "
                 "FROM pg_class c "
                 "JOIN pg_namespace n ON c.relnamespace = n.oid "
                 "WHERE c.relkind = 'r' AND n.nspname NOT IN ('pg_catalog', "
                 "'information_schema', 'pg_toast')");

    for (const auto &row : result) {
      std::string schema = row[0].as<std::string>();
      std::string table = row[1].as<std::string>();

      try {
        auto countResult =
            txn.exec("SELECT COUNT(*) FROM " + txn.quote_name(schema) + "." +
                     txn.quote_name(table));
        if (!countResult.empty()) {
          int64_t size = countResult[0][0].as<int64_t>();
          std::string lowerSchema = StringUtils::toLower(schema);
          std::string lowerTable = StringUtils::toLower(table);
          std::string key = lowerSchema + "|" + lowerTable;
          sizes[key] = size;
        }
      } catch (const std::exception &e) {
        Logger::error(LogCategory::DATABASE, "MetadataRepository",
                      "Error getting size for " + schema + "." + table + ": " +
                          std::string(e.what()));
      }
    }

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "MetadataRepository",
                  "Error getting table sizes batch: " + std::string(e.what()));
  }
  return sizes;
}
