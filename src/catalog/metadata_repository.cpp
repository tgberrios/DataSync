#include "catalog/metadata_repository.h"
#include "core/logger.h"

MetadataRepository::MetadataRepository(std::string connectionString)
    : connectionString_(std::move(connectionString)) {}

pqxx::connection MetadataRepository::getConnection() {
  return pqxx::connection(connectionString_);
}

std::vector<std::string>
MetadataRepository::getConnectionStrings(const std::string &dbEngine) {
  std::vector<std::string> connStrings;
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

std::vector<CatalogEntry>
MetadataRepository::getCatalogEntries(const std::string &dbEngine,
                                      const std::string &connectionString) {
  std::vector<CatalogEntry> entries;
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

void MetadataRepository::insertOrUpdateTable(
    const CatalogTableInfo &tableInfo, const std::string &timeColumn,
    const std::vector<std::string> &pkColumns, bool hasPK, int64_t tableSize,
    const std::string &dbEngine) {
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
          "status, last_offset, active, pk_columns, pk_strategy, "
          "has_pk, table_size) "
          "VALUES ($1, $2, '', $3, $4, NULL, $5, 'PENDING', 0, false, $6, $7, "
          "$8, $9)",
          tableInfo.schema, tableInfo.table, dbEngine,
          tableInfo.connectionString, timeColumn, pkColumnsJSON, pkStrategy,
          hasPK, tableSize);
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

void MetadataRepository::updateClusterName(const std::string &clusterName,
                                           const std::string &connectionString,
                                           const std::string &dbEngine) {
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

void MetadataRepository::deleteTable(const std::string &schema,
                                     const std::string &table,
                                     const std::string &dbEngine,
                                     const std::string &connectionString) {
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);
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

int MetadataRepository::deactivateNoDataTables() {
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);
    auto result = txn.exec("UPDATE metadata.catalog SET active = false "
                           "WHERE status = 'NO_DATA' AND active = true");
    txn.commit();
    return result.affected_rows();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "MetadataRepository",
                  "Error deactivating NO_DATA tables: " +
                      std::string(e.what()));
    return 0;
  }
}

int MetadataRepository::markInactiveTablesAsSkip() {
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);
    auto result = txn.exec("UPDATE metadata.catalog SET "
                           "status = 'SKIP', last_offset = 0, "
                           "last_processed_pk = 0 "
                           "WHERE active = false AND status != 'NO_DATA'");
    txn.commit();
    return result.affected_rows();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "MetadataRepository",
                  "Error marking inactive tables: " + std::string(e.what()));
    return 0;
  }
}

int MetadataRepository::resetTable(const std::string &schema,
                                   const std::string &table,
                                   const std::string &dbEngine) {
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);

    std::string lowerSchema = schema;
    std::transform(lowerSchema.begin(), lowerSchema.end(), lowerSchema.begin(),
                   ::tolower);
    std::string lowerTable = table;
    std::transform(lowerTable.begin(), lowerTable.end(), lowerTable.begin(),
                   ::tolower);

    txn.exec("DROP TABLE IF EXISTS \"" + lowerSchema + "\".\"" + lowerTable +
             "\"");

    auto result = txn.exec_params(
        "UPDATE metadata.catalog SET "
        "status = 'FULL_LOAD', last_offset = 0, last_processed_pk = 0 "
        "WHERE schema_name = $1 AND table_name = $2 AND db_engine = $3",
        schema, table, dbEngine);
    txn.commit();
    return result.affected_rows();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "MetadataRepository",
                  "Error resetting table: " + std::string(e.what()));
    return 0;
  }
}

int MetadataRepository::cleanInvalidOffsets() {
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);

    auto pkResult =
        txn.exec("UPDATE metadata.catalog SET last_offset = NULL "
                 "WHERE pk_strategy = 'PK' AND last_offset IS NOT NULL");

    auto offsetResult = txn.exec(
        "UPDATE metadata.catalog SET last_processed_pk = NULL "
        "WHERE pk_strategy = 'OFFSET' AND last_processed_pk IS NOT NULL");

    txn.commit();
    return pkResult.affected_rows() + offsetResult.affected_rows();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "MetadataRepository",
                  "Error cleaning invalid offsets: " + std::string(e.what()));
    return 0;
  }
}
