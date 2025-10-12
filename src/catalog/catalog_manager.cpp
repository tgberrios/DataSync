#include "catalog/catalog_manager.h"
#include "catalog/catalog_lock.h"
#include "core/Config.h"
#include "core/logger.h"
#include "utils/string_utils.h"
#include <algorithm>

CatalogManager::CatalogManager(std::string metadataConnStr)
    : metadataConnStr_(std::move(metadataConnStr)),
      repo_(std::make_unique<MetadataRepository>(metadataConnStr_)),
      cleaner_(std::make_unique<CatalogCleaner>(metadataConnStr_)) {}

CatalogManager::CatalogManager(std::string metadataConnStr,
                               std::unique_ptr<IMetadataRepository> repo,
                               std::unique_ptr<ICatalogCleaner> cleaner)
    : metadataConnStr_(std::move(metadataConnStr)), repo_(std::move(repo)),
      cleaner_(std::move(cleaner)) {}

void CatalogManager::cleanCatalog() {
  CatalogLock lock(metadataConnStr_, "catalog_clean", 300);
  if (!lock.tryAcquire(30)) {
    Logger::warning(LogCategory::DATABASE, "CatalogManager",
                    "Could not acquire lock for catalog cleaning - another "
                    "instance may be running");
    return;
  }

  try {
    cleaner_->cleanNonExistentPostgresTables();
    cleaner_->cleanNonExistentMariaDBTables();
    cleaner_->cleanNonExistentMSSQLTables();
    cleaner_->cleanOrphanedTables();
    repo_->cleanInvalidOffsets();
    cleaner_->cleanOldLogs(DatabaseDefaults::DEFAULT_LOG_RETENTION_HOURS);
    updateClusterNames();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "CatalogManager",
                  "Error cleaning catalog: " + std::string(e.what()));
  }
}

void CatalogManager::deactivateNoDataTables() {
  try {
    int noDataCount = repo_->deactivateNoDataTables();
    int skipCount = repo_->markInactiveTablesAsSkip();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "CatalogManager",
                  "Error deactivating tables: " + std::string(e.what()));
  }
}

void CatalogManager::updateClusterNames() {
  try {
    pqxx::connection conn(metadataConnStr_);
    pqxx::work txn(conn);
    auto results = txn.exec(
        "SELECT DISTINCT connection_string, db_engine FROM metadata.catalog "
        "WHERE (cluster_name IS NULL OR cluster_name = '') AND active = true");
    txn.commit();

    for (const auto &row : results) {
      std::string connStr = row[0].as<std::string>();
      std::string dbEngine = row[1].as<std::string>();

      std::string clusterName = ClusterNameResolver::resolve(connStr, dbEngine);
      if (!clusterName.empty()) {
        repo_->updateClusterName(clusterName, connStr, dbEngine);
      }
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "CatalogManager",
                  "Error updating cluster names: " + std::string(e.what()));
  }
}

void CatalogManager::validateSchemaConsistency() {
  try {
    pqxx::connection conn(metadataConnStr_);
    pqxx::work txn(conn);
    auto results = txn.exec(
        "SELECT schema_name, table_name, db_engine, connection_string "
        "FROM metadata.catalog "
        "WHERE active = true AND status IN ('LISTENING_CHANGES', 'FULL_LOAD') "
        "ORDER BY db_engine, schema_name, table_name");
    txn.commit();

    for (const auto &row : results) {
      std::string schema = row[0].as<std::string>();
      std::string table = row[1].as<std::string>();
      std::string dbEngine = row[2].as<std::string>();
      std::string connStr = row[3].as<std::string>();

      std::pair<int, int> counts{0, 0};

      if (dbEngine == "MariaDB") {
        MariaDBEngine engine(connStr);
        counts = engine.getColumnCounts(schema, table, metadataConnStr_);
      } else if (dbEngine == "MSSQL") {
        MSSQLEngine engine(connStr);
        counts = engine.getColumnCounts(schema, table, metadataConnStr_);
      } else if (dbEngine == "PostgreSQL") {
        PostgreSQLEngine engine(connStr);
        counts = engine.getColumnCounts(schema, table, metadataConnStr_);
      }

      if (counts.first != counts.second && counts.first > 0) {
        repo_->resetTable(schema, table, dbEngine);
      }
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "CatalogManager",
                  "Error validating schema: " + std::string(e.what()));
  }
}

int64_t CatalogManager::getTableSize(const std::string &schema,
                                     const std::string &table) {
  try {
    pqxx::connection conn(metadataConnStr_);
    pqxx::work txn(conn);

    std::string lowerSchema = StringUtils::toLower(schema);
    std::string lowerTable = StringUtils::toLower(table);

    auto result =
        txn.exec_params("SELECT COALESCE(reltuples::bigint, 0) FROM pg_class "
                        "WHERE relname = $1 AND relnamespace = "
                        "(SELECT oid FROM pg_namespace WHERE nspname = $2)",
                        lowerTable, lowerSchema);
    txn.commit();

    if (!result.empty() && !result[0][0].is_null())
      return result[0][0].as<int64_t>();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "CatalogManager",
                  "Error getting table size: " + std::string(e.what()));
  }
  return 0;
}

void CatalogManager::syncCatalog(const std::string &dbEngine) {
  std::string lockName = "catalog_sync_" + dbEngine;
  CatalogLock lock(metadataConnStr_, lockName, 600);
  if (!lock.tryAcquire(30)) {
    Logger::warning(LogCategory::DATABASE, "CatalogManager",
                    "Could not acquire lock for catalog sync (" + dbEngine +
                        ") - another instance may be running");
    return;
  }

  try {
    auto connStrings = repo_->getConnectionStrings(dbEngine);
    auto tableSizes = repo_->getTableSizesBatch();

    for (const auto &connStr : connStrings) {
      std::unique_ptr<IDatabaseEngine> engine;

      if (dbEngine == "MariaDB")
        engine = std::make_unique<MariaDBEngine>(connStr);
      else if (dbEngine == "MSSQL")
        engine = std::make_unique<MSSQLEngine>(connStr);
      else if (dbEngine == "PostgreSQL")
        engine = std::make_unique<PostgreSQLEngine>(connStr);

      if (!engine)
        continue;

      auto tables = engine->discoverTables();

      for (const auto &table : tables) {
        auto timeColumn = engine->detectTimeColumn(table.schema, table.table);
        auto pkColumns = engine->detectPrimaryKey(table.schema, table.table);
        bool hasPK = !pkColumns.empty();

        std::string lowerSchema = StringUtils::toLower(table.schema);
        std::string lowerTable = StringUtils::toLower(table.table);
        std::string key = lowerSchema + "|" + lowerTable;
        int64_t tableSize =
            (tableSizes.find(key) != tableSizes.end()) ? tableSizes[key] : 0;

        repo_->insertOrUpdateTable(table, timeColumn, pkColumns, hasPK,
                                   tableSize, dbEngine);
      }
    }

    updateClusterNames();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "CatalogManager",
                  "Error syncing catalog: " + std::string(e.what()));
  }
}

void CatalogManager::syncCatalogMariaDBToPostgres() { syncCatalog("MariaDB"); }

void CatalogManager::syncCatalogMSSQLToPostgres() { syncCatalog("MSSQL"); }

void CatalogManager::syncCatalogPostgresToPostgres() {
  syncCatalog("PostgreSQL");
}
