#include "catalog/catalog_cleaner.h"
#include "core/Config.h"
#include "core/logger.h"
#include <algorithm>
#include <set>

CatalogCleaner::CatalogCleaner(std::string metadataConnStr)
    : metadataConnStr_(std::move(metadataConnStr)),
      repo_(std::make_unique<MetadataRepository>(metadataConnStr_)) {}

bool CatalogCleaner::tableExistsInPostgres(const std::string &schema,
                                           const std::string &table) {
  try {
    pqxx::connection conn(metadataConnStr_);
    pqxx::work txn(conn);

    std::string lowerSchema = schema;
    std::transform(lowerSchema.begin(), lowerSchema.end(), lowerSchema.begin(),
                   ::tolower);
    std::string lowerTable = table;
    std::transform(lowerTable.begin(), lowerTable.end(), lowerTable.begin(),
                   ::tolower);

    auto result =
        txn.exec_params("SELECT COUNT(*) FROM information_schema.tables "
                        "WHERE table_schema = $1 AND table_name = $2",
                        lowerSchema, lowerTable);
    txn.commit();

    return !result.empty() && result[0][0].as<int>() > 0;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "CatalogCleaner",
                  "Error checking table existence: " + std::string(e.what()));
    return false;
  }
}

void CatalogCleaner::cleanNonExistentPostgresTables() {
  try {
    pqxx::connection conn(metadataConnStr_);
    pqxx::work txn(conn);

    auto results =
        txn.exec("SELECT schema_name, table_name FROM metadata.catalog "
                 "WHERE db_engine = 'PostgreSQL'");

    size_t totalTables = results.size();
    size_t deletedCount = 0;
    Logger::info(LogCategory::DATABASE, "CatalogCleaner",
                 "Checking " + std::to_string(totalTables) +
                     " PostgreSQL tables for existence");

    for (const auto &row : results) {
      std::string schema = row[0].as<std::string>();
      std::string table = row[1].as<std::string>();

      if (!tableExistsInPostgres(schema, table)) {
        repo_->deleteTable(schema, table, "PostgreSQL");
        deletedCount++;
      }
    }
    txn.commit();

    Logger::info(
        LogCategory::DATABASE, "CatalogCleaner",
        "PostgreSQL cleanup completed: " + std::to_string(deletedCount) +
            " non-existent tables removed");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "CatalogCleaner",
                  "Error cleaning PostgreSQL tables: " + std::string(e.what()));
  }
}

void CatalogCleaner::cleanNonExistentMariaDBTables() {
  try {
    auto connStrings = repo_->getConnectionStrings("MariaDB");
    size_t totalDeleted = 0;

    Logger::info(LogCategory::DATABASE, "CatalogCleaner",
                 "Checking " + std::to_string(connStrings.size()) +
                     " MariaDB connection(s) for non-existent tables");

    for (const auto &connStr : connStrings) {
      MariaDBEngine engine(connStr);
      auto existingTables = engine.discoverTables();

      std::set<std::pair<std::string, std::string>> existingSet;
      for (const auto &table : existingTables) {
        existingSet.insert({table.schema, table.table});
      }

      auto catalogEntries = repo_->getCatalogEntries("MariaDB", connStr);
      for (const auto &entry : catalogEntries) {
        if (existingSet.find({entry.schema, entry.table}) ==
            existingSet.end()) {
          repo_->deleteTable(entry.schema, entry.table, "MariaDB", connStr);
          totalDeleted++;
        }
      }
    }

    Logger::info(LogCategory::DATABASE, "CatalogCleaner",
                 "MariaDB cleanup completed: " + std::to_string(totalDeleted) +
                     " non-existent tables removed");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "CatalogCleaner",
                  "Error cleaning MariaDB tables: " + std::string(e.what()));
  }
}

void CatalogCleaner::cleanNonExistentMSSQLTables() {
  try {
    auto connStrings = repo_->getConnectionStrings("MSSQL");
    size_t totalDeleted = 0;

    Logger::info(LogCategory::DATABASE, "CatalogCleaner",
                 "Checking " + std::to_string(connStrings.size()) +
                     " MSSQL connection(s) for non-existent tables");

    for (const auto &connStr : connStrings) {
      MSSQLEngine engine(connStr);
      auto existingTables = engine.discoverTables();

      std::set<std::pair<std::string, std::string>> existingSet;
      for (const auto &table : existingTables) {
        existingSet.insert({table.schema, table.table});
      }

      auto catalogEntries = repo_->getCatalogEntries("MSSQL", connStr);
      for (const auto &entry : catalogEntries) {
        if (existingSet.find({entry.schema, entry.table}) ==
            existingSet.end()) {
          repo_->deleteTable(entry.schema, entry.table, "MSSQL", connStr);
          totalDeleted++;
        }
      }
    }

    Logger::info(LogCategory::DATABASE, "CatalogCleaner",
                 "MSSQL cleanup completed: " + std::to_string(totalDeleted) +
                     " non-existent tables removed");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "CatalogCleaner",
                  "Error cleaning MSSQL tables: " + std::string(e.what()));
  }
}

void CatalogCleaner::cleanOrphanedTables() {
  try {
    pqxx::connection conn(metadataConnStr_);
    pqxx::work txn(conn);

    Logger::info(LogCategory::DATABASE, "CatalogCleaner",
                 "Cleaning orphaned catalog entries");

    auto result1 =
        txn.exec("DELETE FROM metadata.catalog "
                 "WHERE connection_string IS NULL OR connection_string = ''");
    size_t deleted1 = result1.affected_rows();

    auto result2 =
        txn.exec("DELETE FROM metadata.catalog "
                 "WHERE db_engine NOT IN ('PostgreSQL', 'MariaDB', 'MSSQL')");
    size_t deleted2 = result2.affected_rows();

    auto result3 = txn.exec("DELETE FROM metadata.catalog "
                            "WHERE schema_name IS NULL OR schema_name = '' "
                            "OR table_name IS NULL OR table_name = ''");
    size_t deleted3 = result3.affected_rows();

    txn.commit();

    size_t totalDeleted = deleted1 + deleted2 + deleted3;
    Logger::info(
        LogCategory::DATABASE, "CatalogCleaner",
        "Orphaned tables cleanup completed: " + std::to_string(totalDeleted) +
            " entries removed (empty_conn=" + std::to_string(deleted1) +
            ", invalid_engine=" + std::to_string(deleted2) +
            ", invalid_names=" + std::to_string(deleted3) + ")");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "CatalogCleaner",
                  "Error cleaning orphaned tables: " + std::string(e.what()));
  }
}

void CatalogCleaner::cleanOldLogs(int retentionHours) {
  try {
    pqxx::connection conn(metadataConnStr_);
    pqxx::work txn(conn);

    auto countBefore = txn.exec("SELECT COUNT(*) FROM metadata.logs");
    int logsBefore = countBefore[0][0].as<int>();

    if (logsBefore == 0) {
      Logger::info(LogCategory::MAINTENANCE, "cleanOldLogs",
                   "No logs found in metadata.logs table");
      txn.commit();
      return;
    }

    auto result = txn.exec_params("DELETE FROM metadata.logs WHERE ts < NOW() "
                                  "- make_interval(hours => $1)",
                                  retentionHours);
    int deletedLogs = result.affected_rows();

    auto countAfter = txn.exec("SELECT COUNT(*) FROM metadata.logs");
    int logsAfter = countAfter[0][0].as<int>();

    txn.commit();

    Logger::info(LogCategory::MAINTENANCE, "cleanOldLogs",
                 "Deleted " + std::to_string(deletedLogs) + " old logs (" +
                     std::to_string(retentionHours) +
                     "h retention). Remaining: " + std::to_string(logsAfter));
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "CatalogCleaner",
                  "Error cleaning old logs: " + std::string(e.what()));
  }
}
