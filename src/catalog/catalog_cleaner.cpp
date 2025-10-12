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

    for (const auto &row : results) {
      std::string schema = row[0].as<std::string>();
      std::string table = row[1].as<std::string>();

      if (!tableExistsInPostgres(schema, table)) {
        repo_->deleteTable(schema, table, "PostgreSQL");
      }
    }
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "CatalogCleaner",
                  "Error cleaning PostgreSQL tables: " + std::string(e.what()));
  }
}

void CatalogCleaner::cleanNonExistentMariaDBTables() {
  try {
    auto connStrings = repo_->getConnectionStrings("MariaDB");

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
        }
      }
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "CatalogCleaner",
                  "Error cleaning MariaDB tables: " + std::string(e.what()));
  }
}

void CatalogCleaner::cleanNonExistentMSSQLTables() {
  try {
    auto connStrings = repo_->getConnectionStrings("MSSQL");

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
        }
      }
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "CatalogCleaner",
                  "Error cleaning MSSQL tables: " + std::string(e.what()));
  }
}

void CatalogCleaner::cleanOrphanedTables() {
  try {
    pqxx::connection conn(metadataConnStr_);
    pqxx::work txn(conn);

    txn.exec("DELETE FROM metadata.catalog "
             "WHERE connection_string IS NULL OR connection_string = ''");

    txn.exec("DELETE FROM metadata.catalog "
             "WHERE db_engine NOT IN ('PostgreSQL', 'MariaDB', 'MSSQL')");

    txn.exec("DELETE FROM metadata.catalog "
             "WHERE schema_name IS NULL OR schema_name = '' "
             "OR table_name IS NULL OR table_name = ''");

    txn.commit();
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
