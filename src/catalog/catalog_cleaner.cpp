#include "catalog/catalog_cleaner.h"
#include "core/Config.h"
#include "core/logger.h"
#include "utils/table_utils.h"
#include <algorithm>
#include <set>

CatalogCleaner::CatalogCleaner(std::string metadataConnStr)
    : metadataConnStr_(std::move(metadataConnStr)),
      repo_(std::make_unique<MetadataRepository>(metadataConnStr_)) {}

// This function cleans non-existent PostgreSQL tables from the catalog that
// no longer exist in the source database. It queries the catalog for all
// PostgreSQL tables, checks each one's existence using tableExistsInPostgres,
// and removes entries from both the catalog and the target database for tables
// that no longer exist. This helps maintain catalog consistency when source
// tables are dropped.
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

      if (!TableUtils::tableExistsInPostgres(conn, schema, table)) {
        repo_->deleteTable(schema, table, "PostgreSQL", "", true);
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

// This function cleans non-existent MariaDB tables from the catalog that
// no longer exist in the source database. It connects to each MariaDB source,
// discovers all existing tables, and compares them against catalog entries.
// Tables that exist in the catalog but not in the source database are removed
// from both the catalog and the target PostgreSQL database. This ensures
// catalog consistency when source tables are dropped from MariaDB databases.
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
          repo_->deleteTable(entry.schema, entry.table, "MariaDB", connStr,
                             true);
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

// This function cleans non-existent MSSQL tables from the catalog that
// no longer exist in the source database. It connects to each MSSQL source,
// discovers all existing tables, and compares them against catalog entries.
// Tables that exist in the catalog but not in the source database are removed
// from both the catalog and the target PostgreSQL database. This ensures
// catalog consistency when source tables are dropped from MSSQL databases.
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
          repo_->deleteTable(entry.schema, entry.table, "MSSQL", connStr, true);
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

// This function cleans orphaned or invalid table entries from the catalog.
// It removes three types of invalid entries: (1) entries with NULL or empty
// connection strings, (2) entries with unsupported database engines (only
// PostgreSQL, MariaDB, and MSSQL are supported), and (3) entries with NULL or
// empty schema or table names. This helps maintain catalog data integrity by
// removing entries that cannot be properly processed.
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
                 "WHERE db_engine NOT IN ('PostgreSQL', 'MariaDB', 'MSSQL', 'MongoDB', 'Oracle')");
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

// This function cleans old logs from the metadata.logs table based on a
// retention policy. It deletes all log entries older than the specified
// retention period (in hours). The function checks the log count before and
// after deletion to provide detailed logging information. If no logs exist,
// the function returns early without performing any operations.
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
