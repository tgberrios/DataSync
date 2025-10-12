#ifndef CATALOG_CLEANER_H
#define CATALOG_CLEANER_H

#include "catalog/metadata_repository.h"
#include "engines/mariadb_engine.h"
#include "engines/mssql_engine.h"
#include "engines/postgres_engine.h"
#include <memory>
#include <pqxx/pqxx>
#include <string>

class CatalogCleaner {
  std::string metadataConnStr_;
  std::unique_ptr<MetadataRepository> repo_;

public:
  explicit CatalogCleaner(std::string metadataConnStr);

  void cleanNonExistentPostgresTables();
  void cleanNonExistentMariaDBTables();
  void cleanNonExistentMSSQLTables();
  void cleanOrphanedTables();
  void cleanOldLogs(int retentionHours);

private:
  bool tableExistsInPostgres(const std::string &schema,
                             const std::string &table);
};

#endif
