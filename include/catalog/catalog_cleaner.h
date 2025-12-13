#ifndef CATALOG_CLEANER_H
#define CATALOG_CLEANER_H

#include "catalog/metadata_repository.h"
#include "engines/mariadb_engine.h"
#include "engines/mssql_engine.h"
#include "engines/postgres_engine.h"
#include <memory>
#include <pqxx/pqxx>
#include <string>

class ICatalogCleaner {
public:
  virtual ~ICatalogCleaner() = default;

  virtual void cleanNonExistentPostgresTables() = 0;
  virtual void cleanNonExistentMariaDBTables() = 0;
  virtual void cleanNonExistentMSSQLTables() = 0;
  virtual void cleanNonExistentOracleTables() = 0;
  virtual void cleanNonExistentMongoDBTables() = 0;
  virtual void cleanOrphanedTables() = 0;
  virtual void cleanOldLogs(int retentionHours) = 0;
};

class CatalogCleaner : public ICatalogCleaner {
  std::string metadataConnStr_;
  std::unique_ptr<IMetadataRepository> repo_;

public:
  explicit CatalogCleaner(std::string metadataConnStr);

  void cleanNonExistentPostgresTables() override;
  void cleanNonExistentMariaDBTables() override;
  void cleanNonExistentMSSQLTables() override;
  void cleanNonExistentOracleTables() override;
  void cleanNonExistentMongoDBTables() override;
  void cleanOrphanedTables() override;
  void cleanOldLogs(int retentionHours) override;
};

#endif
