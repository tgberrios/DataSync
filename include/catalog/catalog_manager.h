#ifndef CATALOG_MANAGER_H
#define CATALOG_MANAGER_H

#include "catalog/catalog_cleaner.h"
#include "catalog/metadata_repository.h"
#include "core/Config.h"
#include "engines/mariadb_engine.h"
#include "engines/mssql_engine.h"
#include "engines/postgres_engine.h"
#include "utils/cluster_name_resolver.h"
#include <memory>
#include <string>

class CatalogManager {
  std::string metadataConnStr_;
  std::unique_ptr<MetadataRepository> repo_;
  std::unique_ptr<CatalogCleaner> cleaner_;

public:
  CatalogManager()
      : CatalogManager(DatabaseConfig::getPostgresConnectionString()) {}

  explicit CatalogManager(std::string metadataConnStr);

  void cleanCatalog();
  void deactivateNoDataTables();
  void updateClusterNames();
  void validateSchemaConsistency();
  void syncCatalogMariaDBToPostgres();
  void syncCatalogMSSQLToPostgres();
  void syncCatalogPostgresToPostgres();

private:
  void syncCatalog(const std::string &dbEngine);
  int64_t getTableSize(const std::string &schema, const std::string &table);
};

#endif
