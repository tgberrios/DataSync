#ifndef CATALOG_MANAGER_H
#define CATALOG_MANAGER_H

#include "catalog/catalog_cleaner.h"
#include "catalog/metadata_repository.h"
#include "core/Config.h"
#include "engines/db2_engine.h"
#include "engines/mariadb_engine.h"
#include "engines/mongodb_engine.h"
#include "engines/mssql_engine.h"
#ifdef HAVE_ORACLE
#include "engines/oracle_engine.h"
#endif
#include "engines/postgres_engine.h"
#include "utils/cluster_name_resolver.h"
#include <memory>
#include <string>

class CatalogManager {
  std::string metadataConnStr_;
  std::unique_ptr<IMetadataRepository> repo_;
  std::unique_ptr<ICatalogCleaner> cleaner_;

public:
  CatalogManager()
      : CatalogManager(DatabaseConfig::getPostgresConnectionString()) {}

  explicit CatalogManager(std::string metadataConnStr);

  CatalogManager(std::string metadataConnStr,
                 std::unique_ptr<IMetadataRepository> repo,
                 std::unique_ptr<ICatalogCleaner> cleaner);

  void cleanCatalog();
  void deactivateNoDataTables();
  void updateClusterNames();
  void validateSchemaConsistency();
  void syncCatalogMariaDBToPostgres();
  void syncCatalogMSSQLToPostgres();
  void syncCatalogPostgresToPostgres();
  void syncCatalogMongoDBToPostgres();
  void syncCatalogOracleToPostgres();
  void syncCatalogDB2ToPostgres();

private:
  void syncCatalog(const std::string &dbEngine);
  int64_t getTableSize(const std::string &schema, const std::string &table);
};

#endif
