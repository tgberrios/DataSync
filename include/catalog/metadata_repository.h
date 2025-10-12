#ifndef METADATA_REPOSITORY_H
#define METADATA_REPOSITORY_H

#include "engines/database_engine.h"
#include <cstdint>
#include <pqxx/pqxx>
#include <string>
#include <vector>

struct CatalogEntry {
  std::string schema;
  std::string table;
  std::string dbEngine;
  std::string connectionString;
  std::string status;
  std::string lastSyncColumn;
  std::string pkColumns;
  std::string pkStrategy;
  bool hasPK;
  int64_t tableSize;
};

class MetadataRepository {
  std::string connectionString_;

public:
  explicit MetadataRepository(std::string connectionString);

  std::vector<std::string> getConnectionStrings(const std::string &dbEngine);
  std::vector<CatalogEntry>
  getCatalogEntries(const std::string &dbEngine,
                    const std::string &connectionString);
  void insertOrUpdateTable(const CatalogTableInfo &tableInfo,
                           const std::string &timeColumn,
                           const std::vector<std::string> &pkColumns,
                           bool hasPK, int64_t tableSize,
                           const std::string &dbEngine);
  void updateClusterName(const std::string &clusterName,
                         const std::string &connectionString,
                         const std::string &dbEngine);
  void deleteTable(const std::string &schema, const std::string &table,
                   const std::string &dbEngine,
                   const std::string &connectionString = "");
  int deactivateNoDataTables();
  int markInactiveTablesAsSkip();
  int resetTable(const std::string &schema, const std::string &table,
                 const std::string &dbEngine);
  int cleanInvalidOffsets();

private:
  pqxx::connection getConnection();
};

#endif
