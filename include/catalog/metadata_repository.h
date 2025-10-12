#ifndef METADATA_REPOSITORY_H
#define METADATA_REPOSITORY_H

#include "engines/database_engine.h"
#include <cstdint>
#include <pqxx/pqxx>
#include <string>
#include <unordered_map>
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

class IMetadataRepository {
public:
  virtual ~IMetadataRepository() = default;

  virtual std::vector<std::string>
  getConnectionStrings(const std::string &dbEngine) = 0;
  virtual std::vector<CatalogEntry>
  getCatalogEntries(const std::string &dbEngine,
                    const std::string &connectionString) = 0;
  virtual void insertOrUpdateTable(const CatalogTableInfo &tableInfo,
                                   const std::string &timeColumn,
                                   const std::vector<std::string> &pkColumns,
                                   bool hasPK, int64_t tableSize,
                                   const std::string &dbEngine) = 0;
  virtual void updateClusterName(const std::string &clusterName,
                                 const std::string &connectionString,
                                 const std::string &dbEngine) = 0;
  virtual void deleteTable(const std::string &schema, const std::string &table,
                           const std::string &dbEngine,
                           const std::string &connectionString = "") = 0;
  virtual int deactivateNoDataTables() = 0;
  virtual int markInactiveTablesAsSkip() = 0;
  virtual int resetTable(const std::string &schema, const std::string &table,
                         const std::string &dbEngine) = 0;
  virtual int cleanInvalidOffsets() = 0;
  virtual std::unordered_map<std::string, int64_t> getTableSizesBatch() = 0;
};

class MetadataRepository : public IMetadataRepository {
  std::string connectionString_;

public:
  explicit MetadataRepository(std::string connectionString);

  std::vector<std::string>
  getConnectionStrings(const std::string &dbEngine) override;
  std::vector<CatalogEntry>
  getCatalogEntries(const std::string &dbEngine,
                    const std::string &connectionString) override;
  void insertOrUpdateTable(const CatalogTableInfo &tableInfo,
                           const std::string &timeColumn,
                           const std::vector<std::string> &pkColumns,
                           bool hasPK, int64_t tableSize,
                           const std::string &dbEngine) override;
  void updateClusterName(const std::string &clusterName,
                         const std::string &connectionString,
                         const std::string &dbEngine) override;
  void deleteTable(const std::string &schema, const std::string &table,
                   const std::string &dbEngine,
                   const std::string &connectionString = "") override;
  int deactivateNoDataTables() override;
  int markInactiveTablesAsSkip() override;
  int resetTable(const std::string &schema, const std::string &table,
                 const std::string &dbEngine) override;
  int cleanInvalidOffsets() override;
  std::unordered_map<std::string, int64_t> getTableSizesBatch() override;

private:
  pqxx::connection getConnection();
};

#endif
