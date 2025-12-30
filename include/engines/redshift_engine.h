#ifndef REDSHIFT_ENGINE_H
#define REDSHIFT_ENGINE_H

#include "core/logger.h"
#include "engines/warehouse_engine.h"
#include <pqxx/pqxx>
#include <string>
#include <vector>

class RedshiftEngine : public IWarehouseEngine {
  std::string connectionString_;

public:
  explicit RedshiftEngine(std::string connectionString);

  void createSchema(const std::string &schemaName) override;
  void createTable(const std::string &schemaName, const std::string &tableName,
                   const std::vector<WarehouseColumnInfo> &columns,
                   const std::vector<std::string> &primaryKeys) override;
  void insertData(const std::string &schemaName, const std::string &tableName,
                  const std::vector<std::string> &columns,
                  const std::vector<std::vector<std::string>> &rows) override;
  void upsertData(const std::string &schemaName, const std::string &tableName,
                  const std::vector<std::string> &columns,
                  const std::vector<std::string> &primaryKeys,
                  const std::vector<std::vector<std::string>> &rows) override;
  void createIndex(const std::string &schemaName, const std::string &tableName,
                   const std::vector<std::string> &indexColumns,
                   const std::string &indexName = "") override;
  void createPartition(const std::string &schemaName,
                       const std::string &tableName,
                       const std::string &partitionColumn) override;
  std::vector<json> executeQuery(const std::string &query) override;
  void executeStatement(const std::string &statement) override;
  std::string quoteIdentifier(const std::string &identifier) override;
  std::string quoteValue(const std::string &value) override;
  bool testConnection() override;

private:
  std::unique_ptr<pqxx::connection> getConnection();
  std::string mapDataType(const std::string &dataType);
};

#endif
