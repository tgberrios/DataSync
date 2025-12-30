#ifndef BIGQUERY_ENGINE_H
#define BIGQUERY_ENGINE_H

#include "core/logger.h"
#include "engines/warehouse_engine.h"
#include <string>
#include <vector>

struct BigQueryConfig {
  std::string project_id;
  std::string dataset_id;
  std::string credentials_json;
  std::string access_token;
};

class BigQueryEngine : public IWarehouseEngine {
  BigQueryConfig config_;

public:
  explicit BigQueryEngine(std::string connectionString);
  explicit BigQueryEngine(const BigQueryConfig &config);

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
  void parseConnectionString(const std::string &connectionString);
  std::string getAccessToken();
  std::string makeAPIRequest(const std::string &method,
                             const std::string &endpoint,
                             const std::string &body = "");
  std::string mapDataType(const std::string &dataType);
};

#endif
