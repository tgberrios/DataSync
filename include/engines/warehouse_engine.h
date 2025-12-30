#ifndef WAREHOUSE_ENGINE_H
#define WAREHOUSE_ENGINE_H

#include "third_party/json.hpp"
#include <string>
#include <vector>

using json = nlohmann::json;

struct WarehouseColumnInfo {
  std::string name;
  std::string data_type;
  bool is_nullable;
  std::string default_value;
};

class IWarehouseEngine {
public:
  virtual ~IWarehouseEngine() = default;

  virtual void createSchema(const std::string &schemaName) = 0;
  virtual void createTable(const std::string &schemaName,
                           const std::string &tableName,
                           const std::vector<WarehouseColumnInfo> &columns,
                           const std::vector<std::string> &primaryKeys) = 0;
  virtual void
  insertData(const std::string &schemaName, const std::string &tableName,
             const std::vector<std::string> &columns,
             const std::vector<std::vector<std::string>> &rows) = 0;
  virtual void
  upsertData(const std::string &schemaName, const std::string &tableName,
             const std::vector<std::string> &columns,
             const std::vector<std::string> &primaryKeys,
             const std::vector<std::vector<std::string>> &rows) = 0;
  virtual void createIndex(const std::string &schemaName,
                           const std::string &tableName,
                           const std::vector<std::string> &indexColumns,
                           const std::string &indexName = "") = 0;
  virtual void createPartition(const std::string &schemaName,
                               const std::string &tableName,
                               const std::string &partitionColumn) = 0;
  virtual std::vector<json> executeQuery(const std::string &query) = 0;
  virtual void executeStatement(const std::string &statement) = 0;
  virtual std::string quoteIdentifier(const std::string &identifier) = 0;
  virtual std::string quoteValue(const std::string &value) = 0;
  virtual bool testConnection() = 0;
};

#endif
