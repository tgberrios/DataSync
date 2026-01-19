#ifndef DATA_WAREHOUSE_BUILDER_H
#define DATA_WAREHOUSE_BUILDER_H

#include "catalog/data_warehouse_repository.h"
#include "core/logger.h"
#include "engines/postgres_engine.h"
#include "engines/warehouse_engine.h"
#include "third_party/json.hpp"
#include <memory>
#include <pqxx/pqxx>
#include <string>
#include <vector>

using json = nlohmann::json;

class DataWarehouseBuilder {
  std::string metadataConnectionString_;
  std::unique_ptr<DataWarehouseRepository> warehouseRepo_;

  std::unique_ptr<IWarehouseEngine>
  createWarehouseEngine(const std::string &targetDbEngine,
                        const std::string &targetConnectionString);

  void buildDimensionTable(const DataWarehouseModel &warehouse,
                           const DimensionTable &dimension);

  void buildFactTable(const DataWarehouseModel &warehouse,
                      const FactTable &fact);

  void buildBronzeLayer(const DataWarehouseModel &warehouse);
  void buildSilverLayer(const DataWarehouseModel &warehouse);
  void buildGoldLayer(const DataWarehouseModel &warehouse);

  std::string getLayerSchemaName(const DataWarehouseModel &warehouse);
  std::string getSourceSchemaName(const DataWarehouseModel &warehouse);

  bool tableExistsInSchema(const std::string &connectionString,
                           const std::string &schemaName,
                           const std::string &tableName);

  void buildBronzeTable(const DataWarehouseModel &warehouse,
                        const std::string &tableName,
                        const std::string &sourceQuery,
                        const std::vector<std::string> &columns);

  void buildSilverTable(const DataWarehouseModel &warehouse,
                        const std::string &tableName,
                        const std::vector<std::string> &columns,
                        const std::vector<json> &cleanedData);

  std::vector<json> cleanAndValidateData(const std::vector<json> &rawData,
                                          const std::vector<std::string> &columns);

  void createDimensionTableStructure(const DataWarehouseModel &warehouse,
                                     const DimensionTable &dimension);

  void createFactTableStructure(const DataWarehouseModel &warehouse,
                                const FactTable &fact);

  std::vector<json> executeSourceQuery(const DataWarehouseModel &warehouse,
                                       const std::string &query);

  std::vector<json> executeQueryPostgreSQL(const std::string &connectionString,
                                           const std::string &query);
  std::vector<json> executeQueryMariaDB(const std::string &connectionString,
                                        const std::string &query);
  std::vector<json> executeQueryMSSQL(const std::string &connectionString,
                                      const std::string &query);
  std::vector<json> executeQueryOracle(const std::string &connectionString,
                                       const std::string &query);
  std::vector<json> executeQueryMongoDB(const std::string &connectionString,
                                        const std::string &query);

  std::vector<std::string>
  getQueryColumnNames(const DataWarehouseModel &warehouse,
                      const std::string &query);

  void applySCDType1(const DataWarehouseModel &warehouse,
                     const DimensionTable &dimension,
                     const std::vector<json> &sourceData);

  void applySCDType2(const DataWarehouseModel &warehouse,
                     const DimensionTable &dimension,
                     const std::vector<json> &sourceData);

  void applySCDType3(const DataWarehouseModel &warehouse,
                     const DimensionTable &dimension,
                     const std::vector<json> &sourceData);

  void loadFactFullLoad(const DataWarehouseModel &warehouse,
                        const FactTable &fact,
                        const std::vector<json> &sourceData);

  void createIndexes(const DataWarehouseModel &warehouse,
                     const std::string &schemaName,
                     const std::string &tableName,
                     const std::vector<std::string> &indexColumns);

  void createPartitions(const DataWarehouseModel &warehouse,
                        const std::string &schemaName,
                        const std::string &tableName,
                        const std::string &partitionColumn);

  int64_t logToProcessLog(const std::string &warehouseName,
                          const std::string &status, int64_t totalRowsProcessed,
                          const std::string &errorMessage,
                          const json &metadata);

public:
  explicit DataWarehouseBuilder(std::string metadataConnectionString);
  ~DataWarehouseBuilder();

  DataWarehouseBuilder(const DataWarehouseBuilder &) = delete;
  DataWarehouseBuilder &operator=(const DataWarehouseBuilder &) = delete;

  void buildWarehouse(const std::string &warehouseName);
  int64_t buildWarehouseAndGetLogId(const std::string &warehouseName);
  void buildAllActiveWarehouses();
  DataWarehouseModel getWarehouse(const std::string &warehouseName);

  void validateWarehouseModel(const DataWarehouseModel &warehouse);

  void promoteToSilver(const std::string &warehouseName);
  void promoteToGold(const std::string &warehouseName);
  bool validateDataQuality(const DataWarehouseModel &warehouse);
};

#endif
