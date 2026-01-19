#ifndef DATA_VAULT_BUILDER_H
#define DATA_VAULT_BUILDER_H

#include "catalog/data_vault_repository.h"
#include "core/logger.h"
#include "engines/postgres_engine.h"
#include "engines/warehouse_engine.h"
#include "third_party/json.hpp"
#include <memory>
#include <pqxx/pqxx>
#include <string>
#include <vector>

using json = nlohmann::json;

class DataVaultBuilder {
  std::string metadataConnectionString_;
  std::unique_ptr<DataVaultRepository> vaultRepo_;

  std::unique_ptr<IWarehouseEngine>
  createWarehouseEngine(const std::string &targetDbEngine,
                        const std::string &targetConnectionString);

  void buildHub(const DataVaultModel &vault, const HubTable &hub);
  void buildLink(const DataVaultModel &vault, const LinkTable &link);
  void buildSatellite(const DataVaultModel &vault, const SatelliteTable &satellite);
  void buildPointInTime(const DataVaultModel &vault, const PointInTimeTable &pit);
  void buildBridge(const DataVaultModel &vault, const BridgeTable &bridge);

  void createHubTableStructure(const DataVaultModel &vault, const HubTable &hub);
  void createLinkTableStructure(const DataVaultModel &vault, const LinkTable &link);
  void createSatelliteTableStructure(const DataVaultModel &vault, const SatelliteTable &satellite);
  void createPointInTimeTableStructure(const DataVaultModel &vault, const PointInTimeTable &pit);
  void createBridgeTableStructure(const DataVaultModel &vault, const BridgeTable &bridge);

  std::vector<json> executeSourceQuery(const DataVaultModel &vault,
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
  getQueryColumnNames(const DataVaultModel &vault,
                      const std::string &query);

  void loadHubData(const DataVaultModel &vault, const HubTable &hub,
                   const std::vector<json> &sourceData);

  void loadLinkData(const DataVaultModel &vault, const LinkTable &link,
                    const std::vector<json> &sourceData);

  void loadSatelliteData(const DataVaultModel &vault, const SatelliteTable &satellite,
                         const std::vector<json> &sourceData);

  void loadPointInTimeData(const DataVaultModel &vault, const PointInTimeTable &pit);

  void loadBridgeData(const DataVaultModel &vault, const BridgeTable &bridge);

  std::string generateHashKey(const std::vector<std::string> &businessKeys,
                               const json &row);

  void createIndexes(const DataVaultModel &vault,
                     const std::string &schemaName,
                     const std::string &tableName,
                     const std::vector<std::string> &indexColumns);

  int64_t logToProcessLog(const std::string &vaultName,
                          const std::string &status, int64_t totalRowsProcessed,
                          const std::string &errorMessage,
                          const json &metadata);

  void validateVaultModel(const DataVaultModel &vault);

public:
  explicit DataVaultBuilder(std::string metadataConnectionString);
  ~DataVaultBuilder();

  DataVaultBuilder(const DataVaultBuilder &) = delete;
  DataVaultBuilder &operator=(const DataVaultBuilder &) = delete;

  void buildVault(const std::string &vaultName);
  int64_t buildVaultAndGetLogId(const std::string &vaultName);
  void buildAllActiveVaults();
  DataVaultModel getVault(const std::string &vaultName);
};

#endif
