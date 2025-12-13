#ifndef API_TO_DATABASE_SYNC_H
#define API_TO_DATABASE_SYNC_H

#include "catalog/api_catalog_repository.h"
#include "core/Config.h"
#include "core/logger.h"
#include "engines/api_engine.h"
#include "third_party/json.hpp"
#include <memory>
#include <pqxx/pqxx>
#include <string>
#include <vector>

using json = nlohmann::json;

class APIToDatabaseSync {
  std::string metadataConnectionString_;
  std::unique_ptr<APICatalogRepository> apiRepo_;

  void logToProcessLog(const std::string &processName,
                       const std::string &status,
                       const std::string &targetSchema, int tablesProcessed,
                       int64_t totalRowsProcessed, int tablesSuccess,
                       int tablesFailed, const std::string &errorMessage,
                       const json &metadata);

  void createPostgreSQLTable(const APICatalogEntry &entry,
                             const std::vector<std::string> &columns,
                             const std::vector<std::string> &columnTypes);

  void insertDataToPostgreSQL(const APICatalogEntry &entry,
                              const std::vector<json> &data);

  void createMariaDBTable(const APICatalogEntry &entry,
                          const std::vector<std::string> &columns,
                          const std::vector<std::string> &columnTypes);

  void insertDataToMariaDB(const APICatalogEntry &entry,
                           const std::vector<json> &data);

  void createMSSQLTable(const APICatalogEntry &entry,
                        const std::vector<std::string> &columns,
                        const std::vector<std::string> &columnTypes);

  void insertDataToMSSQL(const APICatalogEntry &entry,
                         const std::vector<json> &data);

  void createMongoDBCollection(const APICatalogEntry &entry,
                               const std::vector<std::string> &columns);

  void insertDataToMongoDB(const APICatalogEntry &entry,
                           const std::vector<json> &data);

  void createOracleTable(const APICatalogEntry &entry,
                         const std::vector<std::string> &columns,
                         const std::vector<std::string> &columnTypes);

  void insertDataToOracle(const APICatalogEntry &entry,
                          const std::vector<json> &data);

  std::string inferSQLType(const json &value);
  std::string convertJSONValueToString(const json &value,
                                       const std::string &sqlType);
  std::vector<std::string>
  detectColumnTypes(const std::vector<json> &data,
                    const std::vector<std::string> &columns);

public:
  explicit APIToDatabaseSync(std::string metadataConnectionString);
  ~APIToDatabaseSync();

  void syncAllAPIs();
  void syncAPIToDatabase(const std::string &apiName);
  void processAPIFullLoad(const APICatalogEntry &entry);
};

#endif
