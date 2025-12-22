#ifndef GOOGLE_SHEETS_TO_DATABASE_SYNC_H
#define GOOGLE_SHEETS_TO_DATABASE_SYNC_H

#include "catalog/google_sheets_catalog_repository.h"
#include "core/Config.h"
#include "core/logger.h"
#include "engines/google_sheets_engine.h"
#include "third_party/json.hpp"
#include <memory>
#include <pqxx/pqxx>
#include <string>
#include <vector>

using json = nlohmann::json;

class GoogleSheetsToDatabaseSync {
  std::string metadataConnectionString_;
  std::unique_ptr<GoogleSheetsCatalogRepository> sheetsRepo_;

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
                              const std::vector<json> &data,
                              const std::vector<std::string> &columns);

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
  explicit GoogleSheetsToDatabaseSync(std::string metadataConnectionString);
  ~GoogleSheetsToDatabaseSync();

  void syncAllGoogleSheets();
  void syncGoogleSheetToDatabase(const std::string &sheetName);
  void processGoogleSheetFullLoad(const APICatalogEntry &entry);
};

#endif
