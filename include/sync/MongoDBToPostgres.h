#ifndef MONGODBTOPOSTGRES_H
#define MONGODBTOPOSTGRES_H

#include "catalog/catalog_manager.h"
#include "engines/mongodb_engine.h"
#include "sync/DatabaseToPostgresSync.h"
#include "sync/TableProcessorThreadPool.h"
#include <algorithm>
#include <chrono>
#include <pqxx/pqxx>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>
#include <mongoc/mongoc.h>
#include <bson/bson.h>

using json = nlohmann::json;
using namespace ParallelProcessing;

class MongoDBToPostgres : public DatabaseToPostgresSync {
public:
  MongoDBToPostgres() = default;
  ~MongoDBToPostgres() { shutdownParallelProcessing(); }

  static std::unordered_map<std::string, std::string> dataTypeMap;

  std::string cleanValueForPostgres(const std::string &value,
                                    const std::string &columnType) override;

  void transferDataMongoDBToPostgresParallel();
  void setupTableTargetMongoDBToPostgres();

private:
  bool shouldSyncCollection(const TableInfo &tableInfo);
  void truncateAndLoadCollection(const TableInfo &tableInfo);
  std::vector<std::vector<std::string>>
  fetchCollectionData(const TableInfo &tableInfo);
  void convertBSONToPostgresRow(bson_t *doc, const std::vector<std::string> &fields,
                                std::vector<std::string> &row,
                                std::unordered_map<std::string, int> &fieldIndexMap);
  std::string inferPostgreSQLType(const bson_value_t *value);
  void createPostgreSQLTable(const TableInfo &tableInfo,
                             const std::vector<std::string> &fields,
                             const std::vector<std::string> &fieldTypes);
  std::vector<std::string> discoverCollectionFields(const std::string &connectionString,
                                                     const std::string &database,
                                                     const std::string &collection);
  std::chrono::system_clock::time_point parseTimestamp(const std::string &timestamp);
  void updateLastSyncTime(pqxx::connection &pgConn, const std::string &schema_name,
                          const std::string &table_name);
};

#endif

