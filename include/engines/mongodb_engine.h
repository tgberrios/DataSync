#ifndef MONGODB_ENGINE_H
#define MONGODB_ENGINE_H

#include "core/Config.h"
#include "core/logger.h"
#include "engines/database_engine.h"
#include <bson/bson.h>
#include <memory>
#include <mongoc/mongoc.h>
#include <string>
#include <vector>

class MongoDBEngine : public IDatabaseEngine {
  std::string connectionString_;
  mongoc_client_t *client_;
  std::string databaseName_;
  std::string host_;
  int port_;
  bool valid_;

public:
  explicit MongoDBEngine(std::string connectionString);
  ~MongoDBEngine();

  MongoDBEngine(const MongoDBEngine &) = delete;
  MongoDBEngine &operator=(const MongoDBEngine &) = delete;

  std::vector<CatalogTableInfo> discoverTables() override;
  std::vector<std::string> detectPrimaryKey(const std::string &schema,
                                            const std::string &table) override;
  std::string detectTimeColumn(const std::string &schema,
                               const std::string &table) override;
  std::pair<int, int>
  getColumnCounts(const std::string &schema, const std::string &table,
                  const std::string &targetConnStr) override;

  bool isValid() const { return valid_ && client_ != nullptr; }
  mongoc_client_t *getClient() const { return client_; }
  std::string getDatabaseName() const { return databaseName_; }

private:
  bool parseConnectionString(const std::string &connectionString);
  bool connect();
  void disconnect();
  long long getCollectionCount(const std::string &database,
                               const std::string &collection);
};

#endif
