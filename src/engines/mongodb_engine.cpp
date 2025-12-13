#include "engines/mongodb_engine.h"
#include "utils/connection_utils.h"
#include <algorithm>
#include <atomic>
#include <iomanip>
#include <mutex>
#include <pqxx/pqxx>
#include <sstream>

MongoDBEngine::MongoDBEngine(std::string connectionString)
    : connectionString_(std::move(connectionString)), client_(nullptr),
      port_(27017), valid_(false), clientMutex_() {
  if (parseConnectionString(connectionString_)) {
    valid_ = connect();
  } else {
    Logger::error(LogCategory::DATABASE, "MongoDBEngine",
                  "Failed to parse connection string");
  }
}

MongoDBEngine::~MongoDBEngine() { disconnect(); }

bool MongoDBEngine::parseConnectionString(const std::string &connectionString) {
  if (connectionString.empty()) {
    return false;
  }

  if (connectionString.find("mongodb://") != 0 &&
      connectionString.find("mongodb+srv://") != 0) {
    Logger::error(LogCategory::DATABASE, "MongoDBEngine",
                  "Invalid MongoDB connection string format");
    return false;
  }

  size_t dbStart = connectionString.find_last_of('/');
  if (dbStart == std::string::npos) {
    Logger::error(LogCategory::DATABASE, "MongoDBEngine",
                  "Database name not found in connection string");
    return false;
  }

  size_t dbEnd = connectionString.find('?', dbStart);
  if (dbEnd == std::string::npos) {
    dbEnd = connectionString.length();
  }

  databaseName_ = connectionString.substr(dbStart + 1, dbEnd - dbStart - 1);
  if (databaseName_.empty()) {
    Logger::error(LogCategory::DATABASE, "MongoDBEngine",
                  "Database name is empty");
    return false;
  }

  size_t hostStart = connectionString.find("://") + 3;
  size_t atPos = connectionString.find('@', hostStart);
  size_t colonPos = connectionString.find(':', hostStart);
  size_t slashPos = connectionString.find('/', hostStart);

  if (atPos != std::string::npos && atPos < slashPos) {
    hostStart = atPos + 1;
  }

  if (colonPos != std::string::npos && colonPos < slashPos) {
    host_ = connectionString.substr(hostStart, colonPos - hostStart);
    std::string portStr =
        connectionString.substr(colonPos + 1, slashPos - colonPos - 1);
    try {
      if (!portStr.empty() && portStr.length() <= 5) {
        port_ = std::stoi(portStr);
        if (port_ <= 0 || port_ > 65535) {
          port_ = 27017;
        }
      } else {
        port_ = 27017;
      }
    } catch (const std::exception &e) {
      Logger::error(LogCategory::DATABASE, "MongoDBEngine",
                    "Failed to parse port: " + std::string(e.what()));
      port_ = 27017;
      port_ = 27017;
    } catch (...) {
      Logger::error(LogCategory::DATABASE, "MongoDBEngine",
                    "Failed to parse port: unknown error");
      port_ = 27017;
    }
  } else {
    host_ = connectionString.substr(hostStart, slashPos - hostStart);
    port_ = 27017;
  }

  return true;
}

bool MongoDBEngine::connect() {
  static std::once_flag initFlag;
  std::call_once(initFlag, []() { mongoc_init(); });

  bson_error_t error;
  client_ = mongoc_client_new(connectionString_.c_str());

  if (!client_) {
    Logger::error(LogCategory::DATABASE, "MongoDBEngine",
                  "Failed to create MongoDB client");
    return false;
  }

  mongoc_client_set_appname(client_, "DataSync");

  bson_t *ping = BCON_NEW("ping", BCON_INT32(1));
  mongoc_database_t *db =
      mongoc_client_get_database(client_, databaseName_.c_str());
  bool ret = mongoc_database_command_simple(db, ping, nullptr, nullptr, &error);
  bson_destroy(ping);
  mongoc_database_destroy(db);

  if (!ret) {
    Logger::error(LogCategory::DATABASE, "MongoDBEngine",
                  "Failed to ping MongoDB: " + std::string(error.message));
    mongoc_client_destroy(client_);
    client_ = nullptr;
    return false;
  }

  Logger::info(LogCategory::DATABASE, "MongoDBEngine",
               "Connected to MongoDB: " + host_ + ":" + std::to_string(port_) +
                   "/" + databaseName_);
  return true;
}

void MongoDBEngine::disconnect() {
  if (client_) {
    mongoc_client_destroy(client_);
    client_ = nullptr;
  }
}

std::vector<CatalogTableInfo> MongoDBEngine::discoverTables() {
  std::vector<CatalogTableInfo> collections;

  if (!isValid()) {
    Logger::error(LogCategory::DATABASE, "MongoDBEngine",
                  "Cannot discover collections: invalid connection");
    return collections;
  }

  try {
    std::lock_guard<std::mutex> lock(clientMutex_);
    mongoc_database_t *db =
        mongoc_client_get_database(client_, databaseName_.c_str());
    bson_error_t error;
    char **collection_names = mongoc_database_get_collection_names(db, &error);

    if (!collection_names) {
      Logger::error(LogCategory::DATABASE, "MongoDBEngine",
                    "Failed to get collection names: " +
                        std::string(error.message));
      mongoc_database_destroy(db);
      return collections;
    }

    for (size_t i = 0; collection_names[i]; i++) {
      CatalogTableInfo info;
      info.schema = databaseName_;
      info.table = collection_names[i];
      info.connectionString = connectionString_;
      collections.push_back(info);
    }

    bson_strfreev(collection_names);
    mongoc_database_destroy(db);

    Logger::info(LogCategory::DATABASE, "MongoDBEngine",
                 "Discovered " + std::to_string(collections.size()) +
                     " collections in database " + databaseName_);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "MongoDBEngine",
                  "Error discovering collections: " + std::string(e.what()));
  }

  return collections;
}

std::vector<std::string>
MongoDBEngine::detectPrimaryKey(const std::string &schema,
                                const std::string &table) {
  std::vector<std::string> pk;
  pk.push_back("_id");
  return pk;
}

std::string MongoDBEngine::detectTimeColumn(const std::string &schema,
                                            const std::string &table) {
  return "";
}

std::pair<int, int>
MongoDBEngine::getColumnCounts(const std::string &schema,
                               const std::string &table,
                               const std::string &targetConnStr) {
  int sourceCount = 0;
  int targetCount = 0;

  if (schema.empty() || table.empty()) {
    Logger::error(LogCategory::DATABASE, "MongoDBEngine",
                  "getColumnCounts: schema and table must not be empty");
    return {sourceCount, targetCount};
  }

  if (!isValid()) {
    return {sourceCount, targetCount};
  }

  try {
    std::lock_guard<std::mutex> lock(clientMutex_);
    mongoc_collection_t *collection =
        mongoc_client_get_collection(client_, schema.c_str(), table.c_str());
    if (collection) {
      long long count = getCollectionCount(schema, table);
      sourceCount = static_cast<int>(count);
      mongoc_collection_destroy(collection);
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "MongoDBEngine",
                  "Error getting collection count: " + std::string(e.what()));
  }

  try {
    pqxx::connection conn(targetConnStr);
    pqxx::work txn(conn);
    auto result =
        txn.exec_params("SELECT COUNT(*) FROM information_schema.tables "
                        "WHERE table_schema = $1 AND table_name = $2",
                        schema, table);
    if (!result.empty() && !result[0][0].is_null()) {
      targetCount = result[0][0].as<int>();
    }
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "MongoDBEngine",
                  "Error getting target table count: " + std::string(e.what()));
  }

  return {sourceCount, targetCount};
}

long long MongoDBEngine::getCollectionCount(const std::string &database,
                                            const std::string &collection) {
  if (!isValid()) {
    return 0;
  }

  try {
    std::lock_guard<std::mutex> lock(clientMutex_);
    mongoc_collection_t *coll = mongoc_client_get_collection(
        client_, database.c_str(), collection.c_str());
    if (!coll) {
      return 0;
    }

    bson_error_t error;
    int64_t count = mongoc_collection_count_documents(coll, nullptr, nullptr,
                                                      nullptr, nullptr, &error);

    mongoc_collection_destroy(coll);

    if (count < 0) {
      Logger::error(LogCategory::DATABASE, "MongoDBEngine",
                    "Error counting documents: " + std::string(error.message));
      return 0;
    }

    return static_cast<long long>(count);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "MongoDBEngine",
                  "Exception counting documents: " + std::string(e.what()));
    return 0;
  }
}
