#include "governance/DataGovernanceMongoDB.h"
#include "core/database_config.h"
#include "core/logger.h"
#include "engines/mongodb_engine.h"
#include <algorithm>
#include <bson/bson.h>
#include <iomanip>
#include <mutex>
#include <pqxx/pqxx>
#include <sstream>

DataGovernanceMongoDB::DataGovernanceMongoDB(
    const std::string &connectionString)
    : connectionString_(connectionString), client_(nullptr) {
  serverName_ = extractServerName(connectionString);
  databaseName_ = extractDatabaseName(connectionString);
  connect(connectionString);
}

DataGovernanceMongoDB::~DataGovernanceMongoDB() { disconnect(); }

std::string
DataGovernanceMongoDB::extractServerName(const std::string &connectionString) {
  if (connectionString.empty()) {
    return "UNKNOWN";
  }

  size_t hostStart = connectionString.find("://");
  if (hostStart == std::string::npos) {
    return "UNKNOWN";
  }
  hostStart += 3;

  size_t atPos = connectionString.find('@', hostStart);
  if (atPos != std::string::npos) {
    hostStart = atPos + 1;
  }

  size_t colonPos = connectionString.find(':', hostStart);
  size_t slashPos = connectionString.find('/', hostStart);

  if (colonPos != std::string::npos && colonPos < slashPos) {
    std::string host = connectionString.substr(hostStart, colonPos - hostStart);
    std::string port =
        connectionString.substr(colonPos + 1, slashPos - colonPos - 1);
    return host + ":" + port;
  } else if (slashPos != std::string::npos) {
    return connectionString.substr(hostStart, slashPos - hostStart);
  }

  return "UNKNOWN";
}

std::string DataGovernanceMongoDB::extractDatabaseName(
    const std::string &connectionString) {
  if (connectionString.empty()) {
    return "";
  }

  size_t dbStart = connectionString.find_last_of('/');
  if (dbStart == std::string::npos) {
    return "";
  }

  size_t dbEnd = connectionString.find('?', dbStart);
  if (dbEnd == std::string::npos) {
    dbEnd = connectionString.length();
  }

  return connectionString.substr(dbStart + 1, dbEnd - dbStart - 1);
}

bool DataGovernanceMongoDB::connect(const std::string &connectionString) {
  try {
    if (connectionString.empty()) {
      Logger::error(LogCategory::GOVERNANCE, "DataGovernanceMongoDB",
                    "Empty connection string");
      return false;
    }

    static std::once_flag initFlag;
    std::call_once(initFlag, []() { mongoc_init(); });

    bson_error_t error;
    client_ = mongoc_client_new(connectionString.c_str());

    if (!client_) {
      Logger::error(LogCategory::GOVERNANCE, "DataGovernanceMongoDB",
                    "Failed to create MongoDB client");
      return false;
    }

    mongoc_client_set_appname(client_, "DataSync");

    size_t dbStart = connectionString.find_last_of('/');
    if (dbStart == std::string::npos) {
      Logger::error(LogCategory::GOVERNANCE, "DataGovernanceMongoDB",
                    "Database name not found in connection string");
      mongoc_client_destroy(client_);
      client_ = nullptr;
      return false;
    }

    size_t dbEnd = connectionString.find('?', dbStart);
    if (dbEnd == std::string::npos) {
      dbEnd = connectionString.length();
    }

    std::string dbName =
        connectionString.substr(dbStart + 1, dbEnd - dbStart - 1);
    if (dbName.empty()) {
      Logger::error(LogCategory::GOVERNANCE, "DataGovernanceMongoDB",
                    "Database name is empty");
      mongoc_client_destroy(client_);
      client_ = nullptr;
      return false;
    }

    bson_t *ping = BCON_NEW("ping", BCON_INT32(1));
    mongoc_database_t *db = mongoc_client_get_database(client_, dbName.c_str());
    bool ret =
        mongoc_database_command_simple(db, ping, nullptr, nullptr, &error);
    bson_destroy(ping);
    mongoc_database_destroy(db);

    if (!ret) {
      Logger::error(LogCategory::GOVERNANCE, "DataGovernanceMongoDB",
                    "Failed to ping MongoDB: " + std::string(error.message));
      mongoc_client_destroy(client_);
      client_ = nullptr;
      return false;
    }

    databaseName_ = dbName;
    return true;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "DataGovernanceMongoDB",
                  "Failed to connect: " + std::string(e.what()));
  }
  return false;
}

void DataGovernanceMongoDB::disconnect() {
  if (client_) {
    mongoc_client_destroy(client_);
    client_ = nullptr;
  }
}

void DataGovernanceMongoDB::collectGovernanceData() {
  Logger::info(LogCategory::GOVERNANCE, "DataGovernanceMongoDB",
               "Starting governance data collection for MongoDB");

  governanceData_.clear();

  if (!client_) {
    Logger::error(LogCategory::GOVERNANCE, "DataGovernanceMongoDB",
                  "MongoDB client not connected");
    return;
  }

  try {
    queryCollectionStats();
    queryIndexStats();
    queryReplicaSetInfo();
    calculateHealthScores();

    Logger::info(LogCategory::GOVERNANCE, "DataGovernanceMongoDB",
                 "Governance data collection completed. Collected " +
                     std::to_string(governanceData_.size()) + " records");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "DataGovernanceMongoDB",
                  "Error collecting governance data: " + std::string(e.what()));
  }
}

void DataGovernanceMongoDB::queryCollectionStats() {
  if (!client_ || databaseName_.empty()) {
    return;
  }

  try {
    mongoc_database_t *database =
        mongoc_client_get_database(client_, databaseName_.c_str());
    if (!database) {
      Logger::error(LogCategory::GOVERNANCE, "DataGovernanceMongoDB",
                    "Failed to get database");
      return;
    }

    bson_error_t error;
    char **collection_names = mongoc_database_get_collection_names_with_opts(
        database, nullptr, &error);
    if (!collection_names) {
      Logger::error(LogCategory::GOVERNANCE, "DataGovernanceMongoDB",
                    "Failed to get collection names: " +
                        std::string(error.message));
      mongoc_database_destroy(database);
      return;
    }

    for (size_t i = 0; collection_names[i]; i++) {
      std::string collectionName = collection_names[i];
      mongoc_collection_t *collection =
          mongoc_database_get_collection(database, collectionName.c_str());
      if (!collection) {
        continue;
      }

      bson_t *command =
          BCON_NEW("collStats", BCON_UTF8(collectionName.c_str()));
      bson_t reply;
      bool success = mongoc_database_command_simple(database, command, nullptr,
                                                    &reply, &error);

      if (success) {
        MongoDBGovernanceData data;
        data.server_name = serverName_;
        data.database_name = databaseName_;
        data.collection_name = collectionName;

        bson_iter_t iter;
        if (bson_iter_init(&iter, &reply)) {
          while (bson_iter_next(&iter)) {
            const char *key = bson_iter_key(&iter);
            if (strcmp(key, "count") == 0 && BSON_ITER_HOLDS_INT32(&iter)) {
              data.document_count = bson_iter_int32(&iter);
            } else if (strcmp(key, "size") == 0 &&
                       BSON_ITER_HOLDS_INT32(&iter)) {
              data.collection_size_mb =
                  bson_iter_int32(&iter) / (1024.0 * 1024.0);
            } else if (strcmp(key, "storageSize") == 0 &&
                       BSON_ITER_HOLDS_INT32(&iter)) {
              data.storage_size_mb = bson_iter_int32(&iter) / (1024.0 * 1024.0);
            } else if (strcmp(key, "totalIndexSize") == 0 &&
                       BSON_ITER_HOLDS_INT32(&iter)) {
              data.index_size_mb = bson_iter_int32(&iter) / (1024.0 * 1024.0);
            } else if (strcmp(key, "totalSize") == 0 &&
                       BSON_ITER_HOLDS_INT32(&iter)) {
              data.total_size_mb = bson_iter_int32(&iter) / (1024.0 * 1024.0);
            } else if (strcmp(key, "avgObjSize") == 0 &&
                       BSON_ITER_HOLDS_INT32(&iter)) {
              data.avg_object_size_bytes = bson_iter_int32(&iter);
            } else if (strcmp(key, "nindexes") == 0 &&
                       BSON_ITER_HOLDS_INT32(&iter)) {
              data.index_count = bson_iter_int32(&iter);
            }
          }
        }

        data.total_size_mb = data.collection_size_mb + data.index_size_mb;
        governanceData_.push_back(data);
        bson_destroy(&reply);
      } else {
        Logger::warning(LogCategory::GOVERNANCE, "DataGovernanceMongoDB",
                        "Failed to get stats for collection " + collectionName +
                            ": " + error.message);
      }

      bson_destroy(command);
      mongoc_collection_destroy(collection);
    }

    bson_strfreev(collection_names);
    mongoc_database_destroy(database);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "DataGovernanceMongoDB",
                  "Error querying collection stats: " + std::string(e.what()));
  }
}

void DataGovernanceMongoDB::queryIndexStats() {
  if (!client_ || databaseName_.empty()) {
    return;
  }

  try {
    mongoc_database_t *database =
        mongoc_client_get_database(client_, databaseName_.c_str());
    if (!database) {
      return;
    }

    bson_error_t error;
    char **collection_names = mongoc_database_get_collection_names_with_opts(
        database, nullptr, &error);
    if (!collection_names) {
      mongoc_database_destroy(database);
      return;
    }

    for (size_t i = 0; collection_names[i]; i++) {
      std::string collectionName = collection_names[i];
      mongoc_collection_t *collection =
          mongoc_database_get_collection(database, collectionName.c_str());
      if (!collection) {
        continue;
      }

      mongoc_cursor_t *cursor =
          mongoc_collection_find_indexes_with_opts(collection, nullptr);
      const bson_t *doc;

      while (mongoc_cursor_next(cursor, &doc)) {
        MongoDBGovernanceData data;
        data.server_name = serverName_;
        data.database_name = databaseName_;
        data.collection_name = collectionName;

        bson_iter_t iter;
        if (bson_iter_init(&iter, doc)) {
          std::ostringstream keysStream;
          bool firstKey = true;

          while (bson_iter_next(&iter)) {
            const char *key = bson_iter_key(&iter);
            if (strcmp(key, "name") == 0 && BSON_ITER_HOLDS_UTF8(&iter)) {
              data.index_name = bson_iter_utf8(&iter, nullptr);
            } else if (strcmp(key, "key") == 0 &&
                       BSON_ITER_HOLDS_DOCUMENT(&iter)) {
              const uint8_t *data_ptr;
              uint32_t len;
              bson_iter_document(&iter, &len, &data_ptr);
              bson_t *keyDoc = bson_new_from_data(data_ptr, len);
              bson_iter_t keyIter;
              if (bson_iter_init(&keyIter, keyDoc)) {
                while (bson_iter_next(&keyIter)) {
                  if (!firstKey) {
                    keysStream << ", ";
                  }
                  keysStream << bson_iter_key(&keyIter);
                  bson_type_t type = bson_iter_type(&keyIter);
                  if (type == BSON_TYPE_INT32) {
                    int32_t value = bson_iter_int32(&keyIter);
                    keysStream << ":" << (value == 1 ? "ASC" : "DESC");
                  }
                  firstKey = false;
                }
              }
              data.index_keys = keysStream.str();
              bson_destroy(keyDoc);
            } else if (strcmp(key, "unique") == 0 &&
                       BSON_ITER_HOLDS_BOOL(&iter)) {
              data.index_unique = bson_iter_bool(&iter);
            } else if (strcmp(key, "sparse") == 0 &&
                       BSON_ITER_HOLDS_BOOL(&iter)) {
              data.index_sparse = bson_iter_bool(&iter);
            }
          }
        }

        if (!data.index_name.empty()) {
          governanceData_.push_back(data);
        }
      }

      mongoc_cursor_destroy(cursor);
      mongoc_collection_destroy(collection);
    }

    bson_strfreev(collection_names);
    mongoc_database_destroy(database);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "DataGovernanceMongoDB",
                  "Error querying index stats: " + std::string(e.what()));
  }
}

void DataGovernanceMongoDB::queryReplicaSetInfo() {
  if (!client_) {
    return;
  }

  try {
    mongoc_database_t *admin_db = mongoc_client_get_database(client_, "admin");
    if (!admin_db) {
      return;
    }

    bson_t *command = BCON_NEW("replSetGetStatus", BCON_INT32(1));
    bson_t reply;
    bson_error_t error;

    bool success = mongoc_database_command_simple(admin_db, command, nullptr,
                                                  &reply, &error);
    if (success) {
      bson_iter_t iter;
      if (bson_iter_init(&iter, &reply)) {
        while (bson_iter_next(&iter)) {
          const char *key = bson_iter_key(&iter);
          if (strcmp(key, "set") == 0 && BSON_ITER_HOLDS_UTF8(&iter)) {
            std::string replicaSetName = bson_iter_utf8(&iter, nullptr);
            for (auto &data : governanceData_) {
              if (data.replica_set_name.empty()) {
                data.replica_set_name = replicaSetName;
              }
            }
          }
        }
      }
      bson_destroy(&reply);
    }

    bson_destroy(command);
    mongoc_database_destroy(admin_db);
  } catch (const std::exception &e) {
    Logger::warning(LogCategory::GOVERNANCE, "DataGovernanceMongoDB",
                    "Could not query replica set info: " +
                        std::string(e.what()));
  }
}

void DataGovernanceMongoDB::calculateHealthScores() {
  for (auto &data : governanceData_) {
    double score = 100.0;

    if (data.index_count == 0 && data.document_count > 1000) {
      score -= 20.0;
      data.recommendation_summary =
          "Consider adding indexes for better query performance";
    }

    if (data.total_size_mb > 10000) {
      score -= 10.0;
      if (!data.recommendation_summary.empty()) {
        data.recommendation_summary += "; ";
      }
      data.recommendation_summary +=
          "Large collection size, consider archiving old data";
    }

    if (data.avg_object_size_bytes > 1024 * 1024) {
      score -= 15.0;
      if (!data.recommendation_summary.empty()) {
        data.recommendation_summary += "; ";
      }
      data.recommendation_summary += "Large average document size, consider "
                                     "document structure optimization";
    }

    if (score >= 80.0) {
      data.health_status = "HEALTHY";
    } else if (score >= 60.0) {
      data.health_status = "WARNING";
    } else {
      data.health_status = "CRITICAL";
    }

    if (data.document_count > 10000) {
      data.access_frequency = "HIGH";
    } else if (data.document_count > 1000) {
      data.access_frequency = "MEDIUM";
    } else {
      data.access_frequency = "LOW";
    }

    data.health_score = score;
  }
}

void DataGovernanceMongoDB::storeGovernanceData() {
  if (governanceData_.empty()) {
    Logger::warning(LogCategory::GOVERNANCE, "DataGovernanceMongoDB",
                    "No governance data to store");
    return;
  }

  try {
    pqxx::connection conn(DatabaseConfig::getPostgresConnectionString());
    if (!conn.is_open()) {
      Logger::error(LogCategory::GOVERNANCE, "DataGovernanceMongoDB",
                    "Failed to connect to PostgreSQL");
      return;
    }

    pqxx::work txn(conn);
    std::string createTableSQL =
        "CREATE TABLE IF NOT EXISTS metadata.data_governance_catalog_mongodb ("
        "id SERIAL PRIMARY KEY,"
        "server_name VARCHAR(200) NOT NULL,"
        "database_name VARCHAR(100) NOT NULL,"
        "collection_name VARCHAR(100) NOT NULL,"
        "index_name VARCHAR(200),"
        "index_keys TEXT,"
        "index_unique BOOLEAN DEFAULT false,"
        "index_sparse BOOLEAN DEFAULT false,"
        "index_type VARCHAR(50),"
        "document_count BIGINT,"
        "collection_size_mb DECIMAL(10,2),"
        "index_size_mb DECIMAL(10,2),"
        "total_size_mb DECIMAL(10,2),"
        "storage_size_mb DECIMAL(10,2),"
        "avg_object_size_bytes DECIMAL(10,2),"
        "index_count INTEGER,"
        "replica_set_name VARCHAR(100),"
        "is_sharded BOOLEAN DEFAULT false,"
        "shard_key VARCHAR(200),"
        "access_frequency VARCHAR(20),"
        "health_status VARCHAR(20),"
        "recommendation_summary TEXT,"
        "health_score DECIMAL(5,2),"
        "mongodb_version VARCHAR(50),"
        "storage_engine VARCHAR(50),"
        "snapshot_date TIMESTAMP DEFAULT NOW(),"
        "CONSTRAINT unique_mongodb_governance UNIQUE (server_name, "
        "database_name, collection_name, index_name)"
        ");";

    txn.exec(createTableSQL);

    std::string createIndexSQL =
        "CREATE INDEX IF NOT EXISTS "
        "idx_mongodb_governance_server_db_collection "
        "ON metadata.data_governance_catalog_mongodb (server_name, "
        "database_name, collection_name);"
        "CREATE INDEX IF NOT EXISTS idx_mongodb_governance_health_status "
        "ON metadata.data_governance_catalog_mongodb (health_status);";

    txn.exec(createIndexSQL);
    txn.commit();

    int successCount = 0;
    int errorCount = 0;

    for (const auto &data : governanceData_) {
      if (data.server_name.empty() || data.database_name.empty() ||
          data.collection_name.empty()) {
        Logger::warning(LogCategory::GOVERNANCE, "DataGovernanceMongoDB",
                        "Skipping record with missing required fields");
        errorCount++;
        continue;
      }

      try {
        pqxx::work insertTxn(conn);

        std::ostringstream insertQuery;
        insertQuery
            << "INSERT INTO metadata.data_governance_catalog_mongodb ("
            << "server_name, database_name, collection_name, "
            << "index_name, index_keys, index_unique, index_sparse, "
               "index_type, "
            << "document_count, collection_size_mb, index_size_mb, "
               "total_size_mb, "
            << "storage_size_mb, avg_object_size_bytes, index_count, "
            << "replica_set_name, is_sharded, shard_key, "
            << "access_frequency, health_status, recommendation_summary, "
            << "health_score, mongodb_version, storage_engine, snapshot_date"
            << ") VALUES (" << insertTxn.quote(data.server_name) << ", "
            << insertTxn.quote(data.database_name) << ", "
            << insertTxn.quote(data.collection_name) << ", "
            << (data.index_name.empty() ? "NULL"
                                        : insertTxn.quote(data.index_name))
            << ", "
            << (data.index_keys.empty() ? "NULL"
                                        : insertTxn.quote(data.index_keys))
            << ", " << (data.index_unique ? "true" : "false") << ", "
            << (data.index_sparse ? "true" : "false") << ", "
            << (data.index_type.empty() ? "NULL"
                                        : insertTxn.quote(data.index_type))
            << ", "
            << (data.document_count == 0 ? "NULL"
                                         : std::to_string(data.document_count))
            << ", "
            << (data.collection_size_mb == 0.0
                    ? "NULL"
                    : std::to_string(data.collection_size_mb))
            << ", "
            << (data.index_size_mb == 0.0 ? "NULL"
                                          : std::to_string(data.index_size_mb))
            << ", "
            << (data.total_size_mb == 0.0 ? "NULL"
                                          : std::to_string(data.total_size_mb))
            << ", "
            << (data.storage_size_mb == 0.0
                    ? "NULL"
                    : std::to_string(data.storage_size_mb))
            << ", "
            << (data.avg_object_size_bytes == 0.0
                    ? "NULL"
                    : std::to_string(data.avg_object_size_bytes))
            << ", "
            << (data.index_count == 0 ? "NULL"
                                      : std::to_string(data.index_count))
            << ", "
            << (data.replica_set_name.empty()
                    ? "NULL"
                    : insertTxn.quote(data.replica_set_name))
            << ", " << (data.is_sharded ? "true" : "false") << ", "
            << (data.shard_key.empty() ? "NULL"
                                       : insertTxn.quote(data.shard_key))
            << ", "
            << (data.access_frequency.empty()
                    ? "NULL"
                    : insertTxn.quote(data.access_frequency))
            << ", "
            << (data.health_status.empty()
                    ? "NULL"
                    : insertTxn.quote(data.health_status))
            << ", "
            << (data.recommendation_summary.empty()
                    ? "NULL"
                    : insertTxn.quote(data.recommendation_summary))
            << ", "
            << (data.health_score == 0.0 ? "NULL"
                                         : std::to_string(data.health_score))
            << ", "
            << (data.mongodb_version.empty()
                    ? "NULL"
                    : insertTxn.quote(data.mongodb_version))
            << ", "
            << (data.storage_engine.empty()
                    ? "NULL"
                    : insertTxn.quote(data.storage_engine))
            << ", "
            << "NOW()"
            << ") ON CONFLICT DO NOTHING;";

        insertTxn.exec(insertQuery.str());
        insertTxn.commit();
        successCount++;
      } catch (const std::exception &e) {
        Logger::error(LogCategory::GOVERNANCE, "DataGovernanceMongoDB",
                      "Error inserting record: " + std::string(e.what()));
        errorCount++;
        try {
          pqxx::work rollbackTxn(conn);
          rollbackTxn.abort();
        } catch (...) {
        }
      }
    }

    Logger::info(LogCategory::GOVERNANCE, "DataGovernanceMongoDB",
                 "Stored " + std::to_string(successCount) +
                     " governance records in PostgreSQL (errors: " +
                     std::to_string(errorCount) + ")");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "DataGovernanceMongoDB",
                  "Error storing governance data: " + std::string(e.what()));
  }
}

void DataGovernanceMongoDB::generateReport() {
  Logger::info(LogCategory::GOVERNANCE, "DataGovernanceMongoDB",
               "Generating governance report for " +
                   std::to_string(governanceData_.size()) + " records");

  int healthyCount = 0;
  int warningCount = 0;
  int criticalCount = 0;

  for (const auto &data : governanceData_) {
    if (data.health_status == "HEALTHY") {
      healthyCount++;
    } else if (data.health_status == "WARNING") {
      warningCount++;
    } else if (data.health_status == "CRITICAL") {
      criticalCount++;
    }
  }

  Logger::info(LogCategory::GOVERNANCE, "DataGovernanceMongoDB",
               "Report: Healthy=" + std::to_string(healthyCount) +
                   ", Warning=" + std::to_string(warningCount) +
                   ", Critical=" + std::to_string(criticalCount));
}
