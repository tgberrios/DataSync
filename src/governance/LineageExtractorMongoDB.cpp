#include "governance/LineageExtractorMongoDB.h"
#include "catalog/metadata_repository.h"
#include "core/database_config.h"
#include "core/logger.h"
#include "engines/mongodb_engine.h"
#include <algorithm>
#include <bson/bson.h>
#include <iomanip>
#include <mutex>
#include <pqxx/pqxx>
#include <set>
#include <sstream>

LineageExtractorMongoDB::LineageExtractorMongoDB(
    const std::string &connectionString)
    : connectionString_(connectionString), client_(nullptr) {
  serverName_ = extractServerName(connectionString);
  databaseName_ = extractDatabaseName(connectionString);
  connect(connectionString);
}

LineageExtractorMongoDB::~LineageExtractorMongoDB() { disconnect(); }

std::string LineageExtractorMongoDB::extractServerName(
    const std::string &connectionString) {
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

std::string LineageExtractorMongoDB::extractDatabaseName(
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

bool LineageExtractorMongoDB::connect(const std::string &connectionString) {
  try {
    if (connectionString.empty()) {
      Logger::error(LogCategory::GOVERNANCE, "LineageExtractorMongoDB",
                    "Empty connection string");
      return false;
    }

    static std::once_flag initFlag;
    std::call_once(initFlag, []() { mongoc_init(); });

    bson_error_t error;
    client_ = mongoc_client_new(connectionString.c_str());

    if (!client_) {
      Logger::error(LogCategory::GOVERNANCE, "LineageExtractorMongoDB",
                    "Failed to create MongoDB client");
      return false;
    }

    mongoc_client_set_appname(client_, "DataSync");

    size_t dbStart = connectionString.find_last_of('/');
    if (dbStart == std::string::npos) {
      Logger::error(LogCategory::GOVERNANCE, "LineageExtractorMongoDB",
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
      Logger::error(LogCategory::GOVERNANCE, "LineageExtractorMongoDB",
                    "Database name is empty");
      mongoc_client_destroy(client_);
      client_ = nullptr;
      return false;
    }

    bson_t *ping = BCON_NEW("ping", BCON_INT32(1));
    if (!ping) {
      Logger::error(LogCategory::GOVERNANCE, "LineageExtractorMongoDB",
                    "Failed to create ping BSON");
      mongoc_client_destroy(client_);
      client_ = nullptr;
      return false;
    }
    mongoc_database_t *db = mongoc_client_get_database(client_, dbName.c_str());
    if (!db) {
      bson_destroy(ping);
      Logger::error(LogCategory::GOVERNANCE, "LineageExtractorMongoDB",
                    "Failed to get database");
      mongoc_client_destroy(client_);
      client_ = nullptr;
      return false;
    }
    bool ret =
        mongoc_database_command_simple(db, ping, nullptr, nullptr, &error);
    bson_destroy(ping);
    mongoc_database_destroy(db);

    if (!ret) {
      Logger::error(LogCategory::GOVERNANCE, "LineageExtractorMongoDB",
                    "Failed to ping MongoDB: " + std::string(error.message));
      mongoc_client_destroy(client_);
      client_ = nullptr;
      return false;
    }

    databaseName_ = dbName;
    return true;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "LineageExtractorMongoDB",
                  "Failed to connect: " + std::string(e.what()));
  }
  return false;
}

void LineageExtractorMongoDB::disconnect() {
  if (client_) {
    mongoc_client_destroy(client_);
    client_ = nullptr;
  }
}

std::string
LineageExtractorMongoDB::generateEdgeKey(const MongoDBLineageEdge &edge) {
  auto escapeKeyComponent = [](const std::string &str) -> std::string {
    std::string escaped = str;
    size_t pos = 0;
    while ((pos = escaped.find('|', pos)) != std::string::npos) {
      escaped.replace(pos, 1, "||");
      pos += 2;
    }
    while ((pos = escaped.find('\n', pos)) != std::string::npos) {
      escaped.replace(pos, 1, "\\n");
      pos += 2;
    }
    while ((pos = escaped.find('\r', pos)) != std::string::npos) {
      escaped.replace(pos, 1, "\\r");
      pos += 2;
    }
    return escaped;
  };

  std::stringstream ss;
  ss << escapeKeyComponent(edge.server_name) << "|"
     << escapeKeyComponent(edge.database_name) << "|"
     << escapeKeyComponent(edge.source_collection) << "|"
     << escapeKeyComponent(edge.source_field) << "|"
     << escapeKeyComponent(edge.target_collection) << "|"
     << escapeKeyComponent(edge.target_field) << "|"
     << escapeKeyComponent(edge.relationship_type);
  return ss.str();
}

void LineageExtractorMongoDB::extractLineage() {
  Logger::info(LogCategory::GOVERNANCE, "LineageExtractorMongoDB",
               "Starting lineage extraction for " + serverName_ + "/" +
                   databaseName_);

  {
    std::lock_guard<std::mutex> lock(lineageEdgesMutex_);
    lineageEdges_.clear();
  }

  if (!client_) {
    Logger::error(LogCategory::GOVERNANCE, "LineageExtractorMongoDB",
                  "MongoDB client not connected");
    return;
  }

  try {
    extractCollectionDependencies();
    Logger::info(LogCategory::GOVERNANCE, "LineageExtractorMongoDB",
                 "After collection extraction: " + std::to_string([this]() {
                   std::lock_guard<std::mutex> lock(lineageEdgesMutex_);
                   return lineageEdges_.size();
                 }()) +
                     " edges");

    extractViewDependencies();
    Logger::info(LogCategory::GOVERNANCE, "LineageExtractorMongoDB",
                 "After view extraction: " + std::to_string([this]() {
                   std::lock_guard<std::mutex> lock(lineageEdgesMutex_);
                   return lineageEdges_.size();
                 }()) +
                     " edges");

    extractPipelineDependencies();
    Logger::info(LogCategory::GOVERNANCE, "LineageExtractorMongoDB",
                 "After pipeline extraction: " + std::to_string([this]() {
                   std::lock_guard<std::mutex> lock(lineageEdgesMutex_);
                   return lineageEdges_.size();
                 }()) +
                     " edges");

    extractDatalakeRelationships();
    Logger::info(LogCategory::GOVERNANCE, "LineageExtractorMongoDB",
                 "After datalake extraction: " + std::to_string([this]() {
                   std::lock_guard<std::mutex> lock(lineageEdgesMutex_);
                   return lineageEdges_.size();
                 }()) +
                     " edges");

    Logger::info(LogCategory::GOVERNANCE, "LineageExtractorMongoDB",
                 "Lineage extraction completed. Found " +
                     std::to_string(lineageEdges_.size()) +
                     " total dependencies");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "LineageExtractorMongoDB",
                  "Error extracting lineage: " + std::string(e.what()));
  }
}

void LineageExtractorMongoDB::extractCollectionDependencies() {
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

    std::set<std::string> collectionSet;
    for (size_t i = 0; collection_names[i]; i++) {
      collectionSet.insert(collection_names[i]);
    }

    for (size_t i = 0; collection_names[i]; i++) {
      std::string collectionName = collection_names[i];
      mongoc_collection_t *collection =
          mongoc_database_get_collection(database, collectionName.c_str());
      if (!collection) {
        continue;
      }

      bson_t *query = bson_new();
      if (!query) {
        mongoc_collection_destroy(collection);
        continue;
      }
      mongoc_cursor_t *cursor =
          mongoc_collection_find_with_opts(collection, query, nullptr, nullptr);
      if (!cursor) {
        bson_destroy(query);
        mongoc_collection_destroy(collection);
        continue;
      }
      const bson_t *doc;
      int sampleCount = 0;
      const int MAX_SAMPLES = 100;

      while (mongoc_cursor_next(cursor, &doc) && sampleCount < MAX_SAMPLES) {
        bson_iter_t iter;
        if (bson_iter_init(&iter, doc)) {
          while (bson_iter_next(&iter)) {
            const char *key = bson_iter_key(&iter);
            bson_type_t type = bson_iter_type(&iter);

            if (type == BSON_TYPE_OID) {
              const bson_oid_t *oid = bson_iter_oid(&iter);
              char oid_str[25];
              bson_oid_to_string(oid, oid_str);

              MongoDBLineageEdge edge;
              edge.server_name = serverName_;
              edge.database_name = databaseName_;
              edge.source_collection = collectionName;
              edge.source_field = key;
              edge.target_collection = "";
              edge.target_field = "";
              edge.relationship_type = "OBJECT_ID_REFERENCE";
              edge.definition_text =
                  "Field " + std::string(key) + " contains ObjectId reference";
              edge.dependency_level = 1;
              edge.discovery_method = "DOCUMENT_SAMPLING";
              edge.confidence_score = 0.7;
              edge.edge_key = generateEdgeKey(edge);

              bool exists = false;
              {
                std::lock_guard<std::mutex> lock(lineageEdgesMutex_);
                for (const auto &existing : lineageEdges_) {
                  if (existing.edge_key == edge.edge_key) {
                    exists = true;
                    break;
                  }
                }
                if (!exists) {
                  lineageEdges_.push_back(edge);
                }
              }
            } else if (type == BSON_TYPE_UTF8) {
              std::string value = bson_iter_utf8(&iter, nullptr);
              if (value.length() == 24) {
                bool isObjectId = true;
                for (char c : value) {
                  if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') ||
                        (c >= 'A' && c <= 'F'))) {
                    isObjectId = false;
                    break;
                  }
                }
                if (isObjectId) {
                  MongoDBLineageEdge edge;
                  edge.server_name = serverName_;
                  edge.database_name = databaseName_;
                  edge.source_collection = collectionName;
                  edge.source_field = key;
                  edge.target_collection = "";
                  edge.target_field = "";
                  edge.relationship_type = "STRING_OBJECTID_REFERENCE";
                  edge.definition_text = "Field " + std::string(key) +
                                         " contains ObjectId string reference";
                  edge.dependency_level = 1;
                  edge.discovery_method = "DOCUMENT_SAMPLING";
                  edge.confidence_score = 0.6;
                  edge.edge_key = generateEdgeKey(edge);

                  bool exists = false;
                  for (const auto &existing : lineageEdges_) {
                    if (existing.edge_key == edge.edge_key) {
                      exists = true;
                      break;
                    }
                  }
                  if (!exists) {
                    lineageEdges_.push_back(edge);
                  }
                }
              }
            }
          }
        }
        sampleCount++;
      }

      mongoc_cursor_destroy(cursor);
      bson_destroy(query);
      mongoc_collection_destroy(collection);
    }

    bson_strfreev(collection_names);
    mongoc_database_destroy(database);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "LineageExtractorMongoDB",
                  "Error extracting collection dependencies: " +
                      std::string(e.what()));
  }
}

void LineageExtractorMongoDB::extractViewDependencies() {
  if (!client_ || databaseName_.empty()) {
    return;
  }

  try {
    mongoc_database_t *database =
        mongoc_client_get_database(client_, databaseName_.c_str());
    if (!database) {
      return;
    }

    bson_t *command = BCON_NEW("listCollections", BCON_INT32(1));
    if (!command) {
      mongoc_database_destroy(database);
      return;
    }
    bson_t reply;
    bson_error_t error;

    bool success = mongoc_database_command_simple(database, command, nullptr,
                                                  &reply, &error);
    bson_destroy(command);
    if (success) {
      bson_iter_t iter;
      if (bson_iter_init(&iter, &reply)) {
        bson_iter_t cursor_iter;
        if (bson_iter_find_descendant(&iter, "cursor.firstBatch",
                                      &cursor_iter) &&
            BSON_ITER_HOLDS_ARRAY(&cursor_iter)) {
          const uint8_t *data;
          uint32_t len;
          bson_iter_array(&cursor_iter, &len, &data);
          bson_t *array = bson_new_from_data(data, len);
          if (!array) {
            bson_destroy(&reply);
            mongoc_database_destroy(database);
            return;
          }
          bson_iter_t array_iter;

          if (bson_iter_init(&array_iter, array)) {
            while (bson_iter_next(&array_iter)) {
              if (BSON_ITER_HOLDS_DOCUMENT(&array_iter)) {
                const uint8_t *doc_data;
                uint32_t doc_len;
                bson_iter_document(&array_iter, &doc_len, &doc_data);
                bson_t *view_doc = bson_new_from_data(doc_data, doc_len);
                if (!view_doc) {
                  continue;
                }
                bson_iter_t view_iter;

                if (bson_iter_init(&view_iter, view_doc)) {
                  std::string view_name;
                  std::string view_on;
                  bool is_view = false;

                  while (bson_iter_next(&view_iter)) {
                    const char *key = bson_iter_key(&view_iter);
                    if (strcmp(key, "name") == 0 &&
                        BSON_ITER_HOLDS_UTF8(&view_iter)) {
                      view_name = bson_iter_utf8(&view_iter, nullptr);
                    } else if (strcmp(key, "type") == 0 &&
                               BSON_ITER_HOLDS_UTF8(&view_iter)) {
                      std::string type = bson_iter_utf8(&view_iter, nullptr);
                      if (type == "view") {
                        is_view = true;
                      }
                    } else if (strcmp(key, "options") == 0 &&
                               BSON_ITER_HOLDS_DOCUMENT(&view_iter)) {
                      const uint8_t *options_data;
                      uint32_t options_len;
                      bson_iter_document(&view_iter, &options_len,
                                         &options_data);
                      bson_t *options =
                          bson_new_from_data(options_data, options_len);
                      if (!options) {
                        bson_destroy(view_doc);
                        continue;
                      }
                      bson_iter_t options_iter;

                      if (bson_iter_init(&options_iter, options)) {
                        while (bson_iter_next(&options_iter)) {
                          const char *opt_key = bson_iter_key(&options_iter);
                          if (strcmp(opt_key, "viewOn") == 0 &&
                              BSON_ITER_HOLDS_UTF8(&options_iter)) {
                            view_on = bson_iter_utf8(&options_iter, nullptr);
                          }
                        }
                      }
                      bson_destroy(options);
                    }
                  }

                  if (is_view && !view_name.empty() && !view_on.empty()) {
                    MongoDBLineageEdge edge;
                    edge.server_name = serverName_;
                    edge.database_name = databaseName_;
                    edge.source_collection = view_name;
                    edge.source_field = "";
                    edge.target_collection = view_on;
                    edge.target_field = "";
                    edge.relationship_type = "VIEW_DEPENDENCY";
                    edge.definition_text = "View " + view_name +
                                           " depends on collection " + view_on;
                    edge.dependency_level = 1;
                    edge.discovery_method = "VIEW_METADATA";
                    edge.confidence_score = 1.0;
                    edge.edge_key = generateEdgeKey(edge);

                    bool exists = false;
                    for (const auto &existing : lineageEdges_) {
                      if (existing.edge_key == edge.edge_key) {
                        exists = true;
                        break;
                      }
                    }
                    if (!exists) {
                      lineageEdges_.push_back(edge);
                    }
                  }
                }
                bson_destroy(view_doc);
              }
            }
          }
          bson_destroy(array);
        }
      }
      bson_destroy(&reply);
    }

    mongoc_database_destroy(database);
  } catch (const std::exception &e) {
    Logger::warning(LogCategory::GOVERNANCE, "LineageExtractorMongoDB",
                    "Error extracting view dependencies: " +
                        std::string(e.what()));
  }
}

void LineageExtractorMongoDB::extractPipelineDependencies() {
  Logger::info(LogCategory::GOVERNANCE, "LineageExtractorMongoDB",
               "Pipeline dependency extraction not yet implemented");
}

void LineageExtractorMongoDB::storeLineage() {
  std::vector<MongoDBLineageEdge> edgesCopy;
  {
    std::lock_guard<std::mutex> lock(lineageEdgesMutex_);
    if (lineageEdges_.empty()) {
      Logger::warning(LogCategory::GOVERNANCE, "LineageExtractorMongoDB",
                      "No lineage data to store");
      return;
    }
    edgesCopy = lineageEdges_;
  }

  try {
    pqxx::connection conn(DatabaseConfig::getPostgresConnectionString());
    if (!conn.is_open()) {
      Logger::error(LogCategory::GOVERNANCE, "LineageExtractorMongoDB",
                    "Failed to connect to PostgreSQL");
      return;
    }

    pqxx::work txn(conn);
    std::string createTableSQL =
        "CREATE TABLE IF NOT EXISTS metadata.mongo_lineage ("
        "id SERIAL PRIMARY KEY,"
        "edge_key VARCHAR(500) UNIQUE NOT NULL,"
        "server_name VARCHAR(200) NOT NULL,"
        "database_name VARCHAR(100) NOT NULL,"
        "source_collection VARCHAR(100) NOT NULL,"
        "source_field VARCHAR(100),"
        "target_collection VARCHAR(100),"
        "target_field VARCHAR(100),"
        "relationship_type VARCHAR(50) NOT NULL,"
        "definition_text TEXT,"
        "dependency_level INTEGER DEFAULT 1,"
        "discovery_method VARCHAR(50),"
        "confidence_score DECIMAL(3,2) DEFAULT 1.0,"
        "snapshot_date TIMESTAMP DEFAULT NOW()"
        ");";

    txn.exec(createTableSQL);

    std::string createIndexSQL =
        "CREATE INDEX IF NOT EXISTS idx_mongo_lineage_server_db "
        "ON metadata.mongo_lineage (server_name, database_name);"
        "CREATE INDEX IF NOT EXISTS idx_mongo_lineage_source "
        "ON metadata.mongo_lineage (source_collection);"
        "CREATE INDEX IF NOT EXISTS idx_mongo_lineage_target "
        "ON metadata.mongo_lineage (target_collection);";

    txn.exec(createIndexSQL);
    txn.commit();

    int successCount = 0;
    int errorCount = 0;

    for (const auto &edge : edgesCopy) {
      try {
        pqxx::work insertTxn(conn);

        std::ostringstream insertQuery;
        insertQuery
            << "INSERT INTO metadata.mongo_lineage ("
            << "edge_key, server_name, database_name, "
            << "object_name, object_type, column_name, "
            << "target_object_name, target_object_type, target_column_name, "
            << "relationship_type, definition_text, "
            << "discovery_method, discovered_by, confidence_score, "
            << "source_collection, source_field, target_collection, "
            << "target_field, dependency_level, snapshot_date"
            << ") VALUES (" << insertTxn.quote(edge.edge_key) << ", "
            << insertTxn.quote(edge.server_name) << ", "
            << insertTxn.quote(edge.database_name) << ", "
            << insertTxn.quote(edge.source_collection) << ", "
            << "'COLLECTION', "
            << (edge.source_field.empty() ? "NULL"
                                          : insertTxn.quote(edge.source_field))
            << ", "
            << (edge.target_collection.empty()
                    ? "NULL"
                    : insertTxn.quote(edge.target_collection))
            << ", "
            << (edge.target_collection.empty() ? "NULL" : "'COLLECTION'")
            << ", "
            << (edge.target_field.empty() ? "NULL"
                                          : insertTxn.quote(edge.target_field))
            << ", " << insertTxn.quote(edge.relationship_type) << ", "
            << (edge.definition_text.empty()
                    ? "NULL"
                    : insertTxn.quote(edge.definition_text))
            << ", "
            << (edge.discovery_method.empty()
                    ? "'AUTO'"
                    : insertTxn.quote(edge.discovery_method))
            << ", 'DataSync', " << std::to_string(edge.confidence_score) << ", "
            << insertTxn.quote(edge.source_collection) << ", "
            << (edge.source_field.empty() ? "NULL"
                                          : insertTxn.quote(edge.source_field))
            << ", "
            << (edge.target_collection.empty()
                    ? "NULL"
                    : insertTxn.quote(edge.target_collection))
            << ", "
            << (edge.target_field.empty() ? "NULL"
                                          : insertTxn.quote(edge.target_field))
            << ", " << std::to_string(edge.dependency_level) << ", "
            << "NOW()"
            << ") ON CONFLICT (edge_key) DO UPDATE SET "
            << "last_seen_at = NOW(), updated_at = NOW();";

        insertTxn.exec(insertQuery.str());
        insertTxn.commit();
        successCount++;
      } catch (const std::exception &e) {
        Logger::error(LogCategory::GOVERNANCE, "LineageExtractorMongoDB",
                      "Error inserting lineage edge: " + std::string(e.what()));
        errorCount++;
      }
    }

    Logger::info(LogCategory::GOVERNANCE, "LineageExtractorMongoDB",
                 "Stored " + std::to_string(successCount) +
                     " lineage edges in PostgreSQL (errors: " +
                     std::to_string(errorCount) + ")");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "LineageExtractorMongoDB",
                  "Error storing lineage data: " + std::string(e.what()));
  }
}

void LineageExtractorMongoDB::extractDatalakeRelationships() {
  try {
    std::string metadataConnStr = DatabaseConfig::getPostgresConnectionString();
    pqxx::connection metadataConn(metadataConnStr);
    if (!metadataConn.is_open()) {
      Logger::warning(LogCategory::GOVERNANCE, "LineageExtractorMongoDB",
                      "Failed to connect to metadata database for datalake relationships");
      return;
    }

    pqxx::work txn(metadataConn);
    std::string query = R"(
      SELECT DISTINCT
        schema_name,
        table_name,
        connection_string as source_connection_string
      FROM metadata.catalog
      WHERE db_engine = 'MongoDB'
        AND active = true
        AND connection_string = $1
    )";

    pqxx::params pqParams;
    pqParams.append(connectionString_);
    auto catalogResults = txn.exec(pqxx::zview(query), pqParams);
    txn.commit();

    for (const auto &catalogRow : catalogResults) {
      std::string sourceDatabase = catalogRow[0].as<std::string>();  // schema_name is database in MongoDB
      std::string sourceCollection = catalogRow[1].as<std::string>();  // table_name is collection in MongoDB
      std::string targetSchema = sourceDatabase;
      std::string targetTable = sourceCollection;

      MongoDBLineageEdge edge;
      edge.server_name = serverName_;
      edge.database_name = databaseName_;
      edge.source_collection = sourceCollection;
      edge.target_collection = targetTable;
      edge.relationship_type = "SYNCED_TO_DATALAKE";
      edge.definition_text = "Collection synced from MongoDB source to DataLake PostgreSQL";
      edge.dependency_level = 0;
      edge.discovery_method = "METADATA_CATALOG";
      edge.confidence_score = 1.0;
      edge.edge_key = generateEdgeKey(edge);

      {
        std::lock_guard<std::mutex> lock(lineageEdgesMutex_);
        lineageEdges_.push_back(edge);
      }

      Logger::debug(LogCategory::GOVERNANCE, "LineageExtractorMongoDB",
                    "Added datalake edge: " + sourceDatabase + "." + sourceCollection +
                        " -> DataLake." + targetSchema + "." + targetTable);
    }

    Logger::info(LogCategory::GOVERNANCE, "LineageExtractorMongoDB",
                 "Extracted " + std::to_string(catalogResults.size()) +
                     " datalake relationships from catalog");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "LineageExtractorMongoDB",
                  "Error extracting datalake relationships: " +
                      std::string(e.what()));
  }
}
