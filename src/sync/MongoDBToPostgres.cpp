#include "sync/MongoDBToPostgres.h"
#include "core/database_config.h"
#include "core/logger.h"
#include "engines/database_engine.h"
#include <algorithm>
#include <ctime>
#include <iomanip>
#include <pqxx/pqxx>
#include <set>
#include <sstream>

std::unordered_map<std::string, std::string> MongoDBToPostgres::dataTypeMap = {
    {"string", "TEXT"},    {"int32", "INTEGER"},      {"int64", "BIGINT"},
    {"double", "NUMERIC"}, {"decimal128", "NUMERIC"}, {"bool", "BOOLEAN"},
    {"date", "TIMESTAMP"}, {"objectId", "TEXT"},      {"array", "JSONB"},
    {"object", "JSONB"},   {"binary", "BYTEA"},       {"null", "TEXT"}};

std::string
MongoDBToPostgres::cleanValueForPostgres(const std::string &value,
                                         const std::string &columnType) {
  if (value.empty() || value == "NULL" || value == "null") {
    std::string upperType = columnType;
    std::transform(upperType.begin(), upperType.end(), upperType.begin(),
                   ::toupper);

    if (upperType.find("INTEGER") != std::string::npos ||
        upperType.find("BIGINT") != std::string::npos) {
      return "0";
    } else if (upperType.find("NUMERIC") != std::string::npos ||
               upperType.find("REAL") != std::string::npos) {
      return "0.0";
    } else if (upperType.find("BOOLEAN") != std::string::npos) {
      return "false";
    } else {
      return "NULL";
    }
  }

  std::string cleanValue = value;
  cleanValue.erase(std::remove_if(cleanValue.begin(), cleanValue.end(),
                                  [](unsigned char c) {
                                    return c < 32 && c != 9 && c != 10 &&
                                           c != 13;
                                  }),
                   cleanValue.end());

  return cleanValue;
}

std::chrono::system_clock::time_point
MongoDBToPostgres::parseTimestamp(const std::string &timestamp) {
  if (timestamp.empty()) {
    return std::chrono::system_clock::time_point::min();
  }

  std::tm tm = {};
  std::istringstream ss(timestamp);
  ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");

  if (ss.fail()) {
    Logger::warning(LogCategory::TRANSFER, "parseTimestamp",
                    "Failed to parse timestamp: " + timestamp);
    return std::chrono::system_clock::time_point::min();
  }

  return std::chrono::system_clock::from_time_t(std::mktime(&tm));
}

bool MongoDBToPostgres::shouldSyncCollection(const TableInfo &tableInfo) {
  if (tableInfo.status != "FULL_LOAD" && tableInfo.status != "full_load") {
    return false;
  }

  if (tableInfo.last_sync_time.empty()) {
    return true;
  }

  auto lastSync = parseTimestamp(tableInfo.last_sync_time);
  if (lastSync == std::chrono::system_clock::time_point::min()) {
    return true;
  }

  auto now = std::chrono::system_clock::now();
  auto hoursSinceLastSync =
      std::chrono::duration_cast<std::chrono::hours>(now - lastSync).count();

  return hoursSinceLastSync >= 24;
}

void MongoDBToPostgres::updateLastSyncTime(pqxx::connection &pgConn,
                                           const std::string &schema_name,
                                           const std::string &table_name) {
  try {
    std::lock_guard<std::mutex> lock(metadataUpdateMutex);
    pqxx::work txn(pgConn);
    txn.exec("UPDATE metadata.catalog SET last_sync_time = NOW() WHERE "
             "schema_name = '" +
             escapeSQL(schema_name) + "' AND table_name = '" +
             escapeSQL(table_name) + "' AND db_engine = 'MongoDB'");
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "updateLastSyncTime",
                  "Error updating last_sync_time: " + std::string(e.what()));
  }
}

std::vector<std::string>
MongoDBToPostgres::discoverCollectionFields(const std::string &connectionString,
                                            const std::string &database,
                                            const std::string &collection) {
  std::vector<std::string> fields;
  fields.push_back("_id");

  try {
    MongoDBEngine engine(connectionString);
    if (!engine.isValid()) {
      Logger::error(LogCategory::TRANSFER, "discoverCollectionFields",
                    "Failed to connect to MongoDB");
      return fields;
    }

    mongoc_collection_t *coll = mongoc_client_get_collection(
        engine.getClient(), database.c_str(), collection.c_str());
    if (!coll) {
      return fields;
    }

    bson_t *query = bson_new();
    mongoc_cursor_t *cursor =
        mongoc_collection_find_with_opts(coll, query, nullptr, nullptr);

    const bson_t *doc;
    std::set<std::string> fieldSet;
    fieldSet.insert("_id");

    int sampleCount = 0;
    const int MAX_SAMPLES = 100;

    while (mongoc_cursor_next(cursor, &doc) && sampleCount < MAX_SAMPLES) {
      bson_iter_t iter;
      if (bson_iter_init(&iter, doc)) {
        while (bson_iter_next(&iter)) {
          const char *key = bson_iter_key(&iter);
          if (key && strcmp(key, "_id") != 0) {
            bson_type_t type = bson_iter_type(&iter);
            if (type == BSON_TYPE_UTF8 || type == BSON_TYPE_INT32 ||
                type == BSON_TYPE_INT64 || type == BSON_TYPE_DOUBLE ||
                type == BSON_TYPE_BOOL || type == BSON_TYPE_DATE_TIME) {
              fieldSet.insert(key);
            }
          }
        }
      }
      sampleCount++;
    }

    mongoc_cursor_destroy(cursor);
    bson_destroy(query);
    mongoc_collection_destroy(coll);

    fields.assign(fieldSet.begin(), fieldSet.end());
    fields.push_back("_document");

  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "discoverCollectionFields",
                  "Error discovering fields: " + std::string(e.what()));
  }

  return fields;
}

std::string MongoDBToPostgres::inferPostgreSQLType(const bson_value_t *value) {
  if (!value) {
    return "TEXT";
  }

  switch (value->value_type) {
  case BSON_TYPE_UTF8:
    return "TEXT";
  case BSON_TYPE_INT32:
    return "INTEGER";
  case BSON_TYPE_INT64:
    return "BIGINT";
  case BSON_TYPE_DOUBLE:
    return "NUMERIC";
  case BSON_TYPE_DECIMAL128:
    return "NUMERIC";
  case BSON_TYPE_BOOL:
    return "BOOLEAN";
  case BSON_TYPE_DATE_TIME:
    return "TIMESTAMP";
  case BSON_TYPE_OID:
    return "TEXT";
  case BSON_TYPE_ARRAY:
    return "JSONB";
  case BSON_TYPE_DOCUMENT:
    return "JSONB";
  case BSON_TYPE_BINARY:
    return "BYTEA";
  default:
    return "TEXT";
  }
}

void MongoDBToPostgres::createPostgreSQLTable(
    const TableInfo &tableInfo, const std::vector<std::string> &fields,
    const std::vector<std::string> &fieldTypes) {
  try {
    pqxx::connection conn(DatabaseConfig::getPostgresConnectionString());
    pqxx::work txn(conn);

    std::string schemaName = tableInfo.schema_name;
    std::transform(schemaName.begin(), schemaName.end(), schemaName.begin(),
                   ::tolower);

    std::string tableName = tableInfo.table_name;
    std::transform(tableName.begin(), tableName.end(), tableName.begin(),
                   ::tolower);

    txn.exec("CREATE SCHEMA IF NOT EXISTS " + txn.quote_name(schemaName));

    std::ostringstream createTable;
    createTable << "CREATE TABLE IF NOT EXISTS " << txn.quote_name(schemaName)
                << "." << txn.quote_name(tableName) << " (";

    for (size_t i = 0; i < fields.size(); i++) {
      if (i > 0)
        createTable << ", ";
      createTable << txn.quote_name(fields[i]) << " " << fieldTypes[i];
      if (fields[i] == "_id") {
        createTable << " PRIMARY KEY";
      }
    }

    createTable << ", _created_at TIMESTAMP DEFAULT NOW()";
    createTable << ", _updated_at TIMESTAMP DEFAULT NOW()";
    createTable << ")";

    txn.exec(createTable.str());
    txn.commit();

    Logger::info(LogCategory::TRANSFER, "createPostgreSQLTable",
                 "Created table " + schemaName + "." + tableName);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "createPostgreSQLTable",
                  "Error creating table: " + std::string(e.what()));
  }
}

void MongoDBToPostgres::convertBSONToPostgresRow(
    bson_t *doc, const std::vector<std::string> &fields,
    std::vector<std::string> &row,
    std::unordered_map<std::string, int> &fieldIndexMap) {
  row.clear();
  row.resize(fields.size(), "");

  bson_iter_t iter;
  if (!bson_iter_init(&iter, doc)) {
    return;
  }

  std::string documentJson;
  char *jsonStr = bson_as_canonical_extended_json(doc, nullptr);
  if (jsonStr) {
    documentJson = jsonStr;
    bson_free(jsonStr);
  }

  while (bson_iter_next(&iter)) {
    const char *key = bson_iter_key(&iter);
    auto it = fieldIndexMap.find(key);
    if (it != fieldIndexMap.end()) {
      int index = it->second;
      const bson_value_t *value = bson_iter_value(&iter);

      if (!value) {
        row[index] = "";
        continue;
      }

      switch (value->value_type) {
      case BSON_TYPE_UTF8:
        row[index] =
            std::string(value->value.v_utf8.str, value->value.v_utf8.len);
        break;
      case BSON_TYPE_INT32:
        row[index] = std::to_string(value->value.v_int32);
        break;
      case BSON_TYPE_INT64:
        row[index] = std::to_string(value->value.v_int64);
        break;
      case BSON_TYPE_DOUBLE:
        row[index] = std::to_string(value->value.v_double);
        break;
      case BSON_TYPE_BOOL:
        row[index] = value->value.v_bool ? "true" : "false";
        break;
      case BSON_TYPE_OID: {
        char oid_str[25];
        bson_oid_to_string(&value->value.v_oid, oid_str);
        row[index] = oid_str;
        break;
      }
      case BSON_TYPE_DATE_TIME: {
        int64_t millis = value->value.v_datetime;
        std::time_t time = millis / 1000;
        std::tm *tm = std::gmtime(&time);
        std::ostringstream oss;
        oss << std::put_time(tm, "%Y-%m-%d %H:%M:%S");
        row[index] = oss.str();
        break;
      }
      default:
        row[index] = "";
        break;
      }
    }
  }

  auto docIt = fieldIndexMap.find("_document");
  if (docIt != fieldIndexMap.end()) {
    row[docIt->second] = documentJson;
  }
}

std::vector<std::vector<std::string>>
MongoDBToPostgres::fetchCollectionData(const TableInfo &tableInfo) {
  std::vector<std::vector<std::string>> results;

  try {
    MongoDBEngine engine(tableInfo.connection_string);
    if (!engine.isValid()) {
      Logger::error(LogCategory::TRANSFER, "fetchCollectionData",
                    "Failed to connect to MongoDB");
      return results;
    }

    mongoc_collection_t *coll = mongoc_client_get_collection(
        engine.getClient(), tableInfo.schema_name.c_str(),
        tableInfo.table_name.c_str());
    if (!coll) {
      Logger::error(LogCategory::TRANSFER, "fetchCollectionData",
                    "Failed to get collection");
      return results;
    }

    std::vector<std::string> fields =
        discoverCollectionFields(tableInfo.connection_string,
                                 tableInfo.schema_name, tableInfo.table_name);

    std::unordered_map<std::string, int> fieldIndexMap;
    for (size_t i = 0; i < fields.size(); i++) {
      fieldIndexMap[fields[i]] = i;
    }

    bson_t *query = bson_new();
    mongoc_cursor_t *cursor =
        mongoc_collection_find_with_opts(coll, query, nullptr, nullptr);

    const bson_t *doc;
    int count = 0;
    while (mongoc_cursor_next(cursor, &doc)) {
      std::vector<std::string> row;
      bson_t *docCopy = bson_copy(doc);
      convertBSONToPostgresRow(docCopy, fields, row, fieldIndexMap);
      bson_destroy(docCopy);
      results.push_back(row);
      count++;

      if (count % 10000 == 0) {
        Logger::info(LogCategory::TRANSFER, "fetchCollectionData",
                     "Fetched " + std::to_string(count) + " documents");
      }
    }

    mongoc_cursor_destroy(cursor);
    bson_destroy(query);
    mongoc_collection_destroy(coll);

    Logger::info(LogCategory::TRANSFER, "fetchCollectionData",
                 "Fetched " + std::to_string(results.size()) +
                     " total documents");

  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "fetchCollectionData",
                  "Error fetching collection data: " + std::string(e.what()));
  }

  return results;
}

void MongoDBToPostgres::truncateAndLoadCollection(const TableInfo &tableInfo) {
  try {
    pqxx::connection conn(DatabaseConfig::getPostgresConnectionString());
    pqxx::work txn(conn);

    std::string schemaName = tableInfo.schema_name;
    std::transform(schemaName.begin(), schemaName.end(), schemaName.begin(),
                   ::tolower);

    std::string tableName = tableInfo.table_name;
    std::transform(tableName.begin(), tableName.end(), tableName.begin(),
                   ::tolower);

    std::string fullTableName =
        txn.quote_name(schemaName) + "." + txn.quote_name(tableName);

    Logger::info(LogCategory::TRANSFER, "truncateAndLoadCollection",
                 "TRUNCATE and loading " + fullTableName);

    try {
      txn.exec("TRUNCATE TABLE " + fullTableName);
      txn.commit();
      Logger::info(LogCategory::TRANSFER, "truncateAndLoadCollection",
                   "TRUNCATE completed for " + fullTableName);
    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER, "truncateAndLoadCollection",
                    "Error during TRUNCATE: " + std::string(e.what()));
      throw;
    }

    std::vector<std::vector<std::string>> data = fetchCollectionData(tableInfo);

    if (data.empty()) {
      Logger::warning(LogCategory::TRANSFER, "truncateAndLoadCollection",
                      "No data to insert for " + fullTableName);
      updateLastSyncTime(conn, tableInfo.schema_name, tableInfo.table_name);
      return;
    }

    std::vector<std::string> fields =
        discoverCollectionFields(tableInfo.connection_string,
                                 tableInfo.schema_name, tableInfo.table_name);

    std::vector<std::string> fieldTypes;
    for (const auto &field : fields) {
      if (field == "_id") {
        fieldTypes.push_back("TEXT");
      } else if (field == "_document") {
        fieldTypes.push_back("JSONB");
      } else {
        fieldTypes.push_back("TEXT");
      }
    }

    pqxx::work insertTxn(conn);
    const size_t BATCH_SIZE = 10000;
    size_t inserted = 0;

    for (size_t i = 0; i < data.size(); i += BATCH_SIZE) {
      size_t batchEnd = std::min(i + BATCH_SIZE, data.size());
      std::ostringstream insertQuery;
      insertQuery << "INSERT INTO " << fullTableName << " (";

      for (size_t j = 0; j < fields.size(); j++) {
        if (j > 0)
          insertQuery << ", ";
        insertQuery << insertTxn.quote_name(fields[j]);
      }
      insertQuery << ") VALUES ";

      for (size_t j = i; j < batchEnd; j++) {
        if (j > i)
          insertQuery << ", ";
        insertQuery << "(";
        for (size_t k = 0; k < fields.size(); k++) {
          if (k > 0)
            insertQuery << ", ";
          if (k < data[j].size()) {
            if (fields[k] == "_document") {
              insertQuery << insertTxn.quote(data[j][k]) << "::jsonb";
            } else {
              insertQuery << insertTxn.quote(
                  cleanValueForPostgres(data[j][k], fieldTypes[k]));
            }
          } else {
            insertQuery << "NULL";
          }
        }
        insertQuery << ")";
      }

      insertTxn.exec(insertQuery.str());
      inserted += (batchEnd - i);

      if (inserted % 10000 == 0) {
        Logger::info(LogCategory::TRANSFER, "truncateAndLoadCollection",
                     "Inserted " + std::to_string(inserted) + " rows");
      }
    }

    insertTxn.commit();
    updateLastSyncTime(conn, tableInfo.schema_name, tableInfo.table_name);

    Logger::info(LogCategory::TRANSFER, "truncateAndLoadCollection",
                 "Completed loading " + std::to_string(inserted) +
                     " rows into " + fullTableName);

  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "truncateAndLoadCollection",
                  "Error truncating and loading collection: " +
                      std::string(e.what()));
  }
}

void MongoDBToPostgres::transferDataMongoDBToPostgresParallel() {
  try {
    pqxx::connection conn(DatabaseConfig::getPostgresConnectionString());
    pqxx::work txn(conn);

    auto result = txn.exec("SELECT schema_name, table_name, connection_string, "
                           "status, last_sync_time "
                           "FROM metadata.catalog "
                           "WHERE db_engine = 'MongoDB' AND active = true");

    txn.commit();

    std::vector<TableInfo> collectionsToSync;

    for (const auto &row : result) {
      TableInfo tableInfo;
      tableInfo.schema_name = row[0].as<std::string>();
      tableInfo.table_name = row[1].as<std::string>();
      tableInfo.connection_string = row[2].as<std::string>();
      tableInfo.status = row[3].as<std::string>();
      tableInfo.last_sync_time =
          row[4].is_null() ? "" : row[4].as<std::string>();

      if (shouldSyncCollection(tableInfo)) {
        collectionsToSync.push_back(tableInfo);
      }
    }

    Logger::info(LogCategory::TRANSFER, "transferDataMongoDBToPostgresParallel",
                 "Found " + std::to_string(collectionsToSync.size()) +
                     " collections to sync out of " + std::to_string(result.size()) + " total MongoDB collections");

    for (const auto &tableInfo : collectionsToSync) {
      try {
        truncateAndLoadCollection(tableInfo);
      } catch (const std::exception &e) {
        Logger::error(LogCategory::TRANSFER,
                      "transferDataMongoDBToPostgresParallel",
                      "Error syncing " + tableInfo.schema_name + "." +
                          tableInfo.table_name + ": " + std::string(e.what()));
      }
    }

  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER,
                  "transferDataMongoDBToPostgresParallel",
                  "Error in transfer: " + std::string(e.what()));
  }
}

void MongoDBToPostgres::setupTableTargetMongoDBToPostgres() {
  try {
    pqxx::connection conn(DatabaseConfig::getPostgresConnectionString());
    pqxx::work txn(conn);

    auto result = txn.exec("SELECT schema_name, table_name, connection_string "
                           "FROM metadata.catalog "
                           "WHERE db_engine = 'MongoDB' AND active = true");

    txn.commit();

    for (const auto &row : result) {
      std::string schemaName = row[0].as<std::string>();
      std::string tableName = row[1].as<std::string>();
      std::string connectionString = row[2].as<std::string>();

      std::vector<std::string> fields =
          discoverCollectionFields(connectionString, schemaName, tableName);

      std::vector<std::string> fieldTypes;
      for (const auto &field : fields) {
        if (field == "_id") {
          fieldTypes.push_back("TEXT");
        } else if (field == "_document") {
          fieldTypes.push_back("JSONB");
        } else {
          fieldTypes.push_back("TEXT");
        }
      }

      TableInfo tableInfo;
      tableInfo.schema_name = schemaName;
      tableInfo.table_name = tableName;
      tableInfo.connection_string = connectionString;

      createPostgreSQLTable(tableInfo, fields, fieldTypes);
    }

  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "setupTableTargetMongoDBToPostgres",
                  "Error setting up tables: " + std::string(e.what()));
  }
}
