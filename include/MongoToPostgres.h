#ifndef MONGOTOPOSTGRES_H
#define MONGOTOPOSTGRES_H

#include "Config.h"
#include "logger.h"
#include <algorithm>
#include <bson/bson.h>
#include <json/json.h>
#include <mongoc/mongoc.h>
#include <pqxx/pqxx>
#include <string>
#include <vector>

class MongoToPostgres {
public:
  MongoToPostgres() = default;
  ~MongoToPostgres() = default;

  void setupTableTargetMongoToPostgres() {
    try {
      Logger::info("setupTableTargetMongoToPostgres",
                   "Starting MongoDB target table setup");
      pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());

      pqxx::work txn(pgConn);
      auto results = txn.exec("SELECT schema_name, table_name, "
                              "connection_string FROM metadata.catalog "
                              "WHERE db_engine='MongoDB' AND active=true;");

      for (const auto &row : results) {
        if (row.size() < 3)
          continue;

        std::string schemaName = row[0].as<std::string>();
        std::string tableName = row[1].as<std::string>();
        std::string mongoConnStr = row[2].as<std::string>();

        Logger::debug("setupTableTargetMongoToPostgres",
                      "Setting up table: " + schemaName + "." + tableName);

        try {
          auto mongoConn = connectMongoDB(mongoConnStr);
          if (!mongoConn) {
            Logger::error("setupTableTargetMongoToPostgres",
                          "Failed to connect to MongoDB");
            continue;
          }

          std::string cleanSchemaName = cleanSchemaNameForPostgres(schemaName);
          std::string lowerSchemaName = toLowerCase(cleanSchemaName);
          createSchemaIfNotExists(txn, lowerSchemaName);

          std::string createTableQuery = buildCreateTableQuery(
              *mongoConn, schemaName, tableName, lowerSchemaName);
          if (!createTableQuery.empty()) {
            txn.exec(createTableQuery);
            Logger::info("setupTableTargetMongoToPostgres",
                         "Created target table: " + lowerSchemaName + "." +
                             tableName);
          }

        } catch (const std::exception &e) {
          Logger::error("setupTableTargetMongoToPostgres",
                        "Error setting up table " + schemaName + "." +
                            tableName + ": " + e.what());
        }
      }

      txn.commit();
      Logger::info("setupTableTargetMongoToPostgres",
                   "Target table setup completed");
    } catch (const std::exception &e) {
      Logger::error("setupTableTargetMongoToPostgres",
                    "Error in setupTableTargetMongoToPostgres: " +
                        std::string(e.what()));
    }
  }

  void transferDataMongoToPostgres() {
    try {
      // Logger::debug("transferDataMongoToPostgres",
      //               "Starting MongoDB to PostgreSQL transfer");
      pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());

      {
        pqxx::work txn(pgConn);
        auto results =
            txn.exec("SELECT schema_name, table_name, connection_string, "
                     "last_offset, status FROM metadata.catalog "
                     "WHERE db_engine='MongoDB' AND active=true AND status != "
                     "'NO_DATA';");

        for (const auto &row : results) {
          if (row.size() < 5)
            continue;

          std::string schemaName = row[0].as<std::string>();
          std::string tableName = row[1].as<std::string>();
          std::string mongoConnStr = row[2].as<std::string>();
          std::string lastOffset = row[3].as<std::string>();
          std::string status = row[4].as<std::string>();

          Logger::debug("transferDataMongoToPostgres",
                        "Processing table: " + schemaName + "." + tableName +
                            " (status: " + status + ")");

          try {
            processTable(pgConn, schemaName, tableName, mongoConnStr,
                         lastOffset, status);
          } catch (const std::exception &e) {
            Logger::error("transferDataMongoToPostgres",
                          "Error processing table " + schemaName + "." +
                              tableName + ": " + e.what());
            updateStatus(pgConn, schemaName, tableName, "ERROR", 0);
          }
        }

        txn.commit();
      }
    } catch (const std::exception &e) {
      Logger::error("transferDataMongoToPostgres",
                    "Error in transferDataMongoToPostgres: " +
                        std::string(e.what()));
    }
  }

private:
  std::unique_ptr<mongoc_client_t, void (*)(mongoc_client_t *)>
  connectMongoDB(const std::string &connStr) {
    mongoc_client_t *client = mongoc_client_new(connStr.c_str());
    if (!client) {
      Logger::error("connectMongoDB", "Failed to create MongoDB client");
      return std::unique_ptr<mongoc_client_t, void (*)(mongoc_client_t *)>(
          nullptr, mongoc_client_destroy);
    }

    // Test connection
    bson_t *ping = BCON_NEW("ping", BCON_INT32(1));
    bson_t reply;
    bson_error_t error;

    bool ret = mongoc_client_command_simple(client, "admin", ping, NULL, &reply,
                                            &error);
    bson_destroy(ping);
    bson_destroy(&reply);

    if (!ret) {
      Logger::error("connectMongoDB",
                    "Failed to ping MongoDB: " + std::string(error.message));
      mongoc_client_destroy(client);
      return std::unique_ptr<mongoc_client_t, void (*)(mongoc_client_t *)>(
          nullptr, mongoc_client_destroy);
    }

    Logger::debug("connectMongoDB",
                  "MongoDB connection established successfully");
    return std::unique_ptr<mongoc_client_t, void (*)(mongoc_client_t *)>(
        client, mongoc_client_destroy);
  }

  std::string toLowerCase(const std::string &str) {
    std::string result = str;
    std::transform(result.begin(), result.end(), result.begin(), ::tolower);
    return result;
  }

  std::string cleanSchemaNameForPostgres(const std::string &schemaName) {
    std::string cleaned = schemaName;
    // Remove semicolons and other problematic characters
    cleaned.erase(std::remove(cleaned.begin(), cleaned.end(), ';'),
                  cleaned.end());
    cleaned.erase(std::remove(cleaned.begin(), cleaned.end(), '.'),
                  cleaned.end());
    cleaned.erase(std::remove(cleaned.begin(), cleaned.end(), '-'),
                  cleaned.end());
    cleaned.erase(std::remove(cleaned.begin(), cleaned.end(), ' '),
                  cleaned.end());

    // If empty after cleaning, use default
    if (cleaned.empty()) {
      cleaned = "default_schema";
    }

    return cleaned;
  }

  void createSchemaIfNotExists(pqxx::work &txn, const std::string &schemaName) {
    txn.exec("CREATE SCHEMA IF NOT EXISTS \"" + schemaName + "\";");
  }

  std::string buildCreateTableQuery(mongoc_client_t &mongoClient,
                                    const std::string &dbName,
                                    const std::string &collectionName,
                                    const std::string &targetSchema) {
    try {
      mongoc_collection_t *collection = mongoc_client_get_collection(
          &mongoClient, dbName.c_str(), collectionName.c_str());

      // Get sample document to determine schema
      bson_t *query = bson_new();
      bson_t *opts = BCON_NEW("limit", BCON_INT32(1));
      mongoc_cursor_t *cursor =
          mongoc_collection_find_with_opts(collection, query, opts, NULL);

      const bson_t *doc;
      if (mongoc_cursor_next(cursor, &doc)) {
        std::string createQuery =
            buildCreateTableFromDocument(doc, targetSchema, collectionName);
        mongoc_cursor_destroy(cursor);
        bson_destroy(query);
        bson_destroy(opts);
        mongoc_collection_destroy(collection);
        return createQuery;
      }

      mongoc_cursor_destroy(cursor);
      bson_destroy(query);
      bson_destroy(opts);
      mongoc_collection_destroy(collection);

      // If no documents, create basic table with _id
      return "CREATE TABLE IF NOT EXISTS \"" + targetSchema + "\".\"" +
             collectionName + "\" (_id VARCHAR(24) PRIMARY KEY, data JSONB);";

    } catch (const std::exception &e) {
      Logger::error("buildCreateTableQuery",
                    "Error building create table query: " +
                        std::string(e.what()));
      return "";
    }
  }

  std::string buildCreateTableFromDocument(const bson_t *doc,
                                           const std::string &targetSchema,
                                           const std::string &tableName) {
    std::string createQuery = "CREATE TABLE IF NOT EXISTS \"" + targetSchema +
                              "\".\"" + tableName + "\" (";
    std::vector<std::string> columns;

    // Always include _id as primary key
    columns.push_back("_id VARCHAR(24) PRIMARY KEY");

    bson_iter_t iter;
    if (bson_iter_init(&iter, doc)) {
      while (bson_iter_next(&iter)) {
        const char *key = bson_iter_key(&iter);
        if (strcmp(key, "_id") == 0)
          continue; // Already added

        std::string columnDef = "\"" + std::string(key) + "\" ";

        switch (bson_iter_type(&iter)) {
        case BSON_TYPE_UTF8:
          columnDef += "TEXT";
          break;
        case BSON_TYPE_INT32:
          columnDef += "INTEGER";
          break;
        case BSON_TYPE_INT64:
          columnDef += "BIGINT";
          break;
        case BSON_TYPE_DOUBLE:
          columnDef += "DOUBLE PRECISION";
          break;
        case BSON_TYPE_BOOL:
          columnDef += "BOOLEAN";
          break;
        case BSON_TYPE_DATE_TIME:
          columnDef += "TIMESTAMP";
          break;
        case BSON_TYPE_OID:
          columnDef += "VARCHAR(24)";
          break;
        case BSON_TYPE_DOCUMENT:
        case BSON_TYPE_ARRAY:
        default:
          columnDef += "JSONB";
          break;
        }

        // Siempre permitir NULL en todas las columnas
        columnDef += "";

        columns.push_back(columnDef);
      }
    }

    createQuery += columns[0];
    for (size_t i = 1; i < columns.size(); ++i) {
      createQuery += ", " + columns[i];
    }
    createQuery += ");";

    return createQuery;
  }

  void processTable(pqxx::connection &pgConn, const std::string &schemaName,
                    const std::string &tableName,
                    const std::string &mongoConnStr,
                    const std::string &lastOffset, const std::string &status) {

    if (status == "RESET") {
      Logger::info("processTable",
                   "Processing RESET table: " + schemaName + "." + tableName);
      {
        pqxx::work txn(pgConn);
        std::string cleanSchemaName = cleanSchemaNameForPostgres(schemaName);
        std::string lowerSchemaName = toLowerCase(cleanSchemaName);
        txn.exec("TRUNCATE TABLE \"" + lowerSchemaName + "\".\"" + tableName +
                 "\" CASCADE;");
        txn.exec("UPDATE metadata.catalog SET last_offset='0' WHERE "
                 "schema_name='" +
                 escapeSQL(schemaName) + "' AND table_name='" +
                 escapeSQL(tableName) + "';");
        txn.commit();
      }
      updateStatus(pgConn, schemaName, tableName, "FULL_LOAD", 0);
      Logger::info("processTable",
                   "Table " + schemaName + "." + tableName +
                       " reset completed, status changed to FULL_LOAD");
      return;
    }

    if (status == "FULL_LOAD") {
      Logger::info("processTable", "Processing FULL_LOAD table: " + schemaName +
                                       "." + tableName);

      pqxx::work txn(pgConn);
      auto offsetCheck = txn.exec(
          "SELECT last_offset FROM metadata.catalog WHERE schema_name='" +
          escapeSQL(schemaName) + "' AND table_name='" + escapeSQL(tableName) +
          "';");

      bool shouldTruncate = true;
      if (!offsetCheck.empty() && !offsetCheck[0][0].is_null()) {
        std::string currentOffset = offsetCheck[0][0].as<std::string>();
        if (currentOffset != "0" && !currentOffset.empty()) {
          shouldTruncate = false;
        }
      }

      if (shouldTruncate) {
        std::string cleanSchemaName = cleanSchemaNameForPostgres(schemaName);
        Logger::info("processTable",
                     "Truncating table: " + toLowerCase(cleanSchemaName) + "." +
                         tableName);
        txn.exec("TRUNCATE TABLE \"" + toLowerCase(cleanSchemaName) + "\".\"" +
                 tableName + "\" CASCADE;");
        Logger::debug("processTable", "Table truncated successfully");
      }
      txn.commit();
    }

    auto mongoClient = connectMongoDB(mongoConnStr);
    if (!mongoClient) {
      updateStatus(pgConn, schemaName, tableName, "ERROR", 0);
      return;
    }

    int sourceCount = getSourceCount(*mongoClient, schemaName, tableName);
    int targetCount = getTargetCount(pgConn, schemaName, tableName);

    Logger::debug("processTable",
                  "Table " + schemaName + "." + tableName +
                      " - Source: " + std::to_string(sourceCount) +
                      ", Target: " + std::to_string(targetCount));

    if (sourceCount == targetCount) {
      updateStatus(pgConn, schemaName, tableName, "PERFECT_MATCH", sourceCount);
    } else if (sourceCount == 0) {
      updateStatus(pgConn, schemaName, tableName, "NO_DATA", 0);
    } else if (sourceCount < targetCount) {
      updateStatus(pgConn, schemaName, tableName, "ERROR", sourceCount);
    } else {
      performDataTransfer(pgConn, *mongoClient, schemaName, tableName,
                          sourceCount);
    }
  }

  int getSourceCount(mongoc_client_t &mongoClient, const std::string &dbName,
                     const std::string &collectionName) {
    try {
      mongoc_collection_t *collection = mongoc_client_get_collection(
          &mongoClient, dbName.c_str(), collectionName.c_str());

      bson_t *query = bson_new();
      int64_t count = mongoc_collection_count_documents(collection, query, NULL,
                                                        NULL, NULL, NULL);

      bson_destroy(query);
      mongoc_collection_destroy(collection);

      return static_cast<int>(count);
    } catch (const std::exception &e) {
      Logger::error("getSourceCount",
                    "Error getting source count: " + std::string(e.what()));
      return 0;
    }
  }

  int getTargetCount(pqxx::connection &pgConn, const std::string &schemaName,
                     const std::string &tableName) {
    try {
      std::string cleanSchemaName = cleanSchemaNameForPostgres(schemaName);
      std::string lowerSchemaName = toLowerCase(cleanSchemaName);
      pqxx::connection countConn(DatabaseConfig::getPostgresConnectionString());
      pqxx::work txn(countConn);
      auto result = txn.exec("SELECT COUNT(*) FROM \"" + lowerSchemaName +
                             "\".\"" + tableName + "\"");
      txn.commit();
      if (!result.empty()) {
        return result[0][0].as<int>();
      }
      return 0;
    } catch (const std::exception &e) {
      Logger::error("getTargetCount",
                    "Error getting target count: " + std::string(e.what()));
      return 0;
    }
  }

  void performDataTransfer(pqxx::connection &pgConn,
                           mongoc_client_t &mongoClient,
                           const std::string &dbName,
                           const std::string &collectionName, int sourceCount) {
    try {
      Logger::info("performDataTransfer",
                   "Transferring data for " + dbName + "." + collectionName);

      std::string lowerSchemaName = toLowerCase(dbName);

      mongoc_collection_t *collection = mongoc_client_get_collection(
          &mongoClient, dbName.c_str(), collectionName.c_str());

      // Procesar en lotes pequeños para evitar crashes del driver
      const int BATCH_SIZE = 10; // Procesar máximo 10 registros por lote
      int skip = 0;
      int totalTransferred = 0;
      bool hasMoreData = true;

      while (hasMoreData) {
        try {
          bson_t *query = bson_new();
          bson_t *opts = BCON_NEW("limit", BCON_INT32(BATCH_SIZE), "skip",
                                  BCON_INT32(skip));

          mongoc_cursor_t *cursor =
              mongoc_collection_find_with_opts(collection, query, opts, NULL);

          {
            pqxx::connection targetConn(
                DatabaseConfig::getPostgresConnectionString());
            pqxx::work targetTxn(targetConn);

            // Solo truncar en el primer lote
            if (skip == 0) {
              std::string truncateQuery = "TRUNCATE TABLE \"" +
                                          lowerSchemaName + "\".\"" +
                                          collectionName + "\" CASCADE;";
              targetTxn.exec(truncateQuery);
              Logger::debug("performDataTransfer",
                            "Table truncated for batch processing");
            }

            const bson_t *doc;
            int batchTransferred = 0;
            int batchProcessed = 0;

            // Recopilar documentos para procesamiento en lote
            std::vector<const bson_t *> batchDocs;
            while (mongoc_cursor_next(cursor, &doc) &&
                   batchProcessed < BATCH_SIZE) {
              batchDocs.push_back(doc);
              batchProcessed++;
            }

            if (!batchDocs.empty()) {
              try {
                performBulkUpsertMongo(targetTxn, batchDocs, lowerSchemaName,
                                       collectionName);
                batchTransferred += batchDocs.size();
              } catch (const std::exception &e) {
                Logger::warning(
                    "performDataTransfer",
                    "Error processing batch: " + std::string(e.what()) +
                        " - skipping batch");
              }
            }

            totalTransferred += batchTransferred;

            // Verificar si hay más datos
            hasMoreData = (batchProcessed == BATCH_SIZE);

            // Actualizar offset solo si se procesaron documentos
            if (batchTransferred > 0 || batchProcessed > 0) {
              try {
                targetTxn.exec("UPDATE metadata.catalog SET last_offset='" +
                               std::to_string(totalTransferred) +
                               "' WHERE schema_name='" + escapeSQL(dbName) +
                               "' AND table_name='" +
                               escapeSQL(collectionName) + "';");
                targetTxn.commit();
                Logger::debug(
                    "performDataTransfer",
                    "Batch " + std::to_string(skip / BATCH_SIZE + 1) +
                        " completed: " + std::to_string(batchTransferred) +
                        " records transferred");
              } catch (const std::exception &e) {
                Logger::warning("performDataTransfer",
                                "Failed to update offset: " +
                                    std::string(e.what()));
              }
            } else {
              targetTxn.commit();
            }

            mongoc_cursor_destroy(cursor);
            bson_destroy(query);
            bson_destroy(opts);

            // Si no se procesaron documentos, salir del bucle
            if (batchProcessed == 0) {
              hasMoreData = false;
            }

            skip += BATCH_SIZE;
          }

        } catch (const std::exception &e) {
          Logger::error("performDataTransfer",
                        "Error in batch processing: " + std::string(e.what()));
          hasMoreData = false; // Salir del bucle en caso de error
        }
      }

      mongoc_collection_destroy(collection);

      updateStatus(pgConn, dbName, collectionName, "PERFECT_MATCH",
                   totalTransferred);

      Logger::info("performDataTransfer", "Successfully transferred " +
                                              std::to_string(totalTransferred) +
                                              " records for " + dbName + "." +
                                              collectionName);

    } catch (const std::exception &e) {
      Logger::error("performDataTransfer",
                    "Error transferring data: " + std::string(e.what()));
      updateStatus(pgConn, dbName, collectionName, "ERROR", 0);
    }
  }

  std::string buildInsertQuerySafe(const bson_t *doc,
                                   const std::string &schemaName,
                                   const std::string &tableName) {
    try {
      std::string cleanSchemaName = cleanSchemaNameForPostgres(schemaName);
      std::string insertQuery = "INSERT INTO \"" + cleanSchemaName + "\".\"" +
                                tableName + "\" VALUES (";
      std::vector<std::string> values;

      bson_iter_t iter;
      if (bson_iter_init(&iter, doc)) {
        while (bson_iter_next(&iter)) {
          const char *key = bson_iter_key(&iter);
          std::string value = getBsonValueAsStringSafe(&iter);
          values.push_back("'" + escapeSQL(value) + "'");
        }
      }

      if (values.empty())
        return "";

      insertQuery += values[0];
      for (size_t i = 1; i < values.size(); ++i) {
        insertQuery += ", " + values[i];
      }
      insertQuery += ");";

      return insertQuery;
    } catch (const std::exception &e) {
      Logger::error("buildInsertQuerySafe",
                    "Error building insert query: " + std::string(e.what()));
      return "";
    }
  }

  std::string buildInsertQuery(const bson_t *doc, const std::string &schemaName,
                               const std::string &tableName) {
    try {
      std::string cleanSchemaName = cleanSchemaNameForPostgres(schemaName);
      std::string insertQuery = "INSERT INTO \"" + cleanSchemaName + "\".\"" +
                                tableName + "\" VALUES (";
      std::vector<std::string> values;

      bson_iter_t iter;
      if (bson_iter_init(&iter, doc)) {
        while (bson_iter_next(&iter)) {
          const char *key = bson_iter_key(&iter);
          std::string value = getBsonValueAsString(&iter);
          values.push_back("'" + escapeSQL(value) + "'");
        }
      }

      if (values.empty())
        return "";

      insertQuery += values[0];
      for (size_t i = 1; i < values.size(); ++i) {
        insertQuery += ", " + values[i];
      }
      insertQuery += ");";

      return insertQuery;
    } catch (const std::exception &e) {
      Logger::error("buildInsertQuery",
                    "Error building insert query: " + std::string(e.what()));
      return "";
    }
  }

  std::string getBsonValueAsStringSafe(bson_iter_t *iter) {
    try {
      switch (bson_iter_type(iter)) {
      case BSON_TYPE_UTF8:
        return bson_iter_utf8(iter, NULL);
      case BSON_TYPE_INT32:
        return std::to_string(bson_iter_int32(iter));
      case BSON_TYPE_INT64:
        return std::to_string(bson_iter_int64(iter));
      case BSON_TYPE_DOUBLE:
        return std::to_string(bson_iter_double(iter));
      case BSON_TYPE_BOOL:
        return bson_iter_bool(iter) ? "true" : "false";
      case BSON_TYPE_OID: {
        char oid_str[25];
        bson_oid_to_string(bson_iter_oid(iter), oid_str);
        return std::string(oid_str);
      }
      case BSON_TYPE_DATE_TIME: {
        int64_t datetime = bson_iter_date_time(iter);
        // Convert to PostgreSQL timestamp format
        time_t time = datetime / 1000;
        struct tm *tm_info = localtime(&time);
        char buffer[30];
        strftime(buffer, 30, "%Y-%m-%d %H:%M:%S", tm_info);
        return std::string(buffer);
      }
      case BSON_TYPE_DOCUMENT:
      case BSON_TYPE_ARRAY: {
        // Convert to JSON string with error handling
        uint32_t doc_len;
        const uint8_t *doc_data;
        bson_iter_document(iter, &doc_len, &doc_data);

        // Verificar que los datos no sean nulos y tengan tamaño válido
        if (doc_data == NULL || doc_len == 0) {
          return "{}";
        }

        try {
          bson_t bson;
          if (bson_init_static(&bson, doc_data, doc_len)) {
            char *json = bson_as_canonical_extended_json(&bson, NULL);
            if (json) {
              std::string result(json);
              bson_free(json);
              return result;
            } else {
              return "{}";
            }
          } else {
            return "{}";
          }
        } catch (...) {
          return "{}"; // Retornar JSON vacío en caso de error
        }
      }
      default:
        return "NULL";
      }
    } catch (const std::exception &e) {
      Logger::warning("getBsonValueAsStringSafe",
                      "Error converting BSON value: " + std::string(e.what()));
      return "NULL";
    }
  }

  std::string getBsonValueAsString(bson_iter_t *iter) {
    switch (bson_iter_type(iter)) {
    case BSON_TYPE_UTF8:
      return bson_iter_utf8(iter, NULL);
    case BSON_TYPE_INT32:
      return std::to_string(bson_iter_int32(iter));
    case BSON_TYPE_INT64:
      return std::to_string(bson_iter_int64(iter));
    case BSON_TYPE_DOUBLE:
      return std::to_string(bson_iter_double(iter));
    case BSON_TYPE_BOOL:
      return bson_iter_bool(iter) ? "true" : "false";
    case BSON_TYPE_OID: {
      char oid_str[25];
      bson_oid_to_string(bson_iter_oid(iter), oid_str);
      return std::string(oid_str);
    }
    case BSON_TYPE_DATE_TIME: {
      int64_t datetime = bson_iter_date_time(iter);
      // Convert to PostgreSQL timestamp format
      time_t time = datetime / 1000;
      struct tm *tm_info = localtime(&time);
      char buffer[30];
      strftime(buffer, 30, "%Y-%m-%d %H:%M:%S", tm_info);
      return std::string(buffer);
    }
    case BSON_TYPE_DOCUMENT:
    case BSON_TYPE_ARRAY: {
      // Convert to JSON string
      uint32_t doc_len;
      const uint8_t *doc_data;
      bson_iter_document(iter, &doc_len, &doc_data);
      bson_t bson;
      bson_init_static(&bson, doc_data, doc_len);
      char *json = bson_as_canonical_extended_json(&bson, NULL);
      std::string result(json);
      bson_free(json);
      return result;
    }
    default:
      return "NULL";
    }
  }

  void updateStatus(pqxx::connection &pgConn, const std::string &schemaName,
                    const std::string &tableName, const std::string &status,
                    int count) {
    try {
      pqxx::connection updateConn(
          DatabaseConfig::getPostgresConnectionString());
      pqxx::work txn(updateConn);
      txn.exec("UPDATE metadata.catalog SET status='" + status +
               "', last_sync_time=NOW(), last_offset='" +
               std::to_string(count) +
               "' "
               "WHERE schema_name='" +
               escapeSQL(schemaName) + "' AND table_name='" +
               escapeSQL(tableName) + "';");
      txn.commit();
    } catch (const std::exception &e) {
      Logger::error("updateStatus",
                    "Error updating status: " + std::string(e.what()));
    }
  }

  std::string escapeSQL(const std::string &value) {
    std::string escaped = value;
    size_t pos = 0;
    while ((pos = escaped.find("'", pos)) != std::string::npos) {
      escaped.replace(pos, 1, "''");
      pos += 2;
    }
    return escaped;
  }

  void performBulkUpsertMongo(pqxx::work &txn,
                              const std::vector<const bson_t *> &batchDocs,
                              const std::string &lowerSchemaName,
                              const std::string &collectionName) {
    try {
      if (batchDocs.empty())
        return;

      // Obtener columnas de primary key para el UPSERT
      std::vector<std::string> pkColumns = getPrimaryKeyColumnsFromPostgres(
          txn.conn(), lowerSchemaName, collectionName);

      // Obtener nombres de columnas del primer documento
      std::vector<std::string> columnNames;
      if (!batchDocs.empty()) {
        bson_iter_t iter;
        if (bson_iter_init(&iter, batchDocs[0])) {
          while (bson_iter_next(&iter)) {
            columnNames.push_back(bson_iter_key(&iter));
          }
        }
      }

      if (columnNames.empty())
        return;

      // Construir query UPSERT o INSERT
      std::string query;
      std::string conflictClause;
      if (pkColumns.empty()) {
        query =
            buildBulkInsertQuery(columnNames, lowerSchemaName, collectionName);
      } else {
        query = buildBulkUpsertQuery(columnNames, pkColumns, lowerSchemaName,
                                     collectionName);
        conflictClause = buildUpsertConflictClause(columnNames, pkColumns);
      }

      // Procesar documentos en lotes más pequeños para evitar queries muy
      // largas
      const size_t SUB_BATCH_SIZE = 100;
      for (size_t start = 0; start < batchDocs.size();
           start += SUB_BATCH_SIZE) {
        size_t end = std::min(start + SUB_BATCH_SIZE, batchDocs.size());

        std::string batchQuery = query;
        std::vector<std::string> values;

        for (size_t i = start; i < end; ++i) {
          std::string rowValues = "(";
          std::vector<std::string> docValues =
              extractDocumentValues(batchDocs[i], columnNames);

          for (size_t j = 0; j < docValues.size(); ++j) {
            if (j > 0)
              rowValues += ", ";
            rowValues += "'" + escapeSQL(docValues[j]) + "'";
          }
          rowValues += ")";
          values.push_back(rowValues);
        }

        if (!values.empty()) {
          batchQuery += values[0];
          for (size_t i = 1; i < values.size(); ++i) {
            batchQuery += ", " + values[i];
          }

          if (!pkColumns.empty()) {
            batchQuery += conflictClause;
          } else {
            batchQuery += ";";
          }

          txn.exec(batchQuery);
        }
      }

      Logger::debug("performBulkUpsertMongo",
                    "Processed " + std::to_string(batchDocs.size()) +
                        " documents with UPSERT for " + lowerSchemaName + "." +
                        collectionName);

    } catch (const std::exception &e) {
      Logger::error("performBulkUpsertMongo",
                    "Error in bulk upsert: " + std::string(e.what()));
      throw;
    }
  }

  std::vector<std::string>
  getPrimaryKeyColumnsFromPostgres(pqxx::connection &pgConn,
                                   const std::string &schemaName,
                                   const std::string &tableName) {
    std::vector<std::string> pkColumns;

    try {
      pqxx::work txn(pgConn);
      std::string query = "SELECT kcu.column_name "
                          "FROM information_schema.table_constraints tc "
                          "JOIN information_schema.key_column_usage kcu "
                          "ON tc.constraint_name = kcu.constraint_name "
                          "AND tc.table_schema = kcu.table_schema "
                          "WHERE tc.constraint_type = 'PRIMARY KEY' "
                          "AND tc.table_schema = '" +
                          schemaName +
                          "' "
                          "AND tc.table_name = '" +
                          tableName +
                          "' "
                          "ORDER BY kcu.ordinal_position;";

      auto results = txn.exec(query);
      txn.commit();

      for (const auto &row : results) {
        if (!row[0].is_null()) {
          std::string colName = row[0].as<std::string>();
          std::transform(colName.begin(), colName.end(), colName.begin(),
                         ::tolower);
          pkColumns.push_back(colName);
        }
      }
    } catch (const std::exception &e) {
      Logger::error("getPrimaryKeyColumnsFromPostgres",
                    "Error getting PK columns: " + std::string(e.what()));
    }

    return pkColumns;
  }

  std::string buildBulkInsertQuery(const std::vector<std::string> &columnNames,
                                   const std::string &schemaName,
                                   const std::string &tableName) {
    std::string query =
        "INSERT INTO \"" + schemaName + "\".\"" + tableName + "\" (";

    for (size_t i = 0; i < columnNames.size(); ++i) {
      if (i > 0)
        query += ", ";
      query += "\"" + columnNames[i] + "\"";
    }
    query += ") VALUES ";

    return query;
  }

  std::string buildBulkUpsertQuery(const std::vector<std::string> &columnNames,
                                   const std::vector<std::string> &pkColumns,
                                   const std::string &schemaName,
                                   const std::string &tableName) {
    std::string query =
        "INSERT INTO \"" + schemaName + "\".\"" + tableName + "\" (";

    for (size_t i = 0; i < columnNames.size(); ++i) {
      if (i > 0)
        query += ", ";
      query += "\"" + columnNames[i] + "\"";
    }
    query += ") VALUES ";

    return query;
  }

  std::string
  buildUpsertConflictClause(const std::vector<std::string> &columnNames,
                            const std::vector<std::string> &pkColumns) {
    std::string conflictClause = " ON CONFLICT (";

    for (size_t i = 0; i < pkColumns.size(); ++i) {
      if (i > 0)
        conflictClause += ", ";
      conflictClause += "\"" + pkColumns[i] + "\"";
    }
    conflictClause += ") DO UPDATE SET ";

    // Construir SET clause para UPDATE
    for (size_t i = 0; i < columnNames.size(); ++i) {
      if (i > 0)
        conflictClause += ", ";
      conflictClause +=
          "\"" + columnNames[i] + "\" = EXCLUDED.\"" + columnNames[i] + "\"";
    }

    return conflictClause;
  }

  std::vector<std::string>
  extractDocumentValues(const bson_t *doc,
                        const std::vector<std::string> &columnNames) {
    std::vector<std::string> values;
    values.reserve(columnNames.size());

    // Crear un mapa de valores del documento
    std::unordered_map<std::string, std::string> docValues;
    bson_iter_t iter;
    if (bson_iter_init(&iter, doc)) {
      while (bson_iter_next(&iter)) {
        const char *key = bson_iter_key(&iter);
        std::string value = getBsonValueAsStringSafe(&iter);
        docValues[key] = value;
      }
    }

    // Extraer valores en el orden de las columnas
    for (const auto &columnName : columnNames) {
      auto it = docValues.find(columnName);
      if (it != docValues.end()) {
        values.push_back(it->second);
      } else {
        values.push_back("NULL");
      }
    }

    return values;
  }
};

#endif // MONGOTOPOSTGRES_H
