#include "sync/DatabaseToPostgresSync.h"
#include "engines/database_engine.h"
#include <algorithm>

std::mutex DatabaseToPostgresSync::metadataUpdateMutex;

void DatabaseToPostgresSync::startParallelProcessing() {
  if (parallelProcessingActive.load()) {
    shutdownParallelProcessing();
  }

  parallelProcessingActive.store(true);

  rawDataQueue.clear();
  preparedBatchQueue.clear();
  resultQueue.clear();

  rawDataQueue.reset_queue();
  preparedBatchQueue.reset_queue();
  resultQueue.reset_queue();

  Logger::info(LogCategory::TRANSFER, "Parallel processing started");
}

void DatabaseToPostgresSync::shutdownParallelProcessing() {
  if (!parallelProcessingActive.load()) {
    return;
  }

  parallelProcessingActive.store(false);

  rawDataQueue.shutdown_queue();
  preparedBatchQueue.shutdown_queue();
  resultQueue.shutdown_queue();

  for (auto &thread : parallelThreads) {
    if (thread.joinable()) {
      thread.join();
    }
  }
  parallelThreads.clear();

  Logger::info(LogCategory::TRANSFER, "Parallel processing shutdown completed");
}

std::vector<std::string>
DatabaseToPostgresSync::parseJSONArray(const std::string &jsonArray) {
  std::vector<std::string> result;
  try {
    if (jsonArray.empty() || jsonArray == "[]") {
      return result;
    }

    auto j = json::parse(jsonArray);
    if (!j.is_array()) {
      throw std::runtime_error("Input is not a JSON array");
    }

    for (const auto &element : j) {
      if (element.is_string()) {
        result.push_back(element.get<std::string>());
      }
    }
  } catch (const json::parse_error &e) {
    Logger::error(LogCategory::TRANSFER, "parseJSONArray",
                  "JSON parse error: " + std::string(e.what()));
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "parseJSONArray",
                  "Error parsing JSON array: " + std::string(e.what()));
  }
  return result;
}

std::vector<std::string>
DatabaseToPostgresSync::parseLastPK(const std::string &lastPK) {
  std::vector<std::string> pkValues;
  try {
    if (!lastPK.empty()) {
      pkValues.push_back(lastPK);
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "parseLastPK",
                  "Error parsing last PK: " + std::string(e.what()));
  }
  return pkValues;
}

void DatabaseToPostgresSync::updateLastProcessedPK(
    pqxx::connection &pgConn, const std::string &schema_name,
    const std::string &table_name, const std::string &lastPK) {
  try {
    std::lock_guard<std::mutex> lock(metadataUpdateMutex);

    pqxx::work txn(pgConn);
    txn.exec("UPDATE metadata.catalog SET last_processed_pk='" +
             escapeSQL(lastPK) + "' WHERE schema_name='" +
             escapeSQL(schema_name) + "' AND table_name='" +
             escapeSQL(table_name) + "'");
    txn.commit();

    Logger::info(LogCategory::TRANSFER, "updateLastProcessedPK",
                 "Successfully updated last_processed_pk to '" + lastPK +
                     "' for " + schema_name + "." + table_name);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "updateLastProcessedPK",
                  "Error updating last processed PK: " + std::string(e.what()));
  }
}

std::string DatabaseToPostgresSync::getPKStrategyFromCatalog(
    pqxx::connection &pgConn, const std::string &schema_name,
    const std::string &table_name) {
  try {
    pqxx::connection separateConn(
        DatabaseConfig::getPostgresConnectionString());
    pqxx::nontransaction ntxn(separateConn);
    auto result = ntxn.exec("SELECT pk_strategy FROM metadata.catalog "
                            "WHERE schema_name='" +
                            escapeSQL(schema_name) + "' AND table_name='" +
                            escapeSQL(table_name) + "'");

    if (!result.empty() && !result[0][0].is_null()) {
      return result[0][0].as<std::string>();
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "getPKStrategyFromCatalog",
                  "Error getting PK strategy: " + std::string(e.what()));
  }
  return "OFFSET";
}

std::vector<std::string> DatabaseToPostgresSync::getPKColumnsFromCatalog(
    pqxx::connection &pgConn, const std::string &schema_name,
    const std::string &table_name) {
  std::vector<std::string> pkColumns;
  try {
    pqxx::connection separateConn(
        DatabaseConfig::getPostgresConnectionString());
    pqxx::nontransaction ntxn(separateConn);
    auto result = ntxn.exec("SELECT pk_columns FROM metadata.catalog "
                            "WHERE schema_name='" +
                            escapeSQL(schema_name) + "' AND table_name='" +
                            escapeSQL(table_name) + "'");

    if (!result.empty() && !result[0][0].is_null()) {
      std::string pkColumnsJSON = result[0][0].as<std::string>();
      pkColumns = parseJSONArray(pkColumnsJSON);
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "getPKColumnsFromCatalog",
                  "Error getting PK columns: " + std::string(e.what()));
  }
  return pkColumns;
}

std::string DatabaseToPostgresSync::getLastProcessedPKFromCatalog(
    pqxx::connection &pgConn, const std::string &schema_name,
    const std::string &table_name) {
  try {
    pqxx::connection separateConn(
        DatabaseConfig::getPostgresConnectionString());
    pqxx::nontransaction ntxn(separateConn);
    auto result = ntxn.exec("SELECT last_processed_pk FROM metadata.catalog "
                            "WHERE schema_name='" +
                            escapeSQL(schema_name) + "' AND table_name='" +
                            escapeSQL(table_name) + "'");

    if (!result.empty() && !result[0][0].is_null()) {
      return result[0][0].as<std::string>();
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "getLastProcessedPKFromCatalog",
                  "Error getting last processed PK: " +
                      std::string(e.what()));
  }
  return "";
}

std::string DatabaseToPostgresSync::getLastPKFromResults(
    const std::vector<std::vector<std::string>> &results,
    const std::vector<std::string> &pkColumns,
    const std::vector<std::string> &columnNames) {
  if (results.empty() || pkColumns.empty()) {
    return "";
  }

  try {
    const auto &lastRow = results.back();

    std::string lastPK;
    if (!pkColumns.empty()) {
      size_t pkIndex = 0;
      for (size_t j = 0; j < columnNames.size(); ++j) {
        if (columnNames[j] == pkColumns[0]) {
          pkIndex = j;
          break;
        }
      }

      if (pkIndex < lastRow.size()) {
        lastPK = lastRow[pkIndex];
      }
    }

    return lastPK;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "getLastPKFromResults",
                  "Error extracting last PK: " + std::string(e.what()));
    return "";
  }
}

size_t DatabaseToPostgresSync::deleteRecordsByPrimaryKey(
    pqxx::connection &pgConn, const std::string &lowerSchemaName,
    const std::string &table_name,
    const std::vector<std::vector<std::string>> &deletedPKs,
    const std::vector<std::string> &pkColumns) {

  if (deletedPKs.empty() || pkColumns.empty()) {
    return 0;
  }

  size_t deletedCount = 0;

  try {
    std::string lowerTableName = table_name;
    std::transform(lowerTableName.begin(), lowerTableName.end(),
                   lowerTableName.begin(), ::tolower);
    pqxx::work txn(pgConn);

    std::string deleteQuery = "DELETE FROM \"" + lowerSchemaName + "\".\"" +
                              lowerTableName + "\" WHERE (";

    for (size_t i = 0; i < deletedPKs.size(); ++i) {
      if (i > 0)
        deleteQuery += " OR ";
      deleteQuery += "(";
      for (size_t j = 0; j < pkColumns.size(); ++j) {
        if (j > 0)
          deleteQuery += " AND ";
        std::string value = deletedPKs[i][j];
        if (value == "NULL") {
          deleteQuery += "\"" + pkColumns[j] + "\" IS NULL";
        } else {
          deleteQuery +=
              "\"" + pkColumns[j] + "\" = '" + escapeSQL(value) + "'";
        }
      }
      deleteQuery += ")";
    }
    deleteQuery += ");";

    auto result = txn.exec(deleteQuery);
    deletedCount = result.affected_rows();

    txn.commit();

  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "deleteRecordsByPrimaryKey",
                  "Error deleting records: " + std::string(e.what()));
  }

  return deletedCount;
}

std::vector<std::string>
DatabaseToPostgresSync::getPrimaryKeyColumnsFromPostgres(
    pqxx::connection &pgConn, const std::string &schemaName,
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
    Logger::error(LogCategory::TRANSFER, "getPrimaryKeyColumnsFromPostgres",
                  "Error getting PK columns: " + std::string(e.what()));
  }

  return pkColumns;
}

std::string DatabaseToPostgresSync::buildUpsertQuery(
    const std::vector<std::string> &columnNames,
    const std::vector<std::string> &pkColumns, const std::string &schemaName,
    const std::string &tableName) {
  std::string lowerTableName = tableName;
  std::transform(lowerTableName.begin(), lowerTableName.end(),
                 lowerTableName.begin(), ::tolower);
  std::string query =
      "INSERT INTO \"" + schemaName + "\".\"" + lowerTableName + "\" (";

  for (size_t i = 0; i < columnNames.size(); ++i) {
    if (i > 0)
      query += ", ";
    std::string col = columnNames[i];
    std::transform(col.begin(), col.end(), col.begin(), ::tolower);
    query += "\"" + col + "\"";
  }
  query += ") VALUES ";

  return query;
}

std::string DatabaseToPostgresSync::buildUpsertConflictClause(
    const std::vector<std::string> &columnNames,
    const std::vector<std::string> &pkColumns) {
  std::string conflictClause = " ON CONFLICT (";

  for (size_t i = 0; i < pkColumns.size(); ++i) {
    if (i > 0)
      conflictClause += ", ";
    std::string col = pkColumns[i];
    std::transform(col.begin(), col.end(), col.begin(), ::tolower);
    conflictClause += "\"" + col + "\"";
  }
  conflictClause += ") DO UPDATE SET ";

  for (size_t i = 0; i < columnNames.size(); ++i) {
    if (i > 0)
      conflictClause += ", ";
    std::string col = columnNames[i];
    std::transform(col.begin(), col.end(), col.begin(), ::tolower);
    conflictClause += "\"" + col + "\" = EXCLUDED.\"" + col + "\"";
  }

  return conflictClause;
}

bool DatabaseToPostgresSync::compareAndUpdateRecord(
    pqxx::connection &pgConn, const std::string &schemaName,
    const std::string &tableName, const std::vector<std::string> &newRecord,
    const std::vector<std::vector<std::string>> &columnNames,
    const std::string &whereClause) {
  try {
    std::string selectQuery = "SELECT * FROM \"" + schemaName + "\".\"" +
                              tableName + "\" WHERE " + whereClause;

    pqxx::work txn(pgConn);
    auto result = txn.exec(selectQuery);
    txn.commit();

    if (result.empty()) {
      return false;
    }

    const auto &currentRow = result[0];

    std::vector<std::string> updateFields;
    bool hasChanges = false;

    for (size_t i = 0; i < columnNames.size(); ++i) {
      std::string columnName = columnNames[i][0];
      std::transform(columnName.begin(), columnName.end(), columnName.begin(),
                     ::tolower);
      std::string newValue = newRecord[i];

      std::string currentValue =
          currentRow[i].is_null() ? "" : currentRow[i].as<std::string>();

      if (currentValue != newValue) {
        std::string cleanNewValue = newValue;

        for (char &c : cleanNewValue) {
          if (static_cast<unsigned char>(c) > 127) {
            c = '?';
          }
        }

        cleanNewValue.erase(
            std::remove_if(cleanNewValue.begin(), cleanNewValue.end(),
                           [](unsigned char c) {
                             return c < 32 && c != 9 && c != 10 && c != 13;
                           }),
            cleanNewValue.end());

        std::string valueToSet;
        if (cleanNewValue.empty() || cleanNewValue == "NULL") {
          valueToSet = "NULL";
        } else {
          valueToSet = "'" + escapeSQL(cleanNewValue) + "'";
        }

        updateFields.push_back("\"" + columnName + "\" = " + valueToSet);
        hasChanges = true;
      }
    }

    if (hasChanges) {
      std::string updateQuery =
          "UPDATE \"" + schemaName + "\".\"" + tableName + "\" SET ";
      for (size_t i = 0; i < updateFields.size(); ++i) {
        if (i > 0)
          updateQuery += ", ";
        updateQuery += updateFields[i];
      }
      updateQuery += " WHERE " + whereClause;

      pqxx::work updateTxn(pgConn);
      updateTxn.exec(updateQuery);
      updateTxn.commit();

      return true;
    }

    return false;

  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "compareAndUpdateRecord",
                  "Error comparing/updating record: " +
                      std::string(e.what()));
    return false;
  }
}

void DatabaseToPostgresSync::performBulkInsert(
    pqxx::connection &pgConn,
    const std::vector<std::vector<std::string>> &results,
    const std::vector<std::string> &columnNames,
    const std::vector<std::string> &columnTypes,
    const std::string &lowerSchemaName, const std::string &tableName) {
  try {
    std::string lowerTableName = tableName;
    std::transform(lowerTableName.begin(), lowerTableName.end(),
                   lowerTableName.begin(), ::tolower);
    std::string insertQuery = "INSERT INTO \"" + lowerSchemaName + "\".\"" +
                              lowerTableName + "\" (";

    for (size_t i = 0; i < columnNames.size(); ++i) {
      if (i > 0)
        insertQuery += ", ";
      std::string col = columnNames[i];
      std::transform(col.begin(), col.end(), col.begin(), ::tolower);
      insertQuery += "\"" + col + "\"";
    }
    insertQuery += ") VALUES ";

    pqxx::work txn(pgConn);
    txn.exec("SET statement_timeout = '600s'");

    const size_t BATCH_SIZE = SyncConfig::getChunkSize();
    size_t totalProcessed = 0;

    for (size_t batchStart = 0; batchStart < results.size();
         batchStart += BATCH_SIZE) {
      size_t batchEnd = std::min(batchStart + BATCH_SIZE, results.size());

      std::string batchQuery = insertQuery;
      std::vector<std::string> values;

      for (size_t i = batchStart; i < batchEnd; ++i) {
        const auto &row = results[i];
        if (row.size() != columnNames.size())
          continue;

        std::string rowValues = "(";
        for (size_t j = 0; j < row.size(); ++j) {
          if (j > 0)
            rowValues += ", ";

          if (row[j].empty()) {
            rowValues += "NULL";
          } else {
            std::string cleanValue =
                cleanValueForPostgres(row[j], columnTypes[j]);
            if (cleanValue == "NULL") {
              rowValues += "NULL";
            } else {
              rowValues += "'" + escapeSQL(cleanValue) + "'";
            }
          }
        }
        rowValues += ")";
        values.push_back(rowValues);
      }

      if (!values.empty()) {
        batchQuery += values[0];
        for (size_t i = 1; i < values.size(); ++i) {
          batchQuery += ", " + values[i];
        }

        txn.exec(batchQuery);
        totalProcessed += values.size();
      }
    }

    txn.commit();

  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "performBulkInsert",
                  "Error in bulk insert: " + std::string(e.what()));
    throw;
  }
}
