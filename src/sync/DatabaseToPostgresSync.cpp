#include "sync/DatabaseToPostgresSync.h"
#include "engines/database_engine.h"
#include <algorithm>

std::mutex DatabaseToPostgresSync::metadataUpdateMutex;

// Initializes parallel processing queues for data transfer. Clears and resets
// all three queues: rawDataQueue (for raw data chunks), preparedBatchQueue
// (for prepared SQL batches), and resultQueue (for processing results). This
// should be called before starting any parallel data transfer operations to
// ensure clean queue state.
void DatabaseToPostgresSync::startParallelProcessing() {
  rawDataQueue.clear();
  preparedBatchQueue.clear();
  resultQueue.clear();

  rawDataQueue.reset_queue();
  preparedBatchQueue.reset_queue();
  resultQueue.reset_queue();

  Logger::info(LogCategory::TRANSFER, "Parallel processing started");
}

// Shuts down parallel processing by signaling all queues to stop accepting new
// items, then joining all parallel threads. Ensures clean shutdown of all
// worker threads before returning. Should be called after all data transfer
// operations are complete to free resources.
void DatabaseToPostgresSync::shutdownParallelProcessing() {
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

// Parses a JSON array string into a vector of strings. Handles empty arrays
// and empty strings by returning an empty vector. Validates that the input is
// actually a JSON array, throwing an exception if not. Extracts only string
// elements from the array, ignoring other types. Catches JSON parse errors and
// general exceptions, logging them and returning an empty vector on failure.
// Used primarily for parsing primary key column arrays from the catalog.
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

// Parses the last processed primary key value into a vector. Currently
// returns a single-element vector containing the lastPK string if it's not
// empty. This is a simplified implementation that may need enhancement for
// composite primary keys. Returns an empty vector if lastPK is empty. Catches
// exceptions and logs errors, returning an empty vector on failure.
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

// Updates the last_processed_pk field in metadata.catalog for a specific table.
// Uses a static mutex to ensure thread-safe updates across multiple threads.
// Escapes SQL values to prevent injection. Commits the transaction after
// updating. Logs success and errors. This tracks the progress of incremental
// syncs by recording the last primary key value that was successfully
// processed.
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

// Retrieves the primary key strategy (e.g., "OFFSET", "PK") from
// metadata.catalog for a specific table. Creates a separate connection to
// avoid transaction conflicts. Escapes SQL values to prevent injection.
// Returns the pk_strategy value if found, or "OFFSET" as the default if not
// found or on error. Logs errors but does not throw exceptions.
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

// Retrieves the primary key columns from metadata.catalog for a specific table.
// Creates a separate connection to avoid transaction conflicts. Escapes SQL
// values to prevent injection. Parses the pk_columns JSON array using
// parseJSONArray. Returns a vector of column names, or an empty vector if not
// found or on error. Logs errors but does not throw exceptions.
std::vector<std::string>
DatabaseToPostgresSync::getPKColumnsFromCatalog(pqxx::connection &pgConn,
                                                const std::string &schema_name,
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

// Retrieves the last_processed_pk value from metadata.catalog for a specific
// table. Creates a separate connection to avoid transaction conflicts. Escapes
// SQL values to prevent injection. Returns the last_processed_pk string if
// found, or an empty string if not found or on error. Logs errors but does not
// throw exceptions. Used to resume incremental syncs from the last processed
// position.
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
                  "Error getting last processed PK: " + std::string(e.what()));
  }
  return "";
}

// Extracts the last primary key value from a result set. Takes the results
// vector, pkColumns vector, and columnNames vector. Returns the value of the
// first primary key column from the last row in the results. Returns an empty
// string if results are empty, pkColumns is empty, or the column is not found.
// Handles exceptions by logging errors and returning an empty string. Used to
// track progress during batch processing.
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

// Deletes records from PostgreSQL by their primary key values. Takes a vector
// of deleted primary key vectors (for composite keys) and the pkColumns
// vector. Builds a DELETE query with OR conditions for each deleted PK,
// handling NULL values appropriately. Escapes SQL values to prevent
// injection. Converts table name to lowercase. Returns the number of deleted
// rows, or 0 if deletedPKs or pkColumns is empty. Logs errors but does not
// throw exceptions. Used for handling deleted records during incremental sync.
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

// Retrieves primary key column names from PostgreSQL's information_schema for
// a specific table. Queries table_constraints and key_column_usage to find
// PRIMARY KEY constraints. Returns columns ordered by ordinal_position for
// composite keys. Converts all column names to lowercase. Returns an empty
// vector if no primary key is found or on error. Logs errors but does not
// throw exceptions. Used to determine which columns to use for upsert
// conflict resolution.
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
                        "AND tc.table_schema = " +
                        txn.quote(schemaName) +
                        " AND tc.table_name = " + txn.quote(tableName) +
                        " ORDER BY kcu.ordinal_position;";

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

// Builds the INSERT portion of an UPSERT query. Takes column names, primary
// key columns, schema name, and table name. Converts table and column names
// to lowercase. Constructs "INSERT INTO schema.table (col1, col2, ...) VALUES"
// query. Returns the query string ready for value appending. Does not include
// the ON CONFLICT clause (see buildUpsertConflictClause).
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

// Builds the ON CONFLICT clause for an UPSERT query. Takes column names and
// primary key columns. Constructs "ON CONFLICT (pk1, pk2, ...) DO UPDATE SET
// col1 = EXCLUDED.col1, col2 = EXCLUDED.col2, ..." clause. Converts all column
// names to lowercase. Used together with buildUpsertQuery to create complete
// UPSERT statements that update existing rows or insert new ones.
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

// Compares a new record with the existing record in PostgreSQL and updates
// only changed fields. Executes a SELECT query to fetch the current record,
// then compares each field. Builds an UPDATE query with only the changed
// fields. Handles NULL values and escapes SQL values. Removes invalid
// characters (non-ASCII, control characters) from values. Returns true if any
// updates were made, false if the record is unchanged or not found. Logs
// errors but does not throw exceptions. Used for incremental syncs to update
// only modified records.
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
                  "Error comparing/updating record: " + std::string(e.what()));
    return false;
  }
}

// Performs a bulk INSERT operation into PostgreSQL. Takes a vector of result
// rows, column names, column types, schema name, and table name. Processes
// rows in batches using SyncConfig::getChunkSize(). Cleans values using
// cleanValueForPostgres and escapes SQL values. Sets statement_timeout to 600s
// for large batches. Commits the transaction after all batches. Throws
// exceptions on error to allow caller to handle failures. Used for initial
// full loads of tables.
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
    std::string insertQuery =
        "INSERT INTO \"" + lowerSchemaName + "\".\"" + lowerTableName + "\" (";

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

// Worker thread function for parallel batch insertion. Continuously pops
// PreparedBatch items from preparedBatchQueue and executes them. Sets
// statement_timeout to 600s for each batch. Tracks total processed rows and
// chunk numbers. Pushes ProcessedResult items to resultQueue after each
// batch. Stops when a batch with batchSize == 0 is received (shutdown
// signal). Logs errors for failed batches but continues processing. Used in
// parallel processing pipeline for high-throughput data transfer.
void DatabaseToPostgresSync::batchInserterThread(pqxx::connection &pgConn) {

  try {
    size_t totalProcessed = 0;

    while (true) {
      PreparedBatch batch;
      if (!preparedBatchQueue.pop(batch, std::chrono::milliseconds(1000))) {
        continue;
      }

      if (batch.batchSize == 0) {
        break;
      }

      ProcessedResult result;
      result.chunkNumber = batch.chunkNumber;
      result.schemaName = batch.schemaName;
      result.tableName = batch.tableName;
      result.success = false;

      try {
        pqxx::work txn(pgConn);
        txn.exec("SET statement_timeout = '600s'");
        txn.exec(batch.batchQuery);
        txn.commit();

        result.rowsProcessed = batch.batchSize;
        result.success = true;
        totalProcessed += batch.batchSize;

      } catch (const std::exception &e) {
        result.errorMessage = e.what();
        result.rowsProcessed = 0;

        Logger::error(LogCategory::TRANSFER,
                      "Error inserting batch " +
                          std::to_string(batch.chunkNumber) + " for " +
                          batch.schemaName + "." + batch.tableName + ": " +
                          result.errorMessage);
      }

      resultQueue.push(std::move(result));
    }

  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "batchInserterThread",
                  "Error in batch inserter thread: " + std::string(e.what()));
  }
}

// Performs a bulk UPSERT (INSERT ... ON CONFLICT DO UPDATE) operation into
// PostgreSQL. Retrieves primary key columns from PostgreSQL. If no primary key
// is found, falls back to performBulkInsert. Processes rows in batches using
// SyncConfig::getChunkSize(). Handles transaction aborts by processing rows
// individually (up to MAX_INDIVIDUAL_PROCESSING limit). Handles binary data
// errors by processing rows individually (up to MAX_BINARY_ERROR_PROCESSING
// limit). Sets statement_timeout to 600s. Throws exceptions on error. Used
// for incremental syncs that need to update existing rows or insert new ones.
void DatabaseToPostgresSync::performBulkUpsert(
    pqxx::connection &pgConn,
    const std::vector<std::vector<std::string>> &results,
    const std::vector<std::string> &columnNames,
    const std::vector<std::string> &columnTypes,
    const std::string &lowerSchemaName, const std::string &tableName,
    const std::string &sourceSchemaName) {
  try {
    std::vector<std::string> pkColumns =
        getPrimaryKeyColumnsFromPostgres(pgConn, lowerSchemaName, tableName);

    if (pkColumns.empty()) {
      Logger::warning(LogCategory::TRANSFER, "performBulkUpsert",
                      "No PK found for table " + lowerSchemaName + "." +
                          tableName +
                          " - using simple INSERT instead of UPSERT");

      performBulkInsert(pgConn, results, columnNames, columnTypes,
                        lowerSchemaName, tableName);
      return;
    }

    std::string upsertQuery =
        buildUpsertQuery(columnNames, pkColumns, lowerSchemaName, tableName);
    std::string conflictClause =
        buildUpsertConflictClause(columnNames, pkColumns);

    pqxx::work txn(pgConn);
    txn.exec("SET statement_timeout = '600s'");

    const size_t BATCH_SIZE = SyncConfig::getChunkSize();
    size_t totalProcessed = 0;

    for (size_t batchStart = 0; batchStart < results.size();
         batchStart += BATCH_SIZE) {
      size_t batchEnd = std::min(batchStart + BATCH_SIZE, results.size());

      std::string batchQuery = upsertQuery;
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
        batchQuery += conflictClause;

        try {
          txn.exec(batchQuery);
          totalProcessed += values.size();
        } catch (const std::exception &e) {
          std::string errorMsg = e.what();

          if (errorMsg.find("current transaction is aborted") !=
              std::string::npos) {
            Logger::warning(LogCategory::TRANSFER, "performBulkUpsert",
                            "Transaction aborted detected, rolling back and "
                            "processing individually");

            try {
              txn.abort();
            } catch (const std::exception &e) {
              Logger::error(LogCategory::TRANSFER, "performBulkUpsert",
                            "Error aborting transaction: " +
                                std::string(e.what()));
            } catch (...) {
              Logger::error(LogCategory::TRANSFER, "performBulkUpsert",
                            "Unknown error aborting transaction");
            }

            size_t individualProcessed = 0;
            const size_t MAX_INDIVIDUAL_PROCESSING = 100;

            for (size_t i = batchStart;
                 i < batchEnd &&
                 individualProcessed < MAX_INDIVIDUAL_PROCESSING;
                 ++i) {
              try {
                const auto &row = results[i];
                if (row.size() != columnNames.size())
                  continue;

                std::string singleRowValues = "(";
                for (size_t j = 0; j < row.size(); ++j) {
                  if (j > 0)
                    singleRowValues += ", ";

                  if (row[j].empty()) {
                    singleRowValues += "NULL";
                  } else {
                    std::string cleanValue =
                        cleanValueForPostgres(row[j], columnTypes[j]);
                    if (cleanValue == "NULL") {
                      singleRowValues += "NULL";
                    } else {
                      singleRowValues += "'" + escapeSQL(cleanValue) + "'";
                    }
                  }
                }
                singleRowValues += ")";

                pqxx::work singleTxn(pgConn);
                singleTxn.exec("SET statement_timeout = '600s'");
                std::string singleQuery =
                    upsertQuery + singleRowValues + conflictClause;
                singleTxn.exec(singleQuery);
                singleTxn.commit();
                totalProcessed++;
                individualProcessed++;

              } catch (const std::exception &singleError) {
                Logger::error(LogCategory::TRANSFER, "performBulkUpsert",
                              "Skipping problematic record for " +
                                  sourceSchemaName + "." + tableName + ": " +
                                  std::string(singleError.what()));
              }
            }

            if (individualProcessed >= MAX_INDIVIDUAL_PROCESSING) {
              Logger::warning(LogCategory::TRANSFER, "performBulkUpsert",
                              "Hit maximum individual processing limit (" +
                                  std::to_string(MAX_INDIVIDUAL_PROCESSING) +
                                  ") - stopping to prevent infinite loop");
            }
          } else if (errorMsg.find("not a valid binary digit") !=
                         std::string::npos ||
                     errorMsg.find("invalid input syntax") !=
                         std::string::npos) {

            Logger::warning(LogCategory::TRANSFER, "performBulkUpsert",
                            "Binary data error detected, processing batch "
                            "individually: " +
                                errorMsg.substr(0, 100));

            size_t binaryErrorProcessed = 0;
            const size_t MAX_BINARY_ERROR_PROCESSING = 50;

            for (size_t i = batchStart;
                 i < batchEnd &&
                 binaryErrorProcessed < MAX_BINARY_ERROR_PROCESSING;
                 ++i) {
              try {
                const auto &row = results[i];
                if (row.size() != columnNames.size())
                  continue;

                std::string singleRowValues = "(";
                for (size_t j = 0; j < row.size(); ++j) {
                  if (j > 0)
                    singleRowValues += ", ";

                  if (row[j].empty()) {
                    singleRowValues += "NULL";
                  } else {
                    std::string cleanValue =
                        cleanValueForPostgres(row[j], columnTypes[j]);
                    if (cleanValue == "NULL") {
                      singleRowValues += "NULL";
                    } else {
                      singleRowValues += "'" + escapeSQL(cleanValue) + "'";
                    }
                  }
                }
                singleRowValues += ")";

                std::string singleQuery =
                    upsertQuery + singleRowValues + conflictClause;
                txn.exec(singleQuery);
                totalProcessed++;
                binaryErrorProcessed++;

              } catch (const std::exception &singleError) {
                Logger::error(LogCategory::TRANSFER, "performBulkUpsert",
                              "Skipping problematic record in batch for " +
                                  sourceSchemaName + "." + tableName + ": " +
                                  std::string(singleError.what()));
              }
            }

            if (binaryErrorProcessed >= MAX_BINARY_ERROR_PROCESSING) {
              Logger::warning(LogCategory::TRANSFER, "performBulkUpsert",
                              "Hit maximum binary error processing limit (" +
                                  std::to_string(MAX_BINARY_ERROR_PROCESSING) +
                                  ") - stopping to prevent infinite loop");
            }
          } else {
            throw;
          }
        }
      }
    }

    try {
      txn.commit();
    } catch (const std::exception &commitError) {
      if (std::string(commitError.what()).find("previously aborted") !=
              std::string::npos ||
          std::string(commitError.what()).find("aborted transaction") !=
              std::string::npos) {
        Logger::warning(LogCategory::TRANSFER, "performBulkUpsert",
                        "Skipping commit for aborted transaction");
      } else {
        throw;
      }
    }

  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "performBulkUpsert",
                  "Error in bulk upsert: " + std::string(e.what()));
    throw;
  }
}
