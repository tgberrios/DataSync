#include "sync/DatabaseToPostgresSync.h"
#include "engines/database_engine.h"
#include <algorithm>
#include <set>

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

std::string DatabaseToPostgresSync::getPKStrategyFromCatalog(
    pqxx::connection & /* pgConn */, const std::string &schema_name,
    const std::string &table_name) {
  try {
    pqxx::connection separateConn(
        DatabaseConfig::getPostgresConnectionString());
    pqxx::nontransaction ntxn(separateConn);
    auto result = ntxn.exec("SELECT pk_strategy FROM metadata.catalog "
                            "WHERE schema_name=" +
                            ntxn.quote(schema_name) +
                            " AND table_name=" + ntxn.quote(table_name));

    if (!result.empty() && !result[0][0].is_null()) {
      std::string strategy = result[0][0].as<std::string>();
      if (strategy == "OFFSET" || strategy == "PK") {
        Logger::info(LogCategory::TRANSFER, "getPKStrategyFromCatalog",
                     "Legacy strategy '" + strategy + "' detected for " +
                         schema_name + "." + table_name +
                         " - defaulting to CDC");
        return "CDC";
      }
      return strategy;
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "getPKStrategyFromCatalog",
                  "Error getting PK strategy: " + std::string(e.what()));
  }
  return "CDC";
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
    if (pgConn.is_open()) {
      try {
        pqxx::nontransaction ntxn(pgConn);
        auto result = ntxn.exec("SELECT pk_columns FROM metadata.catalog "
                                "WHERE schema_name=" +
                                ntxn.quote(schema_name) +
                                " AND table_name=" + ntxn.quote(table_name));

        if (!result.empty() && !result[0][0].is_null()) {
          std::string pkColumnsJSON = result[0][0].as<std::string>();
          pkColumns = parseJSONArray(pkColumnsJSON);
          return pkColumns;
        }
      } catch (const pqxx::broken_connection &) {
      }
    }

    pqxx::connection separateConn(
        DatabaseConfig::getPostgresConnectionString());
    pqxx::nontransaction ntxn(separateConn);
    auto result = ntxn.exec("SELECT pk_columns FROM metadata.catalog "
                            "WHERE schema_name=" +
                            ntxn.quote(schema_name) +
                            " AND table_name=" + ntxn.quote(table_name));

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

  for (const auto &pk : deletedPKs) {
    if (pk.size() != pkColumns.size()) {
      Logger::error(LogCategory::TRANSFER, "deleteRecordsByPrimaryKey",
                    "Mismatch between PK values and columns count");
      return 0;
    }
  }

  size_t deletedCount = 0;

  try {
    std::string lowerTableName = table_name;
    std::transform(lowerTableName.begin(), lowerTableName.end(),
                   lowerTableName.begin(), ::tolower);
    pqxx::work txn(pgConn);

    std::string deleteQuery = "DELETE FROM " + txn.quote_name(lowerSchemaName) +
                              "." + txn.quote_name(lowerTableName) + " WHERE (";

    for (size_t i = 0; i < deletedPKs.size(); ++i) {
      if (i > 0)
        deleteQuery += " OR ";
      deleteQuery += "(";
      for (size_t j = 0; j < pkColumns.size(); ++j) {
        if (j > 0)
          deleteQuery += " AND ";
        std::string value = deletedPKs[i][j];
        if (value == "NULL" || value.empty()) {
          deleteQuery += txn.quote_name(pkColumns[j]) + " IS NULL";
        } else {
          deleteQuery +=
              txn.quote_name(pkColumns[j]) + " = " + txn.quote(value);
        }
      }
      deleteQuery += ")";
    }
    deleteQuery += ")";

    try {
      auto result = txn.exec(deleteQuery);
      deletedCount = result.affected_rows();
      txn.commit();
    } catch (...) {
      try {
        txn.abort();
      } catch (...) {
      }
      throw;
    }

  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "deleteRecordsByPrimaryKey",
                  "Error deleting records: " + std::string(e.what()));
  }

  return deletedCount;
}

size_t DatabaseToPostgresSync::deleteRecordsByHash(
    pqxx::connection &pgConn, const std::string &lowerSchemaName,
    const std::string &table_name,
    const std::vector<std::vector<std::string>> &deletedRecords,
    const std::vector<std::string> &columnNames) {

  if (deletedRecords.empty() || columnNames.empty()) {
    return 0;
  }

  size_t deletedCount = 0;

  try {
    std::string lowerTableName = table_name;
    std::transform(lowerTableName.begin(), lowerTableName.end(),
                   lowerTableName.begin(), ::tolower);
    pqxx::work txn(pgConn);

    for (const auto &record : deletedRecords) {
      if (record.size() < columnNames.size() + 1) {
        continue;
      }

      std::string deleteQuery = "DELETE FROM " +
                                txn.quote_name(lowerSchemaName) + "." +
                                txn.quote_name(lowerTableName) + " WHERE (";

      for (size_t j = 1; j < record.size() && (j - 1) < columnNames.size();
           ++j) {
        if (j > 1)
          deleteQuery += " AND ";
        std::string value = record[j];
        if (value == "NULL" || value.empty()) {
          deleteQuery += txn.quote_name(columnNames[j - 1]) + " IS NULL";
        } else {
          deleteQuery +=
              txn.quote_name(columnNames[j - 1]) + " = " + txn.quote(value);
        }
      }
      deleteQuery += ")";

      try {
        auto result = txn.exec(deleteQuery);
        deletedCount += result.affected_rows();
      } catch (const std::exception &e) {
        Logger::warning(LogCategory::TRANSFER, "deleteRecordsByHash",
                        "Error deleting record: " + std::string(e.what()));
      }
    }

    txn.commit();

  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "deleteRecordsByHash",
                  "Error deleting records by hash: " + std::string(e.what()));
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
    const std::vector<std::string> & /* pkColumns */,
    const std::string &schemaName, const std::string &tableName) {
  if (columnNames.empty() || schemaName.empty() || tableName.empty()) {
    throw std::invalid_argument("Empty column names, schema, or table name");
  }

  for (const auto &col : columnNames) {
    if (col.find('"') != std::string::npos ||
        col.find(';') != std::string::npos) {
      throw std::invalid_argument("Invalid character in column name: " + col);
    }
  }

  std::string lowerTableName = tableName;
  std::transform(lowerTableName.begin(), lowerTableName.end(),
                 lowerTableName.begin(), ::tolower);
  std::string lowerSchemaName = schemaName;
  std::transform(lowerSchemaName.begin(), lowerSchemaName.end(),
                 lowerSchemaName.begin(), ::tolower);

  std::string query =
      "INSERT INTO \"" + lowerSchemaName + "\".\"" + lowerTableName + "\" (";

  for (size_t i = 0; i < columnNames.size(); ++i) {
    if (i > 0)
      query += ", ";
    std::string col = columnNames[i];
    std::transform(col.begin(), col.end(), col.begin(), ::tolower);
    if (col.find('"') != std::string::npos) {
      throw std::invalid_argument("Invalid character in column name");
    }
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
  if (pkColumns.empty() || columnNames.empty()) {
    throw std::invalid_argument("Empty PK columns or column names");
  }

  for (const auto &col : pkColumns) {
    if (col.find('"') != std::string::npos ||
        col.find(';') != std::string::npos) {
      throw std::invalid_argument("Invalid character in PK column name: " +
                                  col);
    }
  }

  std::string conflictClause = " ON CONFLICT (";

  for (size_t i = 0; i < pkColumns.size(); ++i) {
    if (i > 0)
      conflictClause += ", ";
    std::string col = pkColumns[i];
    std::transform(col.begin(), col.end(), col.begin(), ::tolower);
    if (col.find('"') != std::string::npos) {
      throw std::invalid_argument("Invalid character in PK column name");
    }
    conflictClause += "\"" + col + "\"";
  }
  conflictClause += ") DO UPDATE SET ";

  for (size_t i = 0; i < columnNames.size(); ++i) {
    if (i > 0)
      conflictClause += ", ";
    std::string col = columnNames[i];
    std::transform(col.begin(), col.end(), col.begin(), ::tolower);
    if (col.find('"') != std::string::npos) {
      throw std::invalid_argument("Invalid character in column name");
    }
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
    if (newRecord.empty() || columnNames.empty() || whereClause.empty()) {
      Logger::warning(
          LogCategory::TRANSFER, "compareAndUpdateRecord",
          "Invalid parameters: empty record, columns, or where clause");
      return false;
    }

    pqxx::work txn(pgConn);
    std::string selectQuery = "SELECT * FROM " + txn.quote_name(schemaName) +
                              "." + txn.quote_name(tableName) + " WHERE " +
                              whereClause;

    auto result = txn.exec(selectQuery);
    txn.commit();

    if (result.empty()) {
      Logger::debug(LogCategory::TRANSFER, "compareAndUpdateRecord",
                    "Record not found for " + schemaName + "." + tableName);
      return false;
    }

    if (result.size() > 1) {
      Logger::warning(LogCategory::TRANSFER, "compareAndUpdateRecord",
                      "Multiple records found for " + schemaName + "." +
                          tableName + " - using first");
    }

    const auto &currentRow = result[0];

    if (static_cast<size_t>(currentRow.size()) != columnNames.size()) {
      Logger::error(
          LogCategory::TRANSFER, "compareAndUpdateRecord",
          "Column count mismatch: " + std::to_string(currentRow.size()) +
              " vs " + std::to_string(columnNames.size()));
      return false;
    }

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
          std::string escaped = cleanNewValue;
          size_t pos = 0;
          while ((pos = escaped.find("'", pos)) != std::string::npos) {
            escaped.replace(pos, 1, "''");
            pos += 2;
          }
          valueToSet = "'" + escaped + "'";
        }

        updateFields.push_back("\"" + columnName + "\" = " + valueToSet);
        hasChanges = true;
      }
    }

    if (hasChanges) {
      pqxx::work updateTxn(pgConn);
      std::string updateQuery = "UPDATE " + updateTxn.quote_name(schemaName) +
                                "." + updateTxn.quote_name(tableName) + " SET ";
      for (size_t i = 0; i < updateFields.size(); ++i) {
        if (i > 0)
          updateQuery += ", ";
        updateQuery += updateFields[i];
      }
      updateQuery += " WHERE " + whereClause;

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
    if (results.empty() || columnNames.empty() || columnTypes.empty()) {
      Logger::warning(LogCategory::TRANSFER, "performBulkInsert",
                      "Empty results, columns, or types - nothing to insert");
      return;
    }

    if (columnNames.size() != columnTypes.size()) {
      Logger::error(LogCategory::TRANSFER, "performBulkInsert",
                    "Mismatch between column names and types count");
      throw std::invalid_argument("Column names and types count mismatch");
    }

    std::string lowerTableName = tableName;
    std::transform(lowerTableName.begin(), lowerTableName.end(),
                   lowerTableName.begin(), ::tolower);

    pqxx::work txn(pgConn);
    txn.exec("SET statement_timeout = '" +
             std::to_string(STATEMENT_TIMEOUT_SECONDS) + "s'");

    size_t rawBatchSize = SyncConfig::getChunkSize();
    const size_t BATCH_SIZE =
        (rawBatchSize == 0 || rawBatchSize > MAX_BATCH_SIZE)
            ? DEFAULT_BATCH_SIZE
            : rawBatchSize;
    size_t totalProcessed = 0;

    std::string baseInsertQuery = "INSERT INTO " +
                                  txn.quote_name(lowerSchemaName) + "." +
                                  txn.quote_name(lowerTableName) + " (";
    for (size_t i = 0; i < columnNames.size(); ++i) {
      if (i > 0)
        baseInsertQuery += ", ";
      std::string col = columnNames[i];
      std::transform(col.begin(), col.end(), col.begin(), ::tolower);
      baseInsertQuery += txn.quote_name(col);
    }
    baseInsertQuery += ") VALUES ";

    for (size_t batchStart = 0; batchStart < results.size();
         batchStart += BATCH_SIZE) {
      size_t batchEnd = std::min(batchStart + BATCH_SIZE, results.size());

      std::string batchQuery = baseInsertQuery;
      std::vector<std::string> values;
      size_t querySize = baseInsertQuery.length();

      for (size_t i = batchStart; i < batchEnd; ++i) {
        const auto &row = results[i];
        if (row.size() != columnNames.size()) {
          Logger::warning(LogCategory::TRANSFER, "performBulkInsert",
                          "Row size mismatch: " + std::to_string(row.size()) +
                              " vs " + std::to_string(columnNames.size()));
          continue;
        }

        std::string rowValues = "(";
        for (size_t j = 0; j < row.size() && j < columnNames.size(); ++j) {
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
              rowValues += txn.quote(cleanValue);
            }
          }
        }
        rowValues += ")";

        if (querySize + rowValues.length() + 10 > MAX_QUERY_SIZE &&
            !values.empty()) {
          break;
        }

        values.push_back(rowValues);
        querySize += rowValues.length() + 2;
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
        txn.exec("SET statement_timeout = '" +
                 std::to_string(STATEMENT_TIMEOUT_SECONDS) + "s'");
        txn.exec(batch.batchQuery);
        txn.commit();

        result.rowsProcessed = batch.batchSize;
        result.success = true;
        totalProcessed += batch.batchSize;

      } catch (const std::exception &e) {
        result.errorMessage = e.what();
        result.rowsProcessed = 0;

        try {
          pqxx::work abortTxn(pgConn);
          abortTxn.abort();
        } catch (...) {
        }

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
    if (results.empty() || columnNames.empty() || columnTypes.empty()) {
      Logger::warning(LogCategory::TRANSFER, "performBulkUpsert",
                      "Empty results, columns, or types - nothing to upsert");
      return;
    }

    if (columnNames.size() != columnTypes.size()) {
      Logger::error(LogCategory::TRANSFER, "performBulkUpsert",
                    "Mismatch between column names and types count");
      throw std::invalid_argument("Column names and types count mismatch");
    }

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
    txn.exec("SET statement_timeout = '" +
             std::to_string(STATEMENT_TIMEOUT_SECONDS) + "s'");

    size_t rawBatchSize = SyncConfig::getChunkSize();
    const size_t BATCH_SIZE =
        (rawBatchSize == 0 || rawBatchSize > MAX_BATCH_SIZE)
            ? DEFAULT_BATCH_SIZE
            : rawBatchSize;
    size_t totalProcessed = 0;

    auto buildRowValues = [&](const std::vector<std::string> &row,
                              pqxx::work &workTxn) -> std::string {
      std::string rowValues = "(";
      for (size_t j = 0; j < row.size() && j < columnNames.size(); ++j) {
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
            rowValues += workTxn.quote(cleanValue);
          }
        }
      }
      rowValues += ")";
      return rowValues;
    };

    for (size_t batchStart = 0; batchStart < results.size();
         batchStart += BATCH_SIZE) {
      size_t batchEnd = std::min(batchStart + BATCH_SIZE, results.size());

      std::string batchQuery = upsertQuery;
      std::vector<std::string> values;
      size_t querySize = upsertQuery.length() + conflictClause.length();

      // Track PK values to detect duplicates within batch
      std::set<std::string> seenPKs;

      for (size_t i = batchStart; i < batchEnd; ++i) {
        const auto &row = results[i];
        if (row.size() != columnNames.size()) {
          Logger::warning(LogCategory::TRANSFER, "performBulkUpsert",
                          "Row size mismatch: " + std::to_string(row.size()) +
                              " vs " + std::to_string(columnNames.size()));
          continue;
        }

        // Build PK key for duplicate detection
        std::string pkKey;
        bool hasAllPKs = true;
        for (const auto &pkCol : pkColumns) {
          auto it = std::find(columnNames.begin(), columnNames.end(), pkCol);
          if (it != columnNames.end()) {
            size_t pkIdx = std::distance(columnNames.begin(), it);
            if (pkIdx < row.size()) {
              if (!pkKey.empty())
                pkKey += "|";
              std::string pkValue = row[pkIdx];
              if (pkValue.empty() || pkValue == "NULL" || pkValue == "null") {
                pkKey += "<NULL>";
                hasAllPKs = false;
              } else {
                pkKey += pkValue;
              }
            } else {
              hasAllPKs = false;
            }
          } else {
            hasAllPKs = false;
          }
        }

        if (!pkColumns.empty()) {
          if (hasAllPKs && !pkKey.empty()) {
            if (seenPKs.find(pkKey) != seenPKs.end()) {
              Logger::warning(LogCategory::TRANSFER, "performBulkUpsert",
                              "Skipping duplicate PK in batch: " + pkKey);
              continue;
            }
            seenPKs.insert(pkKey);
          } else {
            Logger::warning(LogCategory::TRANSFER, "performBulkUpsert",
                            "Skipping row with incomplete PK for " +
                                lowerSchemaName + "." + tableName);
            continue;
          }
        }

        std::string rowValues = buildRowValues(row, txn);

        if (querySize + rowValues.length() + 10 > MAX_QUERY_SIZE &&
            !values.empty()) {
          break;
        }

        values.push_back(rowValues);
        querySize += rowValues.length() + 2;
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

          if (errorMsg.find("violates not-null constraint") !=
              std::string::npos) {
            Logger::warning(
                LogCategory::TRANSFER, "performBulkUpsert",
                "NOT NULL constraint violation detected for " +
                    lowerSchemaName + "." + tableName +
                    " - attempting to alter columns to allow NULLs");

            try {
              txn.abort();
            } catch (...) {
            }

            std::set<std::string> columnsToAlter;
            size_t searchPos = 0;
            while (true) {
              size_t startPos = errorMsg.find("column \"", searchPos);
              if (startPos == std::string::npos)
                break;
              startPos += 8;
              size_t endPos = errorMsg.find("\"", startPos);
              if (endPos != std::string::npos) {
                std::string columnName =
                    errorMsg.substr(startPos, endPos - startPos);
                std::transform(columnName.begin(), columnName.end(),
                               columnName.begin(), ::tolower);
                columnsToAlter.insert(columnName);
              }
              searchPos = endPos + 1;
            }

            if (!columnsToAlter.empty()) {
              try {
                pqxx::work alterTxn(pgConn);
                for (const auto &columnName : columnsToAlter) {
                  alterTxn.exec("ALTER TABLE \"" + lowerSchemaName + "\".\"" +
                                tableName + "\" ALTER COLUMN \"" + columnName +
                                "\" DROP NOT NULL");
                  Logger::info(LogCategory::TRANSFER, "performBulkUpsert",
                               "Altered column " + columnName +
                                   " to allow NULLs for " + lowerSchemaName +
                                   "." + tableName);
                }
                alterTxn.commit();

                pqxx::work retryTxn(pgConn);
                retryTxn.exec("SET statement_timeout = '" +
                              std::to_string(STATEMENT_TIMEOUT_SECONDS) + "s'");
                retryTxn.exec(batchQuery);
                retryTxn.commit();
                totalProcessed += values.size();
                continue;
              } catch (const std::exception &alterError) {
                Logger::error(LogCategory::TRANSFER, "performBulkUpsert",
                              "Failed to alter columns or retry: " +
                                  std::string(alterError.what()));
              }
            }
          }

          if (errorMsg.find("current transaction is aborted") !=
              std::string::npos) {
            Logger::warning(LogCategory::TRANSFER, "performBulkUpsert",
                            "Transaction aborted detected, rolling back and "
                            "processing individually");

            try {
              txn.abort();
            } catch (const std::exception &e) {
            } catch (...) {
            }

            size_t individualProcessed = 0;

            for (size_t i = batchStart;
                 i < batchEnd &&
                 individualProcessed < MAX_INDIVIDUAL_PROCESSING;
                 ++i) {
              try {
                const auto &row = results[i];
                if (row.size() != columnNames.size()) {
                  Logger::warning(LogCategory::TRANSFER, "performBulkUpsert",
                                  "Row size mismatch in individual processing");
                  continue;
                }

                pqxx::work singleTxn(pgConn);
                singleTxn.exec("SET statement_timeout = '" +
                               std::to_string(STATEMENT_TIMEOUT_SECONDS) +
                               "s'");

                std::string singleRowValues = buildRowValues(row, singleTxn);
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

            if (individualProcessed >=
                DatabaseToPostgresSync::MAX_INDIVIDUAL_PROCESSING) {
              Logger::warning(
                  LogCategory::TRANSFER, "performBulkUpsert",
                  "Hit maximum individual processing limit (" +
                      std::to_string(
                          DatabaseToPostgresSync::MAX_INDIVIDUAL_PROCESSING) +
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

            for (size_t i = batchStart;
                 i < batchEnd &&
                 binaryErrorProcessed <
                     DatabaseToPostgresSync::MAX_BINARY_ERROR_PROCESSING;
                 ++i) {
              try {
                const auto &row = results[i];
                if (row.size() != columnNames.size()) {
                  Logger::warning(
                      LogCategory::TRANSFER, "performBulkUpsert",
                      "Row size mismatch in binary error processing");
                  continue;
                }

                std::string singleRowValues = buildRowValues(row, txn);

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

            if (binaryErrorProcessed >=
                DatabaseToPostgresSync::MAX_BINARY_ERROR_PROCESSING) {
              Logger::warning(
                  LogCategory::TRANSFER, "performBulkUpsert",
                  "Hit maximum binary error processing limit (" +
                      std::to_string(
                          DatabaseToPostgresSync::MAX_BINARY_ERROR_PROCESSING) +
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
        Logger::error(LogCategory::TRANSFER, "performBulkUpsert",
                      "Error committing transaction: " +
                          std::string(commitError.what()));
        throw;
      }
    }

  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "performBulkUpsert",
                  "Error in bulk upsert: " + std::string(e.what()));
    throw;
  }
}

void DatabaseToPostgresSync::performBulkUpsertNoPK(
    pqxx::connection &pgConn,
    const std::vector<std::vector<std::string>> &results,
    const std::vector<std::string> &columnNames,
    const std::vector<std::string> &columnTypes,
    const std::string &lowerSchemaName, const std::string &tableName,
    const std::string &sourceSchemaName) {
  try {
    if (results.empty() || columnNames.empty() || columnTypes.empty()) {
      Logger::warning(LogCategory::TRANSFER, "performBulkUpsertNoPK",
                      "Empty results, columns, or types - nothing to upsert");
      return;
    }

    if (columnNames.size() != columnTypes.size()) {
      Logger::error(LogCategory::TRANSFER, "performBulkUpsertNoPK",
                    "Mismatch between column names and types count");
      throw std::invalid_argument("Column names and types count mismatch");
    }

    std::string lowerTableName = tableName;
    std::transform(lowerTableName.begin(), lowerTableName.end(),
                   lowerTableName.begin(), ::tolower);

    pqxx::work prepTxn(pgConn);
    std::string upsertQuery = "INSERT INTO " +
                              prepTxn.quote_name(lowerSchemaName) + "." +
                              prepTxn.quote_name(lowerTableName) + " (";
    for (size_t i = 0; i < columnNames.size(); ++i) {
      if (i > 0)
        upsertQuery += ", ";
      std::string col = columnNames[i];
      std::transform(col.begin(), col.end(), col.begin(), ::tolower);
      upsertQuery += prepTxn.quote_name(col);
    }
    upsertQuery += ") VALUES ";

    std::string conflictClause = " ON CONFLICT (";
    for (size_t i = 0; i < columnNames.size(); ++i) {
      if (i > 0)
        conflictClause += ", ";
      std::string col = columnNames[i];
      std::transform(col.begin(), col.end(), col.begin(), ::tolower);
      conflictClause += prepTxn.quote_name(col);
    }
    conflictClause += ") DO UPDATE SET ";
    for (size_t i = 0; i < columnNames.size(); ++i) {
      if (i > 0)
        conflictClause += ", ";
      std::string col = columnNames[i];
      std::transform(col.begin(), col.end(), col.begin(), ::tolower);
      conflictClause +=
          prepTxn.quote_name(col) + " = EXCLUDED." + prepTxn.quote_name(col);
    }
    prepTxn.commit();

    pqxx::work txn(pgConn);
    txn.exec("SET statement_timeout = '" +
             std::to_string(STATEMENT_TIMEOUT_SECONDS) + "s'");

    size_t rawBatchSize = SyncConfig::getChunkSize();
    const size_t BATCH_SIZE =
        (rawBatchSize == 0 || rawBatchSize > MAX_BATCH_SIZE)
            ? DEFAULT_BATCH_SIZE
            : rawBatchSize;
    size_t totalProcessed = 0;

    auto buildRowValues = [&](const std::vector<std::string> &row,
                              pqxx::work &workTxn) -> std::string {
      std::string rowValues = "(";
      for (size_t j = 0; j < row.size() && j < columnNames.size(); ++j) {
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
            rowValues += workTxn.quote(cleanValue);
          }
        }
      }
      rowValues += ")";
      return rowValues;
    };

    for (size_t batchStart = 0; batchStart < results.size();
         batchStart += BATCH_SIZE) {
      size_t batchEnd = std::min(batchStart + BATCH_SIZE, results.size());

      std::string batchQuery = upsertQuery;
      std::vector<std::string> values;
      size_t querySize = upsertQuery.length() + conflictClause.length();

      for (size_t i = batchStart; i < batchEnd; ++i) {
        const auto &row = results[i];
        if (row.size() != columnNames.size()) {
          Logger::warning(LogCategory::TRANSFER, "performBulkUpsertNoPK",
                          "Row size mismatch: " + std::to_string(row.size()) +
                              " vs " + std::to_string(columnNames.size()));
          continue;
        }

        std::string rowValues = buildRowValues(row, txn);

        if (querySize + rowValues.length() + 10 > MAX_QUERY_SIZE &&
            !values.empty()) {
          break;
        }

        values.push_back(rowValues);
        querySize += rowValues.length() + 2;
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

          if (errorMsg.find("violates not-null constraint") !=
              std::string::npos) {
            Logger::warning(
                LogCategory::TRANSFER, "performBulkUpsertNoPK",
                "NOT NULL constraint violation detected for " +
                    lowerSchemaName + "." + tableName +
                    " - attempting to alter columns to allow NULLs");

            try {
              txn.abort();
            } catch (...) {
            }

            std::set<std::string> columnsToAlter;
            size_t searchPos = 0;
            while (true) {
              size_t startPos = errorMsg.find("column \"", searchPos);
              if (startPos == std::string::npos)
                break;
              startPos += 8;
              size_t endPos = errorMsg.find("\"", startPos);
              if (endPos != std::string::npos) {
                std::string columnName =
                    errorMsg.substr(startPos, endPos - startPos);
                std::transform(columnName.begin(), columnName.end(),
                               columnName.begin(), ::tolower);
                columnsToAlter.insert(columnName);
              }
              searchPos = endPos + 1;
            }

            if (!columnsToAlter.empty()) {
              try {
                pqxx::work alterTxn(pgConn);
                for (const auto &columnName : columnsToAlter) {
                  alterTxn.exec("ALTER TABLE \"" + lowerSchemaName + "\".\"" +
                                tableName + "\" ALTER COLUMN \"" + columnName +
                                "\" DROP NOT NULL");
                  Logger::info(LogCategory::TRANSFER, "performBulkUpsertNoPK",
                               "Altered column " + columnName +
                                   " to allow NULLs for " + lowerSchemaName +
                                   "." + tableName);
                }
                alterTxn.commit();

                pqxx::work retryTxn(pgConn);
                retryTxn.exec("SET statement_timeout = '" +
                              std::to_string(STATEMENT_TIMEOUT_SECONDS) + "s'");
                retryTxn.exec(batchQuery);
                retryTxn.commit();
                totalProcessed += values.size();
                continue;
              } catch (const std::exception &alterError) {
                Logger::error(LogCategory::TRANSFER, "performBulkUpsertNoPK",
                              "Failed to alter columns: " +
                                  std::string(alterError.what()));
              }
            }
          }

          if (errorMsg.find("ON CONFLICT DO UPDATE command cannot affect row a "
                            "second time") != std::string::npos) {
            Logger::warning(
                LogCategory::TRANSFER, "performBulkUpsertNoPK",
                "Duplicate rows in batch for " + lowerSchemaName + "." +
                    tableName + " - processing individually (up to " +
                    std::to_string(MAX_INDIVIDUAL_PROCESSING) + " rows)");

            try {
              txn.abort();
            } catch (...) {
            }

            size_t individualCount = 0;
            for (size_t i = 0; i < values.size() &&
                               individualCount < MAX_INDIVIDUAL_PROCESSING;
                 ++i) {
              try {
                pqxx::work individualTxn(pgConn);
                individualTxn.exec("SET statement_timeout = '" +
                                   std::to_string(STATEMENT_TIMEOUT_SECONDS) +
                                   "s'");
                std::string individualQuery =
                    upsertQuery + values[i] + conflictClause;
                individualTxn.exec(individualQuery);
                individualTxn.commit();
                individualCount++;
                totalProcessed++;
              } catch (const std::exception &individualError) {
                Logger::warning(LogCategory::TRANSFER, "performBulkUpsertNoPK",
                                "Failed to process individual row: " +
                                    std::string(individualError.what()));
              }
            }

            if (individualCount < values.size()) {
              Logger::error(
                  LogCategory::TRANSFER, "performBulkUpsertNoPK",
                  "CRITICAL ERROR: Bulk upsert failed for chunk in table " +
                      lowerSchemaName + "." + tableName +
                      ": Too many duplicate rows. Processed " +
                      std::to_string(individualCount) + " of " +
                      std::to_string(values.size()) + " rows individually.");
            }
            continue;
          }

          Logger::error(LogCategory::TRANSFER, "performBulkUpsertNoPK",
                        "Error in bulk upsert: " + std::string(e.what()));
          throw;
        }
      }
    }

    txn.commit();

    Logger::info(LogCategory::TRANSFER, "performBulkUpsertNoPK",
                 "Upserted " + std::to_string(totalProcessed) + " rows into " +
                     lowerSchemaName + "." + tableName);

  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "performBulkUpsertNoPK",
                  "Error in bulk upsert: " + std::string(e.what()));
    throw;
  }
}
