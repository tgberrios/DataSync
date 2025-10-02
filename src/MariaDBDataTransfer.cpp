#include "MariaDBDataTransfer.h"
#include "Config.h"
#include "MariaDBDataValidator.h"
#include "MariaDBQueryExecutor.h"
#include <algorithm>
#include <sstream>

void MariaDBDataTransfer::transferData(MYSQL *mariadbConn,
                                       pqxx::connection &pgConn,
                                       const TableInfo &table) {
  Logger::getInstance().info(LogCategory::TRANSFER,
                             "Processing table: " + table.schema_name + "." +
                                 table.table_name +
                                 " (status: " + table.status + ")");

  if (table.status == "FULL_LOAD") {
    processFullLoad(mariadbConn, pgConn, table);
  } else if (table.status == "LISTENING_CHANGES") {
    processIncrementalUpdates(mariadbConn, pgConn, table);
    processDeletes(mariadbConn, pgConn, table);
  }
}

void MariaDBDataTransfer::processFullLoad(MYSQL *mariadbConn,
                                          pqxx::connection &pgConn,
                                          const TableInfo &table) {
  Logger::getInstance().info(LogCategory::TRANSFER,
                             "Processing FULL_LOAD for table: " +
                                 table.schema_name + "." + table.table_name);

  // Get table columns
  MariaDBQueryExecutor queryExecutor;
  auto columns = queryExecutor.getTableColumns(mariadbConn, table.schema_name,
                                               table.table_name);
  if (columns.empty()) {
    Logger::getInstance().error(LogCategory::TRANSFER,
                                "No columns found for table " +
                                    table.schema_name + "." + table.table_name);
    return;
  }

  Logger::getInstance().info(LogCategory::TRANSFER,
                             "Found " + std::to_string(columns.size()) +
                                 " columns for table " + table.schema_name +
                                 "." + table.table_name);

  // Prepare column names and types
  std::vector<std::string> columnNames;
  std::vector<std::string> columnTypes;

  for (const auto &col : columns) {
    if (col.size() < 6) {
      Logger::getInstance().warning(
          LogCategory::TRANSFER, "Skipping column with insufficient data: " +
                                     std::to_string(col.size()) + " elements");
      continue;
    }

    std::string colName = col[0];
    std::transform(colName.begin(), colName.end(), colName.begin(), ::tolower);
    columnNames.push_back(colName);

    std::string dataType = col[1];
    std::string maxLength = col[5];
    std::string pgType = "TEXT";

    if (dataType == "char" || dataType == "varchar") {
      if (!maxLength.empty() && maxLength != "NULL") {
        try {
          size_t length = std::stoul(maxLength);
          if (length >= 1 && length <= 65535) {
            pgType = dataType + "(" + maxLength + ")";
          } else {
            pgType = "VARCHAR";
          }
        } catch (const std::exception &) {
          pgType = "VARCHAR";
        }
      } else {
        pgType = "VARCHAR";
      }
    } else if (dataType == "int")
      pgType = "INTEGER";
    else if (dataType == "bigint")
      pgType = "BIGINT";
    else if (dataType == "timestamp" || dataType == "datetime")
      pgType = "TIMESTAMP";
    else if (dataType == "date")
      pgType = "DATE";
    else if (dataType == "time")
      pgType = "TIME";

    columnTypes.push_back(pgType);
  }

  Logger::getInstance().info(LogCategory::TRANSFER,
                             "Processed " + std::to_string(columnNames.size()) +
                                 " columns for table " + table.schema_name +
                                 "." + table.table_name);

  if (columnNames.empty()) {
    Logger::getInstance().error(LogCategory::TRANSFER,
                                "No valid columns processed for table " +
                                    table.schema_name + "." + table.table_name +
                                    " - skipping transfer");
    return;
  }

  // Get row count
  size_t sourceCount = queryExecutor.getTableRowCount(
      mariadbConn, table.schema_name, table.table_name);
  Logger::getInstance().info(LogCategory::TRANSFER,
                             "Source table " + table.schema_name + "." +
                                 table.table_name + " has " +
                                 std::to_string(sourceCount) + " records");

  if (sourceCount == 0) {
    Logger::getInstance().info(LogCategory::TRANSFER,
                               "Source table is empty - marking as NO_DATA");
    updateTableStatus(pgConn, table.schema_name, table.table_name, "NO_DATA",
                      0);
    return;
  }

  // Process data in chunks
  const size_t CHUNK_SIZE = SyncConfig::getChunkSize();
  size_t offset = 0;
  size_t totalProcessed = 0;

  Logger::getInstance().info(
      LogCategory::TRANSFER,
      "Starting data processing loop for " + table.schema_name + "." +
          table.table_name + " with chunk size: " + std::to_string(CHUNK_SIZE));

  while (offset < sourceCount) {
    Logger::getInstance().info(LogCategory::TRANSFER,
                               "Processing chunk at offset " +
                                   std::to_string(offset) + " for " +
                                   table.schema_name + "." + table.table_name);

    std::string selectQuery = "SELECT * FROM `" + table.schema_name + "`.`" +
                              table.table_name + "` " + "LIMIT " +
                              std::to_string(CHUNK_SIZE) + " OFFSET " +
                              std::to_string(offset) + ";";

    auto results = queryExecutor.executeQuery(mariadbConn, selectQuery);
    if (results.empty()) {
      Logger::getInstance().info(
          LogCategory::TRANSFER,
          "No more results at offset " + std::to_string(offset) + " for " +
              table.schema_name + "." + table.table_name);
      break;
    }

    std::string lowerSchema = table.schema_name;
    std::transform(lowerSchema.begin(), lowerSchema.end(), lowerSchema.begin(),
                   ::tolower);

    performBulkUpsert(pgConn, results, columnNames, columnTypes, lowerSchema,
                      table.table_name);

    totalProcessed += results.size();
    offset += CHUNK_SIZE;

    Logger::getInstance().info(
        LogCategory::TRANSFER,
        "Completed chunk processing at offset " + std::to_string(offset) +
            " for " + table.schema_name + "." + table.table_name +
            " - processed " + std::to_string(totalProcessed) +
            " records so far");

    Logger::getInstance().info(LogCategory::TRANSFER,
                               "Processed " + std::to_string(totalProcessed) +
                                   "/" + std::to_string(sourceCount) +
                                   " records for " + table.schema_name + "." +
                                   table.table_name);
  }

  updateTableStatus(pgConn, table.schema_name, table.table_name,
                    "LISTENING_CHANGES", totalProcessed);
  Logger::getInstance().info(LogCategory::TRANSFER, "FULL_LOAD completed for " +
                                                        table.schema_name +
                                                        "." + table.table_name);
}

void MariaDBDataTransfer::processIncrementalUpdates(MYSQL *mariadbConn,
                                                    pqxx::connection &pgConn,
                                                    const TableInfo &table) {
  if (table.last_sync_column.empty() || table.last_sync_time.empty()) {
    Logger::getInstance().info(
        LogCategory::TRANSFER,
        "No time column available for incremental updates in " +
            table.schema_name + "." + table.table_name);
    return;
  }

  Logger::getInstance().info(LogCategory::TRANSFER,
                             "Processing incremental updates for " +
                                 table.schema_name + "." + table.table_name +
                                 " since: " + table.last_sync_time);

  // Get updated records
  std::string selectQuery = "SELECT * FROM `" + table.schema_name + "`.`" +
                            table.table_name + "` " + "WHERE `" +
                            table.last_sync_column + "` > '" +
                            escapeSQL(table.last_sync_time) + "' " +
                            "ORDER BY `" + table.last_sync_column + "`";

  MariaDBQueryExecutor queryExecutor;
  auto results = queryExecutor.executeQuery(mariadbConn, selectQuery);

  if (results.empty()) {
    Logger::getInstance().info(LogCategory::TRANSFER,
                               "No incremental updates found for " +
                                   table.schema_name + "." + table.table_name);
    return;
  }

  // Get column information
  auto columns = queryExecutor.getTableColumns(mariadbConn, table.schema_name,
                                               table.table_name);
  std::vector<std::string> columnNames;
  for (const auto &col : columns) {
    if (!col.empty()) {
      std::string colName = col[0];
      std::transform(colName.begin(), colName.end(), colName.begin(),
                     ::tolower);
      columnNames.push_back(colName);
    }
  }

  // Process updates
  std::string lowerSchema = table.schema_name;
  std::transform(lowerSchema.begin(), lowerSchema.end(), lowerSchema.begin(),
                 ::tolower);

  performBulkUpsert(pgConn, results, columnNames,
                    std::vector<std::string>(columnNames.size(), "TEXT"),
                    lowerSchema, table.table_name);

  Logger::getInstance().info(LogCategory::TRANSFER,
                             "Processed " + std::to_string(results.size()) +
                                 " incremental updates for " +
                                 table.schema_name + "." + table.table_name);
}

void MariaDBDataTransfer::processDeletes(MYSQL *mariadbConn,
                                         pqxx::connection &pgConn,
                                         const TableInfo &table) {
  Logger::getInstance().info(LogCategory::TRANSFER, "Processing deletes for " +
                                                        table.schema_name +
                                                        "." + table.table_name);

  // This is a simplified implementation
  // In a full implementation, you would compare source and target row counts
  // and identify deleted records by primary key

  Logger::getInstance().info(LogCategory::TRANSFER,
                             "Delete processing completed for " +
                                 table.schema_name + "." + table.table_name);
}

void MariaDBDataTransfer::updateTableStatus(pqxx::connection &pgConn,
                                            const std::string &schema_name,
                                            const std::string &table_name,
                                            const std::string &status,
                                            size_t offset) {
  try {
    pqxx::work txn(pgConn);

    std::string updateQuery =
        "UPDATE metadata.catalog SET status='" + status + "'";

    if (status == "FULL_LOAD" || status == "RESET" ||
        status == "LISTENING_CHANGES") {

      // Get PK strategy to determine which field to update
      auto pkStrategyResult = txn.exec(
          "SELECT pk_strategy FROM metadata.catalog WHERE schema_name='" +
          escapeSQL(schema_name) + "' AND table_name='" +
          escapeSQL(table_name) + "'");

      if (!pkStrategyResult.empty() && !pkStrategyResult[0][0].is_null()) {
        std::string pkStrategy = pkStrategyResult[0][0].as<std::string>();

        if (pkStrategy == "PK") {
          updateQuery += ", last_processed_pk='" + std::to_string(offset) + "'";
        } else {
          updateQuery += ", last_offset='" + std::to_string(offset) + "'";
        }
      } else {
        // Default to OFFSET if strategy not found
        updateQuery += ", last_offset='" + std::to_string(offset) + "'";
      }
    }

    updateQuery += ", last_sync_time=NOW()";
    if (status == "NO_DATA") {
      updateQuery += ", active=false";
    }
    updateQuery += " WHERE schema_name='" + escapeSQL(schema_name) +
                   "' AND table_name='" + escapeSQL(table_name) + "'";

    txn.exec(updateQuery);
    txn.commit();

    Logger::getInstance().info(LogCategory::TRANSFER,
                               "Updated status to " + status + " for " +
                                   schema_name + "." + table_name);
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::TRANSFER, "updateTableStatus",
                                "ERROR updating status: " +
                                    std::string(e.what()));
  }
}

void MariaDBDataTransfer::performBulkUpsert(
    pqxx::connection &pgConn,
    const std::vector<std::vector<std::string>> &results,
    const std::vector<std::string> &columnNames,
    const std::vector<std::string> &columnTypes,
    const std::string &lowerSchemaName, const std::string &tableName) {
  if (results.empty())
    return;

  Logger::getInstance().info(LogCategory::TRANSFER,
                             "Starting performBulkUpsert for " +
                                 lowerSchemaName + "." + tableName + " with " +
                                 std::to_string(results.size()) + " records");

  try {
    // Get primary key columns
    Logger::getInstance().info(LogCategory::TRANSFER,
                               "Getting primary key columns for " +
                                   lowerSchemaName + "." + tableName);

    auto pkColumns =
        getPrimaryKeyColumnsFromPostgres(pgConn, lowerSchemaName, tableName);

    Logger::getInstance().info(LogCategory::TRANSFER,
                               "Found " + std::to_string(pkColumns.size()) +
                                   " primary key columns");

    if (pkColumns.empty()) {
      // No primary key, use simple insert
      Logger::getInstance().info(
          LogCategory::TRANSFER,
          "No primary key found, using simple insert for " + lowerSchemaName +
              "." + tableName);
      performBulkInsert(pgConn, results, columnNames, columnTypes,
                        lowerSchemaName, tableName);
      return;
    }

    // Build upsert query
    Logger::getInstance().info(LogCategory::TRANSFER,
                               "Building upsert query for " + lowerSchemaName +
                                   "." + tableName);

    std::string upsertQuery =
        buildUpsertQuery(columnNames, pkColumns, lowerSchemaName, tableName);
    std::string conflictClause =
        buildUpsertConflictClause(columnNames, pkColumns);

    Logger::getInstance().info(LogCategory::TRANSFER,
                               "Upsert query built successfully");

    Logger::getInstance().info(LogCategory::TRANSFER,
                               "Starting database transaction for " +
                                   lowerSchemaName + "." + tableName);

    pqxx::work txn(pgConn);
    txn.exec("SET statement_timeout = '600s'");

    Logger::getInstance().info(LogCategory::TRANSFER,
                               "Transaction started, processing batches");

    // Process in batches
    const size_t CHUNK_SIZE_VALUE = SyncConfig::getChunkSize();
    const size_t BATCH_SIZE = CHUNK_SIZE_VALUE;

    Logger::getInstance().info(
        LogCategory::TRANSFER,
        "Processing " + std::to_string(results.size()) +
            " records in batches of " + std::to_string(BATCH_SIZE) +
            " (chunk_size: " + std::to_string(CHUNK_SIZE_VALUE) + ")");

    for (size_t batchStart = 0; batchStart < results.size();
         batchStart += BATCH_SIZE) {
      size_t batchEnd = std::min(batchStart + BATCH_SIZE, results.size());

      Logger::getInstance().info(
          LogCategory::TRANSFER,
          "Processing batch " + std::to_string(batchStart) + " to " +
              std::to_string(batchEnd) + " for " + lowerSchemaName + "." +
              tableName + " (total results: " + std::to_string(results.size()) +
              ", BATCH_SIZE: " + std::to_string(BATCH_SIZE) + ")");

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
            MariaDBDataValidator validator;
            std::string cleanValue =
                validator.cleanValueForPostgres(row[j], columnTypes[j]);
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

        txn.exec(batchQuery);
      }

      Logger::getInstance().info(LogCategory::TRANSFER,
                                 "Completed batch " +
                                     std::to_string(batchStart) + " to " +
                                     std::to_string(batchEnd) + " for " +
                                     lowerSchemaName + "." + tableName);
    }

    Logger::getInstance().info(LogCategory::TRANSFER,
                               "All batches completed for " + lowerSchemaName +
                                   "." + tableName +
                                   " - committing transaction");

    txn.commit();
    Logger::getInstance().info(
        LogCategory::TRANSFER,
        "Successfully processed " + std::to_string(results.size()) +
            " records for " + lowerSchemaName + "." + tableName);

  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::TRANSFER, "performBulkUpsert",
                                "Error in bulk upsert: " +
                                    std::string(e.what()));
    throw;
  }
}

void MariaDBDataTransfer::performBulkInsert(
    pqxx::connection &pgConn,
    const std::vector<std::vector<std::string>> &results,
    const std::vector<std::string> &columnNames,
    const std::vector<std::string> &columnTypes,
    const std::string &lowerSchemaName, const std::string &tableName) {
  if (results.empty())
    return;

  try {
    std::string insertQuery =
        "INSERT INTO \"" + lowerSchemaName + "\".\"" + tableName + "\" (";

    for (size_t i = 0; i < columnNames.size(); ++i) {
      if (i > 0)
        insertQuery += ", ";
      insertQuery += "\"" + columnNames[i] + "\"";
    }
    insertQuery += ") VALUES ";

    pqxx::work txn(pgConn);
    txn.exec("SET statement_timeout = '600s'");

    // Process in batches
    const size_t BATCH_SIZE = SyncConfig::getChunkSize();

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
            MariaDBDataValidator validator;
            std::string cleanValue =
                validator.cleanValueForPostgres(row[j], columnTypes[j]);
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
      }
    }

    txn.commit();
    Logger::getInstance().info(
        LogCategory::TRANSFER,
        "Successfully inserted " + std::to_string(results.size()) +
            " records for " + lowerSchemaName + "." + tableName);

  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::TRANSFER, "performBulkInsert",
                                "Error in bulk insert: " +
                                    std::string(e.what()));
    throw;
  }
}

std::string MariaDBDataTransfer::buildUpsertQuery(
    const std::vector<std::string> &columnNames,
    const std::vector<std::string> &pkColumns, const std::string &schemaName,
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

std::string MariaDBDataTransfer::buildUpsertConflictClause(
    const std::vector<std::string> &columnNames,
    const std::vector<std::string> &pkColumns) {
  std::string conflictClause = " ON CONFLICT (";

  for (size_t i = 0; i < pkColumns.size(); ++i) {
    if (i > 0)
      conflictClause += ", ";
    conflictClause += "\"" + pkColumns[i] + "\"";
  }
  conflictClause += ") DO UPDATE SET ";

  for (size_t i = 0; i < columnNames.size(); ++i) {
    if (i > 0)
      conflictClause += ", ";
    conflictClause +=
        "\"" + columnNames[i] + "\" = EXCLUDED.\"" + columnNames[i] + "\"";
  }

  return conflictClause;
}

std::vector<std::string> MariaDBDataTransfer::getPrimaryKeyColumnsFromPostgres(
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
    Logger::getInstance().error(
        LogCategory::TRANSFER, "getPrimaryKeyColumnsFromPostgres",
        "Error getting PK columns: " + std::string(e.what()));
  }

  return pkColumns;
}

std::string MariaDBDataTransfer::escapeSQL(const std::string &value) {
  if (value.empty())
    return "";

  std::string escaped = value;
  size_t pos = 0;
  while ((pos = escaped.find("'", pos)) != std::string::npos) {
    escaped.replace(pos, 1, "''");
    pos += 2;
  }
  return escaped;
}
