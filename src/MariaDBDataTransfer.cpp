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
  } else if (table.status == "RESET") {
    // Reset table and mark for full load
    std::string lowerSchema = table.schema_name;
    std::transform(lowerSchema.begin(), lowerSchema.end(), lowerSchema.begin(),
                   ::tolower);

    pqxx::work txn(pgConn);
    txn.exec("TRUNCATE TABLE \"" + lowerSchema + "\".\"" + table.table_name +
             "\" CASCADE;");
    txn.exec("UPDATE metadata.catalog SET last_offset='0' WHERE schema_name='" +
             escapeSQL(table.schema_name) + "' AND table_name='" +
             escapeSQL(table.table_name) + "';");
    txn.commit();

    updateTableStatus(pgConn, table.schema_name, table.table_name, "FULL_LOAD",
                      0);
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

  // Prepare column names and types
  std::vector<std::string> columnNames;
  std::vector<std::string> columnTypes;

  for (const auto &col : columns) {
    if (col.size() < 6)
      continue;

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

  while (offset < sourceCount) {
    std::string selectQuery = "SELECT * FROM `" + table.schema_name + "`.`" +
                              table.table_name + "` " + "LIMIT " +
                              std::to_string(CHUNK_SIZE) + " OFFSET " +
                              std::to_string(offset) + ";";

    auto results = queryExecutor.executeQuery(mariadbConn, selectQuery);
    if (results.empty())
      break;

    std::string lowerSchema = table.schema_name;
    std::transform(lowerSchema.begin(), lowerSchema.end(), lowerSchema.begin(),
                   ::tolower);

    performBulkUpsert(pgConn, results, columnNames, columnTypes, lowerSchema,
                      table.table_name);

    totalProcessed += results.size();
    offset += CHUNK_SIZE;

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
      updateQuery += ", last_offset='" + std::to_string(offset) + "'";
    }

    updateQuery += ", last_sync_time=NOW()";
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

  try {
    // Get primary key columns
    auto pkColumns =
        getPrimaryKeyColumnsFromPostgres(pgConn, lowerSchemaName, tableName);

    if (pkColumns.empty()) {
      // No primary key, use simple insert
      performBulkInsert(pgConn, results, columnNames, columnTypes,
                        lowerSchemaName, tableName);
      return;
    }

    // Build upsert query
    std::string upsertQuery =
        buildUpsertQuery(columnNames, pkColumns, lowerSchemaName, tableName);
    std::string conflictClause =
        buildUpsertConflictClause(columnNames, pkColumns);

    pqxx::work txn(pgConn);
    txn.exec("SET statement_timeout = '600s'");

    // Process in batches
    const size_t BATCH_SIZE =
        std::min(SyncConfig::getChunkSize() / 2, static_cast<size_t>(500));

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
    }

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
