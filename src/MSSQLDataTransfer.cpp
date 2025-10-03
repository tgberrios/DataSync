#include "MSSQLDataTransfer.h"
#include "Config.h"
#include <algorithm>

void MSSQLDataTransfer::transferDataMSSQLToPostgres() {
  Logger::getInstance().info(LogCategory::TRANSFER,
                             "Starting MSSQL to PostgreSQL data transfer");

  try {
    pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());

    if (!pgConn.is_open()) {
      Logger::getInstance().error(
          LogCategory::TRANSFER,
          "CRITICAL ERROR: Cannot establish PostgreSQL connection "
          "for MSSQL data transfer");
      return;
    }

    Logger::getInstance().info(
        LogCategory::TRANSFER,
        "PostgreSQL connection established for MSSQL data transfer");

    // Get active tables
    std::vector<TableInfo> tables = queryExecutor.getActiveTables(pgConn);

    if (tables.empty()) {
      Logger::getInstance().info(LogCategory::TRANSFER,
                                 "No active MSSQL tables found to transfer");
      return;
    }

    for (const auto &table : tables) {
      if (table.db_engine != "MSSQL") {
        Logger::getInstance().warning(
            LogCategory::TRANSFER,
            "Skipping non-MSSQL table: " + table.db_engine + " - " +
                table.schema_name + "." + table.table_name);
        continue;
      }

      SQLHDBC dbc =
          connectionManager.getMSSQLConnection(table.connection_string);
      if (!dbc) {
        Logger::getInstance().error(
            LogCategory::TRANSFER,
            "CRITICAL ERROR: Failed to get MSSQL connection for table " +
                table.schema_name + "." + table.table_name +
                " - skipping table transfer");
        continue;
      }

      try {
        // Primero cambiar a la base de datos correcta
        std::string databaseName =
            connectionManager.extractDatabaseName(table.connection_string);
        std::string useQuery = "USE [" + databaseName + "];";
        auto useResult = connectionManager.executeQueryMSSQL(dbc, useQuery);

        // Obtener información de columnas
        std::string columnsQuery =
            "SELECT c.name AS COLUMN_NAME, tp.name AS DATA_TYPE "
            "FROM sys.columns c "
            "INNER JOIN sys.tables t ON c.object_id = t.object_id "
            "INNER JOIN sys.schemas s ON t.schema_id = s.schema_id "
            "INNER JOIN sys.types tp ON c.user_type_id = tp.user_type_id "
            "WHERE s.name = '" +
            escapeSQL(table.schema_name) + "' AND t.name = '" +
            escapeSQL(table.table_name) +
            "' "
            "ORDER BY c.column_id;";

        std::vector<std::vector<std::string>> columnInfo =
            connectionManager.executeQueryMSSQL(dbc, columnsQuery);

        if (columnInfo.empty()) {
          Logger::getInstance().error(LogCategory::TRANSFER,
                                      "No columns found for table " +
                                          table.schema_name + "." +
                                          table.table_name + " - skipping");
          continue;
        }

        std::vector<std::string> columnNames;
        std::vector<std::string> columnTypes;
        for (const auto &col : columnInfo) {
          if (col.size() >= 2) {
            columnNames.push_back(col[0]);
            columnTypes.push_back(col[1]);
          }
        }

        // Obtener conteo de registros en origen
        std::string countQuery = "SELECT COUNT(*) FROM [" +
                                 escapeSQL(table.schema_name) + "].[" +
                                 escapeSQL(table.table_name) + "];";
        auto countResult = connectionManager.executeQueryMSSQL(dbc, countQuery);
        size_t sourceCount = 0;
        if (!countResult.empty() && !countResult[0].empty()) {
          sourceCount = std::stoull(countResult[0][0]);
        }

        // Obtener conteo de registros en destino
        std::string lowerSchema = table.schema_name;
        std::transform(lowerSchema.begin(), lowerSchema.end(),
                       lowerSchema.begin(), ::tolower);

        pqxx::work txn(pgConn);
        auto targetCount =
            txn.query_value<size_t>("SELECT COUNT(*) FROM \"" + lowerSchema +
                                    "\".\"" + table.table_name + "\";");
        txn.commit();

        Logger::getInstance().info(
            LogCategory::TRANSFER,
            "Table " + table.schema_name + "." + table.table_name +
                " - Source count: " + std::to_string(sourceCount) +
                ", Target count: " + std::to_string(targetCount));

        // Si es FULL_LOAD, procesar por lotes usando PK o OFFSET
        if (table.status == "FULL_LOAD") {
          std::string pkStrategy = queryExecutor.getPKStrategyFromCatalog(
              pgConn, table.schema_name, table.table_name);

          if (pkStrategy == "PK") {
            std::vector<std::string> pkColumns =
                queryExecutor.getPKColumnsFromCatalog(pgConn, table.schema_name,
                                                      table.table_name);
            if (pkColumns.empty()) {
              Logger::getInstance().error(
                  LogCategory::TRANSFER,
                  "No PK columns found for table " + table.schema_name + "." +
                      table.table_name + " - falling back to OFFSET");
              pkStrategy = "OFFSET";
            }

            if (pkStrategy == "PK") {
              std::string lastPK = queryExecutor.getLastProcessedPKFromCatalog(
                  pgConn, table.schema_name, table.table_name);

              std::string selectQuery =
                  "SELECT " + std::string(columnNames.empty() ? "*" : "");
              if (!columnNames.empty()) {
                for (size_t i = 0; i < columnNames.size(); ++i) {
                  selectQuery += "[" + columnNames[i] + "]";
                  if (i < columnNames.size() - 1)
                    selectQuery += ", ";
                }
              }
              selectQuery += " FROM [" + escapeSQL(table.schema_name) + "].[" +
                             escapeSQL(table.table_name) + "]";

              if (!lastPK.empty()) {
                std::vector<std::string> lastPKValues =
                    queryExecutor.parseLastPK(lastPK);
                if (lastPKValues.size() == pkColumns.size()) {
                  selectQuery += " WHERE (";
                  for (size_t i = 0; i < pkColumns.size(); ++i) {
                    if (i > 0)
                      selectQuery += " OR ";
                    selectQuery += "(";
                    for (size_t j = 0; j <= i; ++j) {
                      if (j > 0)
                        selectQuery += " AND ";
                      if (j < i)
                        selectQuery +=
                            "[" + pkColumns[j] + "] = " + lastPKValues[j];
                      else
                        selectQuery +=
                            "[" + pkColumns[j] + "] > " + lastPKValues[j];
                    }
                    selectQuery += ")";
                  }
                  selectQuery += ")";
                }
              }

              selectQuery += " ORDER BY ";
              for (size_t i = 0; i < pkColumns.size(); ++i) {
                selectQuery += "[" + pkColumns[i] + "]";
                if (i < pkColumns.size() - 1)
                  selectQuery += ", ";
              }
              selectQuery += " OFFSET 0 ROWS FETCH NEXT " +
                             std::to_string(SyncConfig::getChunkSize()) +
                             " ROWS ONLY;";

              std::vector<std::vector<std::string>> results;
              do {
                results = connectionManager.executeQueryMSSQL(dbc, selectQuery);
                if (!results.empty()) {
                  performBulkUpsert(pgConn, results, columnNames, columnTypes,
                                    lowerSchema, table.table_name,
                                    table.schema_name);

                  std::string newLastPK = queryExecutor.getLastPKFromResults(
                      results, pkColumns, columnNames);
                  if (!newLastPK.empty()) {
                    updateLastProcessedPK(pgConn, table.schema_name,
                                          table.table_name, newLastPK);
                    lastPK = newLastPK;

                    std::vector<std::string> lastPKValues =
                        queryExecutor.parseLastPK(lastPK);
                    if (lastPKValues.size() == pkColumns.size()) {
                      selectQuery = "SELECT " +
                                    std::string(columnNames.empty() ? "*" : "");
                      if (!columnNames.empty()) {
                        for (size_t i = 0; i < columnNames.size(); ++i) {
                          selectQuery += "[" + columnNames[i] + "]";
                          if (i < columnNames.size() - 1)
                            selectQuery += ", ";
                        }
                      }
                      selectQuery += " FROM [" + escapeSQL(table.schema_name) +
                                     "].[" + escapeSQL(table.table_name) + "]";
                      selectQuery += " WHERE (";
                      for (size_t i = 0; i < pkColumns.size(); ++i) {
                        if (i > 0)
                          selectQuery += " OR ";
                        selectQuery += "(";
                        for (size_t j = 0; j <= i; ++j) {
                          if (j > 0)
                            selectQuery += " AND ";
                          if (j < i)
                            selectQuery +=
                                "[" + pkColumns[j] + "] = " + lastPKValues[j];
                          else
                            selectQuery +=
                                "[" + pkColumns[j] + "] > " + lastPKValues[j];
                        }
                        selectQuery += ")";
                      }
                      selectQuery += ")";
                      selectQuery += " ORDER BY ";
                      for (size_t i = 0; i < pkColumns.size(); ++i) {
                        selectQuery += "[" + pkColumns[i] + "]";
                        if (i < pkColumns.size() - 1)
                          selectQuery += ", ";
                      }
                      selectQuery +=
                          " OFFSET 0 ROWS FETCH NEXT " +
                          std::to_string(SyncConfig::getChunkSize()) +
                          " ROWS ONLY;";
                    }
                  }
                }
              } while (!results.empty());
            }
          }

          if (pkStrategy == "OFFSET") {
            size_t offset = 0;
            std::string offsetStr = table.last_offset;
            if (!offsetStr.empty()) {
              try {
                offset = std::stoull(offsetStr);
              } catch (...) {
                offset = 0;
              }
            }

            std::string selectQuery;
            std::vector<std::vector<std::string>> results;
            do {
              selectQuery =
                  "SELECT " + std::string(columnNames.empty() ? "*" : "");
              if (!columnNames.empty()) {
                for (size_t i = 0; i < columnNames.size(); ++i) {
                  selectQuery += "[" + columnNames[i] + "]";
                  if (i < columnNames.size() - 1)
                    selectQuery += ", ";
                }
              }
              selectQuery += " FROM [" + escapeSQL(table.schema_name) + "].[" +
                             escapeSQL(table.table_name) + "]";
              selectQuery += " ORDER BY (SELECT NULL)";
              selectQuery +=
                  " OFFSET " + std::to_string(offset) + " ROWS FETCH NEXT " +
                  std::to_string(SyncConfig::getChunkSize()) + " ROWS ONLY;";

              results = connectionManager.executeQueryMSSQL(dbc, selectQuery);
              if (!results.empty()) {
                performBulkUpsert(pgConn, results, columnNames, columnTypes,
                                  lowerSchema, table.table_name,
                                  table.schema_name);
                offset += results.size();
                updateStatus(pgConn, table.schema_name, table.table_name,
                             "FULL_LOAD", offset);
              }
            } while (!results.empty());
          }

          // Actualizar estado a LISTENING_CHANGES
          updateStatus(pgConn, table.schema_name, table.table_name,
                       "LISTENING_CHANGES");
        }

        // Si es INCREMENTAL, procesar actualizaciones y eliminaciones
        else if (table.status == "INCREMENTAL") {
          std::string timeColumn = table.last_sync_column;
          std::string lastSyncTime = getLastSyncTimeOptimized(
              pgConn, table.schema_name, table.table_name, timeColumn);

          if (!timeColumn.empty() && !lastSyncTime.empty()) {
            processUpdatesByPrimaryKey(table.schema_name, table.table_name, dbc,
                                       pgConn, timeColumn, lastSyncTime);
          }

          processDeletesByPrimaryKey(table.schema_name, table.table_name, dbc,
                                     pgConn);
        }

      } catch (const std::exception &e) {
        Logger::getInstance().error(
            LogCategory::TRANSFER,
            "Error processing table " + table.schema_name + "." +
                table.table_name + ": " + std::string(e.what()));
      }

      connectionManager.closeMSSQLConnection(dbc);
    }

    Logger::getInstance().info(LogCategory::TRANSFER,
                               "MSSQL data transfer completed");

  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::TRANSFER,
                                "Error in transferDataMSSQLToPostgres: " +
                                    std::string(e.what()));
  }
}
void MSSQLDataTransfer::performBulkUpsert(
    pqxx::connection &pgConn,
    const std::vector<std::vector<std::string>> &results,
    const std::vector<std::string> &columnNames,
    const std::vector<std::string> &columnTypes,
    const std::string &lowerSchemaName, const std::string &tableName,
    const std::string &sourceSchemaName) {
  if (results.empty() || columnNames.empty())
    return;

  try {
    std::vector<std::string> pkColumns =
        getPrimaryKeyColumnsFromPostgres(pgConn, lowerSchemaName, tableName);

    if (pkColumns.empty()) {
      performBulkInsert(pgConn, results, columnNames, columnTypes,
                        lowerSchemaName, tableName);
      return;
    }

    std::string upsertQuery =
        buildUpsertQuery(columnNames, pkColumns, lowerSchemaName, tableName);

    pqxx::work txn(pgConn);
    for (const auto &row : results) {
      if (row.size() != columnNames.size())
        continue;

      std::string values = "(";
      for (size_t i = 0; i < row.size(); ++i) {
        if (i > 0)
          values += ", ";
        values += dataValidator.cleanValueForPostgres(row[i], columnTypes[i]);
      }
      values += ")";

      txn.exec(upsertQuery + values);
    }
    txn.commit();

  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::TRANSFER,
                                "Error in performBulkUpsert for " +
                                    lowerSchemaName + "." + tableName + ": " +
                                    std::string(e.what()));
  }
}

void MSSQLDataTransfer::performBulkInsert(
    pqxx::connection &pgConn,
    const std::vector<std::vector<std::string>> &results,
    const std::vector<std::string> &columnNames,
    const std::vector<std::string> &columnTypes,
    const std::string &lowerSchemaName, const std::string &tableName) {
  if (results.empty() || columnNames.empty())
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
    for (const auto &row : results) {
      if (row.size() != columnNames.size())
        continue;

      std::string values = "(";
      for (size_t i = 0; i < row.size(); ++i) {
        if (i > 0)
          values += ", ";
        values += dataValidator.cleanValueForPostgres(row[i], columnTypes[i]);
      }
      values += ")";

      txn.exec(insertQuery + values);
    }
    txn.commit();

  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::TRANSFER,
                                "Error in performBulkInsert for " +
                                    lowerSchemaName + "." + tableName + ": " +
                                    std::string(e.what()));
  }
}

std::vector<std::string> MSSQLDataTransfer::getPrimaryKeyColumnsFromPostgres(
    pqxx::connection &pgConn, const std::string &schemaName,
    const std::string &tableName) {
  std::vector<std::string> pkColumns;

  try {
    pqxx::work txn(pgConn);
    auto result = txn.exec("SELECT a.attname "
                           "FROM pg_index i "
                           "JOIN pg_attribute a ON a.attrelid = i.indrelid "
                           "AND a.attnum = ANY(i.indkey) "
                           "WHERE i.indrelid = '\"" +
                           schemaName + "\".\"" + tableName +
                           "\"'::regclass "
                           "AND i.indisprimary;");
    txn.commit();

    for (const auto &row : result) {
      pkColumns.push_back(row[0].as<std::string>());
    }
  } catch (const std::exception &e) {
    Logger::getInstance().error(
        LogCategory::TRANSFER, "Error getting PK columns from PostgreSQL for " +
                                   schemaName + "." + tableName + ": " +
                                   std::string(e.what()));
  }

  return pkColumns;
}

std::string
MSSQLDataTransfer::buildUpsertQuery(const std::vector<std::string> &columnNames,
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
  query += ") VALUES (";
  for (size_t i = 0; i < columnNames.size(); ++i) {
    if (i > 0)
      query += ", ";
    query += "$" + std::to_string(i + 1);
  }
  query += ") ";

  query += buildUpsertConflictClause(columnNames, pkColumns);

  return query;
}

std::string MSSQLDataTransfer::buildUpsertConflictClause(
    const std::vector<std::string> &columnNames,
    const std::vector<std::string> &pkColumns) {
  std::string clause = " ON CONFLICT (";
  for (size_t i = 0; i < pkColumns.size(); ++i) {
    if (i > 0)
      clause += ", ";
    clause += "\"" + pkColumns[i] + "\"";
  }
  clause += ") DO UPDATE SET ";

  bool first = true;
  for (const auto &col : columnNames) {
    if (std::find(pkColumns.begin(), pkColumns.end(), col) != pkColumns.end())
      continue;

    if (!first)
      clause += ", ";
    clause += "\"" + col + "\" = EXCLUDED.\"" + col + "\"";
    first = false;
  }

  return clause;
}

void MSSQLDataTransfer::processDeletesByPrimaryKey(
    const std::string &schema_name, const std::string &table_name,
    SQLHDBC mssqlConn, pqxx::connection &pgConn) {
  try {
    std::string lowerSchema = schema_name;
    std::transform(lowerSchema.begin(), lowerSchema.end(), lowerSchema.begin(),
                   ::tolower);

    std::vector<std::string> pkColumns =
        queryExecutor.getPKColumnsFromCatalog(pgConn, schema_name, table_name);

    if (pkColumns.empty()) {
      Logger::getInstance().warning(LogCategory::TRANSFER,
                                    "No PK columns found for table " +
                                        schema_name + "." + table_name +
                                        " - skipping delete processing");
      return;
    }

    pqxx::work txn(pgConn);
    std::string selectPKQuery = "SELECT ";
    for (size_t i = 0; i < pkColumns.size(); ++i) {
      if (i > 0)
        selectPKQuery += ", ";
      selectPKQuery += "\"" + pkColumns[i] + "\"";
    }
    selectPKQuery += " FROM \"" + lowerSchema + "\".\"" + table_name + "\";";

    auto pgPKs = txn.exec(selectPKQuery);
    txn.commit();

    std::vector<std::vector<std::string>> pgPKValues;
    for (const auto &row : pgPKs) {
      std::vector<std::string> pkValue;
      for (size_t i = 0; i < pkColumns.size(); ++i) {
        pkValue.push_back(row[i].is_null() ? "NULL" : row[i].as<std::string>());
      }
      pgPKValues.push_back(pkValue);
    }

    auto deletedPKs = queryExecutor.findDeletedPrimaryKeys(
        mssqlConn, schema_name, table_name, pgPKValues, pkColumns);

    if (!deletedPKs.empty()) {
      size_t deletedCount = deleteRecordsByPrimaryKey(
          pgConn, lowerSchema, table_name, deletedPKs, pkColumns);

      Logger::getInstance().info(LogCategory::TRANSFER,
                                 "Deleted " + std::to_string(deletedCount) +
                                     " records from " + schema_name + "." +
                                     table_name);
    }

  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::TRANSFER,
                                "Error processing deletes for " + schema_name +
                                    "." + table_name + ": " +
                                    std::string(e.what()));
  }
}

void MSSQLDataTransfer::processUpdatesByPrimaryKey(
    const std::string &schema_name, const std::string &table_name,
    SQLHDBC mssqlConn, pqxx::connection &pgConn, const std::string &timeColumn,
    const std::string &lastSyncTime) {
  try {
    std::string lowerSchema = schema_name;
    std::transform(lowerSchema.begin(), lowerSchema.end(), lowerSchema.begin(),
                   ::tolower);

    std::vector<std::string> pkColumns =
        queryExecutor.getPKColumnsFromCatalog(pgConn, schema_name, table_name);

    if (pkColumns.empty()) {
      Logger::getInstance().warning(LogCategory::TRANSFER,
                                    "No PK columns found for table " +
                                        schema_name + "." + table_name +
                                        " - skipping update processing");
      return;
    }

    std::string selectQuery = "SELECT * FROM [" + escapeSQL(schema_name) +
                              "].[" + escapeSQL(table_name) + "] WHERE [" +
                              escapeSQL(timeColumn) + "] > '" +
                              escapeSQL(lastSyncTime) + "';";

    auto results = connectionManager.executeQueryMSSQL(mssqlConn, selectQuery);

    if (!results.empty()) {
      performBulkUpsert(pgConn, results, pkColumns, std::vector<std::string>(),
                        lowerSchema, table_name, schema_name);

      Logger::getInstance().info(LogCategory::TRANSFER,
                                 "Updated " + std::to_string(results.size()) +
                                     " records in " + schema_name + "." +
                                     table_name);
    }

  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::TRANSFER,
                                "Error processing updates for " + schema_name +
                                    "." + table_name + ": " +
                                    std::string(e.what()));
  }
}

size_t MSSQLDataTransfer::deleteRecordsByPrimaryKey(
    pqxx::connection &pgConn, const std::string &lowerSchemaName,
    const std::string &table_name,
    const std::vector<std::vector<std::string>> &deletedPKs,
    const std::vector<std::string> &pkColumns) {
  size_t deletedCount = 0;

  try {
    pqxx::work txn(pgConn);
    for (const auto &pkValues : deletedPKs) {
      if (pkValues.size() != pkColumns.size())
        continue;

      std::string deleteQuery = "DELETE FROM \"" + lowerSchemaName + "\".\"" +
                                table_name + "\" WHERE ";
      for (size_t i = 0; i < pkColumns.size(); ++i) {
        if (i > 0)
          deleteQuery += " AND ";
        deleteQuery += "\"" + pkColumns[i] + "\" = " + pkValues[i];
      }
      deleteQuery += ";";

      txn.exec(deleteQuery);
      deletedCount++;
    }
    txn.commit();
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::TRANSFER,
                                "Error deleting records from " +
                                    lowerSchemaName + "." + table_name + ": " +
                                    std::string(e.what()));
  }

  return deletedCount;
}

void MSSQLDataTransfer::updateStatus(pqxx::connection &pgConn,
                                     const std::string &schema_name,
                                     const std::string &table_name,
                                     const std::string &status, size_t offset) {
  try {
    pqxx::work txn(pgConn);
    std::string updateQuery =
        "UPDATE metadata.catalog SET status = '" + escapeSQL(status) + "'";
    // Get PK strategy to determine which field to update
    auto pkStrategyResult = txn.exec(
        "SELECT pk_strategy FROM metadata.catalog WHERE schema_name='" +
        escapeSQL(schema_name) + "' AND table_name='" + escapeSQL(table_name) +
        "'");

    if (!pkStrategyResult.empty() && !pkStrategyResult[0][0].is_null()) {
      std::string pkStrategy = pkStrategyResult[0][0].as<std::string>();
      if (pkStrategy == "PK") {
        updateQuery += ", last_processed_pk='" + std::to_string(offset) +
                       "', last_offset=NULL";
      } else {
        updateQuery += ", last_offset='" + std::to_string(offset) +
                       "', last_processed_pk=NULL";
      }
    } else {
      // Default to OFFSET if strategy not found
      updateQuery += ", last_offset='" + std::to_string(offset) +
                     "', last_processed_pk=NULL";
    }
    updateQuery += " WHERE schema_name = '" + escapeSQL(schema_name) +
                   "' AND table_name = '" + escapeSQL(table_name) + "';";
    txn.exec(updateQuery);
    txn.commit();
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::TRANSFER,
                                "Error updating status for " + schema_name +
                                    "." + table_name + ": " +
                                    std::string(e.what()));
  }
}

void MSSQLDataTransfer::updateLastProcessedPK(pqxx::connection &pgConn,
                                              const std::string &schema_name,
                                              const std::string &table_name,
                                              const std::string &lastPK) {
  try {
    pqxx::work txn(pgConn);
    std::string updateQuery =
        "UPDATE metadata.catalog SET last_processed_pk = '" +
        escapeSQL(lastPK) + "' WHERE schema_name = '" + escapeSQL(schema_name) +
        "' AND table_name = '" + escapeSQL(table_name) + "';";
    txn.exec(updateQuery);
    txn.commit();
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::TRANSFER,
                                "Error updating last processed PK for " +
                                    schema_name + "." + table_name + ": " +
                                    std::string(e.what()));
  }
}

std::string MSSQLDataTransfer::getLastSyncTimeOptimized(
    pqxx::connection &pgConn, const std::string &schema_name,
    const std::string &table_name, const std::string &lastSyncColumn) {
  std::string lastSyncTime;

  try {
    pqxx::work txn(pgConn);
    auto result = txn.exec1(
        "SELECT last_sync_time FROM metadata.catalog WHERE schema_name = '" +
        escapeSQL(schema_name) + "' AND table_name = '" +
        escapeSQL(table_name) + "';");
    txn.commit();

    if (!result[0].is_null()) {
      lastSyncTime = result[0].as<std::string>();
    }
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::TRANSFER,
                                "Error getting last sync time for " +
                                    schema_name + "." + table_name + ": " +
                                    std::string(e.what()));
  }

  return lastSyncTime;
}

std::string MSSQLDataTransfer::escapeSQL(const std::string &value) {
  std::string escaped = value;
  size_t pos = 0;
  while ((pos = escaped.find('\'', pos)) != std::string::npos) {
    escaped.insert(pos, "'");
    pos += 2;
  }
  return escaped;
}
