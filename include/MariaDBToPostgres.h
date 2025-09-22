#ifndef MARIADBTOPOSTGRES_H
#define MARIADBTOPOSTGRES_H

#include "Config.h"
#include "ConnectionPool.h"
#include "SyncReporter.h"
#include "logger.h"
#include <algorithm>
#include <atomic>
#include <cctype>
#include <iostream>
#include <mysql/mysql.h>
#include <pqxx/pqxx>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

class MariaDBToPostgres {
public:
  MariaDBToPostgres() = default;
  ~MariaDBToPostgres() = default;

  static std::unordered_map<std::string, std::string> dataTypeMap;
  static std::unordered_map<std::string, std::string> collationMap;

  struct TableInfo {
    std::string schema_name;
    std::string table_name;
    std::string cluster_name;
    std::string db_engine;
    std::string connection_string;
    std::string last_sync_time;
    std::string last_sync_column;
    std::string status;
    std::string last_offset;
  };

  std::vector<TableInfo> getActiveTables(pqxx::connection &pgConn) {
    std::vector<TableInfo> data;

    try {
      pqxx::work txn(pgConn);
      auto results = txn.exec(
          "SELECT schema_name, table_name, cluster_name, db_engine, "
          "connection_string, last_sync_time, last_sync_column, "
          "status, last_offset "
          "FROM metadata.catalog "
          "WHERE active=true AND db_engine='MariaDB' AND status != 'NO_DATA' "
          "ORDER BY schema_name, table_name;");
      txn.commit();

      for (const auto &row : results) {
        if (row.size() < 9)
          continue;

        TableInfo t;
        t.schema_name = row[0].is_null() ? "" : row[0].as<std::string>();
        t.table_name = row[1].is_null() ? "" : row[1].as<std::string>();
        t.cluster_name = row[2].is_null() ? "" : row[2].as<std::string>();
        t.db_engine = row[3].is_null() ? "" : row[3].as<std::string>();
        t.connection_string = row[4].is_null() ? "" : row[4].as<std::string>();
        t.last_sync_time = row[5].is_null() ? "" : row[5].as<std::string>();
        t.last_sync_column = row[6].is_null() ? "" : row[6].as<std::string>();
        t.status = row[7].is_null() ? "" : row[7].as<std::string>();
        t.last_offset = row[8].is_null() ? "" : row[8].as<std::string>();
        data.push_back(t);
      }
    } catch (const std::exception &e) {
      Logger::error("getActiveTables",
                    "Error getting active tables: " + std::string(e.what()));
    }

    return data;
  }

  void syncIndexesAndConstraints(const std::string &schema_name,
                                 const std::string &table_name,
                                 pqxx::connection &pgConn,
                                 const std::string &lowerSchemaName) {
    ConnectionGuard mariadbGuard(g_connectionPool.get(), DatabaseType::MARIADB);
    auto mariadbConn = mariadbGuard.get<MYSQL>().get();

    std::string query = "SELECT INDEX_NAME, NON_UNIQUE, COLUMN_NAME "
                        "FROM information_schema.statistics "
                        "WHERE table_schema = '" +
                        schema_name + "' AND table_name = '" + table_name +
                        "' AND INDEX_NAME != 'PRIMARY' "
                        "ORDER BY INDEX_NAME, SEQ_IN_INDEX;";

    auto results = executeQueryMariaDB(mariadbConn, query);

    for (const auto &row : results) {
      if (row.size() < 3)
        continue;

      std::string indexName = row[0];
      std::string nonUnique = row[1];
      std::string columnName = row[2];
      std::transform(columnName.begin(), columnName.end(), columnName.begin(),
                     ::tolower);

      std::string createQuery = "CREATE INDEX IF NOT EXISTS \"" + indexName +
                                "\" ON \"" + lowerSchemaName + "\".\"" +
                                table_name + "\" (\"" + columnName + "\");";

      try {
        pqxx::work txn(pgConn);
        txn.exec(createQuery);
        txn.commit();
      } catch (const std::exception &e) {
        Logger::error("syncIndexesAndConstraints",
                      "Error creating index '" + indexName +
                          "': " + std::string(e.what()));
      }
    }
  }

  void setupTableTargetMariaDBToPostgres() {
    try {
      pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());
      auto tables = getActiveTables(pgConn);
      // std::cerr << "=== STARTING setupTableTargetMariaDBToPostgres ===" <<
      // std::endl; std::cerr << "Found " << tables.size() << " active MariaDB
      // tables to process" << std::endl;

      for (const auto &table : tables) {
        if (table.db_engine != "MariaDB")
          continue;

        // std::cerr << "Processing table: " << table.schema_name << "." <<
        // table.table_name
        //           << " (status: " << table.status << ")" << std::endl;

        ConnectionConfig connectionConfig =
            parseConnectionString(table.connection_string);
        ConnectionGuard mariadbGuard(g_connectionPool.get(),
                                     DatabaseType::MARIADB);
        auto mariadbConn = mariadbGuard.get<MYSQL>().get();
        if (!mariadbConn) {
          Logger::error("setupTableTargetMariaDBToPostgres",
                        "Failed to connect to MariaDB for " +
                            table.schema_name + "." + table.table_name);
          continue;
        }
        // std::cerr << "Connected to MariaDB successfully" << std::endl;

        std::string query = "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, "
                            "COLUMN_KEY, EXTRA, CHARACTER_MAXIMUM_LENGTH "
                            "FROM information_schema.columns "
                            "WHERE table_schema = '" +
                            table.schema_name + "' AND table_name = '" +
                            table.table_name + "';";

        auto columns = executeQueryMariaDB(mariadbConn, query);
        // std::cerr << "Got " << columns.size() << " columns from MariaDB" <<
        // std::endl;

        if (columns.empty()) {
          Logger::error("setupTableTargetMariaDBToPostgres",
                        "No columns found for table " + table.schema_name +
                            "." + table.table_name + " - skipping");
          continue;
        }

        std::string lowerSchema = table.schema_name;
        std::transform(lowerSchema.begin(), lowerSchema.end(),
                       lowerSchema.begin(), ::tolower);

        {
          pqxx::work txn(pgConn);
          txn.exec("CREATE SCHEMA IF NOT EXISTS \"" + lowerSchema + "\";");
          txn.commit();
        }

        std::string createQuery = "CREATE TABLE IF NOT EXISTS \"" +
                                  lowerSchema + "\".\"" + table.table_name +
                                  "\" (";
        std::vector<std::string> primaryKeys;

        for (const auto &col : columns) {
          if (col.size() < 6)
            continue;

          std::string colName = col[0];
          std::transform(colName.begin(), colName.end(), colName.begin(),
                         ::tolower);
          std::string dataType = col[1];
          std::string nullable = "";
          std::string columnKey = col[3];
          std::string extra = col[4];
          std::string maxLength = col[5];

          // std::cerr << "Column: " << colName << " | Type: " << dataType << "
          // | Nullable: " << col[2] << " | Nullable SQL: " << nullable <<
          // std::endl;

          std::string pgType = "TEXT";
          if (extra == "auto_increment") {
            pgType = (dataType == "bigint") ? "BIGINT" : "INTEGER";
          } else if (dataType == "timestamp" || dataType == "datetime") {
            pgType = "TIMESTAMP";
          } else if (dataType == "date") {
            pgType = "DATE";
          } else if (dataType == "time") {
            pgType = "TIME";
          } else if (dataType == "char" || dataType == "varchar") {
            pgType = (!maxLength.empty() && maxLength != "NULL")
                         ? dataType + "(" + maxLength + ")"
                         : "VARCHAR";
          } else if (dataTypeMap.count(dataType)) {
            pgType = dataTypeMap[dataType];
          }

          createQuery += "\"" + colName + "\" " + pgType + nullable;
          if (columnKey == "PRI")
            primaryKeys.push_back(colName);
          createQuery += ", ";
        }

        if (!primaryKeys.empty()) {
          createQuery += "PRIMARY KEY (";
          for (size_t i = 0; i < primaryKeys.size(); ++i) {
            createQuery += "\"" + primaryKeys[i] + "\"";
            if (i < primaryKeys.size() - 1)
              createQuery += ", ";
          }
          createQuery += ")";
        } else {
          createQuery.erase(createQuery.size() - 2, 2);
        }
        createQuery += ");";

        {
          pqxx::work txn(pgConn);
          txn.exec(createQuery);
          txn.commit();
        }

        // No actualizar la columna de tiempo aquí - ya fue detectada
        // correctamente en catalog_manager.h
      }
    } catch (const std::exception &e) {
      Logger::error("setupTableTargetMariaDBToPostgres",
                    "Error in setupTableTargetMariaDBToPostgres: " +
                        std::string(e.what()));
    }
  }

  void processDeletesByPrimaryKey(const std::string &schema_name,
                                  const std::string &table_name,
                                  MYSQL *mariadbConn,
                                  pqxx::connection &pgConn) {
    try {
      std::string lowerSchemaName = schema_name;
      std::transform(lowerSchemaName.begin(), lowerSchemaName.end(),
                     lowerSchemaName.begin(), ::tolower);

      // 1. Obtener columnas de primary key
      std::vector<std::string> pkColumns =
          getPrimaryKeyColumns(mariadbConn, schema_name, table_name);

      if (pkColumns.empty()) {
        Logger::debug("processDeletesByPrimaryKey",
                      "No primary key found for " + schema_name + "." +
                          table_name + " - skipping delete detection");
        return;
      }

      Logger::debug(
          "processDeletesByPrimaryKey",
          "Processing deletes for " + schema_name + "." + table_name +
              " using PK columns: " + std::to_string(pkColumns.size()));

      // 2. Obtener todas las PKs de PostgreSQL en batches
      const size_t BATCH_SIZE = 1000;
      size_t offset = 0;
      size_t totalDeleted = 0;

      while (true) {
        // Construir query para obtener PKs de PostgreSQL
        std::string pkSelectQuery = "SELECT ";
        for (size_t i = 0; i < pkColumns.size(); ++i) {
          if (i > 0)
            pkSelectQuery += ", ";
          pkSelectQuery += "\"" + pkColumns[i] + "\"";
        }
        pkSelectQuery +=
            " FROM \"" + lowerSchemaName + "\".\"" + table_name + "\"";
        pkSelectQuery += " LIMIT " + std::to_string(BATCH_SIZE) + " OFFSET " +
                         std::to_string(offset) + ";";

        // Ejecutar query en PostgreSQL
        std::vector<std::vector<std::string>> pgPKs;
        try {
          pqxx::work txn(pgConn);
          auto results = txn.exec(pkSelectQuery);
          txn.commit();

          for (const auto &row : results) {
            std::vector<std::string> pkValues;
            for (size_t i = 0; i < pkColumns.size(); ++i) {
              pkValues.push_back(row[i].is_null() ? "NULL"
                                                  : row[i].as<std::string>());
            }
            pgPKs.push_back(pkValues);
          }
        } catch (const std::exception &e) {
          Logger::error("processDeletesByPrimaryKey",
                        "Error getting PKs from PostgreSQL: " +
                            std::string(e.what()));
          break;
        }

        if (pgPKs.empty()) {
          break; // No more data
        }

        // 3. Verificar cuáles PKs no existen en MariaDB
        std::vector<std::vector<std::string>> deletedPKs =
            findDeletedPrimaryKeys(mariadbConn, schema_name, table_name, pgPKs,
                                   pkColumns);

        // 4. Eliminar registros en PostgreSQL
        if (!deletedPKs.empty()) {
          size_t deletedCount = deleteRecordsByPrimaryKey(
              pgConn, lowerSchemaName, table_name, deletedPKs, pkColumns);
          totalDeleted += deletedCount;

          Logger::info("processDeletesByPrimaryKey",
                       "Deleted " + std::to_string(deletedCount) +
                           " records from batch in " + schema_name + "." +
                           table_name);
        }

        offset += BATCH_SIZE;

        // Si obtuvimos menos registros que el batch size, hemos terminado
        if (pgPKs.size() < BATCH_SIZE) {
          break;
        }
      }

      if (totalDeleted > 0) {
        Logger::info("processDeletesByPrimaryKey",
                     "Total deleted records: " + std::to_string(totalDeleted) +
                         " from " + schema_name + "." + table_name);
      }

    } catch (const std::exception &e) {
      Logger::error("processDeletesByPrimaryKey",
                    "Error processing deletes for " + schema_name + "." +
                        table_name + ": " + std::string(e.what()));
    }
  }

  void processUpdatesByPrimaryKey(const std::string &schema_name,
                                  const std::string &table_name,
                                  MYSQL *mariadbConn, pqxx::connection &pgConn,
                                  const std::string &timeColumn,
                                  const std::string &lastSyncTime) {
    try {
      if (timeColumn.empty() || lastSyncTime.empty()) {
        Logger::debug("processUpdatesByPrimaryKey",
                      "No time column or sync time for " + schema_name + "." +
                          table_name + " - skipping updates");
        return;
      }

      std::string lowerSchemaName = schema_name;
      std::transform(lowerSchemaName.begin(), lowerSchemaName.end(),
                     lowerSchemaName.begin(), ::tolower);

      // 1. Obtener columnas de primary key
      std::vector<std::string> pkColumns =
          getPrimaryKeyColumns(mariadbConn, schema_name, table_name);

      if (pkColumns.empty()) {
        Logger::debug("processUpdatesByPrimaryKey",
                      "No primary key found for " + schema_name + "." +
                          table_name + " - skipping updates");
        return;
      }

      Logger::info("processUpdatesByPrimaryKey",
                   "Processing updates for " + schema_name + "." + table_name +
                       " using time column: " + timeColumn +
                       " since: " + lastSyncTime);

      // 2. Obtener registros modificados desde MariaDB
      std::string selectQuery = "SELECT * FROM `" + schema_name + "`.`" +
                                table_name + "` WHERE `" + timeColumn +
                                "` > '" + escapeSQL(lastSyncTime) +
                                "' ORDER BY `" + timeColumn + "`";

      auto modifiedRecords = executeQueryMariaDB(mariadbConn, selectQuery);
      Logger::debug("processUpdatesByPrimaryKey",
                    "Found " + std::to_string(modifiedRecords.size()) +
                        " modified records in MariaDB");

      if (modifiedRecords.empty()) {
        Logger::debug("processUpdatesByPrimaryKey",
                      "No modified records found for " + schema_name + "." +
                          table_name);
        return;
      }

      // 3. Obtener nombres de columnas de MariaDB
      std::string columnQuery =
          "SELECT COLUMN_NAME FROM information_schema.columns "
          "WHERE table_schema = '" +
          escapeSQL(schema_name) + "' AND table_name = '" +
          escapeSQL(table_name) + "' ORDER BY ORDINAL_POSITION";

      auto columnNames = executeQueryMariaDB(mariadbConn, columnQuery);
      if (columnNames.empty() || columnNames[0].empty()) {
        Logger::error("processUpdatesByPrimaryKey",
                      "Could not get column names for " + schema_name + "." +
                          table_name);
        return;
      }

      // 4. Procesar cada registro modificado
      size_t totalUpdated = 0;
      for (const auto &record : modifiedRecords) {
        if (record.size() != columnNames.size()) {
          Logger::warning("processUpdatesByPrimaryKey",
                          "Record size mismatch for " + schema_name + "." +
                              table_name + " - skipping record");
          continue;
        }

        // Construir WHERE clause para primary key
        std::string whereClause = "";
        for (size_t i = 0; i < pkColumns.size(); ++i) {
          // Encontrar el índice de la columna PK en el record
          size_t pkIndex = 0;
          for (size_t j = 0; j < columnNames.size(); ++j) {
            if (columnNames[j][0] == pkColumns[i]) {
              pkIndex = j;
              break;
            }
          }

          if (i > 0)
            whereClause += " AND ";
          std::string pkValue = record[pkIndex];

          // Limpiar caracteres de control invisibles
          for (char &c : pkValue) {
            if (static_cast<unsigned char>(c) > 127) {
              c = '?';
            }
          }

          pkValue.erase(std::remove_if(pkValue.begin(), pkValue.end(),
                                       [](unsigned char c) {
                                         return c < 32 && c != 9 && c != 10 &&
                                                c != 13;
                                       }),
                        pkValue.end());

          whereClause += "\"" + pkColumns[i] + "\" = " +
                         (pkValue.empty() || pkValue == "NULL"
                              ? "NULL"
                              : "'" + escapeSQL(pkValue) + "'");
        }

        // Verificar si el registro existe en PostgreSQL
        std::string checkQuery = "SELECT COUNT(*) FROM \"" + lowerSchemaName +
                                 "\".\"" + table_name + "\" WHERE " +
                                 whereClause;

        pqxx::work txn(pgConn);
        auto result = txn.exec(checkQuery);
        txn.commit();

        if (result[0][0].as<int>() > 0) {
          // El registro existe, verificar si necesita actualización
          bool needsUpdate =
              compareAndUpdateRecord(pgConn, lowerSchemaName, table_name,
                                     record, columnNames, whereClause);

          if (needsUpdate) {
            totalUpdated++;
          }
        }
      }

      if (totalUpdated > 0) {
        Logger::info("processUpdatesByPrimaryKey",
                     "Updated " + std::to_string(totalUpdated) +
                         " records in " + schema_name + "." + table_name);
      } else {
        Logger::debug("processUpdatesByPrimaryKey",
                      "No records needed updates in " + schema_name + "." +
                          table_name);
      }

    } catch (const std::exception &e) {
      Logger::error("processUpdatesByPrimaryKey",
                    "Error processing updates for " + schema_name + "." +
                        table_name + ": " + std::string(e.what()));
    }
  }

  bool compareAndUpdateRecord(
      pqxx::connection &pgConn, const std::string &schemaName,
      const std::string &tableName, const std::vector<std::string> &newRecord,
      const std::vector<std::vector<std::string>> &columnNames,
      const std::string &whereClause) {
    try {
      // Obtener el registro actual de PostgreSQL
      std::string selectQuery = "SELECT * FROM \"" + schemaName + "\".\"" +
                                tableName + "\" WHERE " + whereClause;

      pqxx::work txn(pgConn);
      auto result = txn.exec(selectQuery);
      txn.commit();

      if (result.empty()) {
        return false; // No existe el registro
      }

      const auto &currentRow = result[0];

      // Comparar cada columna (excepto primary keys)
      std::vector<std::string> updateFields;
      bool hasChanges = false;

      for (size_t i = 0; i < columnNames.size(); ++i) {
        std::string columnName = columnNames[i][0];
        std::string newValue = newRecord[i];

        // Obtener valor actual de PostgreSQL
        std::string currentValue =
            currentRow[i].is_null() ? "" : currentRow[i].as<std::string>();

        // Comparar valores (normalizar para comparación)
        if (currentValue != newValue) {
          std::string cleanNewValue = newValue;

          // Limpiar caracteres de control invisibles
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
        // Ejecutar UPDATE
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

        Logger::debug("compareAndUpdateRecord",
                      "Updated record in " + schemaName + "." + tableName +
                          " WHERE " + whereClause);
        return true;
      }

      return false; // No había cambios

    } catch (const std::exception &e) {
      Logger::error("compareAndUpdateRecord",
                    "Error comparing/updating record: " +
                        std::string(e.what()));
      return false;
    }
  }

  void transferDataMariaDBToPostgres() {
    try {
      // std::cerr << "=== STARTING transferDataMariaDBToPostgres ===" <<
      // std::endl;
      pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());
      auto tables = getActiveTables(pgConn);
      // std::cerr << "Found " << tables.size() << " active tables to process"
      // << std::endl;

      for (auto &table : tables) {
        if (table.db_engine != "MariaDB")
          continue;

        // Actualizar tabla actualmente procesando para el dashboard
        SyncReporter::currentProcessingTable = table.schema_name + "." +
                                               table.table_name + " (" +
                                               table.status + ")";

        ConnectionConfig connectionConfig =
            parseConnectionString(table.connection_string);
        ConnectionGuard mariadbGuard(g_connectionPool.get(),
                                     DatabaseType::MARIADB);
        auto mariadbConn = mariadbGuard.get<MYSQL>().get();
        if (!mariadbConn) {
          Logger::error("transferDataMariaDBToPostgres",
                        "Failed to connect to MariaDB for " +
                            table.schema_name + "." + table.table_name);
          updateStatus(pgConn, table.schema_name, table.table_name, "ERROR");
          continue;
        }

        std::string schema_name = table.schema_name;
        std::string table_name = table.table_name;
        std::string lowerSchemaName = schema_name;
        std::transform(lowerSchemaName.begin(), lowerSchemaName.end(),
                       lowerSchemaName.begin(), ::tolower);

        auto countRes = executeQueryMariaDB(
            mariadbConn,
            "SELECT COUNT(*) FROM `" + schema_name + "`.`" + table_name + "`;");
        size_t sourceCount = 0;
        if (!countRes.empty() && !countRes[0][0].empty()) {
          sourceCount = std::stoul(countRes[0][0]);
        }
        // Obtener conteo de registros en la tabla destino
        std::string targetCountQuery = "SELECT COUNT(*) FROM \"" +
                                       lowerSchemaName + "\".\"" + table_name +
                                       "\";";
        size_t targetCount = 0;
        try {
          pqxx::work txn(pgConn);
          auto targetResult = txn.exec(targetCountQuery);
          if (!targetResult.empty()) {
            targetCount = targetResult[0][0].as<size_t>();
          }
          txn.commit();
        } catch (const std::exception &e) {
          Logger::debug("transferDataMariaDBToPostgres",
                        "Target table might not exist yet: " +
                            std::string(e.what()));
        }

        // Lógica simple basada en counts reales
        // std::cerr << "Logic check: sourceCount=" << sourceCount << ",
        // targetCount=" << targetCount << std::endl;
        if (sourceCount == 0) {
          // std::cerr << "Source count is 0, setting NO_DATA or ERROR" <<
          // std::endl;
          if (targetCount == 0) {
            updateStatus(pgConn, schema_name, table_name, "NO_DATA", 0);
          } else {
            updateStatus(pgConn, schema_name, table_name, "ERROR", 0);
          }
          continue;
        }

        // Si sourceCount = targetCount, verificar si hay cambios incrementales
        if (sourceCount == targetCount) {
          // Procesar UPDATEs si hay columna de tiempo y last_sync_time
          if (!table.last_sync_column.empty() &&
              !table.last_sync_time.empty()) {
            Logger::info("transferDataMariaDBToPostgres",
                         "Processing updates for " + schema_name + "." +
                             table_name +
                             " using time column: " + table.last_sync_column +
                             " since: " + table.last_sync_time);
            processUpdatesByPrimaryKey(schema_name, table_name, mariadbConn,
                                       pgConn, table.last_sync_column,
                                       table.last_sync_time);
          }

          // Verificar si hay datos nuevos usando last_offset
          size_t lastOffset = 0;
          try {
            pqxx::work txn(pgConn);
            auto offsetRes = txn.exec(
                "SELECT last_offset FROM metadata.catalog WHERE schema_name='" +
                escapeSQL(schema_name) + "' AND table_name='" +
                escapeSQL(table_name) + "';");
            txn.commit();

            if (!offsetRes.empty() && !offsetRes[0][0].is_null()) {
              lastOffset = std::stoul(offsetRes[0][0].as<std::string>());
            }
          } catch (...) {
            lastOffset = 0;
          }

          if (lastOffset >= sourceCount) {
            updateStatus(pgConn, schema_name, table_name, "PERFECT_MATCH",
                         targetCount);
          } else {
            updateStatus(pgConn, schema_name, table_name, "LISTENING_CHANGES",
                         targetCount);
          }
          continue;
        }

        // Si sourceCount < targetCount, hay registros eliminados en el origen
        // Procesar DELETEs por Primary Key
        if (sourceCount < targetCount) {
          Logger::info("transferDataMariaDBToPostgres",
                       "Detected " + std::to_string(targetCount - sourceCount) +
                           " deleted records in " + schema_name + "." +
                           table_name + " - processing deletes");
          processDeletesByPrimaryKey(schema_name, table_name, mariadbConn,
                                     pgConn);

          // Después de procesar DELETEs, verificar el nuevo conteo
          std::string lowerSchemaName = schema_name;
          std::transform(lowerSchemaName.begin(), lowerSchemaName.end(),
                         lowerSchemaName.begin(), ::tolower);
          pqxx::work countTxn(pgConn);
          auto newTargetCount =
              countTxn.exec("SELECT COUNT(*) FROM \"" + lowerSchemaName +
                            "\".\"" + table_name + "\";");
          countTxn.commit();
          targetCount = newTargetCount[0][0].as<int>();
          Logger::info("transferDataMariaDBToPostgres",
                       "After deletes: source=" + std::to_string(sourceCount) +
                           ", target=" + std::to_string(targetCount));
        }

        // std::cerr << "Source > Target, proceeding with data transfer..." <<
        // std::endl; std::cerr << "Table status: " << table.status <<
        // std::endl;

        auto columns = executeQueryMariaDB(
            mariadbConn,
            "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_KEY, EXTRA, "
            "CHARACTER_MAXIMUM_LENGTH FROM information_schema.columns WHERE "
            "table_schema = '" +
                schema_name + "' AND table_name = '" + table_name + "';");

        if (columns.empty()) {
          updateStatus(pgConn, schema_name, table_name, "ERROR");
          continue;
        }

        std::vector<std::string> columnNames;
        std::vector<std::string> columnTypes;

        for (const auto &col : columns) {
          if (col.size() < 6)
            continue;

          std::string colName = col[0];
          std::transform(colName.begin(), colName.end(), colName.begin(),
                         ::tolower);
          columnNames.push_back(colName);

          std::string dataType = col[1];
          std::string maxLength = col[5];

          std::string pgType = "TEXT";
          if (dataType == "char" || dataType == "varchar") {
            pgType = (!maxLength.empty() && maxLength != "NULL")
                         ? dataType + "(" + maxLength + ")"
                         : "VARCHAR";
          } else if (dataTypeMap.count(dataType)) {
            pgType = dataTypeMap[dataType];
          }

          columnTypes.push_back(pgType);
        }

        if (columnNames.empty()) {
          updateStatus(pgConn, schema_name, table_name, "ERROR");
          continue;
        }

        if (table.status == "FULL_LOAD") {
          // std::cerr << "Processing FULL_LOAD table: " << schema_name << "."
          // << table_name << std::endl;
          pqxx::work txn(pgConn);
          auto offsetCheck = txn.exec(
              "SELECT last_offset FROM metadata.catalog WHERE schema_name='" +
              escapeSQL(schema_name) + "' AND table_name='" +
              escapeSQL(table_name) + "';");
          txn.commit();

          bool shouldTruncate = true;
          if (!offsetCheck.empty() && !offsetCheck[0][0].is_null()) {
            std::string currentOffset = offsetCheck[0][0].as<std::string>();
            // std::cerr << "Current offset: " << currentOffset << std::endl;
            if (currentOffset != "0" && !currentOffset.empty()) {
              shouldTruncate = false;
              // std::cerr << "Skipping truncate due to non-zero offset" <<
              // std::endl;
            }
          }

          if (shouldTruncate) {
            Logger::info("transferDataMariaDBToPostgres",
                         "Truncating table: " + lowerSchemaName + "." +
                             table_name);
            pqxx::work txn(pgConn);
            txn.exec("TRUNCATE TABLE \"" + lowerSchemaName + "\".\"" +
                     table_name + "\" CASCADE;");
            txn.commit();
            Logger::debug("transferDataMariaDBToPostgres",
                          "Table truncated successfully");
          }
        } else if (table.status == "RESET") {
          Logger::info("transferDataMariaDBToPostgres",
                       "Processing RESET table: " + schema_name + "." +
                           table_name);
          pqxx::work txn(pgConn);
          txn.exec("TRUNCATE TABLE \"" + lowerSchemaName + "\".\"" +
                   table_name + "\" CASCADE;");
          txn.exec("UPDATE metadata.catalog SET last_offset='0' WHERE "
                   "schema_name='" +
                   escapeSQL(schema_name) + "' AND table_name='" +
                   escapeSQL(table_name) + "';");
          txn.commit();

          updateStatus(pgConn, schema_name, table_name, "FULL_LOAD", 0);
          continue;
        }

        size_t totalProcessed = 0;

        std::string offsetQuery =
            "SELECT last_offset FROM metadata.catalog WHERE schema_name='" +
            escapeSQL(schema_name) + "' AND table_name='" +
            escapeSQL(table_name) + "';";
        pqxx::work txn(pgConn);
        auto currentOffsetRes = txn.exec(offsetQuery);
        txn.commit();

        if (!currentOffsetRes.empty() && !currentOffsetRes[0][0].is_null()) {
          try {
            totalProcessed =
                std::stoul(currentOffsetRes[0][0].as<std::string>());
          } catch (...) {
            totalProcessed = 0;
          }
        }

        bool hasMoreData = true;
        while (hasMoreData) {
          const size_t CHUNK_SIZE =
              std::min(SyncConfig::getChunkSize(), static_cast<size_t>(10000));
          // std::cerr << "Building select query..." << std::endl;
          std::string selectQuery =
              "SELECT * FROM `" + schema_name + "`.`" + table_name + "`";

          // Usar last_offset para paginación simple y eficiente
          selectQuery += " LIMIT " + std::to_string(CHUNK_SIZE) + " OFFSET " +
                         std::to_string(targetCount) + ";";

          auto results = executeQueryMariaDB(mariadbConn, selectQuery);

          if (results.size() > 0) {
            Logger::info("transferDataMariaDBToPostgres",
                         "Processing chunk of " +
                             std::to_string(results.size()) + " rows for " +
                             schema_name + "." + table_name);
          }

          if (results.empty()) {
            // std::cerr << "No more data, ending transfer loop" << std::endl;
            hasMoreData = false;
            break;
          }

          size_t rowsInserted = 0;

          try {
            std::string columnsStr;
            for (size_t i = 0; i < columnNames.size(); ++i) {
              columnsStr += "\"" + columnNames[i] + "\"";
              if (i < columnNames.size() - 1)
                columnsStr += ",";
            }

            rowsInserted = results.size();

            if (rowsInserted > 0) {
              try {
                pqxx::work txn(pgConn);
                txn.exec("SET statement_timeout = '300s'");
                std::string tableName =
                    "\"" + lowerSchemaName + "\".\"" + table_name + "\"";
                pqxx::stream_to stream(txn, tableName);

                try {
                  for (const auto &row : results) {
                    if (row.size() == columnNames.size()) {
                      std::vector<std::optional<std::string>> values;
                      for (size_t i = 0; i < row.size(); ++i) {
                        if (row[i] == "NULL" || row[i].empty()) {
                          values.push_back(std::nullopt);
                        } else {
                          std::string cleanValue = row[i];
                          std::string columnType = columnTypes[i];
                          std::transform(columnType.begin(), columnType.end(),
                                         columnType.begin(), ::toupper);

                          if (cleanValue.empty()) {
                            values.push_back(std::nullopt);
                            continue;
                          }

                          if (columnType.find("BOOLEAN") != std::string::npos ||
                              columnType.find("BOOL") != std::string::npos) {
                            if (cleanValue == "N" || cleanValue == "0" ||
                                cleanValue == "false" ||
                                cleanValue == "FALSE") {
                              cleanValue = "false";
                            } else if (cleanValue == "Y" || cleanValue == "1" ||
                                       cleanValue == "true" ||
                                       cleanValue == "TRUE") {
                              cleanValue = "true";
                            }
                          } else if (columnType.find("BIT") !=
                                     std::string::npos) {
                            if (cleanValue == "0" || cleanValue == "false" ||
                                cleanValue == "FALSE" || cleanValue.empty()) {
                              values.push_back(std::nullopt);
                              continue;
                            } else if (cleanValue == "1" ||
                                       cleanValue == "true" ||
                                       cleanValue == "TRUE") {
                              cleanValue = "1";
                            } else {
                              values.push_back(std::nullopt);
                              continue;
                            }
                          }

                          if (columnType.find("TIMESTAMP") !=
                                  std::string::npos ||
                              columnType.find("DATETIME") !=
                                  std::string::npos ||
                              columnType.find("DATE") != std::string::npos) {
                            if (cleanValue == "0000-00-00 00:00:00" ||
                                cleanValue == "0000-00-00") {
                              cleanValue = "1970-01-01 00:00:00";
                            } else if (cleanValue.find("0000-00-00") !=
                                       std::string::npos) {
                              cleanValue = "1970-01-01 00:00:00";
                            } else if (cleanValue.find("-00 00:00:00") !=
                                       std::string::npos) {
                              size_t pos = cleanValue.find("-00 00:00:00");
                              if (pos != std::string::npos) {
                                cleanValue.replace(pos, 3, "-01");
                              }
                            } else if (cleanValue.find("-00") !=
                                       std::string::npos) {
                              size_t pos = cleanValue.find("-00");
                              if (pos != std::string::npos) {
                                cleanValue.replace(pos, 3, "-01");
                              }
                            }
                          }

                          for (char &c : cleanValue) {
                            if (static_cast<unsigned char>(c) > 127) {
                              c = '?';
                            }
                          }

                          cleanValue.erase(std::remove_if(cleanValue.begin(),
                                                          cleanValue.end(),
                                                          [](unsigned char c) {
                                                            return c < 32 &&
                                                                   c != 9 &&
                                                                   c != 10 &&
                                                                   c != 13;
                                                          }),
                                           cleanValue.end());

                          values.push_back(cleanValue);
                        }
                      }
                      stream << values;
                    }
                  }
                  stream.complete();
                  txn.commit();
                  Logger::info("transferDataMariaDBToPostgres",
                               "Successfully copied " +
                                   std::to_string(rowsInserted) + " rows to " +
                                   schema_name + "." + table_name);
                } catch (const std::exception &e) {
                  try {
                    stream.complete();
                  } catch (...) {
                  }
                  throw;
                }
              } catch (const std::exception &e) {
                Logger::error("transferDataMariaDBToPostgres",
                              "COPY failed: " + std::string(e.what()));
                rowsInserted = 0;
              }
            }

          } catch (const std::exception &e) {
            Logger::error("transferDataMariaDBToPostgres",
                          "Error processing data: " + std::string(e.what()));
          }

          // Always update targetCount and last_offset, even if COPY failed
          // This prevents infinite loops when there are duplicate key
          // violations
          targetCount += rowsInserted;

          // If COPY failed but we have data, advance the offset by 1
          // to prevent infinite loops on duplicate key violations
          if (rowsInserted == 0 && !results.empty()) {
            targetCount += 1; // Advance by 1 to skip the problematic record
            Logger::info("transferDataMariaDBToPostgres",
                         "COPY failed, advancing offset by 1 to skip "
                         "problematic record for " +
                             schema_name + "." + table_name);
          }

          // Update last_offset in database to prevent infinite loops
          try {
            pqxx::work updateTxn(pgConn);
            updateTxn.exec("UPDATE metadata.catalog SET last_offset='" +
                           std::to_string(targetCount) +
                           "' WHERE schema_name='" + escapeSQL(schema_name) +
                           "' AND table_name='" + escapeSQL(table_name) + "';");
            updateTxn.commit();
            Logger::debug("transferDataMariaDBToPostgres",
                          "Updated last_offset to " +
                              std::to_string(targetCount) + " for " +
                              schema_name + "." + table_name);
          } catch (const std::exception &e) {
            Logger::warning("transferDataMariaDBToPostgres",
                            "Failed to update last_offset: " +
                                std::string(e.what()));
          }

          if (targetCount >= sourceCount) {
            hasMoreData = false;
          }
        }

        // DELETEs ya fueron procesados arriba cuando sourceCount < targetCount

        if (targetCount > 0) {
          if (targetCount >= sourceCount) {
            Logger::info("transferDataMariaDBToPostgres",
                         "Table " + schema_name + "." + table_name +
                             " synchronized - PERFECT_MATCH");
            updateStatus(pgConn, schema_name, table_name, "PERFECT_MATCH",
                         targetCount);
          } else {
            Logger::info("transferDataMariaDBToPostgres",
                         "Table " + schema_name + "." + table_name +
                             " partially synchronized - LISTENING_CHANGES");
            updateStatus(pgConn, schema_name, table_name, "LISTENING_CHANGES",
                         targetCount);
          }
        }

        // Limpiar tabla actualmente procesando cuando termine
        SyncReporter::lastProcessingTable =
            SyncReporter::currentProcessingTable;
        SyncReporter::currentProcessingTable = "";
      }
    } catch (const std::exception &e) {
      Logger::error("transferDataMariaDBToPostgres",
                    "Error in transferDataMariaDBToPostgres: " +
                        std::string(e.what()));
    }
  }

  void updateStatus(pqxx::connection &pgConn, const std::string &schema_name,
                    const std::string &table_name, const std::string &status,
                    size_t offset = 0) {
    try {
      pqxx::work txn(pgConn);

      auto columnQuery =
          txn.exec("SELECT last_sync_column FROM metadata.catalog "
                   "WHERE schema_name='" +
                   escapeSQL(schema_name) + "' AND table_name='" +
                   escapeSQL(table_name) + "';");

      std::string lastSyncColumn = "";
      if (!columnQuery.empty() && !columnQuery[0][0].is_null()) {
        lastSyncColumn = columnQuery[0][0].as<std::string>();
      }

      std::string updateQuery = "UPDATE metadata.catalog SET status='" +
                                status + "', last_offset='" +
                                std::to_string(offset) + "'";

      if (!lastSyncColumn.empty()) {

        auto tableCheck =
            txn.exec("SELECT COUNT(*) FROM information_schema.tables "
                     "WHERE table_schema='" +
                     schema_name +
                     "' "
                     "AND table_name='" +
                     table_name + "';");

        if (!tableCheck.empty() && tableCheck[0][0].as<int>() > 0) {
          updateQuery += ", last_sync_time=(SELECT MAX(\"" + lastSyncColumn +
                         "\")::timestamp FROM \"" + schema_name + "\".\"" +
                         table_name + "\")";
        } else {
          updateQuery += ", last_sync_time=NOW()";
        }
      } else {
        updateQuery += ", last_sync_time=NOW()";
      }

      updateQuery += " WHERE schema_name='" + escapeSQL(schema_name) +
                     "' AND table_name='" + escapeSQL(table_name) + "';";

      txn.exec(updateQuery);
      txn.commit();
    } catch (const std::exception &e) {
      Logger::error("updateStatus",
                    "Error updating status: " + std::string(e.what()));
    }
  }

private:
  std::vector<std::string> getPrimaryKeyColumns(MYSQL *mariadbConn,
                                                const std::string &schema_name,
                                                const std::string &table_name) {
    std::vector<std::string> pkColumns;

    std::string query = "SELECT COLUMN_NAME "
                        "FROM information_schema.key_column_usage "
                        "WHERE table_schema = '" +
                        schema_name +
                        "' "
                        "AND table_name = '" +
                        table_name +
                        "' "
                        "AND constraint_name = 'PRIMARY' "
                        "ORDER BY ordinal_position;";

    auto results = executeQueryMariaDB(mariadbConn, query);

    for (const auto &row : results) {
      if (!row.empty()) {
        std::string colName = row[0];
        std::transform(colName.begin(), colName.end(), colName.begin(),
                       ::tolower);
        pkColumns.push_back(colName);
      }
    }

    return pkColumns;
  }

  std::vector<std::vector<std::string>>
  findDeletedPrimaryKeys(MYSQL *mariadbConn, const std::string &schema_name,
                         const std::string &table_name,
                         const std::vector<std::vector<std::string>> &pgPKs,
                         const std::vector<std::string> &pkColumns) {

    std::vector<std::vector<std::string>> deletedPKs;

    if (pgPKs.empty() || pkColumns.empty()) {
      return deletedPKs;
    }

    // Procesar en batches para evitar consultas muy largas
    const size_t CHECK_BATCH_SIZE = 500;

    for (size_t batchStart = 0; batchStart < pgPKs.size();
         batchStart += CHECK_BATCH_SIZE) {
      size_t batchEnd = std::min(batchStart + CHECK_BATCH_SIZE, pgPKs.size());

      // Construir query para verificar existencia en MariaDB
      std::string checkQuery = "SELECT ";
      for (size_t i = 0; i < pkColumns.size(); ++i) {
        if (i > 0)
          checkQuery += ", ";
        checkQuery += "`" + pkColumns[i] + "`";
      }
      checkQuery += " FROM `" + schema_name + "`.`" + table_name + "` WHERE (";

      for (size_t i = batchStart; i < batchEnd; ++i) {
        if (i > batchStart)
          checkQuery += " OR ";
        checkQuery += "(";
        for (size_t j = 0; j < pkColumns.size(); ++j) {
          if (j > 0)
            checkQuery += " AND ";
          std::string value = pgPKs[i][j];
          if (value == "NULL") {
            checkQuery += "`" + pkColumns[j] + "` IS NULL";
          } else {
            checkQuery += "`" + pkColumns[j] + "` = '" + escapeSQL(value) + "'";
          }
        }
        checkQuery += ")";
      }
      checkQuery += ");";

      // Ejecutar query en MariaDB
      auto existingResults = executeQueryMariaDB(mariadbConn, checkQuery);

      // Crear set de PKs que SÍ existen en MariaDB
      std::set<std::vector<std::string>> existingPKs;
      for (const auto &row : existingResults) {
        std::vector<std::string> pkValues;
        for (size_t i = 0; i < pkColumns.size(); ++i) {
          pkValues.push_back(row[i]);
        }
        existingPKs.insert(pkValues);
      }

      // Encontrar PKs que NO existen en MariaDB (deleted)
      for (size_t i = batchStart; i < batchEnd; ++i) {
        if (existingPKs.find(pgPKs[i]) == existingPKs.end()) {
          deletedPKs.push_back(pgPKs[i]);
        }
      }
    }

    return deletedPKs;
  }

  size_t deleteRecordsByPrimaryKey(
      pqxx::connection &pgConn, const std::string &lowerSchemaName,
      const std::string &table_name,
      const std::vector<std::vector<std::string>> &deletedPKs,
      const std::vector<std::string> &pkColumns) {

    if (deletedPKs.empty() || pkColumns.empty()) {
      return 0;
    }

    size_t deletedCount = 0;

    try {
      pqxx::work txn(pgConn);

      // Construir query DELETE
      std::string deleteQuery = "DELETE FROM \"" + lowerSchemaName + "\".\"" +
                                table_name + "\" WHERE (";

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

      // Ejecutar DELETE
      auto result = txn.exec(deleteQuery);
      deletedCount = result.affected_rows();

      txn.commit();

    } catch (const std::exception &e) {
      Logger::error("deleteRecordsByPrimaryKey",
                    "Error deleting records: " + std::string(e.what()));
    }

    return deletedCount;
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

  ConnectionConfig parseConnectionString(const std::string &connStr) {
    ConnectionConfig config;
    config.type = DatabaseType::MARIADB;
    config.connectionString = connStr;
    return config;
  }

  std::vector<std::vector<std::string>>
  executeQueryMariaDB(MYSQL *conn, const std::string &query) {
    std::vector<std::vector<std::string>> results;
    if (!conn) {
      Logger::error("executeQueryMariaDB", "No valid MariaDB connection");
      return results;
    }

    if (mysql_query(conn, query.c_str())) {
      Logger::error("executeQueryMariaDB", "Query execution failed: " +
                                               std::string(mysql_error(conn)));
      return results;
    }

    MYSQL_RES *res = mysql_store_result(conn);
    if (!res) {
      if (mysql_field_count(conn) > 0) {
        Logger::error("executeQueryMariaDB",
                      "mysql_store_result() failed: " +
                          std::string(mysql_error(conn)));
      }
      return results;
    }

    unsigned int num_fields = mysql_num_fields(res);
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(res))) {
      std::vector<std::string> rowData;
      rowData.reserve(num_fields);
      for (unsigned int i = 0; i < num_fields; ++i) {
        rowData.push_back(row[i] ? row[i] : "NULL");
      }
      results.push_back(rowData);
    }
    mysql_free_result(res);
    return results;
  }
};

// Definición de variables estáticas
std::unordered_map<std::string, std::string> MariaDBToPostgres::dataTypeMap = {
    {"int", "INTEGER"},
    {"bigint", "BIGINT"},
    {"smallint", "SMALLINT"},
    {"tinyint", "SMALLINT"},
    {"decimal", "NUMERIC"},
    {"float", "REAL"},
    {"double", "DOUBLE PRECISION"},
    {"varchar", "VARCHAR"},
    {"char", "CHAR"},
    {"text", "TEXT"},
    {"longtext", "TEXT"},
    {"mediumtext", "TEXT"},
    {"tinytext", "TEXT"},
    {"blob", "BYTEA"},
    {"longblob", "BYTEA"},
    {"mediumblob", "BYTEA"},
    {"tinyblob", "BYTEA"},
    {"json", "JSON"},
    {"boolean", "BOOLEAN"},
    {"bit", "BIT"},
    {"timestamp", "TIMESTAMP"},
    {"datetime", "TIMESTAMP"},
    {"date", "DATE"},
    {"time", "TIME"}};

std::unordered_map<std::string, std::string> MariaDBToPostgres::collationMap = {
    {"utf8_general_ci", "en_US.utf8"},
    {"utf8mb4_general_ci", "en_US.utf8"},
    {"latin1_swedish_ci", "C"},
    {"ascii_general_ci", "C"}};

#endif