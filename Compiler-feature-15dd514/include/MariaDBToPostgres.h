#ifndef MARIADBTOPOSTGRES_H
#define MARIADBTOPOSTGRES_H

#include "Config.h"
#include "catalog_manager.h"
#include "logger.h"
#include <algorithm>
#include <atomic>
#include <cctype>
#include <iostream>
#include <mutex>
#include <mysql/mysql.h>
#include <pqxx/pqxx>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

class MariaDBToPostgres {
private:
  std::mutex connectionMutex;

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

  MYSQL *getMariaDBConnection(const std::string &connectionString) {
    std::lock_guard<std::mutex> lock(connectionMutex);

    // Parsear connection string
    std::string host, user, password, db, port;
    std::istringstream ss(connectionString);
    std::string token;
    while (std::getline(ss, token, ';')) {
      auto pos = token.find('=');
      if (pos == std::string::npos)
        continue;
      std::string key = token.substr(0, pos);
      std::string value = token.substr(pos + 1);
      key.erase(0, key.find_first_not_of(" \t\r\n"));
      key.erase(key.find_last_not_of(" \t\r\n") + 1);
      value.erase(0, value.find_first_not_of(" \t\r\n"));
      value.erase(value.find_last_not_of(" \t\r\n") + 1);
      if (key == "host")
        host = value;
      else if (key == "user")
        user = value;
      else if (key == "password")
        password = value;
      else if (key == "db")
        db = value;
      else if (key == "port")
        port = value;
    }

    // Crear nueva conexión
    MYSQL *mariadbConn = mysql_init(nullptr);
    if (!mariadbConn) {
      Logger::error("getMariaDBConnection", "mysql_init() failed");
      return nullptr;
    }

    unsigned int portNum = 3306;
    if (!port.empty()) {
      try {
        portNum = std::stoul(port);
      } catch (...) {
        portNum = 3306;
      }
    }

    if (mysql_real_connect(mariadbConn, host.c_str(), user.c_str(),
                           password.c_str(), db.c_str(), portNum, nullptr,
                           0) == nullptr) {
      Logger::error("getMariaDBConnection",
                    "MariaDB connection failed: " +
                        std::string(mysql_error(mariadbConn)));
      mysql_close(mariadbConn);
      return nullptr;
    }

    // Set connection timeouts for large tables
    {
      std::string timeoutQuery =
          "SET SESSION wait_timeout = " +
          std::to_string(SyncConfig::getConnectionTimeout()) +
          ", interactive_timeout = " +
          std::to_string(SyncConfig::getConnectionTimeout()) +
          ", net_read_timeout = 600" + ", net_write_timeout = 600";
      mysql_query(mariadbConn, timeoutQuery.c_str());
    }

    return mariadbConn;
  }

  void closeMariaDBConnection(MYSQL *conn) {
    if (conn) {
      mysql_close(conn);
    }
  }

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
                                 const std::string &lowerSchemaName,
                                 const std::string &connection_string) {
    MYSQL *mariadbConn = getMariaDBConnection(connection_string);
    if (!mariadbConn) {
      Logger::error("syncIndexesAndConstraints",
                    "Failed to get MariaDB connection");
      return;
    }

    std::string query = "SELECT INDEX_NAME, NON_UNIQUE, COLUMN_NAME "
                        "FROM information_schema.statistics "
                        "WHERE table_schema = '" +
                        schema_name + "' AND table_name = '" + table_name +
                        "' AND INDEX_NAME != 'PRIMARY' "
                        "ORDER BY INDEX_NAME, SEQ_IN_INDEX;";

    std::vector<std::vector<std::string>> results =
        executeQueryMariaDB(mariadbConn, query);

    closeMariaDBConnection(mariadbConn);

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

      // Sort tables by priority: FULL_LOAD, RESET, PERFECT_MATCH,
      // LISTENING_CHANGES
      std::sort(
          tables.begin(), tables.end(),
          [](const TableInfo &a, const TableInfo &b) {
            if (a.status == "FULL_LOAD" && b.status != "FULL_LOAD")
              return true;
            if (a.status != "FULL_LOAD" && b.status == "FULL_LOAD")
              return false;
            if (a.status == "RESET" && b.status != "RESET")
              return true;
            if (a.status != "RESET" && b.status == "RESET")
              return false;
            if (a.status == "PERFECT_MATCH" && b.status != "PERFECT_MATCH")
              return true;
            if (a.status != "PERFECT_MATCH" && b.status == "PERFECT_MATCH")
              return false;
            if (a.status == "LISTENING_CHANGES" &&
                b.status != "LISTENING_CHANGES")
              return true;
            if (a.status != "LISTENING_CHANGES" &&
                b.status == "LISTENING_CHANGES")
              return false;
            return false; // Keep original order for same priority
          });

      Logger::info("setupTableTargetMariaDBToPostgres",
                   "Processing " + std::to_string(tables.size()) +
                       " MariaDB tables in priority order");
      for (size_t i = 0; i < tables.size(); ++i) {
        if (tables[i].db_engine == "MariaDB") {
          Logger::info("setupTableTargetMariaDBToPostgres",
                       "[" + std::to_string(i + 1) + "/" +
                           std::to_string(tables.size()) + "] " +
                           tables[i].schema_name + "." + tables[i].table_name +
                           " (status: " + tables[i].status + ")");
        }
      }

      for (const auto &table : tables) {
        if (table.db_engine != "MariaDB")
          continue;

        MYSQL *mariadbConn = getMariaDBConnection(table.connection_string);
        if (!mariadbConn) {
          Logger::error("setupTableTargetMariaDBToPostgres",
                        "Failed to get MariaDB connection");
          continue;
        }

        std::string query = "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, "
                            "COLUMN_KEY, EXTRA, CHARACTER_MAXIMUM_LENGTH "
                            "FROM information_schema.columns "
                            "WHERE table_schema = '" +
                            table.schema_name + "' AND table_name = '" +
                            table.table_name + "';";

        std::vector<std::vector<std::string>> columns =
            executeQueryMariaDB(mariadbConn, query);
        
        closeMariaDBConnection(mariadbConn);

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

        std::vector<std::string> columnDefinitions;

        for (const std::vector<std::string> &col : columns) {
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

          std::string columnDef = "\"" + colName + "\" " + pgType + nullable;
          columnDefinitions.push_back(columnDef);

          if (columnKey == "PRI")
            primaryKeys.push_back(colName);
        }

        if (columnDefinitions.empty()) {
          Logger::error("setupTableTargetMariaDBToPostgres",
                        "No valid columns found for table " +
                            table.schema_name + "." + table.table_name +
                            " - skipping");
          continue;
        }

        for (size_t i = 0; i < columnDefinitions.size(); ++i) {
          if (i > 0)
            createQuery += ", ";
          createQuery += columnDefinitions[i];
        }

        if (!primaryKeys.empty()) {
          createQuery += ", PRIMARY KEY (";
          for (size_t i = 0; i < primaryKeys.size(); ++i) {
            if (i > 0)
              createQuery += ", ";
            createQuery += "\"" + primaryKeys[i] + "\"";
          }
          createQuery += ")";
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
                                  const std::string &connection_string,
                                  pqxx::connection &pgConn) {
    MYSQL *mariadbConn = getMariaDBConnection(connection_string);
    if (!mariadbConn) {
      Logger::error("processDeletesByPrimaryKey",
                    "Failed to get MariaDB connection");
      return;
    }

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
      const size_t BATCH_SIZE = SyncConfig::getChunkSize();
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
    
    closeMariaDBConnection(mariadbConn);
  }

  void processUpdatesByPrimaryKey(const std::string &schema_name,
                                  const std::string &table_name,
                                  const std::string &connection_string,
                                  pqxx::connection &pgConn,
                                  const std::string &timeColumn,
                                  const std::string &lastSyncTime) {
    MYSQL *mariadbConn = getMariaDBConnection(connection_string);
    if (!mariadbConn) {
      Logger::error("processUpdatesByPrimaryKey",
                    "Failed to get MariaDB connection");
      return;
    }

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

      std::vector<std::vector<std::string>> modifiedRecords =
          executeQueryMariaDB(mariadbConn, selectQuery);
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

      std::vector<std::vector<std::string>> columnNames =
          executeQueryMariaDB(mariadbConn, columnQuery);
      if (columnNames.empty() || columnNames[0].empty()) {
        Logger::error("processUpdatesByPrimaryKey",
                      "Could not get column names for " + schema_name + "." +
                          table_name);
        return;
      }

      // 4. Procesar cada registro modificado
      size_t totalUpdated = 0;
      for (const std::vector<std::string> &record : modifiedRecords) {
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
    
    closeMariaDBConnection(mariadbConn);
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
      pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());
      auto tables = getActiveTables(pgConn);

      // Sort tables by priority: FULL_LOAD, RESET, PERFECT_MATCH,
      // LISTENING_CHANGES
      std::sort(
          tables.begin(), tables.end(),
          [](const TableInfo &a, const TableInfo &b) {
            if (a.status == "FULL_LOAD" && b.status != "FULL_LOAD")
              return true;
            if (a.status != "FULL_LOAD" && b.status == "FULL_LOAD")
              return false;
            if (a.status == "RESET" && b.status != "RESET")
              return true;
            if (a.status != "RESET" && b.status == "RESET")
              return false;
            if (a.status == "PERFECT_MATCH" && b.status != "PERFECT_MATCH")
              return true;
            if (a.status != "PERFECT_MATCH" && b.status == "PERFECT_MATCH")
              return false;
            if (a.status == "LISTENING_CHANGES" &&
                b.status != "LISTENING_CHANGES")
              return true;
            if (a.status != "LISTENING_CHANGES" &&
                b.status == "LISTENING_CHANGES")
              return false;
            return false; // Keep original order for same priority
          });

      Logger::info("transferDataMariaDBToPostgres",
                   "Processing " + std::to_string(tables.size()) +
                       " MariaDB tables in priority order");
      for (size_t i = 0; i < tables.size(); ++i) {
        if (tables[i].db_engine == "MariaDB") {
          Logger::info("transferDataMariaDBToPostgres",
                       "[" + std::to_string(i + 1) + "/" +
                           std::to_string(tables.size()) + "] " +
                           tables[i].schema_name + "." + tables[i].table_name +
                           " (status: " + tables[i].status + ")");
        }
      }

      for (auto &table : tables) {
        if (table.db_engine != "MariaDB")
          continue;

        MYSQL *mariadbConn = getMariaDBConnection(table.connection_string);
        if (!mariadbConn) {
          Logger::error("transferDataMariaDBToPostgres",
                        "Failed to get MariaDB connection");
          updateStatus(pgConn, table.schema_name, table.table_name, "ERROR");
          continue;
        }

        std::string schema_name = table.schema_name;
        std::string table_name = table.table_name;
        std::string lowerSchemaName = schema_name;
        std::transform(lowerSchemaName.begin(), lowerSchemaName.end(),
                       lowerSchemaName.begin(), ::tolower);

        std::vector<std::vector<std::string>> countRes = executeQueryMariaDB(
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
            processUpdatesByPrimaryKey(schema_name, table_name, table.connection_string,
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
          processDeletesByPrimaryKey(schema_name, table_name, table.connection_string,
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

        std::vector<std::vector<std::string>> columns = executeQueryMariaDB(
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

        for (const std::vector<std::string> &col : columns) {
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
          const size_t CHUNK_SIZE = SyncConfig::getChunkSize();
          // std::cerr << "Building select query..." << std::endl;
          std::string selectQuery =
              "SELECT * FROM `" + schema_name + "`.`" + table_name + "`";

          // Usar last_offset para paginación simple y eficiente
          selectQuery += " LIMIT " + std::to_string(CHUNK_SIZE) + " OFFSET " +
                         std::to_string(targetCount) + ";";

          std::vector<std::vector<std::string>> results =
              executeQueryMariaDB(mariadbConn, selectQuery);

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
                performBulkUpsert(pgConn, results, columnNames, columnTypes,
                                  lowerSchemaName, table_name, schema_name);
                Logger::info("transferDataMariaDBToPostgres",
                             "Successfully processed " +
                                 std::to_string(rowsInserted) + " rows for " +
                                 schema_name + "." + table_name);
              } catch (const std::exception &e) {
                Logger::error("transferDataMariaDBToPostgres",
                              "Bulk upsert failed: " + std::string(e.what()));
                rowsInserted = 0;
              }
            }

          } catch (const std::exception &e) {
            Logger::error("transferDataMariaDBToPostgres",
                          "Error processing data: " + std::string(e.what()));
          }

          // Update targetCount and last_offset based on actual processed rows
          targetCount += rowsInserted;

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

        // Tabla procesada completamente
        closeMariaDBConnection(mariadbConn);
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

    std::vector<std::vector<std::string>> results =
        executeQueryMariaDB(mariadbConn, query);

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
    const size_t CHECK_BATCH_SIZE =
        std::min(SyncConfig::getChunkSize() / 2, static_cast<size_t>(500));

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
      std::vector<std::vector<std::string>> existingResults =
          executeQueryMariaDB(mariadbConn, checkQuery);

      // Crear set de PKs que SÍ existen en MariaDB
      std::set<std::vector<std::string>> existingPKs;
      for (const std::vector<std::string> &row : existingResults) {
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

  // Connection string parsing is no longer needed with direct connections

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
        rowData.push_back(row[i] ? row[i] : "");
      }
      results.push_back(rowData);
    }
    mysql_free_result(res);
    return results;
  }

  void performBulkUpsert(pqxx::connection &pgConn,
                         const std::vector<std::vector<std::string>> &results,
                         const std::vector<std::string> &columnNames,
                         const std::vector<std::string> &columnTypes,
                         const std::string &lowerSchemaName,
                         const std::string &tableName,
                         const std::string &sourceSchemaName) {
    try {
      // Obtener columnas de primary key para el UPSERT
      std::vector<std::string> pkColumns =
          getPrimaryKeyColumnsFromPostgres(pgConn, lowerSchemaName, tableName);

      if (pkColumns.empty()) {
        // Si no hay PK, usar INSERT simple
        performBulkInsert(pgConn, results, columnNames, columnTypes,
                          lowerSchemaName, tableName);
        return;
      }

      // Construir query UPSERT
      std::string upsertQuery =
          buildUpsertQuery(columnNames, pkColumns, lowerSchemaName, tableName);
      std::string conflictClause =
          buildUpsertConflictClause(columnNames, pkColumns);

      pqxx::work txn(pgConn);
      txn.exec("SET statement_timeout = '300s'");

      // Procesar en batches para evitar queries muy largas
      const size_t BATCH_SIZE =
          std::min(SyncConfig::getChunkSize() / 2, static_cast<size_t>(500));
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

          txn.exec(batchQuery);
          totalProcessed += values.size();
        }
      }

      txn.commit();
      Logger::debug("performBulkUpsert",
                    "Processed " + std::to_string(totalProcessed) +
                        " rows with UPSERT for " + sourceSchemaName + "." +
                        tableName);

    } catch (const std::exception &e) {
      Logger::error("performBulkUpsert",
                    "Error in bulk upsert: " + std::string(e.what()));
      throw;
    }
  }

  void performBulkInsert(pqxx::connection &pgConn,
                         const std::vector<std::vector<std::string>> &results,
                         const std::vector<std::string> &columnNames,
                         const std::vector<std::string> &columnTypes,
                         const std::string &lowerSchemaName,
                         const std::string &tableName) {
    try {
      std::string insertQuery =
          "INSERT INTO \"" + lowerSchemaName + "\".\"" + tableName + "\" (";

      // Construir lista de columnas
      for (size_t i = 0; i < columnNames.size(); ++i) {
        if (i > 0)
          insertQuery += ", ";
        insertQuery += "\"" + columnNames[i] + "\"";
      }
      insertQuery += ") VALUES ";

      pqxx::work txn(pgConn);
      txn.exec("SET statement_timeout = '300s'");

      // Procesar en batches
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
      Logger::debug("performBulkInsert", "Processed " +
                                             std::to_string(totalProcessed) +
                                             " rows with INSERT for " +
                                             lowerSchemaName + "." + tableName);

    } catch (const std::exception &e) {
      Logger::error("performBulkInsert",
                    "Error in bulk insert: " + std::string(e.what()));
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

  std::string buildUpsertQuery(const std::vector<std::string> &columnNames,
                               const std::vector<std::string> &pkColumns,
                               const std::string &schemaName,
                               const std::string &tableName) {
    std::string query =
        "INSERT INTO \"" + schemaName + "\".\"" + tableName + "\" (";

    // Lista de columnas
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

  std::string cleanValueForPostgres(const std::string &value,
                                    const std::string &columnType) {
    std::string cleanValue = value;
    std::string upperType = columnType;
    std::transform(upperType.begin(), upperType.end(), upperType.begin(),
                   ::toupper);

    if (cleanValue.empty()) {
      return "NULL";
    }

    // Limpiar caracteres de control
    for (char &c : cleanValue) {
      if (static_cast<unsigned char>(c) > 127) {
        c = '?';
      }
    }

    cleanValue.erase(std::remove_if(cleanValue.begin(), cleanValue.end(),
                                    [](unsigned char c) {
                                      return c < 32 && c != 9 && c != 10 &&
                                             c != 13;
                                    }),
                     cleanValue.end());

    // Manejar tipos específicos
    if (upperType.find("BOOLEAN") != std::string::npos ||
        upperType.find("BOOL") != std::string::npos) {
      if (cleanValue == "N" || cleanValue == "0" || cleanValue == "false" ||
          cleanValue == "FALSE") {
        cleanValue = "false";
      } else if (cleanValue == "Y" || cleanValue == "1" ||
                 cleanValue == "true" || cleanValue == "TRUE") {
        cleanValue = "true";
      }
    } else if (upperType.find("BIT") != std::string::npos) {
      if (cleanValue == "0" || cleanValue == "false" || cleanValue == "FALSE" ||
          cleanValue.empty()) {
        return "NULL";
      } else if (cleanValue == "1" || cleanValue == "true" ||
                 cleanValue == "TRUE") {
        cleanValue = "1";
      } else {
        return "NULL";
      }
    } else if (upperType.find("TIMESTAMP") != std::string::npos ||
               upperType.find("DATETIME") != std::string::npos ||
               upperType.find("DATE") != std::string::npos) {
      // Convertir fechas inválidas de MariaDB a NULL para PostgreSQL
      if (cleanValue == "0000-00-00 00:00:00" || cleanValue == "0000-00-00" ||
          cleanValue == "0000-01-01" || cleanValue == "0000-01-01 00:00:00") {
        return "NULL";
      } else if (cleanValue.find("0000-00-00") != std::string::npos) {
        return "NULL";
      } else if (cleanValue.find("0000-01-01") != std::string::npos) {
        return "NULL";
      } else if (cleanValue.find("-00 00:00:00") != std::string::npos) {
        return "NULL";
      } else if (cleanValue.find("-00") != std::string::npos) {
        return "NULL";
      }
    }

    return cleanValue;
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