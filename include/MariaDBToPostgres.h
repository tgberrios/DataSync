#ifndef MARIADBTOPOSTGRES_H
#define MARIADBTOPOSTGRES_H

#include "Config.h"
#include "catalog_manager.h"
#include "logger.h"
#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
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
    std::string last_processed_pk;
    std::string pk_strategy;
    std::string pk_columns;
    std::string candidate_columns;
    bool has_pk;
  };

  MYSQL *getMariaDBConnection(const std::string &connectionString) {
    // Validate connection string
    if (connectionString.empty()) {
      Logger::error(LogCategory::TRANSFER, "getMariaDBConnection",
                    "Empty connection string provided");
      return nullptr;
    }

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

    // Validate required parameters
    if (host.empty() || user.empty() || db.empty()) {
      Logger::error(
          LogCategory::TRANSFER, "getMariaDBConnection",
          "Missing required connection parameters (host, user, or db)");
      return nullptr;
    }

    // Crear nueva conexión directa (sin mutex)
    MYSQL *conn = mysql_init(nullptr);
    if (!conn) {
      Logger::error(LogCategory::TRANSFER, "getMariaDBConnection",
                    "mysql_init() failed");
      return nullptr;
    }

    unsigned int portNum = 3306;
    if (!port.empty()) {
      try {
        portNum = std::stoul(port);
        // Validate port range
        if (portNum == 0 || portNum > 65535) {
          Logger::warning(LogCategory::TRANSFER, "getMariaDBConnection",
                          "Invalid port number " + port +
                              ", using default 3306");
          portNum = 3306;
        }
      } catch (const std::exception &e) {
        Logger::warning(LogCategory::TRANSFER, "getMariaDBConnection",
                        "Could not parse port " + port + ": " +
                            std::string(e.what()) + ", using default 3306");
        portNum = 3306;
      }
    }

    if (mysql_real_connect(conn, host.c_str(), user.c_str(), password.c_str(),
                           db.c_str(), portNum, nullptr, 0) == nullptr) {
      std::string errorMsg = mysql_error(conn);
      Logger::error(LogCategory::TRANSFER, "getMariaDBConnection",
                    "MariaDB connection failed: " + errorMsg +
                        " (host: " + host + ", user: " + user + ", db: " + db +
                        ", port: " + std::to_string(portNum) + ")");
      mysql_close(conn);
      return nullptr;
    }

    // Test connection with a simple query
    if (mysql_query(conn, "SELECT 1")) {
      std::string errorMsg = mysql_error(conn);
      Logger::error(LogCategory::TRANSFER, "getMariaDBConnection",
                    "Connection test failed: " + errorMsg);
      mysql_close(conn);
      return nullptr;
    }

    // Free the test result
    MYSQL_RES *testResult = mysql_store_result(conn);
    if (testResult) {
      mysql_free_result(testResult);
    }

    {
      std::string timeoutQuery =
          "SET SESSION wait_timeout = 600" +
          std::string(", interactive_timeout = 600") +
          std::string(", net_read_timeout = 600") +
          std::string(", net_write_timeout = 600") +
          std::string(", innodb_lock_wait_timeout = 600") +
          std::string(", lock_wait_timeout = 600");
      mysql_query(conn, timeoutQuery.c_str());
    }

    return conn;
  }

  std::vector<TableInfo> getActiveTables(pqxx::connection &pgConn) {
    std::vector<TableInfo> data;

    try {
      pqxx::work txn(pgConn);
      auto results = txn.exec(
          "SELECT schema_name, table_name, cluster_name, db_engine, "
          "connection_string, last_sync_time, last_sync_column, "
          "status, last_offset, last_processed_pk, pk_strategy, "
          "pk_columns, candidate_columns, has_pk, table_size "
          "FROM metadata.catalog "
          "WHERE active=true AND db_engine='MariaDB' AND status != 'NO_DATA' "
          "ORDER BY table_size ASC, schema_name, table_name;");
      txn.commit();

      for (const auto &row : results) {
        if (row.size() < 15)
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
        t.last_processed_pk = row[9].is_null() ? "" : row[9].as<std::string>();
        t.pk_strategy = row[10].is_null() ? "" : row[10].as<std::string>();
        t.pk_columns = row[11].is_null() ? "" : row[11].as<std::string>();
        t.candidate_columns =
            row[12].is_null() ? "" : row[12].as<std::string>();
        t.has_pk = row[13].is_null() ? false : row[13].as<bool>();
        data.push_back(t);
      }
    } catch (const pqxx::sql_error &e) {
      Logger::error(
          LogCategory::TRANSFER, "getActiveTables",
          "SQL ERROR getting active tables: " + std::string(e.what()) +
              " [SQL State: " + e.sqlstate() + "]");
    } catch (const pqxx::broken_connection &e) {
      Logger::error(LogCategory::TRANSFER, "getActiveTables",
                    "CONNECTION ERROR getting active tables: " +
                        std::string(e.what()));
    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER, "getActiveTables",
                    "ERROR getting active tables: " + std::string(e.what()));
    }

    return data;
  }

  void syncIndexesAndConstraints(const std::string &schema_name,
                                 const std::string &table_name,
                                 pqxx::connection &pgConn,
                                 const std::string &lowerSchemaName,
                                 const std::string &connection_string) {
    // Validate input parameters
    if (schema_name.empty() || table_name.empty() || lowerSchemaName.empty() ||
        connection_string.empty()) {
      Logger::error(LogCategory::TRANSFER, "syncIndexesAndConstraints",
                    "Invalid parameters: schema_name, table_name, "
                    "lowerSchemaName, or connection_string is empty");
      return;
    }

    MYSQL *mariadbConn = getMariaDBConnection(connection_string);
    if (!mariadbConn) {
      Logger::error(LogCategory::TRANSFER, "syncIndexesAndConstraints",
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
      } catch (const pqxx::sql_error &e) {
        Logger::error(LogCategory::TRANSFER, "syncIndexesAndConstraints",
                      "SQL ERROR creating index '" + indexName +
                          "': " + std::string(e.what()) +
                          " [SQL State: " + e.sqlstate() + "]");
      } catch (const pqxx::broken_connection &e) {
        Logger::error(LogCategory::TRANSFER, "syncIndexesAndConstraints",
                      "CONNECTION ERROR creating index '" + indexName +
                          "': " + std::string(e.what()));
      } catch (const std::exception &e) {
        Logger::error(LogCategory::TRANSFER, "syncIndexesAndConstraints",
                      "ERROR creating index '" + indexName +
                          "': " + std::string(e.what()));
      }
    }

    // Cerrar conexión MariaDB con verificación
    if (mariadbConn) {
      mysql_close(mariadbConn);
      mariadbConn = nullptr;
    }
  }

  void setupTableTargetMariaDBToPostgres() {
    Logger::info(LogCategory::TRANSFER, "Starting MariaDB table target setup");

    try {
      pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());

      if (!pgConn.is_open()) {
        Logger::error(LogCategory::TRANSFER,
                      "CRITICAL ERROR: Cannot establish PostgreSQL connection "
                      "for MariaDB table setup");
        return;
      }

      Logger::info(LogCategory::TRANSFER,
                   "PostgreSQL connection established for MariaDB table setup");

      auto tables = getActiveTables(pgConn);

      if (tables.empty()) {
        Logger::info(LogCategory::TRANSFER,
                     "No active MariaDB tables found to setup");
        return;
      }

      Logger::info(LogCategory::TRANSFER, "Sorting " +
                                              std::to_string(tables.size()) +
                                              " MariaDB tables by priority");

      std::sort(tables.begin(), tables.end(),
                [](const TableInfo &a, const TableInfo &b) {
                  if (a.status == "FULL_LOAD" && b.status != "FULL_LOAD")
                    return true;
                  if (a.status != "FULL_LOAD" && b.status == "FULL_LOAD")
                    return false;
                  if (a.status == "RESET" && b.status != "RESET")
                    return true;
                  if (a.status != "RESET" && b.status == "RESET")
                    return false;
                  if (a.status == "LISTENING_CHANGES" &&
                      b.status != "LISTENING_CHANGES")
                    return true;
                  if (a.status != "LISTENING_CHANGES" &&
                      b.status == "LISTENING_CHANGES")
                    return false;
                  return false;
                });

      Logger::info(LogCategory::TRANSFER,
                   "Processing " + std::to_string(tables.size()) +
                       " MariaDB tables in priority order");
      // Removed individual table status logs to reduce noise

      for (const auto &table : tables) {
        if (table.db_engine != "MariaDB")
          continue;

        MYSQL *mariadbConn = getMariaDBConnection(table.connection_string);
        if (!mariadbConn) {
          Logger::error(LogCategory::TRANSFER,
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

        if (columns.empty()) {
          Logger::error(LogCategory::TRANSFER,
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
          if (col.size() < 1)
            continue;

          std::string colName = col[0];
          if (colName.empty())
            continue;

          std::transform(colName.begin(), colName.end(), colName.begin(),
                         ::tolower);
          std::string dataType = col.size() > 1 ? col[1] : "varchar";
          std::string isNullable = col.size() > 2 ? col[2] : "YES";
          std::string columnKey = col.size() > 3 ? col[3] : "";
          std::string extra = col.size() > 4 ? col[4] : "";
          std::string maxLength = col.size() > 5 ? col[5] : "";

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
            // Validar que maxLength sea razonable antes de usarlo
            if (!maxLength.empty() && maxLength != "NULL") {
              try {
                size_t length = std::stoul(maxLength);
                // Solo usar longitud si es razonable (entre 1 y 65535)
                if (length >= 1 && length <= 65535) {
                  pgType = dataType + "(" + maxLength + ")";
                } else {
                  pgType = "VARCHAR"; // Usar VARCHAR sin restricción si la
                                      // longitud es inválida
                }
              } catch (const std::exception &) {
                pgType = "VARCHAR"; // Usar VARCHAR sin restricción si no se
                                    // puede parsear
              }
            } else {
              pgType = "VARCHAR";
            }
          } else if (dataTypeMap.count(dataType)) {
            pgType = dataTypeMap[dataType];
          }

          // Crear todas las columnas como nullable para evitar problemas con
          // valores NULL
          std::string columnDef = "\"" + colName + "\" " + pgType;
          columnDefinitions.push_back(columnDef);

          if (columnKey == "PRI")
            primaryKeys.push_back(colName);
        }

        if (columnDefinitions.empty()) {
          Logger::error(LogCategory::TRANSFER,
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

        // Cerrar conexión MariaDB
        mysql_close(mariadbConn);
      }
    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER,
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
        Logger::info(LogCategory::TRANSFER,
                     "No primary key columns found for " + schema_name + "." +
                         table_name + " - skipping delete processing");
        return;
      }

      Logger::info(LogCategory::TRANSFER,
                   "Found " + std::to_string(pkColumns.size()) +
                       " PK columns for " + schema_name + "." + table_name);

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
          // Validate column name is not empty and not a number
          if (pkColumns[i].empty() ||
              std::all_of(pkColumns[i].begin(), pkColumns[i].end(),
                          ::isdigit)) {
            Logger::error(LogCategory::TRANSFER,
                          "Invalid PK column name: '" + pkColumns[i] +
                              "' for table " + schema_name + "." + table_name +
                              " - skipping delete processing");
            return;
          }
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
            for (size_t i = 0; i < pkColumns.size() && i < row.size(); ++i) {
              pkValues.push_back(row[i].is_null() ? "NULL"
                                                  : row[i].as<std::string>());
            }
            pgPKs.push_back(pkValues);
          }
        } catch (const std::exception &e) {
          Logger::error(LogCategory::TRANSFER,
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

          Logger::info(LogCategory::TRANSFER,
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
        Logger::info(LogCategory::TRANSFER,
                     "Total deleted records: " + std::to_string(totalDeleted) +
                         " from " + schema_name + "." + table_name);
      }

    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER, "Error processing deletes for " +
                                               schema_name + "." + table_name +
                                               ": " + std::string(e.what()));
    }
  }

  void processUpdatesByPrimaryKey(const std::string &schema_name,
                                  const std::string &table_name,
                                  MYSQL *mariadbConn, pqxx::connection &pgConn,
                                  const std::string &timeColumn,
                                  const std::string &lastSyncTime) {
    try {
      if (timeColumn.empty() || lastSyncTime.empty()) {
        return;
      }

      std::string lowerSchemaName = schema_name;
      std::transform(lowerSchemaName.begin(), lowerSchemaName.end(),
                     lowerSchemaName.begin(), ::tolower);

      // 1. Obtener columnas de primary key
      std::vector<std::string> pkColumns =
          getPrimaryKeyColumns(mariadbConn, schema_name, table_name);

      if (pkColumns.empty()) {
        return;
      }

      Logger::info(LogCategory::TRANSFER,
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

      if (modifiedRecords.empty()) {
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
        Logger::warning(LogCategory::TRANSFER,
                        "Could not get column names for " + schema_name + "." +
                            table_name + " - skipping update processing");
        return;
      }

      // 4. Procesar cada registro modificado
      size_t totalUpdated = 0;
      for (const std::vector<std::string> &record : modifiedRecords) {
        if (record.size() != columnNames.size()) {
          Logger::warning(LogCategory::TRANSFER,
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

          std::string lowerPkColumn = pkColumns[i];
          std::transform(lowerPkColumn.begin(), lowerPkColumn.end(),
                         lowerPkColumn.begin(), ::tolower);
          whereClause += "\"" + lowerPkColumn + "\" = " +
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
        Logger::info(LogCategory::TRANSFER,
                     "Updated " + std::to_string(totalUpdated) +
                         " records in " + schema_name + "." + table_name);
      } else {
      }

    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER, "Error processing updates for " +
                                               schema_name + "." + table_name +
                                               ": " + std::string(e.what()));
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
        std::transform(columnName.begin(), columnName.end(), columnName.begin(),
                       ::tolower);
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
            // Usar cleanValueForPostgres para manejar fechas inválidas y otros
            // valores problemáticos
            // TODO: Necesitamos obtener el tipo real de la columna, por ahora
            // usar TEXT como fallback
            std::string cleanedValue =
                cleanValueForPostgres(cleanNewValue, "TEXT");
            if (cleanedValue == "NULL") {
              valueToSet = "NULL";
            } else {
              valueToSet = "'" + escapeSQL(cleanedValue) + "'";
            }
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

        return true;
      }

      return false; // No había cambios

    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER, "Error comparing/updating record: " +
                                               std::string(e.what()));
      return false;
    }
  }

  void transferDataMariaDBToPostgres() {
    Logger::info(LogCategory::TRANSFER,
                 "Starting MariaDB to PostgreSQL data transfer");

    try {
      pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());

      if (!pgConn.is_open()) {
        Logger::error(LogCategory::TRANSFER,
                      "CRITICAL ERROR: Cannot establish PostgreSQL connection "
                      "for MariaDB data transfer");
        return;
      }

      Logger::info(
          LogCategory::TRANSFER,
          "PostgreSQL connection established for MariaDB data transfer");

      auto tables = getActiveTables(pgConn);

      if (tables.empty()) {
        Logger::info(LogCategory::TRANSFER,
                     "No active MariaDB tables found for data transfer");
        return;
      }

      // Sort tables by priority: FULL_LOAD, RESET, LISTENING_CHANGES
      std::sort(tables.begin(), tables.end(),
                [](const TableInfo &a, const TableInfo &b) {
                  if (a.status == "FULL_LOAD" && b.status != "FULL_LOAD")
                    return true;
                  if (a.status != "FULL_LOAD" && b.status == "FULL_LOAD")
                    return false;
                  if (a.status == "RESET" && b.status != "RESET")
                    return true;
                  if (a.status != "RESET" && b.status == "RESET")
                    return false;
                  if (a.status == "LISTENING_CHANGES" &&
                      b.status != "LISTENING_CHANGES")
                    return true;
                  if (a.status != "LISTENING_CHANGES" &&
                      b.status == "LISTENING_CHANGES")
                    return false;
                  return false; // Keep original order for same priority
                });

      Logger::info(LogCategory::TRANSFER,
                   "Processing " + std::to_string(tables.size()) +
                       " MariaDB tables in priority order");
      // Removed individual table status logs to reduce noise

      for (auto &table : tables) {
        if (table.db_engine != "MariaDB") {
          Logger::warning(
              LogCategory::TRANSFER,
              "Skipping non-MariaDB table in transfer: " + table.db_engine +
                  " - " + table.schema_name + "." + table.table_name);
          continue;
        }

        MYSQL *mariadbConn = getMariaDBConnection(table.connection_string);
        if (!mariadbConn) {
          Logger::error(
              LogCategory::TRANSFER,
              "CRITICAL ERROR: Failed to get MariaDB connection for table " +
                  table.schema_name + "." + table.table_name +
                  " - marking as ERROR and skipping");
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
        if (!countRes.empty() && !countRes[0].empty() &&
            !countRes[0][0].empty()) {
          try {
            std::string countStr = countRes[0][0];
            if (!countStr.empty() &&
                std::all_of(countStr.begin(), countStr.end(), ::isdigit)) {
              sourceCount = std::stoul(countStr);
              Logger::info(LogCategory::TRANSFER,
                           "Source table " + schema_name + "." + table_name +
                               " has " + std::to_string(sourceCount) +
                               " records");
            } else {
              Logger::warning(LogCategory::TRANSFER,
                              "Invalid count value for table " + schema_name +
                                  "." + table_name + " - using 0");
              sourceCount = 0;
            }
          } catch (const std::exception &e) {
            Logger::warning(LogCategory::TRANSFER,
                            "Could not parse source count for table " +
                                schema_name + "." + table_name +
                                " - using 0: " + std::string(e.what()));
            sourceCount = 0;
          }
        } else {
          Logger::warning(LogCategory::TRANSFER,
                          "Could not get source count for table " +
                              schema_name + "." + table_name + " - using 0");
          sourceCount = 0;
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
            Logger::info(LogCategory::TRANSFER,
                         "Target table " + lowerSchemaName + "." + table_name +
                             " has " + std::to_string(targetCount) +
                             " records");
          } else {
            Logger::error(
                LogCategory::TRANSFER,
                "ERROR: Target count query returned no results for table " +
                    lowerSchemaName + "." + table_name);
          }
          txn.commit();
        } catch (const std::exception &e) {
          Logger::error(LogCategory::TRANSFER,
                        "ERROR getting target count for table " +
                            lowerSchemaName + "." + table_name + ": " +
                            std::string(e.what()));
        }

        // Lógica simple basada en counts reales
        if (sourceCount != targetCount) {
          Logger::info(LogCategory::TRANSFER,
                       "Source: " + std::to_string(sourceCount) +
                           ", Target: " + std::to_string(targetCount));
        }

        if (sourceCount == 0) {
          Logger::info(LogCategory::TRANSFER, "Source table " + schema_name +
                                                  "." + table_name +
                                                  " has no data");
          if (targetCount == 0) {
            Logger::info(
                LogCategory::TRANSFER,
                "Both source and target are empty - marking as NO_DATA");
            updateStatus(pgConn, schema_name, table_name, "NO_DATA", 0);
          } else {
            Logger::warning(LogCategory::TRANSFER,
                            "Source is empty but target has " +
                                std::to_string(targetCount) +
                                " records - marking as LISTENING_CHANGES");
            updateStatus(pgConn, schema_name, table_name, "LISTENING_CHANGES",
                         0);
          }
          continue;
        }

        // Si sourceCount = targetCount, verificar si hay cambios incrementales
        if (sourceCount == targetCount) {
          Logger::info(LogCategory::TRANSFER,
                       "Source and target counts match (" +
                           std::to_string(sourceCount) +
                           ") - checking for incremental changes");

          // Procesar UPDATEs si hay columna de tiempo y last_sync_time
          if (!table.last_sync_column.empty() &&
              !table.last_sync_time.empty()) {
            Logger::info(LogCategory::TRANSFER,
                         "Processing updates for " + schema_name + "." +
                             table_name + " since: " + table.last_sync_time);
            try {
              processUpdatesByPrimaryKey(schema_name, table_name, mariadbConn,
                                         pgConn, table.last_sync_column,
                                         table.last_sync_time);
              Logger::info(LogCategory::TRANSFER,
                           "Update processing completed for " + schema_name +
                               "." + table_name);
            } catch (const std::exception &e) {
              Logger::error(LogCategory::TRANSFER,
                            "ERROR processing updates for " + schema_name +
                                "." + table_name + ": " +
                                std::string(e.what()));
            }
          } else {
            Logger::info(
                LogCategory::TRANSFER,
                "No time column available for incremental updates in " +
                    schema_name + "." + table_name);
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

          // Simplificado: Siempre usar LISTENING_CHANGES para sincronización
          // incremental
          updateStatus(pgConn, schema_name, table_name, "LISTENING_CHANGES",
                       targetCount);

          // Actualizar last_processed_pk para tablas sincronizadas
          std::string pkStrategy =
              getPKStrategyFromCatalog(pgConn, schema_name, table_name);
          std::vector<std::string> pkColumns =
              getPKColumnsFromCatalog(pgConn, schema_name, table_name);

          if (pkStrategy == "PK" && !pkColumns.empty()) {
            try {
              // Obtener el último PK de la tabla para marcar como procesada
              std::string maxPKQuery = "SELECT ";
              for (size_t i = 0; i < pkColumns.size(); ++i) {
                if (i > 0)
                  maxPKQuery += ", ";
                maxPKQuery += "`" + pkColumns[i] + "`";
              }
              maxPKQuery +=
                  " FROM `" + schema_name + "`.`" + table_name + "` ORDER BY ";
              for (size_t i = 0; i < pkColumns.size(); ++i) {
                if (i > 0)
                  maxPKQuery += ", ";
                maxPKQuery += "`" + pkColumns[i] + "`";
              }
              maxPKQuery += " DESC LIMIT 1;";

              std::vector<std::vector<std::string>> maxPKResults =
                  executeQueryMariaDB(mariadbConn, maxPKQuery);

              if (!maxPKResults.empty() && !maxPKResults[0].empty()) {
                std::string lastPK;
                for (size_t i = 0; i < maxPKResults[0].size(); ++i) {
                  if (i > 0)
                    lastPK += "|";
                  lastPK += maxPKResults[0][i];
                }

                updateLastProcessedPK(pgConn, schema_name, table_name, lastPK);
                Logger::info(LogCategory::TRANSFER,
                             "Updated last_processed_pk to " + lastPK +
                                 " for synchronized table " + schema_name +
                                 "." + table_name);
              }
            } catch (const std::exception &e) {
              Logger::error(LogCategory::TRANSFER,
                            "ERROR: Failed to update last_processed_pk for "
                            "synchronized table " +
                                schema_name + "." + table_name + ": " +
                                std::string(e.what()));
            }
          }

          // IMPORTANTE: NO continuar con el procesamiento de datos si los
          // counts coinciden Solo procesar DELETEs si es necesario y luego
          // cerrar la conexión
          // Removed synchronized table log to reduce noise

          // Cerrar conexión MariaDB antes de continuar
          mysql_close(mariadbConn);
          continue;
        }

        // Si sourceCount < targetCount, hay registros eliminados en el origen
        // Procesar DELETEs por Primary Key
        if (sourceCount < targetCount) {
          size_t deletedCount = targetCount - sourceCount;
          Logger::info(LogCategory::TRANSFER,
                       "Detected " + std::to_string(deletedCount) +
                           " deleted records in " + schema_name + "." +
                           table_name + " - processing deletes");
          try {
            processDeletesByPrimaryKey(schema_name, table_name, mariadbConn,
                                       pgConn);
            Logger::info(LogCategory::TRANSFER,
                         "Delete processing completed for " + schema_name +
                             "." + table_name);
          } catch (const std::exception &e) {
            Logger::error(LogCategory::TRANSFER,
                          "ERROR processing deletes for " + schema_name + "." +
                              table_name + ": " + std::string(e.what()));
          }

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
          Logger::info(LogCategory::TRANSFER,
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
            // Validar que maxLength sea razonable antes de usarlo
            if (!maxLength.empty() && maxLength != "NULL") {
              try {
                size_t length = std::stoul(maxLength);
                // Solo usar longitud si es razonable (entre 1 y 65535)
                if (length >= 1 && length <= 65535) {
                  pgType = dataType + "(" + maxLength + ")";
                } else {
                  pgType = "VARCHAR"; // Usar VARCHAR sin restricción si la
                                      // longitud es inválida
                }
              } catch (const std::exception &) {
                pgType = "VARCHAR"; // Usar VARCHAR sin restricción si no se
                                    // puede parsear
              }
            } else {
              pgType = "VARCHAR";
            }
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
            Logger::info(LogCategory::TRANSFER,
                         "Truncating table: " + lowerSchemaName + "." +
                             table_name);
            pqxx::work txn(pgConn);
            txn.exec("TRUNCATE TABLE \"" + lowerSchemaName + "\".\"" +
                     table_name + "\" CASCADE;");
            txn.commit();
          }
        } else if (table.status == "RESET") {
          Logger::info(LogCategory::TRANSFER,
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

        Logger::info(LogCategory::TRANSFER,
                     "Starting data transfer for table " + schema_name + "." +
                         table_name + " - chunk size: " +
                         std::to_string(SyncConfig::getChunkSize()));

        // Obtener información de PK del catálogo (fuera del loop para
        // reutilizar)
        std::string pkStrategy =
            getPKStrategyFromCatalog(pgConn, schema_name, table_name);
        std::vector<std::string> pkColumns =
            getPKColumnsFromCatalog(pgConn, schema_name, table_name);
        std::vector<std::string> candidateColumns =
            getCandidateColumnsFromCatalog(pgConn, schema_name, table_name);
        std::string lastProcessedPK =
            getLastProcessedPKFromCatalog(pgConn, schema_name, table_name);

        bool hasMoreData = true;
        size_t chunkNumber = 0;
        size_t currentOffset = totalProcessed; // Usar el last_offset de la BD

        // CRITICAL: Add timeout to prevent infinite loops
        auto startTime = std::chrono::steady_clock::now();
        const auto MAX_PROCESSING_TIME =
            std::chrono::hours(2); // 2 hours max per table

        while (hasMoreData) {
          chunkNumber++;
          const size_t CHUNK_SIZE = SyncConfig::getChunkSize();

          // CRITICAL: Check timeout to prevent infinite loops
          auto currentTime = std::chrono::steady_clock::now();
          auto elapsedTime = currentTime - startTime;
          if (elapsedTime > MAX_PROCESSING_TIME) {
            Logger::error(
                LogCategory::TRANSFER,
                "CRITICAL: Maximum processing time reached (" +
                    std::to_string(
                        std::chrono::duration_cast<std::chrono::minutes>(
                            elapsedTime)
                            .count()) +
                    " minutes) for table " + schema_name + "." + table_name +
                    " - breaking to prevent infinite loop");
            hasMoreData = false;
            break;
          }

          // CRITICAL: Add maximum chunk limit to prevent infinite loops
          if (chunkNumber > 10000) {
            Logger::error(LogCategory::TRANSFER,
                          "CRITICAL: Maximum chunk limit reached (" +
                              std::to_string(chunkNumber) + ") for table " +
                              schema_name + "." + table_name +
                              " - breaking to prevent infinite loop");
            hasMoreData = false;
            break;
          }

          Logger::info(LogCategory::TRANSFER,
                       "Processing chunk " + std::to_string(chunkNumber) +
                           " for table " + schema_name + "." + table_name +
                           " (size: " + std::to_string(CHUNK_SIZE) +
                           ", offset: " + std::to_string(currentOffset) + ")");

          // OPTIMIZED: Usar cursor-based pagination con primary key o columnas
          // candidatas
          std::string selectQuery =
              "SELECT * FROM `" + schema_name + "`.`" + table_name + "`";

          if (pkStrategy == "PK" && !pkColumns.empty()) {
            // CURSOR-BASED PAGINATION: Usar PK real para paginación eficiente
            if (!lastProcessedPK.empty()) {
              selectQuery += " WHERE ";
              std::vector<std::string> lastPKValues =
                  parseLastPK(lastProcessedPK);

              // Simplificado: usar solo la primera columna del PK para
              // paginación Esto es más simple y confiable que la lógica
              // compleja de PKs compuestos
              if (!lastPKValues.empty()) {
                selectQuery += "`" + pkColumns[0] + "` > '" +
                               escapeSQL(lastPKValues[0]) + "'";
              }
            }

            // Simplificado: ordenar solo por la primera columna del PK
            selectQuery += " ORDER BY `" + pkColumns[0] + "`";
            selectQuery += " LIMIT " + std::to_string(CHUNK_SIZE) + ";";
          } else if (pkStrategy == "TEMPORAL_PK" && !candidateColumns.empty()) {
            // CURSOR-BASED PAGINATION: Usar columnas candidatas para paginación
            // eficiente
            if (!lastProcessedPK.empty()) {
              selectQuery += " WHERE `" + candidateColumns[0] + "` > '" +
                             escapeSQL(lastProcessedPK) + "'";
            }

            // Ordenar por la primera columna candidata
            selectQuery += " ORDER BY `" + candidateColumns[0] + "`";
            selectQuery += " LIMIT " + std::to_string(CHUNK_SIZE) + ";";
          } else {
            // FALLBACK: Usar OFFSET pagination para tablas sin PK ni columnas
            // candidatas
            selectQuery += " LIMIT " + std::to_string(CHUNK_SIZE) + " OFFSET " +
                           std::to_string(currentOffset) + ";";
          }

          Logger::info(LogCategory::TRANSFER,
                       "Executing data transfer query for chunk " +
                           std::to_string(chunkNumber));

          std::vector<std::vector<std::string>> results =
              executeQueryMariaDB(mariadbConn, selectQuery);

          if (results.size() > 0) {
            Logger::info(LogCategory::TRANSFER,
                         "Retrieved chunk " + std::to_string(chunkNumber) +
                             " with " + std::to_string(results.size()) +
                             " rows for " + schema_name + "." + table_name);
          }

          if (results.empty()) {
            Logger::info(LogCategory::TRANSFER,
                         "No more data available for table " + schema_name +
                             "." + table_name + " - ending transfer loop");
            hasMoreData = false;
            break;
          }

          size_t rowsInserted = 0;

          try {
            Logger::info(LogCategory::TRANSFER,
                         "Preparing bulk upsert for chunk " +
                             std::to_string(chunkNumber) + " with " +
                             std::to_string(results.size()) + " rows");

            std::string columnsStr;
            for (size_t i = 0; i < columnNames.size(); ++i) {
              columnsStr += "\"" + columnNames[i] + "\"";
              if (i < columnNames.size() - 1)
                columnsStr += ",";
            }

            Logger::info(LogCategory::TRANSFER,
                         "Columns for bulk upsert: " + columnsStr);

            rowsInserted = results.size();

            if (rowsInserted > 0) {
              try {
                Logger::info(LogCategory::TRANSFER,
                             "Executing bulk upsert for chunk " +
                                 std::to_string(chunkNumber));
                performBulkUpsert(pgConn, results, columnNames, columnTypes,
                                  lowerSchemaName, table_name, schema_name);
                Logger::info(LogCategory::TRANSFER,
                             "Successfully processed chunk " +
                                 std::to_string(chunkNumber) + " with " +
                                 std::to_string(rowsInserted) + " rows for " +
                                 schema_name + "." + table_name);
              } catch (const std::exception &e) {
                std::string errorMsg = e.what();
                Logger::error(LogCategory::TRANSFER,
                              "CRITICAL ERROR: Bulk upsert failed for chunk " +
                                  std::to_string(chunkNumber) + " in table " +
                                  schema_name + "." + table_name + ": " +
                                  errorMsg);

                // CRITICAL: Check for transaction abort errors that cause
                // infinite loops
                if (errorMsg.find("current transaction is aborted") !=
                        std::string::npos ||
                    errorMsg.find("previously aborted") != std::string::npos ||
                    errorMsg.find("aborted transaction") != std::string::npos) {
                  Logger::error(LogCategory::TRANSFER,
                                "CRITICAL: Transaction abort detected - "
                                "breaking loop to prevent infinite hang");
                  hasMoreData = false;
                  break;
                }

                rowsInserted = 0;
              }
            } else {
              Logger::warning(LogCategory::TRANSFER,
                              "No rows to process in chunk " +
                                  std::to_string(chunkNumber) + " for table " +
                                  schema_name + "." + table_name);
            }

          } catch (const std::exception &e) {
            std::string errorMsg = e.what();
            Logger::error(LogCategory::TRANSFER,
                          "ERROR processing data for chunk " +
                              std::to_string(chunkNumber) + " in table " +
                              schema_name + "." + table_name + ": " + errorMsg);

            // CRITICAL: Check for critical errors that require breaking the
            // loop
            if (errorMsg.find("current transaction is aborted") !=
                    std::string::npos ||
                errorMsg.find("previously aborted") != std::string::npos ||
                errorMsg.find("aborted transaction") != std::string::npos ||
                errorMsg.find("connection") != std::string::npos ||
                errorMsg.find("timeout") != std::string::npos) {
              Logger::error(LogCategory::TRANSFER,
                            "CRITICAL: Critical error detected - breaking loop "
                            "to prevent infinite hang");
              hasMoreData = false;
              break;
            }
          }

          // Update targetCount and currentOffset based on actual processed rows
          targetCount += rowsInserted;

          // Solo incrementar currentOffset para tablas sin PK ni columnas
          // candidatas (OFFSET pagination) Para tablas con PK o TEMPORAL_PK se
          // usa cursor-based pagination con last_processed_pk
          if (pkStrategy != "PK" && pkStrategy != "TEMPORAL_PK") {
            currentOffset += rowsInserted;
          }
          Logger::info(
              LogCategory::TRANSFER,
              "Updated target count to " + std::to_string(targetCount) +
                  " and current offset to " + std::to_string(currentOffset) +
                  " after processing chunk " + std::to_string(chunkNumber));

          // OPTIMIZED: Update last_processed_pk for cursor-based pagination
          if (((pkStrategy == "PK" && !pkColumns.empty()) ||
               (pkStrategy == "TEMPORAL_PK" && !candidateColumns.empty())) &&
              !results.empty()) {
            try {
              // Obtener el último PK del chunk procesado
              std::vector<std::string> columnsToUse =
                  (pkStrategy == "PK") ? pkColumns : candidateColumns;
              std::string lastPK =
                  getLastPKFromResults(results, columnsToUse, columnNames);
              if (!lastPK.empty()) {
                updateLastProcessedPK(pgConn, schema_name, table_name, lastPK);
                // Actualizar la variable local para el siguiente chunk
                lastProcessedPK = lastPK;
                Logger::info(LogCategory::TRANSFER,
                             "Updated last_processed_pk to " + lastPK +
                                 " for table " + schema_name + "." +
                                 table_name + " (strategy: " + pkStrategy +
                                 ")");
              }
            } catch (const std::exception &e) {
              Logger::error(
                  LogCategory::TRANSFER,
                  "ERROR: Failed to update last_processed_pk for table " +
                      schema_name + "." + table_name + ": " +
                      std::string(e.what()));
            }
          }

          // Update last_offset in database solo para tablas sin PK (OFFSET
          // pagination) Para tablas con PK o TEMPORAL_PK se usa
          // last_processed_pk en lugar de last_offset
          if (pkStrategy != "PK" && pkStrategy != "TEMPORAL_PK") {
            try {
              Logger::info(LogCategory::TRANSFER,
                           "Updating last_offset to " +
                               std::to_string(currentOffset) + " for table " +
                               schema_name + "." + table_name);
              pqxx::work updateTxn(pgConn);
              updateTxn.exec("UPDATE metadata.catalog SET last_offset='" +
                             std::to_string(currentOffset) +
                             "' WHERE schema_name='" + escapeSQL(schema_name) +
                             "' AND table_name='" + escapeSQL(table_name) +
                             "';");
              updateTxn.commit();
            } catch (const std::exception &e) {
              Logger::error(LogCategory::TRANSFER,
                            "ERROR: Failed to update last_offset for " +
                                schema_name + "." + table_name + ": " +
                                std::string(e.what()));
            }
          }

          // Verificar si hemos procesado todos los datos disponibles
          if (results.size() < CHUNK_SIZE) {
            Logger::info(LogCategory::TRANSFER,
                         "Retrieved " + std::to_string(results.size()) +
                             " rows (less than chunk size " +
                             std::to_string(CHUNK_SIZE) +
                             ") - ending data transfer");
            hasMoreData = false;
          } else if (targetCount >= sourceCount) {
            Logger::info(LogCategory::TRANSFER,
                         "Target count (" + std::to_string(targetCount) +
                             ") >= source count (" +
                             std::to_string(sourceCount) +
                             ") - ending data transfer");
            hasMoreData = false;
          }
        }

        // DELETEs ya fueron procesados arriba cuando sourceCount < targetCount

        // OPTIMIZED: Update last_processed_pk for completed transfer (even if
        // single chunk)
        if (pkStrategy == "PK" && !pkColumns.empty() && targetCount > 0) {
          try {
            // Obtener el último PK de la tabla para marcar como completamente
            // procesada
            std::string maxPKQuery = "SELECT ";
            for (size_t i = 0; i < pkColumns.size(); ++i) {
              if (i > 0)
                maxPKQuery += ", ";
              maxPKQuery += "`" + pkColumns[i] + "`";
            }
            maxPKQuery +=
                " FROM `" + schema_name + "`.`" + table_name + "` ORDER BY ";
            for (size_t i = 0; i < pkColumns.size(); ++i) {
              if (i > 0)
                maxPKQuery += ", ";
              maxPKQuery += "`" + pkColumns[i] + "`";
            }
            maxPKQuery += " DESC LIMIT 1;";

            Logger::info(LogCategory::TRANSFER,
                         "DEBUG: Executing maxPKQuery for " + schema_name +
                             "." + table_name + ": " + maxPKQuery);

            std::vector<std::vector<std::string>> maxPKResults =
                executeQueryMariaDB(mariadbConn, maxPKQuery);

            Logger::info(LogCategory::TRANSFER,
                         "DEBUG: maxPKQuery result for " + schema_name + "." +
                             table_name + " - rows returned: " +
                             std::to_string(maxPKResults.size()));

            if (!maxPKResults.empty() && !maxPKResults[0].empty()) {
              std::string lastPK;
              for (size_t i = 0; i < maxPKResults[0].size(); ++i) {
                if (i > 0)
                  lastPK += "|";
                lastPK += maxPKResults[0][i];
              }

              Logger::info(LogCategory::TRANSFER,
                           "DEBUG: Updating last_processed_pk to " + lastPK +
                               " for " + schema_name + "." + table_name);

              updateLastProcessedPK(pgConn, schema_name, table_name, lastPK);
              Logger::info(LogCategory::TRANSFER,
                           "Updated last_processed_pk to " + lastPK +
                               " for synchronized table " + schema_name + "." +
                               table_name);
            } else {
              Logger::warning(
                  LogCategory::TRANSFER,
                  "No PK data found for synchronized table " + schema_name +
                      "." + table_name + " - maxPKResults.empty()=" +
                      (maxPKResults.empty() ? "true" : "false") +
                      ", first row empty=" +
                      (!maxPKResults.empty() && maxPKResults[0].empty()
                           ? "true"
                           : "false"));
            }
          } catch (const std::exception &e) {
            Logger::error(LogCategory::TRANSFER,
                          "ERROR: Failed to update last_processed_pk for "
                          "completed table " +
                              schema_name + "." + table_name + ": " +
                              std::string(e.what()));
          }
        }

        if (targetCount > 0) {
          // Simplificado: Siempre usar LISTENING_CHANGES para sincronización
          // incremental
          Logger::info(LogCategory::TRANSFER,
                       "Table " + schema_name + "." + table_name +
                           " synchronized - LISTENING_CHANGES (source: " +
                           std::to_string(sourceCount) +
                           ", target: " + std::to_string(targetCount) + ")");
          try {
            // Para tablas con PK, usar targetCount como last_offset (registros
            // realmente procesados) Para tablas sin PK, usar targetCount como
            // last_offset
            updateStatus(pgConn, schema_name, table_name, "LISTENING_CHANGES",
                         targetCount);
            Logger::info(
                LogCategory::TRANSFER,
                "Successfully updated status to LISTENING_CHANGES for " +
                    schema_name + "." + table_name);
          } catch (const std::exception &e) {
            Logger::error(LogCategory::TRANSFER,
                          "ERROR updating status to LISTENING_CHANGES for " +
                              schema_name + "." + table_name + ": " +
                              std::string(e.what()));
          }
        } else {
          Logger::warning(LogCategory::TRANSFER,
                          "No data transferred for table " + schema_name + "." +
                              table_name + " - keeping current status");
        }

        Logger::info(LogCategory::TRANSFER, "Table processing completed for " +
                                                schema_name + "." + table_name);

        // Cerrar conexión MariaDB con verificación
        if (mariadbConn) {
          mysql_close(mariadbConn);
          mariadbConn = nullptr;
        }
      }

      Logger::info(
          LogCategory::TRANSFER,
          "MariaDB to PostgreSQL data transfer completed successfully");
    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER,
                    "CRITICAL ERROR in transferDataMariaDBToPostgres: " +
                        std::string(e.what()) +
                        " - MariaDB data transfer completely failed");
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

      std::string updateQuery =
          "UPDATE metadata.catalog SET status='" + status + "'";

      // Actualizar last_offset para todos los status que requieren tracking
      if (status == "FULL_LOAD" || status == "RESET" ||
          status == "LISTENING_CHANGES") {
        updateQuery += ", last_offset='" + std::to_string(offset) + "'";
      }

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
    } catch (const pqxx::sql_error &e) {
      Logger::error(LogCategory::TRANSFER,
                    "SQL ERROR updating status: " + std::string(e.what()) +
                        " [SQL State: " + e.sqlstate() + "]");
    } catch (const pqxx::broken_connection &e) {
      Logger::error(LogCategory::TRANSFER,
                    "CONNECTION ERROR updating status: " +
                        std::string(e.what()));
    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER,
                    "ERROR updating status: " + std::string(e.what()));
    }
  }

private:
  std::vector<std::string> getPrimaryKeyColumns(MYSQL *mariadbConn,
                                                const std::string &schema_name,
                                                const std::string &table_name) {
    std::vector<std::string> pkColumns;

    // Validate input parameters
    if (!mariadbConn) {
      Logger::error(LogCategory::TRANSFER, "getPrimaryKeyColumns",
                    "MariaDB connection is null");
      return pkColumns;
    }

    if (schema_name.empty() || table_name.empty()) {
      Logger::error(LogCategory::TRANSFER, "getPrimaryKeyColumns",
                    "Schema name or table name is empty");
      return pkColumns;
    }

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
        for (size_t i = 0; i < pkColumns.size() && i < row.size(); ++i) {
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
      Logger::error(LogCategory::TRANSFER, "deleteRecordsByPrimaryKey",
                    "Error deleting records: " + std::string(e.what()));
    }

    return deletedCount;
  }

  std::string escapeSQL(const std::string &value) {
    if (value.empty()) {
      return "";
    }
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
      Logger::warning(LogCategory::TRANSFER, "No valid MariaDB connection");
      return results;
    }

    if (mysql_query(conn, query.c_str())) {
      Logger::warning(
          LogCategory::TRANSFER,
          "Query execution failed: " + std::string(mysql_error(conn)) +
              " for query: " + query.substr(0, 100) + "...");
      return results;
    }

    MYSQL_RES *res = mysql_store_result(conn);
    if (!res) {
      if (mysql_field_count(conn) > 0) {
        Logger::warning(LogCategory::TRANSFER,
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
      txn.exec("SET statement_timeout = '600s'");

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

          try {
            txn.exec(batchQuery);
            totalProcessed += values.size();
          } catch (const std::exception &e) {
            std::string errorMsg = e.what();

            // Detectar transacción abortada
            if (errorMsg.find("current transaction is aborted") !=
                std::string::npos) {
              Logger::warning(LogCategory::TRANSFER, "performBulkUpsert",
                              "Transaction aborted detected, rolling back and "
                              "processing individually");

              try {
                txn.abort(); // Rollback la transacción abortada
              } catch (...) {
                // Ignorar errores de rollback
              }

              // CRITICAL: Limit individual processing to prevent infinite loops
              size_t individualProcessed = 0;
              const size_t MAX_INDIVIDUAL_PROCESSING = 100;

              // Procesar registros individualmente con nuevas transacciones
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

                  // Crear nueva transacción para cada registro
                  pqxx::work singleTxn(pgConn);
                  singleTxn.exec("SET statement_timeout = '600s'");
                  std::string singleQuery =
                      upsertQuery + singleRowValues + conflictClause;
                  singleTxn.exec(singleQuery);
                  singleTxn.commit();
                  totalProcessed++;
                  individualProcessed++;

                } catch (const std::exception &singleError) {
                  Logger::error(
                      LogCategory::TRANSFER, "performBulkUpsert",
                      "Skipping problematic record: " +
                          std::string(singleError.what()).substr(0, 100));
                  // Continuar con el siguiente registro
                }
              }

              // CRITICAL: Log if we hit the processing limit
              if (individualProcessed >= MAX_INDIVIDUAL_PROCESSING) {
                Logger::warning(LogCategory::TRANSFER, "performBulkUpsert",
                                "Hit maximum individual processing limit (" +
                                    std::to_string(MAX_INDIVIDUAL_PROCESSING) +
                                    ") - stopping to prevent infinite loop");
              }
            }
            // Manejo específico para errores de datos binarios inválidos
            else if (errorMsg.find("not a valid binary digit") !=
                         std::string::npos ||
                     errorMsg.find("invalid input syntax") !=
                         std::string::npos) {

              Logger::warning(LogCategory::TRANSFER, "performBulkUpsert",
                              "Binary data error detected, processing batch "
                              "individually: " +
                                  errorMsg.substr(0, 100));

              // CRITICAL: Limit individual processing to prevent infinite loops
              size_t binaryErrorProcessed = 0;
              const size_t MAX_BINARY_ERROR_PROCESSING = 50;

              // Procesar registros individualmente para identificar el
              // problemático
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
                  Logger::error(
                      LogCategory::TRANSFER, "performBulkUpsert",
                      "Skipping problematic record in batch: " +
                          std::string(singleError.what()).substr(0, 100));
                  // Continuar con el siguiente registro
                }
              }

              // CRITICAL: Log if we hit the processing limit
              if (binaryErrorProcessed >= MAX_BINARY_ERROR_PROCESSING) {
                Logger::warning(
                    LogCategory::TRANSFER, "performBulkUpsert",
                    "Hit maximum binary error processing limit (" +
                        std::to_string(MAX_BINARY_ERROR_PROCESSING) +
                        ") - stopping to prevent infinite loop");
              }
            } else {
              // Re-lanzar errores que no sean de datos binarios o transacciones
              // abortadas
              throw;
            }
          }
        }
      }

      // Solo hacer commit si la transacción no fue abortada
      try {
        txn.commit();
      } catch (const std::exception &commitError) {
        // Si el commit falla porque la transacción fue abortada, ignorar el
        // error
        if (std::string(commitError.what()).find("previously aborted") !=
                std::string::npos ||
            std::string(commitError.what()).find("aborted transaction") !=
                std::string::npos) {
          Logger::warning(LogCategory::TRANSFER, "performBulkUpsert",
                          "Skipping commit for aborted transaction");
        } else {
          // Re-lanzar otros errores de commit
          throw;
        }
      }

    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER,
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
      txn.exec("SET statement_timeout = '600s'");

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

    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER,
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
      Logger::error(LogCategory::TRANSFER,
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

    // Detectar valores NULL de MariaDB - MEJORADO
    bool isNull =
        (cleanValue.empty() || cleanValue == "NULL" || cleanValue == "null" ||
         cleanValue == "\\N" || cleanValue == "\\0" || cleanValue == "0" ||
         cleanValue == "0.0" || cleanValue == "0.00" || cleanValue == "0.000" ||
         cleanValue.find("0000-") != std::string::npos ||
         cleanValue.find("0000-00-00") != std::string::npos ||
         cleanValue.find("1900-01-01") != std::string::npos ||
         cleanValue.find("1970-01-01") != std::string::npos);

    // Limpiar caracteres de control y caracteres problemáticos
    for (char &c : cleanValue) {
      if (static_cast<unsigned char>(c) > 127 || c < 32) {
        isNull = true;
        break;
      }
    }

    // NUEVO: Manejo específico para VARCHAR/CHAR con longitud limitada
    if (upperType.find("VARCHAR") != std::string::npos ||
        upperType.find("CHAR") != std::string::npos) {
      // Extraer longitud máxima del tipo (ej: VARCHAR(2) -> 2)
      size_t openParen = upperType.find('(');
      size_t closeParen = upperType.find(')');
      if (openParen != std::string::npos && closeParen != std::string::npos) {
        try {
          size_t maxLen = std::stoul(
              upperType.substr(openParen + 1, closeParen - openParen - 1));
          if (cleanValue.length() > maxLen) {
            Logger::warning(
                LogCategory::TRANSFER, "cleanValueForPostgres",
                "Value too long for " + upperType + ", truncating from " +
                    std::to_string(cleanValue.length()) + " to " +
                    std::to_string(maxLen) +
                    " characters: " + cleanValue.substr(0, 20) + "...");
            cleanValue = cleanValue.substr(0, maxLen);
            // Si el valor truncado está vacío, marcarlo como NULL
            if (cleanValue.empty()) {
              isNull = true;
            }
          }
        } catch (const std::exception &e) {
          // Si no se puede parsear la longitud, continuar sin truncar
        }
      }
    }

    // NUEVO: Manejo específico para datos binarios inválidos
    if (upperType.find("BYTEA") != std::string::npos ||
        upperType.find("BLOB") != std::string::npos ||
        upperType.find("BIT") != std::string::npos) {

      // Verificar si contiene caracteres no binarios válidos
      bool hasInvalidBinaryChars = false;
      for (char c : cleanValue) {
        // Solo permitir caracteres hex válidos (0-9, A-F, a-f) y espacios
        if (!std::isxdigit(c) && c != ' ' && c != '\\' && c != 'x') {
          hasInvalidBinaryChars = true;
          break;
        }
      }

      if (hasInvalidBinaryChars) {
        Logger::warning(LogCategory::TRANSFER, "cleanValueForPostgres",
                        "Invalid binary data detected, converting to NULL: " +
                            cleanValue.substr(0, 50) + "...");
        isNull = true;
      } else if (!cleanValue.empty() && cleanValue.length() > 1000) {
        // Datos binarios muy grandes pueden causar problemas
        Logger::warning(LogCategory::TRANSFER, "cleanValueForPostgres",
                        "Large binary data detected, truncating: " +
                            std::to_string(cleanValue.length()) + " bytes");
        cleanValue = cleanValue.substr(0, 1000);
      }
    }

    // Para fechas, cualquier valor que no sea una fecha válida = NULL
    if (upperType.find("TIMESTAMP") != std::string::npos ||
        upperType.find("DATETIME") != std::string::npos ||
        upperType.find("DATE") != std::string::npos) {
      // Detectar valores numéricos como inválidos para fechas
      bool isNumeric = true;
      bool hasDecimal = false;
      for (char c : cleanValue) {
        if (!std::isdigit(c) && c != '.' && c != '-') {
          isNumeric = false;
          break;
        }
        if (c == '.') {
          hasDecimal = true;
        }
      }

      // Si es puramente numérico (incluyendo decimales) o no tiene formato de
      // fecha válido
      if (isNumeric || cleanValue.length() < 10 ||
          cleanValue.find("-") == std::string::npos ||
          cleanValue.find("0000") != std::string::npos ||
          cleanValue.find("0000-00-00") != std::string::npos) {
        isNull = true;
      }
    }

    // Si es NULL, generar valor por defecto en lugar de NULL
    if (isNull) {
      if (upperType.find("INTEGER") != std::string::npos ||
          upperType.find("BIGINT") != std::string::npos ||
          upperType.find("SMALLINT") != std::string::npos) {
        return "0"; // Valor por defecto para enteros
      } else if (upperType.find("REAL") != std::string::npos ||
                 upperType.find("FLOAT") != std::string::npos ||
                 upperType.find("DOUBLE") != std::string::npos ||
                 upperType.find("NUMERIC") != std::string::npos) {
        return "0.0"; // Valor por defecto para números decimales
      } else if (upperType == "TEXT") {
        // Fallback para TEXT: devolver NULL para que PostgreSQL use el valor
        // por defecto de la columna
        return "NULL";
      } else if (upperType.find("VARCHAR") != std::string::npos ||
                 upperType.find("TEXT") != std::string::npos ||
                 upperType.find("CHAR") != std::string::npos) {
        return "DEFAULT"; // Valor por defecto para texto
      } else if (upperType.find("TIMESTAMP") != std::string::npos ||
                 upperType.find("DATETIME") != std::string::npos) {
        return "1970-01-01 00:00:00"; // Valor por defecto para fechas
      } else if (upperType.find("DATE") != std::string::npos) {
        return "1970-01-01"; // Valor por defecto para fechas
      } else if (upperType.find("TIME") != std::string::npos) {
        return "00:00:00"; // Valor por defecto para tiempo
      } else {
        return "DEFAULT"; // Valor por defecto genérico
      }
    }

    return cleanValue;
  }

  // NUEVAS FUNCIONES PARA CURSOR-BASED PAGINATION

  std::string getPKStrategyFromCatalog(pqxx::connection &pgConn,
                                       const std::string &schema_name,
                                       const std::string &table_name) {
    try {
      pqxx::work txn(pgConn);
      auto result = txn.exec("SELECT pk_strategy FROM metadata.catalog "
                             "WHERE schema_name='" +
                             escapeSQL(schema_name) + "' AND table_name='" +
                             escapeSQL(table_name) + "'");
      txn.commit();

      if (!result.empty() && !result[0][0].is_null()) {
        return result[0][0].as<std::string>();
      }
    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER, "getPKStrategyFromCatalog",
                    "Error getting PK strategy: " + std::string(e.what()));
    }
    return "OFFSET"; // Fallback
  }

  std::vector<std::string>
  getPKColumnsFromCatalog(pqxx::connection &pgConn,
                          const std::string &schema_name,
                          const std::string &table_name) {
    std::vector<std::string> pkColumns;
    try {
      pqxx::work txn(pgConn);
      auto result = txn.exec("SELECT pk_columns FROM metadata.catalog "
                             "WHERE schema_name='" +
                             escapeSQL(schema_name) + "' AND table_name='" +
                             escapeSQL(table_name) + "'");
      txn.commit();

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

  std::string getLastProcessedPKFromCatalog(pqxx::connection &pgConn,
                                            const std::string &schema_name,
                                            const std::string &table_name) {
    try {
      pqxx::work txn(pgConn);
      auto result = txn.exec("SELECT last_processed_pk FROM metadata.catalog "
                             "WHERE schema_name='" +
                             escapeSQL(schema_name) + "' AND table_name='" +
                             escapeSQL(table_name) + "'");
      txn.commit();

      if (!result.empty() && !result[0][0].is_null()) {
        return result[0][0].as<std::string>();
      }
    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER, "getLastProcessedPKFromCatalog",
                    "Error getting last processed PK: " +
                        std::string(e.what()));
    }
    return ""; // Fallback
  }

  std::vector<std::string>
  getCandidateColumnsFromCatalog(pqxx::connection &pgConn,
                                 const std::string &schema_name,
                                 const std::string &table_name) {
    std::vector<std::string> candidateColumns;
    try {
      pqxx::work txn(pgConn);
      auto result = txn.exec("SELECT candidate_columns FROM metadata.catalog "
                             "WHERE schema_name='" +
                             escapeSQL(schema_name) + "' AND table_name='" +
                             escapeSQL(table_name) + "'");
      txn.commit();

      if (!result.empty() && !result[0][0].is_null()) {
        std::string candidateColumnsJSON = result[0][0].as<std::string>();
        candidateColumns = parseJSONArray(candidateColumnsJSON);
      }
    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER, "getCandidateColumnsFromCatalog",
                    "Error getting candidate columns: " +
                        std::string(e.what()));
    }
    return candidateColumns;
  }

  std::vector<std::string> parseJSONArray(const std::string &jsonArray) {
    std::vector<std::string> result;
    try {
      // Simple JSON array parser for ["col1", "col2", "col3"]
      if (jsonArray.empty() || jsonArray == "[]") {
        return result;
      }

      std::string content = jsonArray;
      // Remove brackets
      if (content.front() == '[' && content.back() == ']') {
        content = content.substr(1, content.length() - 2);
      }

      // Split by comma and remove quotes
      std::istringstream ss(content);
      std::string item;
      while (std::getline(ss, item, ',')) {
        // Remove quotes and whitespace
        item.erase(std::remove(item.begin(), item.end(), '"'), item.end());
        item.erase(std::remove(item.begin(), item.end(), ' '), item.end());
        if (!item.empty()) {
          result.push_back(item);
        }
      }
    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER, "parseJSONArray",
                    "Error parsing JSON array: " + std::string(e.what()));
    }
    return result;
  }

  void updateLastProcessedPK(pqxx::connection &pgConn,
                             const std::string &schema_name,
                             const std::string &table_name,
                             const std::string &lastPK) {
    try {
      pqxx::work txn(pgConn);
      txn.exec("UPDATE metadata.catalog SET last_processed_pk='" +
               escapeSQL(lastPK) + "' WHERE schema_name='" +
               escapeSQL(schema_name) + "' AND table_name='" +
               escapeSQL(table_name) + "'");
      txn.commit();
    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER, "updateLastProcessedPK",
                    "Error updating last processed PK: " +
                        std::string(e.what()));
    }
  }

  std::string
  getLastPKFromResults(const std::vector<std::vector<std::string>> &results,
                       const std::vector<std::string> &pkColumns,
                       const std::vector<std::string> &columnNames) {
    if (results.empty() || pkColumns.empty()) {
      return "";
    }

    try {
      // Obtener el último registro (results ya está ordenado por PK)
      const auto &lastRow = results.back();

      // Simplificado: extraer solo la primera columna del PK
      std::string lastPK;
      if (!pkColumns.empty()) {
        // Encontrar el índice de la primera columna PK en columnNames
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

  std::vector<std::string> parseLastPK(const std::string &lastPK) {
    std::vector<std::string> pkValues;
    try {
      // Simplificado: solo extraer la primera columna del PK
      // (ya no usamos separadores | para PKs compuestos)
      if (!lastPK.empty()) {
        pkValues.push_back(lastPK);
      }
    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER, "parseLastPK",
                    "Error parsing last PK: " + std::string(e.what()));
    }
    return pkValues;
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