#ifndef MARIADBTOPOSTGRES_H
#define MARIADBTOPOSTGRES_H

#include "catalog/catalog_manager.h"
#include "engines/database_engine.h"
#include "sync/DatabaseToPostgresSync.h"
#include "sync/TableProcessorThreadPool.h"
#include <algorithm>
#include <cctype>
#include <chrono>
#include <iostream>
#include <mysql/mysql.h>
#include <pqxx/pqxx>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

using json = nlohmann::json;
using namespace ParallelProcessing;

class MariaDBToPostgres : public DatabaseToPostgresSync {
public:
  MariaDBToPostgres() = default;
  ~MariaDBToPostgres() { shutdownParallelProcessing(); }

  static std::unordered_map<std::string, std::string> dataTypeMap;
  static std::unordered_map<std::string, std::string> collationMap;

  std::string cleanValueForPostgres(const std::string &value,
                                    const std::string &columnType) override;

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
          "status, last_processed_pk, pk_strategy, "
          "pk_columns, has_pk "
          "FROM metadata.catalog "
          "WHERE active=true AND db_engine='MariaDB' AND status != 'NO_DATA' "
          "ORDER BY schema_name, table_name;");
      txn.commit();

      Logger::info(LogCategory::TRANSFER, "getActiveTables",
                   "Query returned " + std::to_string(results.size()) +
                       " rows from catalog");

      for (const auto &row : results) {
        if (row.size() < 12) {
          Logger::warning(LogCategory::TRANSFER, "getActiveTables",
                          "Row has only " + std::to_string(row.size()) +
                              " columns, expected 12 - skipping");
          continue;
        }

        Logger::info(LogCategory::TRANSFER, "getActiveTables",
                     "Processing table: " +
                         (row[0].is_null() ? "" : row[0].as<std::string>()) +
                         "." +
                         (row[1].is_null() ? "" : row[1].as<std::string>()));

        TableInfo t;
        t.schema_name = row[0].is_null() ? "" : row[0].as<std::string>();
        t.table_name = row[1].is_null() ? "" : row[1].as<std::string>();
        t.cluster_name = row[2].is_null() ? "" : row[2].as<std::string>();
        t.db_engine = row[3].is_null() ? "" : row[3].as<std::string>();
        t.connection_string = row[4].is_null() ? "" : row[4].as<std::string>();
        t.last_sync_time = row[5].is_null() ? "" : row[5].as<std::string>();
        t.last_sync_column = row[6].is_null() ? "" : row[6].as<std::string>();
        t.status = row[7].is_null() ? "" : row[7].as<std::string>();
        t.last_processed_pk = row[8].is_null() ? "" : row[8].as<std::string>();
        t.pk_strategy = row[9].is_null() ? "" : row[9].as<std::string>();
        t.pk_columns = row[10].is_null() ? "" : row[10].as<std::string>();
        t.has_pk = row[11].is_null() ? false : row[11].as<bool>();
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

    std::string lowerTableName = table_name;
    std::transform(lowerTableName.begin(), lowerTableName.end(),
                   lowerTableName.begin(), ::tolower);

    for (const auto &row : results) {
      if (row.size() < 3)
        continue;

      std::string indexName = row[0];
      std::string nonUnique = row[1];
      std::string columnName = row[2];
      std::transform(columnName.begin(), columnName.end(), columnName.begin(),
                     ::tolower);
      std::transform(indexName.begin(), indexName.end(), indexName.begin(),
                     ::tolower);

      std::string createQuery = "CREATE INDEX IF NOT EXISTS \"" + indexName +
                                "\" ON \"" + lowerSchemaName + "\".\"" +
                                lowerTableName + "\" (\"" + columnName + "\");";

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
    Logger::info(LogCategory::TRANSFER,
                 "Starting MariaDB to PostgreSQL table setup");

    try {
      pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());

      if (!pgConn.is_open()) {
        Logger::error(LogCategory::TRANSFER,
                      "setupTableTargetMariaDBToPostgres",
                      "CRITICAL ERROR: Cannot establish PostgreSQL connection "
                      "for MariaDB table setup");
        return;
      }

      auto tables = getActiveTables(pgConn);

      if (tables.empty()) {

        return;
      }

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

      // Removed individual table status logs to reduce noise

      Logger::info(LogCategory::TRANSFER, "Setting up " +
                                              std::to_string(tables.size()) +
                                              " MariaDB tables in PostgreSQL");

      for (const auto &table : tables) {
        if (table.db_engine != "MariaDB")
          continue;

        MYSQL *mariadbConn = getMariaDBConnection(table.connection_string);
        if (!mariadbConn) {
          Logger::error(LogCategory::TRANSFER,
                        "setupTableTargetMariaDBToPostgres",
                        "Failed to get MariaDB connection for table " +
                            table.schema_name + "." + table.table_name);
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
                        "setupTableTargetMariaDBToPostgres",
                        "No columns found for table " + table.schema_name +
                            "." + table.table_name + " - skipping");
          continue;
        }

        std::string lowerSchema = table.schema_name;
        std::transform(lowerSchema.begin(), lowerSchema.end(),
                       lowerSchema.begin(), ::tolower);
        std::string lowerTableName = table.table_name;
        std::transform(lowerTableName.begin(), lowerTableName.end(),
                       lowerTableName.begin(), ::tolower);

        {
          pqxx::work txn(pgConn);
          txn.exec("CREATE SCHEMA IF NOT EXISTS \"" + lowerSchema + "\";");
          txn.commit();
        }

        std::string createQuery = "CREATE TABLE IF NOT EXISTS \"" +
                                  lowerSchema + "\".\"" + lowerTableName +
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
              } catch (const std::exception &e) {
                Logger::error(LogCategory::TRANSFER,
                              "setupTableTargetMariaDBToPostgres",
                              "Error parsing VARCHAR length for table " +
                                  table.schema_name + "." + table.table_name +
                                  ": " + std::string(e.what()));
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
          Logger::error(
              LogCategory::TRANSFER, "setupTableTargetMariaDBToPostgres",
              "No valid columns found for table " + table.schema_name + "." +
                  table.table_name + " - skipping");
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

      Logger::info(LogCategory::TRANSFER,
                   "MariaDB to PostgreSQL table setup completed successfully");
    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER, "setupTableTargetMariaDBToPostgres",
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

        return;
      }

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
            Logger::error(LogCategory::TRANSFER, "processDeletesByPrimaryKey",
                          "Invalid PK column name: '" + pkColumns[i] +
                              "' for table " + schema_name + "." + table_name +
                              " - skipping delete processing");
            return;
          }
          pkSelectQuery += "\"" + pkColumns[i] + "\"";
        }
        std::string lowerTableName = table_name;
        std::transform(lowerTableName.begin(), lowerTableName.end(),
                       lowerTableName.begin(), ::tolower);
        pkSelectQuery +=
            " FROM \"" + lowerSchemaName + "\".\"" + lowerTableName + "\"";
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
          Logger::error(LogCategory::TRANSFER, "processDeletesByPrimaryKey",
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
        }

        offset += BATCH_SIZE;

        // Si obtuvimos menos registros que el batch size, hemos terminado
        if (pgPKs.size() < BATCH_SIZE) {
          break;
        }
      }

      if (totalDeleted > 0) {
      }

    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER, "processDeletesByPrimaryKey",
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
      // If we don't have a time column or lastSyncTime, fall back to comparing
      // all rows by PK

      std::string lowerSchemaName = schema_name;
      std::transform(lowerSchemaName.begin(), lowerSchemaName.end(),
                     lowerSchemaName.begin(), ::tolower);

      // 1. Obtener columnas de primary key
      std::vector<std::string> pkColumns =
          getPrimaryKeyColumns(mariadbConn, schema_name, table_name);

      if (pkColumns.empty()) {
        return;
      }

      // 2. Obtener registros modificados desde MariaDB
      std::string selectQuery;

      if (timeColumn.empty() || lastSyncTime.empty()) {
        selectQuery = "SELECT * FROM `" + schema_name + "`.`" + table_name +
                      "`"; // full compare by PK
      } else {
        selectQuery = "SELECT * FROM `" + schema_name + "`.`" + table_name +
                      "` WHERE `" + timeColumn + "` > '" +
                      escapeSQL(lastSyncTime) + "' ORDER BY `" + timeColumn +
                      "`";
      }

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
      for (auto &row : columnNames) {
        if (!row.empty()) {
          std::transform(row[0].begin(), row[0].end(), row[0].begin(),
                         ::tolower);
        }
      }
      if (columnNames.empty() || columnNames[0].empty()) {
        Logger::warning(LogCategory::TRANSFER,
                        "Could not get column names for " + schema_name + "." +
                            table_name + " - skipping update processing");
        return;
      }

      // 4. Procesar cada registro modificado con límite de seguridad
      size_t totalUpdated = 0;
      size_t processedRecords = 0;
      const size_t MAX_PROCESSED_RECORDS =
          10000; // Límite adicional de seguridad

      for (const std::vector<std::string> &record : modifiedRecords) {
        // Verificar límite de registros procesados para evitar bucles infinitos
        if (processedRecords >= MAX_PROCESSED_RECORDS) {
          Logger::warning(
              LogCategory::TRANSFER,
              "Update processing reached maximum processed records limit (" +
                  std::to_string(MAX_PROCESSED_RECORDS) + ") for " +
                  schema_name + "." + table_name +
                  " - stopping to prevent infinite loop");
          break;
        }

        if (record.size() != columnNames.size()) {
          Logger::warning(LogCategory::TRANSFER,
                          "Record size mismatch for " + schema_name + "." +
                              table_name + " - skipping record");
          continue;
        }

        processedRecords++;

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
        std::string lowerTableName = table_name;
        std::transform(lowerTableName.begin(), lowerTableName.end(),
                       lowerTableName.begin(), ::tolower);
        std::string checkQuery = "SELECT COUNT(*) FROM \"" + lowerSchemaName +
                                 "\".\"" + lowerTableName + "\" WHERE " +
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

      } else {
      }

    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER, "processUpdatesByPrimaryKey",
                    "Error processing updates for " + schema_name + "." +
                        table_name + ": " + std::string(e.what()));
    }
  }

  // SINGULAR PROCESSING REMOVED - ONLY PARALLEL PROCESSING REMAINS
  void transferDataMariaDBToPostgres() {
    // Redirect to parallel processing
    transferDataMariaDBToPostgresParallel();
  }

  void transferDataMariaDBToPostgresOld() {
    Logger::info(LogCategory::TRANSFER,
                 "Starting MariaDB to PostgreSQL data transfer");

    try {
      pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());

      if (!pgConn.is_open()) {
        Logger::error(LogCategory::TRANSFER, "transferDataMariaDBToPostgres",
                      "CRITICAL ERROR: Cannot establish PostgreSQL connection "
                      "for MariaDB data transfer");
        return;
      }

      auto tables = getActiveTables(pgConn);

      if (tables.empty()) {

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

      // Removed individual table status logs to reduce noise

      for (auto &table : tables) {
        if (table.db_engine != "MariaDB") {
          Logger::warning(
              LogCategory::TRANSFER,
              "Skipping non-MariaDB table in transfer: " + table.db_engine +
                  " - " + table.schema_name + "." + table.table_name);
          continue;
        }

        Logger::info(LogCategory::TRANSFER,
                     "Processing table: " + table.schema_name + "." +
                         table.table_name + " (status: " + table.status + ")");

        MYSQL *mariadbConn = getMariaDBConnection(table.connection_string);
        if (!mariadbConn) {
          Logger::error(
              LogCategory::TRANSFER, "transferDataMariaDBToPostgres",
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
                std::all_of(countStr.begin(), countStr.end(),
                            [](unsigned char c) { return std::isdigit(c); })) {
              sourceCount = std::stoul(countStr);

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

        std::string lowerTableName = table_name;
        std::transform(lowerTableName.begin(), lowerTableName.end(),
                       lowerTableName.begin(), ::tolower);
        std::string targetCountQuery = "SELECT COUNT(*) FROM \"" +
                                       lowerSchemaName + "\".\"" +
                                       lowerTableName + "\";";
        size_t targetCount = 0;
        try {
          pqxx::work txn(pgConn);
          auto targetResult = txn.exec(targetCountQuery);
          if (!targetResult.empty()) {
            targetCount = targetResult[0][0].as<size_t>();

          } else {
            Logger::error(
                LogCategory::TRANSFER,
                "ERROR: Target count query returned no results for table " +
                    lowerSchemaName + "." + table_name);
          }
          txn.commit();
        } catch (const std::exception &e) {
          Logger::error(LogCategory::TRANSFER, "transferDataMariaDBToPostgres",
                        "ERROR getting target count for table " +
                            lowerSchemaName + "." + table_name + ": " +
                            std::string(e.what()));
        }

        // Lógica simple basada en counts reales
        if (sourceCount != targetCount) {
        }

        if (sourceCount == 0) {
          Logger::info(LogCategory::TRANSFER, "Table " + schema_name + "." +
                                                  table_name +
                                                  " has no source data");
          if (targetCount == 0) {
            Logger::info(LogCategory::TRANSFER,
                         "Marking table as NO_DATA: " + schema_name + "." +
                             table_name);
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
          // Si es una tabla FULL_LOAD que se completó, marcarla como
          // LISTENING_CHANGES
          if (table.status == "FULL_LOAD") {
            Logger::info(LogCategory::TRANSFER,
                         "FULL_LOAD completed for " + schema_name + "." +
                             table_name +
                             ", transitioning to LISTENING_CHANGES");
            updateStatus(pgConn, schema_name, table_name, "LISTENING_CHANGES",
                         targetCount);

            // Cerrar conexión MariaDB antes de continuar
            mysql_close(mariadbConn);
            continue;
          }

          // Para tablas que NO son FULL_LOAD, procesar cambios incrementales

          // Procesar UPDATEs si hay columna de tiempo y last_sync_time
          if (!table.last_sync_column.empty() &&
              !table.last_sync_time.empty()) {

            try {
              processUpdatesByPrimaryKey(schema_name, table_name, mariadbConn,
                                         pgConn, table.last_sync_column,
                                         table.last_sync_time);

            } catch (const std::exception &e) {
              Logger::error(
                  LogCategory::TRANSFER, "transferDataMariaDBToPostgres",
                  "ERROR processing updates for " + schema_name + "." +
                      table_name + ": " + std::string(e.what()));
            }
          } else {
          }

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
              }
            } catch (const std::exception &e) {
              Logger::error(LogCategory::TRANSFER,
                            "transferDataMariaDBToPostgres",
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
        if (sourceCount < targetCount) {
          size_t deletedCount = targetCount - sourceCount;
          Logger::info(LogCategory::TRANSFER,
                       "Detected " + std::to_string(deletedCount) +
                           " deleted records in " + schema_name + "." +
                           table_name + " - processing deletes");

          // Obtener estrategia PK para determinar el método de eliminación
          std::string pkStrategy =
              getPKStrategyFromCatalog(pgConn, schema_name, table_name);

          if (pkStrategy == "PK") {
            // Para tablas con PK, usar eliminación por PK
            try {
              processDeletesByPrimaryKey(schema_name, table_name, mariadbConn,
                                         pgConn);
              Logger::info(LogCategory::TRANSFER,
                           "Delete processing completed for " + schema_name +
                               "." + table_name);
            } catch (const std::exception &e) {
              Logger::error(
                  LogCategory::TRANSFER, "transferDataMariaDBToPostgres",
                  "ERROR processing deletes for " + schema_name + "." +
                      table_name + ": " + std::string(e.what()));
            }
          } else {
            // Para tablas OFFSET, hacer TRUNCATE + re-sincronización completa
            Logger::info(LogCategory::TRANSFER,
                         "OFFSET table with deletes detected - performing "
                         "TRUNCATE + full resync for " +
                             schema_name + "." + table_name);
            try {
              std::string lowerSchemaName = schema_name;
              std::transform(lowerSchemaName.begin(), lowerSchemaName.end(),
                             lowerSchemaName.begin(), ::tolower);

              // TRUNCATE la tabla destino
              pqxx::work truncateTxn(pgConn);
              truncateTxn.exec("TRUNCATE TABLE \"" + lowerSchemaName + "\".\"" +
                               lowerTableName + "\" CASCADE;");
              truncateTxn.commit();

              updateStatus(pgConn, schema_name, table_name, "FULL_LOAD", 0);

              Logger::info(
                  LogCategory::TRANSFER,
                  "OFFSET table truncated and reset for full resync: " +
                      schema_name + "." + table_name);
            } catch (const std::exception &e) {
              Logger::error(
                  LogCategory::TRANSFER, "transferDataMariaDBToPostgres",
                  "ERROR truncating OFFSET table " + schema_name + "." +
                      table_name + ": " + std::string(e.what()));
            }
          }

          // Después de procesar DELETEs, verificar el nuevo conteo
          std::string lowerSchemaName = schema_name;
          std::transform(lowerSchemaName.begin(), lowerSchemaName.end(),
                         lowerSchemaName.begin(), ::tolower);
          pqxx::work countTxn(pgConn);
          auto newTargetCount =
              countTxn.exec("SELECT COUNT(*) FROM \"" + lowerSchemaName +
                            "\".\"" + lowerTableName + "\";");
          countTxn.commit();
          targetCount = newTargetCount[0][0].as<int>();
          Logger::info(LogCategory::TRANSFER,
                       "After deletes: source=" + std::to_string(sourceCount) +
                           ", target=" + std::to_string(targetCount));
        }

        // Para tablas OFFSET, si sourceCount > targetCount, hay nuevos INSERTs
        // Necesitamos re-sincronizar completamente para mantener el orden
        // correcto
        std::string pkStrategy =
            getPKStrategyFromCatalog(pgConn, schema_name, table_name);
        if (pkStrategy == "OFFSET" && sourceCount > targetCount) {
          Logger::info(LogCategory::TRANSFER,
                       "OFFSET table with new INSERTs detected - performing "
                       "full resync for " +
                           schema_name + "." + table_name +
                           " (source: " + std::to_string(sourceCount) +
                           ", target: " + std::to_string(targetCount) + ")");
          try {
            std::string lowerSchemaName = schema_name;
            std::transform(lowerSchemaName.begin(), lowerSchemaName.end(),
                           lowerSchemaName.begin(), ::tolower);

            // TRUNCATE la tabla destino
            pqxx::work truncateTxn(pgConn);
            truncateTxn.exec("TRUNCATE TABLE \"" + lowerSchemaName + "\".\"" +
                             lowerTableName + "\" CASCADE;");
            truncateTxn.commit();

            // Actualizar status a FULL_LOAD para procesamiento completo
            updateStatus(pgConn, schema_name, table_name, "FULL_LOAD", 0);

            Logger::info(LogCategory::TRANSFER,
                         "OFFSET table truncated and reset for full resync due "
                         "to new INSERTs: " +
                             schema_name + "." + table_name);
          } catch (const std::exception &e) {
            Logger::error(
                LogCategory::TRANSFER, "transferDataMariaDBToPostgres",
                "ERROR truncating OFFSET table for new INSERTs " + schema_name +
                    "." + table_name + ": " + std::string(e.what()));
          }
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
              } catch (const std::exception &e) {
                Logger::error(
                    LogCategory::TRANSFER, "transferDataMariaDBToPostgres",
                    "Error parsing VARCHAR length for table " + schema_name +
                        "." + table_name + ": " + std::string(e.what()));
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
          Logger::info(LogCategory::TRANSFER,
                       "Truncating table: " + lowerSchemaName + "." +
                           table_name);
          pqxx::work txn(pgConn);
          std::string lowerTableName = table_name;
          std::transform(lowerTableName.begin(), lowerTableName.end(),
                         lowerTableName.begin(), ::tolower);
          txn.exec("TRUNCATE TABLE \"" + lowerSchemaName + "\".\"" +
                   lowerTableName + "\" CASCADE;");
          txn.commit();
        } else if (table.status == "RESET") {
          Logger::info(LogCategory::TRANSFER,
                       "Processing RESET table: " + schema_name + "." +
                           table_name);
          pqxx::work txn(pgConn);
          std::string lowerTableName = table_name;
          std::transform(lowerTableName.begin(), lowerTableName.end(),
                         lowerTableName.begin(), ::tolower);
          txn.exec("TRUNCATE TABLE \"" + lowerSchemaName + "\".\"" +
                   lowerTableName + "\" CASCADE;");
          txn.commit();

          updateStatus(pgConn, schema_name, table_name, "FULL_LOAD", 0);
          continue;
        }

        std::vector<std::string> pkColumns =
            getPKColumnsFromCatalog(pgConn, schema_name, table_name);
        std::string lastProcessedPK =
            getLastProcessedPKFromCatalog(pgConn, schema_name, table_name);

        bool hasMoreData = true;
        size_t chunkNumber = 0;

        // CRITICAL: Add timeout to prevent infinite loops
        auto startTime = std::chrono::steady_clock::now();
        const auto MAX_PROCESSING_TIME = std::chrono::hours(24);

        while (hasMoreData) {
          chunkNumber++;
          const size_t CHUNK_SIZE = SyncConfig::getChunkSize();

          auto currentTime = std::chrono::steady_clock::now();

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
          } else {
            selectQuery += " LIMIT " + std::to_string(CHUNK_SIZE) + ";";
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

            rowsInserted = results.size();

            if (rowsInserted > 0) {
              try {

                performBulkUpsert(pgConn, results, columnNames, columnTypes,
                                  lowerSchemaName, table_name, schema_name);

              } catch (const std::exception &e) {
                std::string errorMsg = e.what();
                Logger::error(
                    LogCategory::TRANSFER, "transferDataMariaDBToPostgres",
                    "CRITICAL ERROR: Bulk upsert failed for chunk " +
                        std::to_string(chunkNumber) + " in table " +
                        schema_name + "." + table_name + ": " + errorMsg);

                // CRITICAL: Check for transaction abort errors that cause
                // infinite loops
                if (errorMsg.find("current transaction is aborted") !=
                        std::string::npos ||
                    errorMsg.find("previously aborted") != std::string::npos ||
                    errorMsg.find("aborted transaction") != std::string::npos) {
                  Logger::error(LogCategory::TRANSFER,
                                "transferDataMariaDBToPostgres",
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
                          "transferDataMariaDBToPostgres",
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
                            "transferDataMariaDBToPostgres",
                            "CRITICAL: Critical error detected - breaking loop "
                            "to prevent infinite hang");
              hasMoreData = false;
              break;
            }
          }

          targetCount += rowsInserted;

          // OPTIMIZED: Update last_processed_pk for cursor-based pagination
          if ((pkStrategy == "PK" && !pkColumns.empty()) && !results.empty()) {
            try {
              // Obtener el último PK del chunk procesado
              std::string lastPK =
                  getLastPKFromResults(results, pkColumns, columnNames);
              if (!lastPK.empty()) {
                updateLastProcessedPK(pgConn, schema_name, table_name, lastPK);
                // Actualizar la variable local para el siguiente chunk
                lastProcessedPK = lastPK;
              }
            } catch (const std::exception &e) {
              Logger::error(
                  LogCategory::TRANSFER,
                  "ERROR: Failed to update last_processed_pk for table " +
                      schema_name + "." + table_name + ": " +
                      std::string(e.what()));
            }
          }

          // Verificar si hemos procesado todos los datos disponibles
          if (results.size() < CHUNK_SIZE) {

            hasMoreData = false;
          } else if (targetCount >= sourceCount) {

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
                          "transferDataMariaDBToPostgres",
                          "ERROR: Failed to update last_processed_pk for "
                          "completed table " +
                              schema_name + "." + table_name + ": " +
                              std::string(e.what()));
          }
        }

        if (targetCount > 0) {
          Logger::info(
              LogCategory::TRANSFER,
              "Table " + schema_name + "." + table_name +
                  " transfer completed: " + std::to_string(targetCount) +
                  " rows (source: " + std::to_string(sourceCount) + ")");
          // Simplificado: Siempre usar LISTENING_CHANGES para sincronización
          // incremental
          Logger::info(LogCategory::TRANSFER,
                       "Table " + schema_name + "." + table_name +
                           " synchronized - LISTENING_CHANGES (source: " +
                           std::to_string(sourceCount) +
                           ", target: " + std::to_string(targetCount) + ")");
          try {
            updateStatus(pgConn, schema_name, table_name, "LISTENING_CHANGES",
                         targetCount);
            Logger::info(
                LogCategory::TRANSFER,
                "Successfully updated status to LISTENING_CHANGES for " +
                    schema_name + "." + table_name);
          } catch (const std::exception &e) {
            Logger::error(LogCategory::TRANSFER,
                          "transferDataMariaDBToPostgres",
                          "ERROR updating status to LISTENING_CHANGES for " +
                              schema_name + "." + table_name + ": " +
                              std::string(e.what()));
          }
        } else {
          Logger::warning(LogCategory::TRANSFER,
                          "No data transferred for table " + schema_name + "." +
                              table_name + " - keeping current status");
        }

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
      Logger::error(LogCategory::TRANSFER, "transferDataMariaDBToPostgres",
                    "CRITICAL ERROR in transferDataMariaDBToPostgres: " +
                        std::string(e.what()) +
                        " - MariaDB data transfer completely failed");
    }
  }

  void transferDataMariaDBToPostgresParallel() {
    Logger::info(LogCategory::TRANSFER,
                 "Starting parallel MariaDB to PostgreSQL data transfer");

    try {
      startParallelProcessing();

      pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());

      if (!pgConn.is_open()) {
        Logger::error(LogCategory::TRANSFER,
                      "transferDataMariaDBToPostgresParallel",
                      "CRITICAL ERROR: Cannot establish PostgreSQL connection "
                      "for parallel MariaDB data transfer");
        shutdownParallelProcessing();
        return;
      }

      auto tables = getActiveTables(pgConn);

      Logger::info(LogCategory::TRANSFER,
                   "Found " + std::to_string(tables.size()) +
                       " active MariaDB tables to process");

      if (tables.empty()) {
        Logger::info(
            LogCategory::TRANSFER,
            "No active MariaDB tables found - skipping transfer cycle");
        shutdownParallelProcessing();
        return;
      }

      // Sort tables by priority
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
                      b.status != "LISTENING_CHANGES")
                    return false;
                  return false;
                });

      // Process multiple tables in parallel (bounded by config)
      size_t tablesCap = SyncConfig::getMaxTablesPerCycle();
      if (tablesCap > 0 && tables.size() > tablesCap) {
        tables.resize(tablesCap);
      }

      size_t maxWorkers = std::max<size_t>(1, SyncConfig::getMaxWorkers());
      TableProcessorThreadPool pool(maxWorkers);
      pool.enableMonitoring(true);

      Logger::info(LogCategory::TRANSFER,
                   "Created thread pool with " + std::to_string(maxWorkers) +
                       " workers for " + std::to_string(tables.size()) +
                       " tables (monitoring enabled)");

      size_t skipped = 0;
      for (const auto &table : tables) {
        if (table.db_engine != "MariaDB") {
          Logger::warning(LogCategory::TRANSFER,
                          "Skipping non-MariaDB table in parallel transfer: " +
                              table.db_engine + " - " + table.schema_name +
                              "." + table.table_name);
          skipped++;
          continue;
        }

        pool.submitTask(table,
                        [this](const DatabaseToPostgresSync::TableInfo &t) {
                          this->processTableParallelWithConnection(t);
                        });
      }

      Logger::info(LogCategory::TRANSFER,
                   "Submitted " + std::to_string(tables.size() - skipped) +
                       " MariaDB tables to thread pool (skipped " +
                       std::to_string(skipped) + ")");

      pool.waitForCompletion();

      Logger::info(LogCategory::TRANSFER,
                   "Thread pool completed - Completed: " +
                       std::to_string(pool.completedTasks()) +
                       " | Failed: " + std::to_string(pool.failedTasks()));

      shutdownParallelProcessing();

    } catch (const std::exception &e) {
      Logger::error(
          LogCategory::TRANSFER, "transferDataMariaDBToPostgresParallel",
          "CRITICAL ERROR in transferDataMariaDBToPostgresParallel: " +
              std::string(e.what()) +
              " - Parallel MariaDB data transfer completely failed");
      shutdownParallelProcessing();
    }
  }

  void processTableParallelWithConnection(const TableInfo &table) {
    Logger::info(LogCategory::TRANSFER,
                 "Starting HYBRID parallel processing for table " +
                     table.schema_name + "." + table.table_name);

    try {
      pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());
      if (!pgConn.is_open()) {
        Logger::error(LogCategory::TRANSFER,
                      "Failed to establish PostgreSQL connection for table " +
                          table.schema_name + "." + table.table_name);
        return;
      }

      processTableParallel(table, pgConn);

    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER, "processTableParallelWithConnection",
                    "Error in hybrid parallel table processing: " +
                        std::string(e.what()));
    }
  }

  void processTableParallel(const TableInfo &table, pqxx::connection &pgConn) {
    std::string tableKey = table.schema_name + "." + table.table_name;

    Logger::info(LogCategory::TRANSFER,
                 "Starting parallel processing for table " + tableKey);

    try {
      setTableProcessingState(tableKey, true);

      MYSQL *mariadbConn = getMariaDBConnection(table.connection_string);
      if (!mariadbConn) {
        Logger::error(
            LogCategory::TRANSFER,
            "Failed to get MariaDB connection for parallel processing");
        updateStatus(pgConn, table.schema_name, table.table_name, "ERROR");
        return;
      }

      Logger::info(LogCategory::TRANSFER, "processTableParallel",
                   "Getting table metadata for " + tableKey);

      // Get table metadata
      std::string query = "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, "
                          "COLUMN_KEY, EXTRA, CHARACTER_MAXIMUM_LENGTH "
                          "FROM information_schema.columns "
                          "WHERE table_schema = '" +
                          table.schema_name + "' AND table_name = '" +
                          table.table_name + "';";

      std::vector<std::vector<std::string>> columns =
          executeQueryMariaDB(mariadbConn, query);

      Logger::info(LogCategory::TRANSFER, "processTableParallel",
                   "Retrieved " + std::to_string(columns.size()) +
                       " columns for " + tableKey);

      if (columns.empty()) {
        Logger::error(LogCategory::TRANSFER,
                      "No columns found for table " + table.schema_name + "." +
                          table.table_name + " - skipping parallel processing");
        mysql_close(mariadbConn);
        return;
      }

      // Prepare column information
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
          if (!maxLength.empty() && maxLength != "NULL") {
            try {
              size_t length = std::stoul(maxLength);
              if (length >= 1 && length <= 65535) {
                pgType = dataType + "(" + maxLength + ")";
              } else {
                pgType = "VARCHAR";
              }
            } catch (const std::exception &e) {
              Logger::error(LogCategory::TRANSFER, "processTableParallel",
                            "Error parsing VARCHAR length for table " +
                                table.schema_name + "." + table.table_name +
                                ": " + std::string(e.what()));
              pgType = "VARCHAR";
            }
          } else {
            pgType = "VARCHAR";
          }
        } else if (dataTypeMap.count(dataType)) {
          pgType = dataTypeMap[dataType];
        }

        columnTypes.push_back(pgType);
      }

      // PRE-COUNT Y DECISIONES (alinear con no-paralelo)
      // 1) Contar origen (MariaDB)
      size_t sourceCount = 0;
      {
        std::vector<std::vector<std::string>> countRes = executeQueryMariaDB(
            mariadbConn, "SELECT COUNT(*) FROM `" + table.schema_name + "`.`" +
                             table.table_name + "`;");
        if (!countRes.empty() && !countRes[0].empty() &&
            !countRes[0][0].empty()) {
          try {
            std::string countStr = countRes[0][0];
            if (!countStr.empty() &&
                std::all_of(countStr.begin(), countStr.end(),
                            [](unsigned char c) { return std::isdigit(c); })) {
              sourceCount = std::stoul(countStr);
            }
          } catch (const std::exception &e) {
            Logger::error(LogCategory::TRANSFER, "processTableParallel",
                          "Error parsing source count for table " +
                              table.schema_name + "." + table.table_name +
                              ": " + std::string(e.what()));
            sourceCount = 0;
          } catch (...) {
            Logger::error(LogCategory::TRANSFER, "processTableParallel",
                          "Unknown error parsing source count for table " +
                              table.schema_name + "." + table.table_name);
            sourceCount = 0;
          }
        }
      }

      // 2) Contar destino (PostgreSQL)
      std::string lowerSchemaName = table.schema_name;
      std::transform(lowerSchemaName.begin(), lowerSchemaName.end(),
                     lowerSchemaName.begin(), ::tolower);
      std::string lowerTableNamePG = table.table_name;
      std::transform(lowerTableNamePG.begin(), lowerTableNamePG.end(),
                     lowerTableNamePG.begin(), ::tolower);
      size_t targetCount = 0;
      try {
        pqxx::work txn(pgConn);
        auto targetResult =
            txn.exec("SELECT COUNT(*) FROM \"" + lowerSchemaName + "\".\"" +
                     lowerTableNamePG + "\";");
        if (!targetResult.empty()) {
          targetCount = targetResult[0][0].as<size_t>();
        }
        txn.commit();
      } catch (const std::exception &e) {
        Logger::error(LogCategory::TRANSFER, "processTableParallel",
                      "Error getting target count for table " +
                          table.schema_name + "." + table.table_name + ": " +
                          std::string(e.what()));
        targetCount = 0;
      } catch (...) {
        Logger::error(LogCategory::TRANSFER, "processTableParallel",
                      "Unknown error getting target count for table " +
                          table.schema_name + "." + table.table_name);
        targetCount = 0;
      }

      // 3) FULL_LOAD / RESET: SIEMPRE truncar para evitar duplicados
      if (table.status == "FULL_LOAD" || table.status == "RESET") {
        Logger::info(
            LogCategory::TRANSFER, "processTableParallel",
            "FULL_LOAD/RESET detected - performing mandatory truncate for " +
                table.schema_name + "." + table.table_name);

        try {
          pqxx::work txn(pgConn);
          txn.exec("TRUNCATE TABLE \"" + lowerSchemaName + "\".\"" +
                   lowerTableNamePG + "\" CASCADE;");

          // Resetear metadatos para tablas OFFSET y PK
          std::string pkStrategy = getPKStrategyFromCatalog(
              pgConn, table.schema_name, table.table_name);

          if (pkStrategy != "PK") {
            txn.exec("UPDATE metadata.catalog SET last_processed_pk=NULL WHERE "
                     "schema_name='" +
                     escapeSQL(table.schema_name) + "' AND table_name='" +
                     escapeSQL(table.table_name) + "';");
            Logger::info(LogCategory::TRANSFER, "processTableParallel",
                         "Reset last_processed_pk for table " +
                             table.schema_name + "." + table.table_name);
          } else {
            txn.exec("UPDATE metadata.catalog SET last_processed_pk=NULL WHERE "
                     "schema_name='" +
                     escapeSQL(table.schema_name) + "' AND table_name='" +
                     escapeSQL(table.table_name) + "';");
            Logger::info(LogCategory::TRANSFER, "processTableParallel",
                         "Reset last_processed_pk for PK table " +
                             table.schema_name + "." + table.table_name);
          }

          txn.commit();
          targetCount = 0;

          Logger::info(LogCategory::TRANSFER, "processTableParallel",
                       "Successfully truncated and reset metadata for " +
                           table.schema_name + "." + table.table_name);

        } catch (const std::exception &e) {
          Logger::error(LogCategory::TRANSFER, "processTableParallel",
                        "Error truncating table " + table.schema_name + "." +
                            table.table_name + ": " + std::string(e.what()));
          // Continuar con el procesamiento aunque falle el truncate
        } catch (...) {
          Logger::error(LogCategory::TRANSFER, "processTableParallel",
                        "Unknown error truncating table " + table.schema_name +
                            "." + table.table_name);
        }
      }

      // 4) Rama por conteos (NO_DATA / iguales / deletes / inserts)
      Logger::info(LogCategory::TRANSFER, "processTableParallel",
                   "Counts for " + table.schema_name + "." + table.table_name +
                       ": source=" + std::to_string(sourceCount) +
                       ", target=" + std::to_string(targetCount));

      if (sourceCount == 0) {
        if (targetCount == 0) {
          updateStatus(pgConn, table.schema_name, table.table_name, "NO_DATA",
                       0);
        } else {
          // Mantener coherencia: cuando origen vacío y destino tiene datos,
          // pasar a LISTENING_CHANGES
          updateStatus(pgConn, table.schema_name, table.table_name,
                       "LISTENING_CHANGES", targetCount);
        }
        mysql_close(mariadbConn);
        removeTableProcessingState(tableKey);
        return;
      }

      if (sourceCount == targetCount) {
        // CRÍTICO: Verificar consistencia real de datos antes de asumir
        // sincronización
        Logger::info(LogCategory::TRANSFER, "processTableParallel",
                     "Counts match (" + std::to_string(sourceCount) +
                         "), verifying data consistency for " +
                         table.schema_name + "." + table.table_name);

        bool isConsistent = verifyDataConsistency(mariadbConn, pgConn, table);

        if (isConsistent) {
          // Los datos están realmente sincronizados
          Logger::info(LogCategory::TRANSFER, "processTableParallel",
                       "Data consistency verified for " + table.schema_name +
                           "." + table.table_name);

          // Si es FULL_LOAD, ya quedó completado; marcar LISTENING_CHANGES
          if (table.status == "FULL_LOAD") {
            updateStatus(pgConn, table.schema_name, table.table_name,
                         "LISTENING_CHANGES", targetCount);
            mysql_close(mariadbConn);
            removeTableProcessingState(tableKey);
            return;
          }

          // Si hay columna de tiempo configurada, ejecutar updates
          // incrementales
          if (!table.last_sync_column.empty() &&
              !table.last_sync_time.empty()) {
            try {
              processUpdatesByPrimaryKey(
                  table.schema_name, table.table_name, mariadbConn, pgConn,
                  table.last_sync_column, table.last_sync_time);
            } catch (const std::exception &e) {
              Logger::error(LogCategory::TRANSFER, "processTableParallel",
                            "Error processing updates for table " +
                                table.schema_name + "." + table.table_name +
                                ": " + std::string(e.what()));
            } catch (...) {
              Logger::error(LogCategory::TRANSFER, "processTableParallel",
                            "Unknown error processing updates for table " +
                                table.schema_name + "." + table.table_name);
            }
          }

          updateStatus(pgConn, table.schema_name, table.table_name,
                       "LISTENING_CHANGES", targetCount);
          mysql_close(mariadbConn);
          removeTableProcessingState(tableKey);
          return;
        } else {
          // Los datos NO están sincronizados a pesar de tener conteos iguales
          Logger::warning(
              LogCategory::TRANSFER, "processTableParallel",
              "Data inconsistency detected despite matching counts for " +
                  table.schema_name + "." + table.table_name +
                  " - proceeding with full data transfer");

          // Continuar con la transferencia completa para sincronizar los datos
        }
      }

      if (sourceCount > targetCount) {
        // CRÍTICO: Hay más datos en origen que en destino - proceder con
        // transferencia
        Logger::info(LogCategory::TRANSFER, "processTableParallel",
                     "Source has more data (" + std::to_string(sourceCount) +
                         ") than target (" + std::to_string(targetCount) +
                         ") - proceeding with data transfer for " +
                         table.schema_name + "." + table.table_name);

        // Continuar con la transferencia de datos
      }

      if (sourceCount < targetCount) {
        // Para OFFSET: TRUNCATE + resync completo
        std::string pkStrategy = getPKStrategyFromCatalog(
            pgConn, table.schema_name, table.table_name);
        if (pkStrategy != "PK") {
          try {
            pqxx::work truncateTxn(pgConn);
            truncateTxn.exec("TRUNCATE TABLE \"" + lowerSchemaName + "\".\"" +
                             lowerTableNamePG + "\" CASCADE;");
            truncateTxn.commit();
            pqxx::work resetTxn(pgConn);
            updateStatus(pgConn, table.schema_name, table.table_name,
                         "FULL_LOAD", 0);
            targetCount = 0;
          } catch (const std::exception &e) {
            Logger::error(LogCategory::TRANSFER, "processTableParallel",
                          "Error truncating OFFSET table for deletes " +
                              table.schema_name + "." + table.table_name +
                              ": " + std::string(e.what()));
          } catch (...) {
            Logger::error(LogCategory::TRANSFER, "processTableParallel",
                          "Unknown error truncating OFFSET table for deletes " +
                              table.schema_name + "." + table.table_name);
          }
        }
        // Continuar hacia transferencia completa
      }

      // 5) Transferencia: usar dataFetcherThread (hace INSERT/UPSERT y
      // actualiza last_processed_pk)
      dataFetcherThread(tableKey, mariadbConn, table, columnNames, columnTypes);

      size_t finalTargetCount = 0;
      try {
        pqxx::work txn(pgConn);
        auto res = txn.exec("SELECT COUNT(*) FROM \"" + lowerSchemaName +
                            "\".\"" + lowerTableNamePG + "\";");
        if (!res.empty())
          finalTargetCount = res[0][0].as<size_t>();
        txn.commit();
      } catch (const std::exception &e) {
        Logger::error(LogCategory::TRANSFER, "processTableParallel",
                      "Error getting final target count for table " +
                          table.schema_name + "." + table.table_name + ": " +
                          std::string(e.what()));
        finalTargetCount = 0;
      } catch (...) {
        Logger::error(LogCategory::TRANSFER, "processTableParallel",
                      "Unknown error getting final target count for table " +
                          table.schema_name + "." + table.table_name);
        finalTargetCount = 0;
      }
      updateStatus(pgConn, table.schema_name, table.table_name,
                   "LISTENING_CHANGES", finalTargetCount);

      mysql_close(mariadbConn);
      removeTableProcessingState(tableKey);

      Logger::info(LogCategory::TRANSFER,
                   "Parallel processing completed for table " +
                       table.schema_name + "." + table.table_name);

    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER, "processTableParallel",
                    "Error in parallel table processing: " +
                        std::string(e.what()));
      removeTableProcessingState(tableKey);
    }
  }

  void dataFetcherThread(const std::string &tableKey, MYSQL *mariadbConn,
                         const TableInfo &table,
                         const std::vector<std::string> &columnNames,
                         const std::vector<std::string> &columnTypes) {

    try {
      size_t chunkNumber = 0;
      const size_t CHUNK_SIZE = SyncConfig::getChunkSize();

      // Get PK strategy and columns
      pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());
      std::string pkStrategy =
          getPKStrategyFromCatalog(pgConn, table.schema_name, table.table_name);
      std::vector<std::string> pkColumns =
          getPKColumnsFromCatalog(pgConn, table.schema_name, table.table_name);
      std::string lastProcessedPK = getLastProcessedPKFromCatalog(
          pgConn, table.schema_name, table.table_name);

      Logger::info(LogCategory::TRANSFER, "dataFetcherThread",
                   "Starting data fetch for " + table.schema_name + "." +
                       table.table_name + " - pkStrategy=" + pkStrategy +
                       ", lastProcessedPK=" +
                       (lastProcessedPK.empty() ? "empty" : lastProcessedPK));

      bool hasMoreData = true;
      while (hasMoreData) {
        chunkNumber++;

        // Build select query
        std::string selectQuery = "SELECT * FROM `" + table.schema_name +
                                  "`.`" + table.table_name + "`";

        if (pkStrategy == "PK" && !pkColumns.empty()) {
          if (!lastProcessedPK.empty()) {
            selectQuery += " WHERE ";
            std::vector<std::string> lastPKValues =
                parseLastPK(lastProcessedPK);
            if (!lastPKValues.empty()) {
              selectQuery += "`" + pkColumns[0] + "` > '" +
                             escapeSQL(lastPKValues[0]) + "'";
            }
          } else {
            // CRÍTICO: Obtener el primer PK de la tabla para inicializar cursor
            // correctamente
            Logger::info(LogCategory::TRANSFER,
                         "Initializing PK cursor for " + table.schema_name +
                             "." + table.table_name +
                             " - getting first PK value");

            std::string firstPKQuery = "SELECT `" + pkColumns[0] + "` FROM `" +
                                       table.schema_name + "`.`" +
                                       table.table_name + "` ORDER BY `" +
                                       pkColumns[0] + "` LIMIT 1";

            auto firstPKRes = executeQueryMariaDB(mariadbConn, firstPKQuery);
            if (!firstPKRes.empty() && !firstPKRes[0].empty()) {
              std::string firstPK = firstPKRes[0][0];
              selectQuery += " WHERE `" + pkColumns[0] + "` >= '" +
                             escapeSQL(firstPK) + "'";
              Logger::info(LogCategory::TRANSFER,
                           "PK cursor initialized with first value: " +
                               firstPK);
            } else {
              // Tabla vacía, no hay datos que procesar
              Logger::info(LogCategory::TRANSFER, "Table " + table.schema_name +
                                                      "." + table.table_name +
                                                      " is empty");
              hasMoreData = false;
              break;
            }
          }
          selectQuery += " ORDER BY `" + pkColumns[0] + "`";
          selectQuery += " LIMIT " + std::to_string(CHUNK_SIZE) + ";";
        } else {
          selectQuery += " LIMIT " + std::to_string(CHUNK_SIZE) + ";";
        }

        // Fetch data
        Logger::info(LogCategory::TRANSFER, "dataFetcherThread",
                     "Executing query for chunk " +
                         std::to_string(chunkNumber) + " on " +
                         table.schema_name + "." + table.table_name);

        std::vector<std::vector<std::string>> results =
            executeQueryMariaDB(mariadbConn, selectQuery);

        Logger::info(LogCategory::TRANSFER, "dataFetcherThread",
                     "Retrieved " + std::to_string(results.size()) +
                         " rows for chunk " + std::to_string(chunkNumber) +
                         " on " + table.schema_name + "." + table.table_name);

        if (results.empty()) {
          Logger::info(LogCategory::TRANSFER, "dataFetcherThread",
                       "No more data to fetch for " + table.schema_name + "." +
                           table.table_name);
          hasMoreData = false;
          break;
        }

        // Process data immediately like non-parallel method
        size_t rowsInserted = 0;

        try {

          rowsInserted = results.size();

          if (rowsInserted > 0) {
            try {

              std::string lowerSchemaName = table.schema_name;
              std::transform(lowerSchemaName.begin(), lowerSchemaName.end(),
                             lowerSchemaName.begin(), ::tolower);

              performBulkUpsert(pgConn, results, columnNames, columnTypes,
                                lowerSchemaName, table.table_name,
                                table.schema_name);
              Logger::info(LogCategory::TRANSFER,
                           "Successfully processed chunk " +
                               std::to_string(chunkNumber) + " with " +
                               std::to_string(rowsInserted) + " rows for " +
                               table.schema_name + "." + table.table_name);
            } catch (const std::exception &e) {
              std::string errorMsg = e.what();
              Logger::error(LogCategory::TRANSFER,
                            "CRITICAL ERROR: Bulk upsert failed for chunk " +
                                std::to_string(chunkNumber) + " in table " +
                                table.schema_name + "." + table.table_name +
                                ": " + errorMsg);
              rowsInserted = 0;
            }
          } else {
            Logger::warning(LogCategory::TRANSFER,
                            "No rows to process in chunk " +
                                std::to_string(chunkNumber) + " for table " +
                                table.schema_name + "." + table.table_name);
          }

        } catch (const std::exception &e) {
          std::string errorMsg = e.what();
          Logger::error(LogCategory::TRANSFER,
                        "ERROR processing data for chunk " +
                            std::to_string(chunkNumber) + " in table " +
                            table.schema_name + "." + table.table_name + ": " +
                            errorMsg);
          rowsInserted = 0;
        }

        // OPTIMIZED: Update last_processed_pk for cursor-based pagination
        // (identical to non-parallel)
        if ((pkStrategy == "PK" && !pkColumns.empty()) && !results.empty()) {
          try {
            // Obtener el último PK del chunk procesado
            std::string lastPK =
                getLastPKFromResults(results, pkColumns, columnNames);
            if (!lastPK.empty()) {
              updateLastProcessedPK(pgConn, table.schema_name, table.table_name,
                                    lastPK);
              // Actualizar la variable local para el siguiente chunk
              lastProcessedPK = lastPK;
              Logger::info(LogCategory::TRANSFER,
                           "Updated last_processed_pk to " + lastPK +
                               " for table " + table.schema_name + "." +
                               table.table_name + " (strategy: " + pkStrategy +
                               ")");
            }
          } catch (const std::exception &e) {
            Logger::error(
                LogCategory::TRANSFER,
                "ERROR: Failed to update last_processed_pk for table " +
                    table.schema_name + "." + table.table_name + ": " +
                    std::string(e.what()));
          }
        }

        // Verificar si hemos procesado todos los datos disponibles (identical
        // to non-parallel)
        if (results.size() < CHUNK_SIZE) {
          Logger::info(LogCategory::TRANSFER,
                       "Retrieved " + std::to_string(results.size()) +
                           " rows (less than chunk size " +
                           std::to_string(CHUNK_SIZE) +
                           ") - ending data transfer");
          hasMoreData = false;
        }
      }

      // COMENTADO: Este bloque actualizaba last_processed_pk al final, causando
      // problemas cuando la tabla estaba en FULL_LOAD y acababa de ser
      // truncada. El last_processed_pk se actualiza correctamente dentro del
      // bucle de transferencia.

      // if (pkStrategy == "PK" && !pkColumns.empty()) {
      //   // ... código comentado ...
      // }

    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER, "dataFetcherThread",
                    "Error in data fetcher thread: " + std::string(e.what()));
    }
  }

  void batchPreparerThread(const std::vector<std::string> &columnNames,
                           const std::vector<std::string> &columnTypes) {

    try {
      while (true) {
        DataChunk chunk;
        if (!rawDataQueue.pop(chunk, std::chrono::milliseconds(1000))) {
          continue;
        }

        if (chunk.isLastChunk) {
          // Push last chunk marker to batch queue and exit
          PreparedBatch lastBatch;
          lastBatch.batchSize = 0;
          lastBatch.chunkNumber = chunk.chunkNumber;
          lastBatch.schemaName = chunk.schemaName;
          lastBatch.tableName = chunk.tableName;
          preparedBatchQueue.push(std::move(lastBatch));
          break;
        }

        // Prepare batches from raw data
        const size_t BATCH_SIZE = SyncConfig::getChunkSize();

        for (size_t batchStart = 0; batchStart < chunk.rawData.size();
             batchStart += BATCH_SIZE) {
          size_t batchEnd =
              std::min(batchStart + BATCH_SIZE, chunk.rawData.size());

          PreparedBatch preparedBatch;
          preparedBatch.chunkNumber = chunk.chunkNumber;
          preparedBatch.schemaName = chunk.schemaName;
          preparedBatch.tableName = chunk.tableName;
          preparedBatch.batchSize = batchEnd - batchStart;

          // Build batch query
          std::string lowerSchemaName = chunk.schemaName;
          std::transform(lowerSchemaName.begin(), lowerSchemaName.end(),
                         lowerSchemaName.begin(), ::tolower);
          std::string lowerTableName = chunk.tableName;
          std::transform(lowerTableName.begin(), lowerTableName.end(),
                         lowerTableName.begin(), ::tolower);

          // Get PK columns for UPSERT
          pqxx::connection pgConn(
              DatabaseConfig::getPostgresConnectionString());
          std::vector<std::string> pkColumns = getPrimaryKeyColumnsFromPostgres(
              pgConn, lowerSchemaName, lowerTableName);

          if (!pkColumns.empty()) {
            preparedBatch.batchQuery = buildUpsertQuery(
                columnNames, pkColumns, lowerSchemaName, lowerTableName);
          } else {
            // Simple INSERT
            std::string insertQuery = "INSERT INTO \"" + lowerSchemaName +
                                      "\".\"" + lowerTableName + "\" (";
            for (size_t i = 0; i < columnNames.size(); ++i) {
              if (i > 0)
                insertQuery += ", ";
              insertQuery += "\"" + columnNames[i] + "\"";
            }
            insertQuery += ") VALUES ";
            preparedBatch.batchQuery = insertQuery;
          }

          // Build VALUES clause
          std::string valuesClause;
          for (size_t i = batchStart; i < batchEnd; ++i) {
            if (i > batchStart)
              valuesClause += ", ";

            const auto &row = chunk.rawData[i];
            if (row.size() != columnNames.size())
              continue;

            valuesClause += "(";
            for (size_t j = 0; j < row.size(); ++j) {
              if (j > 0)
                valuesClause += ", ";

              if (row[j].empty()) {
                valuesClause += "NULL";
              } else {
                std::string cleanValue =
                    cleanValueForPostgres(row[j], columnTypes[j]);
                if (cleanValue == "NULL") {
                  valuesClause += "NULL";
                } else {
                  valuesClause += "'" + escapeSQL(cleanValue) + "'";
                }
              }
            }
            valuesClause += ")";
          }

          preparedBatch.batchQuery += valuesClause + ";";

          // Push prepared batch to queue
          preparedBatchQueue.push(std::move(preparedBatch));
        }

        Logger::info(LogCategory::TRANSFER,
                     "Prepared batches for chunk " +
                         std::to_string(chunk.chunkNumber) + " (" +
                         std::to_string(chunk.rawData.size()) + " rows)");
      }

    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER, "batchPreparerThread",
                    "Error in batch preparer thread: " + std::string(e.what()));
    }
  }

  void updateStatus(pqxx::connection &pgConn, const std::string &schema_name,
                    const std::string &table_name, const std::string &status,
                    size_t offset = 0) {
    try {
      // Thread-safe: Proteger la actualización de metadatos
      std::lock_guard<std::mutex> lock(metadataUpdateMutex);

      pqxx::work txn(pgConn);

      auto columnQuery =
          txn.exec("SELECT last_sync_column, pk_strategy FROM metadata.catalog "
                   "WHERE schema_name='" +
                   escapeSQL(schema_name) + "' AND table_name='" +
                   escapeSQL(table_name) + "';");

      std::string lastSyncColumn = "";
      std::string pkStrategy = "";
      if (!columnQuery.empty()) {
        if (!columnQuery[0][0].is_null()) {
          lastSyncColumn = columnQuery[0][0].as<std::string>();
        }
        if (!columnQuery[0][1].is_null()) {
          pkStrategy = columnQuery[0][1].as<std::string>();
        }
      }

      std::string updateQuery =
          "UPDATE metadata.catalog SET status='" + status + "'";

      if (pkStrategy == "PK" && (status == "FULL_LOAD" || status == "RESET" ||
                                 status == "LISTENING_CHANGES")) {
        updateQuery += ", last_processed_pk=NULL";
      }

      if (!lastSyncColumn.empty()) {
        std::string lowerSchemaName = schema_name;
        std::transform(lowerSchemaName.begin(), lowerSchemaName.end(),
                       lowerSchemaName.begin(), ::tolower);
        std::string lowerTableName = table_name;
        std::transform(lowerTableName.begin(), lowerTableName.end(),
                       lowerTableName.begin(), ::tolower);
        std::string lowerLastSyncColumn = lastSyncColumn;
        std::transform(lowerLastSyncColumn.begin(), lowerLastSyncColumn.end(),
                       lowerLastSyncColumn.begin(), ::tolower);

        auto tableCheck =
            txn.exec("SELECT COUNT(*) FROM information_schema.tables "
                     "WHERE table_schema='" +
                     lowerSchemaName +
                     "' "
                     "AND table_name='" +
                     lowerTableName + "';");

        if (!tableCheck.empty() && tableCheck[0][0].as<int>() > 0) {
          // Verificar si la tabla tiene datos antes de intentar obtener MAX
          auto countCheck =
              txn.exec("SELECT COUNT(*) FROM \"" + lowerSchemaName + "\".\"" +
                       lowerTableName + "\"");
          if (!countCheck.empty() && countCheck[0][0].as<int>() > 0) {
            updateQuery += ", last_sync_time=(SELECT MAX(\"" +
                           lowerLastSyncColumn + "\")::timestamp FROM \"" +
                           lowerSchemaName + "\".\"" + lowerTableName + "\")";
          } else {
            updateQuery += ", last_sync_time=NOW()";
          }
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
      Logger::error(LogCategory::TRANSFER, "updateStatus",
                    "SQL ERROR updating status: " + std::string(e.what()) +
                        " [SQL State: " + e.sqlstate() + "]");
    } catch (const pqxx::broken_connection &e) {
      Logger::error(LogCategory::TRANSFER, "updateStatus",
                    "CONNECTION ERROR updating status: " +
                        std::string(e.what()));
    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER, "updateStatus",
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
    const size_t CHECK_BATCH_SIZE = SyncConfig::getChunkSize();

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

  // NUEVA FUNCIÓN: Verificar consistencia real de datos
  bool verifyDataConsistency(MYSQL *mariadbConn, pqxx::connection &pgConn,
                             const TableInfo &table) {
    try {
      std::string lowerSchemaName = table.schema_name;
      std::transform(lowerSchemaName.begin(), lowerSchemaName.end(),
                     lowerSchemaName.begin(), ::tolower);
      std::string lowerTableName = table.table_name;
      std::transform(lowerTableName.begin(), lowerTableName.end(),
                     lowerTableName.begin(), ::tolower);

      // Obtener PK columns
      std::vector<std::string> pkColumns =
          getPKColumnsFromCatalog(pgConn, table.schema_name, table.table_name);

      if (pkColumns.empty()) {
        // Sin PK: comparar conteos y algunos registros aleatorios
        Logger::info(LogCategory::TRANSFER, "verifyDataConsistency",
                     "No PK found, using count-based verification for " +
                         table.schema_name + "." + table.table_name);

        // Obtener conteo de MariaDB
        auto mariaCountRes = executeQueryMariaDB(
            mariadbConn, "SELECT COUNT(*) FROM `" + table.schema_name + "`.`" +
                             table.table_name + "`");
        size_t mariaCount = 0;
        if (!mariaCountRes.empty() && !mariaCountRes[0].empty()) {
          mariaCount = std::stoul(mariaCountRes[0][0]);
        }

        // Obtener conteo de PostgreSQL
        pqxx::work txn(pgConn);
        auto pgCountRes = txn.exec("SELECT COUNT(*) FROM \"" + lowerSchemaName +
                                   "\".\"" + lowerTableName + "\"");
        size_t pgCount = 0;
        if (!pgCountRes.empty()) {
          pgCount = pgCountRes[0][0].as<size_t>();
        }
        txn.commit();

        return (mariaCount == pgCount);
      }

      // Con PK: verificar que todos los PKs de MariaDB existan en PostgreSQL
      Logger::info(LogCategory::TRANSFER, "verifyDataConsistency",
                   "Using PK-based verification for " + table.schema_name +
                       "." + table.table_name);

      // Obtener todos los PKs de MariaDB
      std::string pkQuery = "SELECT ";
      for (size_t i = 0; i < pkColumns.size(); ++i) {
        if (i > 0)
          pkQuery += ", ";
        pkQuery += "`" + pkColumns[i] + "`";
      }
      pkQuery += " FROM `" + table.schema_name + "`.`" + table.table_name +
                 "` ORDER BY `" + pkColumns[0] + "`";

      auto mariaPKs = executeQueryMariaDB(mariadbConn, pkQuery);

      Logger::info(LogCategory::TRANSFER, "verifyDataConsistency",
                   "MariaDB table " + table.schema_name + "." +
                       table.table_name + " has " +
                       std::to_string(mariaPKs.size()) + " records");

      if (mariaPKs.empty()) {
        // Tabla vacía en MariaDB, verificar que PostgreSQL también esté vacía
        Logger::info(LogCategory::TRANSFER, "verifyDataConsistency",
                     "MariaDB table " + table.schema_name + "." +
                         table.table_name + " is empty");
        pqxx::work txn(pgConn);
        auto pgCountRes = txn.exec("SELECT COUNT(*) FROM \"" + lowerSchemaName +
                                   "\".\"" + lowerTableName + "\"");
        size_t pgCount = 0;
        if (!pgCountRes.empty()) {
          pgCount = pgCountRes[0][0].as<size_t>();
        }
        txn.commit();
        Logger::info(LogCategory::TRANSFER, "verifyDataConsistency",
                     "PostgreSQL table " + lowerSchemaName + "." +
                         lowerTableName + " has " + std::to_string(pgCount) +
                         " rows");
        return (pgCount == 0);
      }

      // Verificar que cada PK de MariaDB exista en PostgreSQL
      const size_t BATCH_SIZE = 1000;
      for (size_t batchStart = 0; batchStart < mariaPKs.size();
           batchStart += BATCH_SIZE) {
        size_t batchEnd = std::min(batchStart + BATCH_SIZE, mariaPKs.size());

        std::string checkQuery = "SELECT COUNT(*) FROM \"" + lowerSchemaName +
                                 "\".\"" + lowerTableName + "\" WHERE (";

        for (size_t i = batchStart; i < batchEnd; ++i) {
          if (i > batchStart)
            checkQuery += " OR ";
          checkQuery += "(";
          for (size_t j = 0; j < pkColumns.size() && j < mariaPKs[i].size();
               ++j) {
            if (j > 0)
              checkQuery += " AND ";
            std::string value = mariaPKs[i][j];
            if (value.empty() || value == "NULL") {
              checkQuery += "\"" + pkColumns[j] + "\" IS NULL";
            } else {
              checkQuery +=
                  "\"" + pkColumns[j] + "\" = '" + escapeSQL(value) + "'";
            }
          }
          checkQuery += ")";
        }
        checkQuery += ")";

        pqxx::work txn(pgConn);
        auto result = txn.exec(checkQuery);
        txn.commit();

        size_t foundCount = 0;
        if (!result.empty()) {
          foundCount = result[0][0].as<size_t>();
        }

        // Si no encontramos todos los registros del batch, no están
        // sincronizados
        if (foundCount != (batchEnd - batchStart)) {
          Logger::warning(
              LogCategory::TRANSFER, "verifyDataConsistency",
              "Data inconsistency detected: batch " +
                  std::to_string(batchStart) + "-" + std::to_string(batchEnd) +
                  " found " + std::to_string(foundCount) + " out of " +
                  std::to_string(batchEnd - batchStart) + " records");
          return false;
        }
      }

      Logger::info(LogCategory::TRANSFER, "verifyDataConsistency",
                   "Data consistency verified for " + table.schema_name + "." +
                       table.table_name);
      return true;

    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER, "verifyDataConsistency",
                    "Error verifying data consistency: " +
                        std::string(e.what()));
      return false;
    }
  }
};

#endif