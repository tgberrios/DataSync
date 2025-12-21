#ifndef MARIADBTOPOSTGRES_H
#define MARIADBTOPOSTGRES_H

#include "catalog/catalog_manager.h"
#include "engines/database_engine.h"
#include "sync/DatabaseToPostgresSync.h"
#include "sync/ICDCHandler.h"
#include "sync/SchemaSync.h"
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

class MariaDBToPostgres : public DatabaseToPostgresSync, public ICDCHandler {
public:
  MariaDBToPostgres() = default;
  ~MariaDBToPostgres() { shutdownParallelProcessing(); }

  static std::unordered_map<std::string, std::string> dataTypeMap;
  static std::unordered_map<std::string, std::string> collationMap;

  std::string cleanValueForPostgres(const std::string &value,
                                    const std::string &columnType) override;

  void processTableCDC(const DatabaseToPostgresSync::TableInfo &table,
                       pqxx::connection &pgConn) override;

  bool supportsCDC() const override { return true; }
  std::string getCDCMechanism() const override {
    return "Change Log Table (ds_change_log)";
  }

  std::vector<TableInfo> getActiveTables(pqxx::connection &pgConn);

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

      MYSQL *setupConn = nullptr;
      for (const auto &table : tables) {
        if (table.db_engine == "MariaDB") {
          setupConn = getMariaDBConnection(table.connection_string);
          if (setupConn) {
            break;
          }
        }
      }

      if (!setupConn) {
        Logger::error(LogCategory::TRANSFER,
                      "setupTableTargetMariaDBToPostgres",
                      "Failed to get MariaDB connection for setup");
        return;
      }

      std::string query = "CREATE DATABASE IF NOT EXISTS datasync_metadata";
      if (mysql_query(setupConn, query.c_str())) {
        Logger::error(LogCategory::TRANSFER,
                      "setupTableTargetMariaDBToPostgres",
                      "Failed to create datasync_metadata database: " +
                          std::string(mysql_error(setupConn)));
      } else {
        Logger::info(LogCategory::TRANSFER, "setupTableTargetMariaDBToPostgres",
                     "Ensured datasync_metadata database exists");
      }

      query = "CREATE TABLE IF NOT EXISTS datasync_metadata.ds_change_log ("
              "change_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,"
              "change_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,"
              "operation CHAR(1) NOT NULL,"
              "schema_name VARCHAR(255) NOT NULL,"
              "table_name VARCHAR(255) NOT NULL,"
              "pk_values JSON NOT NULL,"
              "row_data JSON NOT NULL,"
              "INDEX idx_ds_change_log_table_time (schema_name, table_name, "
              "change_time),"
              "INDEX idx_ds_change_log_table_change (schema_name, "
              "table_name, change_id)) "
              "ENGINE=InnoDB";

      if (mysql_query(setupConn, query.c_str())) {
        Logger::error(LogCategory::TRANSFER,
                      "setupTableTargetMariaDBToPostgres",
                      "Failed to create datasync_metadata.ds_change_log: " +
                          std::string(mysql_error(setupConn)));
      } else {
        Logger::info(LogCategory::TRANSFER, "setupTableTargetMariaDBToPostgres",
                     "Ensured datasync_metadata.ds_change_log table exists");
      }

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

        std::string triggerSchema = table.schema_name;
        std::string triggerTable = table.table_name;
        std::vector<std::string> pkColumns =
            getPrimaryKeyColumns(mariadbConn, triggerSchema, triggerTable);

        query = "SELECT COLUMN_NAME FROM information_schema.columns "
                "WHERE table_schema = '" +
                escapeSQL(triggerSchema) + "' AND table_name = '" +
                escapeSQL(triggerTable) + "' ORDER BY ORDINAL_POSITION";
        std::vector<std::vector<std::string>> allColumns =
            executeQueryMariaDB(mariadbConn, query);

        if (allColumns.empty()) {
          Logger::warning(LogCategory::TRANSFER,
                          "setupTableTargetMariaDBToPostgres",
                          "No columns found for " + triggerSchema + "." +
                              triggerTable + " - skipping trigger creation");
          continue;
        }

        bool hasPK = !pkColumns.empty();
        std::string jsonObjectNew;
        std::string jsonObjectOld;
        std::string rowDataNew;
        std::string rowDataOld;

        if (hasPK) {
          jsonObjectNew = "JSON_OBJECT(";
          jsonObjectOld = "JSON_OBJECT(";
          for (size_t i = 0; i < pkColumns.size(); ++i) {
            if (i > 0) {
              jsonObjectNew += ", ";
              jsonObjectOld += ", ";
            }
            jsonObjectNew +=
                "'" + pkColumns[i] + "', NEW.`" + pkColumns[i] + "`";
            jsonObjectOld +=
                "'" + pkColumns[i] + "', OLD.`" + pkColumns[i] + "`";
          }
          jsonObjectNew += ")";
          jsonObjectOld += ")";
        } else {
          std::string concatFields = "CONCAT_WS('|', ";
          for (size_t i = 0; i < allColumns.size(); ++i) {
            if (i > 0) {
              concatFields += ", ";
            }
            std::string colName = allColumns[i][0];
            concatFields += "COALESCE(CAST(NEW.`" + colName + "` AS CHAR), '')";
          }
          concatFields += ")";
          jsonObjectNew = "JSON_OBJECT('_hash', MD5(" + concatFields + "))";

          concatFields = "CONCAT_WS('|', ";
          for (size_t i = 0; i < allColumns.size(); ++i) {
            if (i > 0) {
              concatFields += ", ";
            }
            std::string colName = allColumns[i][0];
            concatFields += "COALESCE(CAST(OLD.`" + colName + "` AS CHAR), '')";
          }
          concatFields += ")";
          jsonObjectOld = "JSON_OBJECT('_hash', MD5(" + concatFields + "))";
        }

        rowDataNew = "JSON_OBJECT(";
        rowDataOld = "JSON_OBJECT(";
        for (size_t i = 0; i < allColumns.size(); ++i) {
          if (i > 0) {
            rowDataNew += ", ";
            rowDataOld += ", ";
          }
          std::string colName = allColumns[i][0];
          rowDataNew += "'" + colName + "', NEW.`" + colName + "`";
          rowDataOld += "'" + colName + "', OLD.`" + colName + "`";
        }
        rowDataNew += ")";
        rowDataOld += ")";

        std::string triggerInsert =
            "ds_tr_" + triggerSchema + "_" + triggerTable + "_ai";
        std::string triggerUpdate =
            "ds_tr_" + triggerSchema + "_" + triggerTable + "_au";
        std::string triggerDelete =
            "ds_tr_" + triggerSchema + "_" + triggerTable + "_ad";

        std::string dropInsert = "DROP TRIGGER IF EXISTS `" + triggerSchema +
                                 "`.`" + triggerInsert + "`";
        std::string dropUpdate = "DROP TRIGGER IF EXISTS `" + triggerSchema +
                                 "`.`" + triggerUpdate + "`";
        std::string dropDelete = "DROP TRIGGER IF EXISTS `" + triggerSchema +
                                 "`.`" + triggerDelete + "`";

        if (mysql_query(mariadbConn, dropInsert.c_str())) {
          Logger::error(
              LogCategory::TRANSFER, "setupTableTargetMariaDBToPostgres",
              "Failed to drop insert trigger for " + triggerSchema + "." +
                  triggerTable + ": " + std::string(mysql_error(mariadbConn)));
        }
        if (mysql_query(mariadbConn, dropUpdate.c_str())) {
          Logger::error(
              LogCategory::TRANSFER, "setupTableTargetMariaDBToPostgres",
              "Failed to drop update trigger for " + triggerSchema + "." +
                  triggerTable + ": " + std::string(mysql_error(mariadbConn)));
        }
        if (mysql_query(mariadbConn, dropDelete.c_str())) {
          Logger::error(
              LogCategory::TRANSFER, "setupTableTargetMariaDBToPostgres",
              "Failed to drop delete trigger for " + triggerSchema + "." +
                  triggerTable + ": " + std::string(mysql_error(mariadbConn)));
        }

        std::string createInsertTrigger =
            "CREATE TRIGGER `" + triggerSchema + "`.`" + triggerInsert +
            "` AFTER INSERT ON `" + triggerSchema + "`.`" + triggerTable +
            "` FOR EACH ROW INSERT INTO datasync_metadata.ds_change_log "
            "(operation, schema_name, table_name, pk_values, row_data) "
            "VALUES ('I', '" +
            triggerSchema + "', '" + triggerTable + "', " + jsonObjectNew +
            ", " + rowDataNew + ")";

        std::string createUpdateTrigger =
            "CREATE TRIGGER `" + triggerSchema + "`.`" + triggerUpdate +
            "` AFTER UPDATE ON `" + triggerSchema + "`.`" + triggerTable +
            "` FOR EACH ROW INSERT INTO datasync_metadata.ds_change_log "
            "(operation, schema_name, table_name, pk_values, row_data) "
            "VALUES ('U', '" +
            triggerSchema + "', '" + triggerTable + "', " + jsonObjectNew +
            ", " + rowDataNew + ")";

        std::string createDeleteTrigger =
            "CREATE TRIGGER `" + triggerSchema + "`.`" + triggerDelete +
            "` AFTER DELETE ON `" + triggerSchema + "`.`" + triggerTable +
            "` FOR EACH ROW INSERT INTO datasync_metadata.ds_change_log "
            "(operation, schema_name, table_name, pk_values, row_data) "
            "VALUES ('D', '" +
            triggerSchema + "', '" + triggerTable + "', " + jsonObjectOld +
            ", " + rowDataOld + ")";

        if (mysql_query(mariadbConn, createInsertTrigger.c_str())) {
          Logger::error(
              LogCategory::TRANSFER, "setupTableTargetMariaDBToPostgres",
              "Failed to create insert trigger for " + triggerSchema + "." +
                  triggerTable + ": " + std::string(mysql_error(mariadbConn)));
        } else {
          Logger::info(LogCategory::TRANSFER,
                       "setupTableTargetMariaDBToPostgres",
                       "Created insert trigger for " + triggerSchema + "." +
                           triggerTable +
                           (hasPK ? " (with PK)" : " (no PK, using hash)"));
        }

        if (mysql_query(mariadbConn, createUpdateTrigger.c_str())) {
          Logger::error(
              LogCategory::TRANSFER, "setupTableTargetMariaDBToPostgres",
              "Failed to create update trigger for " + triggerSchema + "." +
                  triggerTable + ": " + std::string(mysql_error(mariadbConn)));
        } else {
          Logger::info(LogCategory::TRANSFER,
                       "setupTableTargetMariaDBToPostgres",
                       "Created update trigger for " + triggerSchema + "." +
                           triggerTable +
                           (hasPK ? " (with PK)" : " (no PK, using hash)"));
        }

        if (mysql_query(mariadbConn, createDeleteTrigger.c_str())) {
          Logger::error(
              LogCategory::TRANSFER, "setupTableTargetMariaDBToPostgres",
              "Failed to create delete trigger for " + triggerSchema + "." +
                  triggerTable + ": " + std::string(mysql_error(mariadbConn)));
        } else {
          Logger::info(LogCategory::TRANSFER,
                       "setupTableTargetMariaDBToPostgres",
                       "Created delete trigger for " + triggerSchema + "." +
                           triggerTable +
                           (hasPK ? " (with PK)" : " (no PK, using hash)"));
        }

        query = "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, "
                "COLUMN_KEY, EXTRA, CHARACTER_MAXIMUM_LENGTH "
                "FROM information_schema.columns "
                "WHERE table_schema = '" +
                table.schema_name + "' AND table_name = '" + table.table_name +
                "';";

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
          } else if (dataType == "char") {
            // CHAR siempre se mapea a TEXT para evitar truncamiento
            pgType = "TEXT";
          } else if (dataType == "varchar") {
            // Validar que maxLength sea razonable antes de usarlo
            if (!maxLength.empty() && maxLength != "NULL") {
              try {
                size_t length = std::stoul(maxLength);
                // Solo usar longitud si es razonable (entre 1 y 65535)
                if (length >= 1 && length <= 65535) {
                  pgType = "VARCHAR(" + maxLength + ")";
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

          // Solo la PK debe ser NOT NULL, todas las demás columnas permiten
          // NULL
          std::string nullable = (columnKey == "PRI") ? " NOT NULL" : "";
          std::string columnDef = "\"" + colName + "\" " + pgType + nullable;
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

        // Check for duplicate PKs before creating table - if duplicates found,
        // don't create PK
        bool hasDuplicatePKs = false;
        if (!primaryKeys.empty()) {
          try {
            // Get a sample of data to check for duplicates
            std::string sampleQuery = "SELECT ";
            for (size_t i = 0; i < primaryKeys.size(); ++i) {
              if (i > 0)
                sampleQuery += ", ";
              sampleQuery += "`" + primaryKeys[i] + "`";
            }
            sampleQuery += " FROM `" + table.schema_name + "`.`" +
                           table.table_name + "` LIMIT 1000";

            std::vector<std::vector<std::string>> sampleData =
                executeQueryMariaDB(mariadbConn, sampleQuery);
            std::set<std::string> seenPKs;

            for (const auto &row : sampleData) {
              if (row.size() != primaryKeys.size())
                continue;
              std::string pkKey;
              for (size_t i = 0; i < row.size(); ++i) {
                if (i > 0)
                  pkKey += "|";
                pkKey += row[i];
              }
              if (seenPKs.find(pkKey) != seenPKs.end()) {
                hasDuplicatePKs = true;
                Logger::warning(
                    LogCategory::TRANSFER, "setupTableTargetMariaDBToPostgres",
                    "Duplicate PK values detected in sample data for " +
                        table.schema_name + "." + table.table_name +
                        " - creating table without PK constraint");
                break;
              }
              seenPKs.insert(pkKey);
            }
          } catch (const std::exception &e) {
            Logger::warning(
                LogCategory::TRANSFER, "setupTableTargetMariaDBToPostgres",
                "Error checking for duplicate PKs: " + std::string(e.what()) +
                    " - creating table without PK constraint");
            hasDuplicatePKs = true;
          }
        }

        if (!primaryKeys.empty() && !hasDuplicatePKs) {
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

  // SINGULAR PROCESSING REMOVED - ONLY PARALLEL PROCESSING REMAINS
  void transferDataMariaDBToPostgres() {
    // Redirect to parallel processing
    transferDataMariaDBToPostgresParallel();
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

        try {
          MariaDBEngine engine(table.connection_string);
          std::vector<ColumnInfo> sourceColumns =
              engine.getTableColumns(table.schema_name, table.table_name);

          if (!sourceColumns.empty()) {
            SchemaSync::syncSchema(pgConn, table.schema_name, table.table_name,
                                   sourceColumns, "MariaDB");
          }
        } catch (const std::exception &e) {
          Logger::warning(
              LogCategory::TRANSFER, "transferDataMariaDBToPostgresParallel",
              "Error syncing schema for " + table.schema_name + "." +
                  table.table_name + ": " + std::string(e.what()) +
                  " - continuing with sync");
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

    std::string originalStatus = table.status;

    try {
      setTableProcessingState(tableKey, true);
      updateStatus(pgConn, table.schema_name, table.table_name, "IN_PROGRESS");

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

        if (dataType == "char") {
          // CHAR siempre se mapea a TEXT para evitar truncamiento
          pgType = "TEXT";
        } else if (dataType == "varchar") {
          if (!maxLength.empty() && maxLength != "NULL") {
            try {
              size_t length = std::stoul(maxLength);
              if (length >= 1 && length <= 65535) {
                pgType = "VARCHAR(" + maxLength + ")";
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

      std::string lowerSchemaName = table.schema_name;
      std::transform(lowerSchemaName.begin(), lowerSchemaName.end(),
                     lowerSchemaName.begin(), ::tolower);
      std::string lowerTableNamePG = table.table_name;
      std::transform(lowerTableNamePG.begin(), lowerTableNamePG.end(),
                     lowerTableNamePG.begin(), ::tolower);

      {
        pqxx::work schemaTxn(pgConn);
        schemaTxn.exec("CREATE SCHEMA IF NOT EXISTS \"" + lowerSchemaName +
                       "\";");
        schemaTxn.commit();
      }

      {
        auto tableExists = [&]() {
          pqxx::work checkTxn(pgConn);
          auto result = checkTxn.exec(
              "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE "
              "table_schema = " +
              checkTxn.quote(lowerSchemaName) +
              " AND table_name = " + checkTxn.quote(lowerTableNamePG) + ")");
          checkTxn.commit();
          return !result.empty() && result[0][0].as<bool>();
        }();

        if (!tableExists) {
          std::string createQuery = "CREATE TABLE IF NOT EXISTS \"" +
                                    lowerSchemaName + "\".\"" +
                                    lowerTableNamePG + "\" (";
          std::vector<std::string> primaryKeys;

          for (size_t i = 0; i < columns.size(); ++i) {
            if (columns[i].size() < 6)
              continue;

            std::string colName = columns[i][0];
            if (colName.empty())
              continue;
            std::transform(colName.begin(), colName.end(), colName.begin(),
                           ::tolower);
            std::string dataType = columns[i][1];
            std::string isNullable =
                columns[i].size() > 2 ? columns[i][2] : "YES";
            std::string columnKey = columns[i].size() > 3 ? columns[i][3] : "";
            std::string extra = columns[i].size() > 4 ? columns[i][4] : "";
            std::string maxLength = columns[i].size() > 5 ? columns[i][5] : "";

            std::string pgType = "TEXT";
            if (extra == "auto_increment") {
              pgType = (dataType == "bigint") ? "BIGINT" : "INTEGER";
            } else if (dataType == "timestamp" || dataType == "datetime") {
              pgType = "TIMESTAMP";
            } else if (dataType == "date") {
              pgType = "DATE";
            } else if (dataType == "time") {
              pgType = "TIME";
            } else if (dataType == "char") {
              pgType = "TEXT";
            } else if (dataType == "varchar") {
              if (!maxLength.empty() && maxLength != "NULL") {
                try {
                  size_t length = std::stoul(maxLength);
                  if (length >= 1 && length <= 65535) {
                    pgType = "VARCHAR(" + maxLength + ")";
                  } else {
                    pgType = "VARCHAR";
                  }
                } catch (const std::exception &e) {
                  pgType = "VARCHAR";
                }
              } else {
                pgType = "VARCHAR";
              }
            } else if (dataTypeMap.count(dataType)) {
              pgType = dataTypeMap[dataType];
            }

            std::string nullable = (columnKey == "PRI") ? " NOT NULL" : "";
            createQuery += "\"" + colName + "\" " + pgType + nullable;
            if (columnKey == "PRI") {
              primaryKeys.push_back(colName);
            }
            createQuery += ", ";
          }

          bool hasDuplicatePKs = false;
          bool hasNullPKs = false;
          if (!primaryKeys.empty()) {
            try {
              std::string sampleQuery = "SELECT ";
              for (size_t i = 0; i < primaryKeys.size(); ++i) {
                if (i > 0)
                  sampleQuery += ", ";
                sampleQuery += "`" + primaryKeys[i] + "`";
              }
              sampleQuery += " FROM `" + table.schema_name + "`.`" +
                             table.table_name + "` LIMIT 1000";

              std::vector<std::vector<std::string>> sampleData =
                  executeQueryMariaDB(mariadbConn, sampleQuery);
              std::set<std::string> seenPKs;

              for (const auto &row : sampleData) {
                if (row.size() != primaryKeys.size())
                  continue;
                std::string pkKey;
                bool hasNull = false;
                for (size_t i = 0; i < row.size(); ++i) {
                  if (i > 0)
                    pkKey += "|";
                  std::string pkValue = row[i];
                  if (pkValue.empty() || pkValue == "NULL" ||
                      pkValue == "null") {
                    pkKey += "<NULL>";
                    hasNull = true;
                    hasNullPKs = true;
                  } else {
                    pkKey += pkValue;
                  }
                }
                if (hasNull) {
                  continue;
                }
                if (seenPKs.find(pkKey) != seenPKs.end()) {
                  hasDuplicatePKs = true;
                  Logger::warning(
                      LogCategory::TRANSFER, "processTableParallel",
                      "Duplicate PK values detected in sample data for " +
                          table.schema_name + "." + table.table_name +
                          " - creating table without PK constraint");
                  break;
                }
                seenPKs.insert(pkKey);
              }
            } catch (const std::exception &e) {
              Logger::warning(LogCategory::TRANSFER, "processTableParallel",
                              "Error checking PKs for duplicates/NULLs: " +
                                  std::string(e.what()) +
                                  " - creating table without PK constraint");
              hasDuplicatePKs = true;
            }
          }

          if (!primaryKeys.empty() && !hasDuplicatePKs && !hasNullPKs) {
            createQuery += "PRIMARY KEY (";
            for (size_t i = 0; i < primaryKeys.size(); ++i) {
              createQuery += "\"" + primaryKeys[i] + "\"";
              if (i < primaryKeys.size() - 1)
                createQuery += ", ";
            }
            createQuery += ")";
          } else {
            if (createQuery.size() > 2) {
              createQuery.erase(createQuery.size() - 2, 2);
            }
            if (hasNullPKs) {
              Logger::warning(LogCategory::TRANSFER, "processTableParallel",
                              "NULL values detected in PK columns for " +
                                  table.schema_name + "." + table.table_name +
                                  " - creating table without PK constraint");
            }
          }
          createQuery += ");";

          pqxx::work createTxn(pgConn);
          createTxn.exec(createQuery);
          createTxn.commit();

          Logger::info(LogCategory::TRANSFER, "processTableParallel",
                       "Created table " + lowerSchemaName + "." +
                           lowerTableNamePG);
        }
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

      if (table.status == "FULL_LOAD" || table.status == "RESET") {
        Logger::info(
            LogCategory::TRANSFER, "processTableParallel",
            "FULL_LOAD/RESET detected - performing mandatory truncate for " +
                table.schema_name + "." + table.table_name);

        try {
          pqxx::work txn(pgConn);
          txn.exec("TRUNCATE TABLE \"" + lowerSchemaName + "\".\"" +
                   lowerTableNamePG + "\" CASCADE;");

          std::string pkStrategy = getPKStrategyFromCatalog(
              pgConn, table.schema_name, table.table_name);

          if (pkStrategy == "CDC") {
            txn.exec(
                "UPDATE metadata.catalog SET sync_metadata = "
                "COALESCE(sync_metadata, '{}'::jsonb) || "
                "jsonb_build_object('last_change_id', 0) WHERE schema_name='" +
                escapeSQL(table.schema_name) + "' AND table_name='" +
                escapeSQL(table.table_name) + "' AND db_engine='MariaDB';");
            Logger::info(LogCategory::TRANSFER, "processTableParallel",
                         "Reset last_change_id for CDC table " +
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

      std::string pkStrategy =
          getPKStrategyFromCatalog(pgConn, table.schema_name, table.table_name);

      Logger::info(LogCategory::TRANSFER, "processTableParallel",
                   "Counts for " + table.schema_name + "." + table.table_name +
                       ": source=" + std::to_string(sourceCount) +
                       ", target=" + std::to_string(targetCount) +
                       ", pkStrategy=" + pkStrategy +
                       ", status=" + table.status);

      if (pkStrategy == "CDC" && table.status != "FULL_LOAD") {
        Logger::info(LogCategory::TRANSFER, "processTableParallel",
                     "CDC strategy detected for " + table.schema_name + "." +
                         table.table_name + " - processing changes only");
        processTableCDC(tableKey, mariadbConn, table, pgConn, columnNames,
                        columnTypes);

        size_t finalCount = 0;
        try {
          pqxx::work txn(pgConn);
          auto res = txn.exec("SELECT COUNT(*) FROM \"" + lowerSchemaName +
                              "\".\"" + lowerTableNamePG + "\";");
          if (!res.empty())
            finalCount = res[0][0].as<size_t>();
          txn.commit();
        } catch (const std::exception &e) {
          Logger::error(LogCategory::TRANSFER, "processTableParallel",
                        "Error getting final count for CDC table " +
                            table.schema_name + "." + table.table_name + ": " +
                            std::string(e.what()));
        }

        updateStatus(pgConn, table.schema_name, table.table_name,
                     "LISTENING_CHANGES", finalCount);
        mysql_close(mariadbConn);
        removeTableProcessingState(tableKey);
        return;
      }

      if (sourceCount == 0) {
        if (targetCount == 0) {
          updateStatus(pgConn, table.schema_name, table.table_name, "NO_DATA",
                       0);
        } else {
          updateStatus(pgConn, table.schema_name, table.table_name,
                       "LISTENING_CHANGES", targetCount);
        }
        mysql_close(mariadbConn);
        removeTableProcessingState(tableKey);
        return;
      }

      if (sourceCount == targetCount && table.status != "FULL_LOAD") {
        Logger::info(LogCategory::TRANSFER, "processTableParallel",
                     "Counts match (" + std::to_string(sourceCount) + ") for " +
                         table.schema_name + "." + table.table_name);
        updateStatus(pgConn, table.schema_name, table.table_name,
                     "LISTENING_CHANGES", targetCount);
        mysql_close(mariadbConn);
        removeTableProcessingState(tableKey);
        return;
      }

      if (sourceCount < targetCount && pkStrategy != "PK" &&
          pkStrategy != "CDC") {
        try {
          pqxx::work truncateTxn(pgConn);
          truncateTxn.exec("TRUNCATE TABLE \"" + lowerSchemaName + "\".\"" +
                           lowerTableNamePG + "\" CASCADE;");
          truncateTxn.commit();
          updateStatus(pgConn, table.schema_name, table.table_name, "FULL_LOAD",
                       0);
          targetCount = 0;
        } catch (const std::exception &e) {
          Logger::error(LogCategory::TRANSFER, "processTableParallel",
                        "Error truncating OFFSET table for deletes " +
                            table.schema_name + "." + table.table_name + ": " +
                            std::string(e.what()));
        }
      }

      Logger::info(LogCategory::TRANSFER, "processTableParallel",
                   "Proceeding with FULL_LOAD for " + table.schema_name + "." +
                       table.table_name);
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
      updateStatus(pgConn, table.schema_name, table.table_name, "ERROR");
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

      pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());

      Logger::info(LogCategory::TRANSFER, "dataFetcherThread",
                   "Starting FULL_LOAD data fetch for " + table.schema_name +
                       "." + table.table_name);

      std::vector<std::string> pkColumns =
          getPKColumnsFromCatalog(pgConn, table.schema_name, table.table_name);

      bool hasMoreData = true;
      size_t lastProcessedOffset = 0;

      while (hasMoreData) {
        chunkNumber++;

        std::string selectQuery = "SELECT * FROM `" + table.schema_name +
                                  "`.`" + table.table_name + "`";

        if (!pkColumns.empty()) {
          selectQuery += " ORDER BY ";
          for (size_t i = 0; i < pkColumns.size(); ++i) {
            if (i > 0)
              selectQuery += ", ";
            selectQuery += "`" + pkColumns[i] + "`";
          }
        }

        selectQuery += " LIMIT " + std::to_string(CHUNK_SIZE) + " OFFSET " +
                       std::to_string(lastProcessedOffset) + ";";

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
                           std::to_string(results.size()) + " rows for " +
                           table.schema_name + "." + table.table_name);

          lastProcessedOffset += results.size();

          if (results.size() < CHUNK_SIZE) {
            Logger::info(LogCategory::TRANSFER, "dataFetcherThread",
                         "Retrieved " + std::to_string(results.size()) +
                             " rows (less than chunk size " +
                             std::to_string(CHUNK_SIZE) +
                             ") - ending data transfer");
            hasMoreData = false;
          }
        } catch (const std::exception &e) {
          std::string errorMsg = e.what();
          Logger::error(LogCategory::TRANSFER, "dataFetcherThread",
                        "CRITICAL ERROR: Bulk upsert failed for chunk " +
                            std::to_string(chunkNumber) + " in table " +
                            table.schema_name + "." + table.table_name + ": " +
                            errorMsg);

          if (errorMsg.find("current transaction is aborted") !=
                  std::string::npos ||
              errorMsg.find("previously aborted") != std::string::npos ||
              errorMsg.find("aborted transaction") != std::string::npos) {
            Logger::error(LogCategory::TRANSFER, "dataFetcherThread",
                          "Transaction abort detected - breaking loop");
            hasMoreData = false;
            break;
          }
        }
      }

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
                    size_t /* rowCount */ = 0) {
    try {
      // Thread-safe: Proteger la actualización de metadatos
      std::lock_guard<std::mutex> lock(metadataUpdateMutex);

      pqxx::work txn(pgConn);

      auto columnQuery =
          txn.exec("SELECT pk_strategy FROM metadata.catalog "
                   "WHERE schema_name='" +
                   escapeSQL(schema_name) + "' AND table_name='" +
                   escapeSQL(table_name) + "';");
      std::string pkStrategy = "";
      if (!columnQuery.empty()) {
        if (!columnQuery[0][0].is_null()) {
          pkStrategy = columnQuery[0][0].as<std::string>();
        }
      }

      std::string updateQuery = "UPDATE metadata.catalog SET status='" +
                                status +
                                "' "
                                "WHERE schema_name='" +
                                escapeSQL(schema_name) + "' AND table_name='" +
                                escapeSQL(table_name) + "';";

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
  std::string escapeSQL(const std::string &value) {
    if (value.empty()) {
      return value;
    }
    std::string escaped = value;
    size_t pos = 0;
    while ((pos = escaped.find('\'', pos)) != std::string::npos) {
      escaped.replace(pos, 1, "''");
      pos += 2;
    }
    pos = 0;
    while ((pos = escaped.find('\\', pos)) != std::string::npos) {
      escaped.replace(pos, 1, "\\\\");
      pos += 2;
    }
    return escaped;
  }

  void processTableCDC(const std::string &tableKey, MYSQL *mariadbConn,
                       const TableInfo &table, pqxx::connection &pgConn,
                       const std::vector<std::string> &columnNames,
                       const std::vector<std::string> &columnTypes);

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
};

#endif