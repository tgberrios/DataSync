#ifndef MSSQLTOPOSTGRES_H
#define MSSQLTOPOSTGRES_H

#include "catalog/catalog_manager.h"
#include "engines/database_engine.h"
#include "sync/DatabaseToPostgresSync.h"
#include "sync/ICDCHandler.h"
#include "sync/SchemaSync.h"
#include "sync/TableProcessorThreadPool.h"
#include "third_party/json.hpp"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <iostream>
#include <pqxx/pqxx>
#include <set>
#include <sql.h>
#include <sqlext.h>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

using json = nlohmann::json;
using namespace ParallelProcessing;

class MSSQLToPostgres : public DatabaseToPostgresSync, public ICDCHandler {
public:
  MSSQLToPostgres() = default;
  ~MSSQLToPostgres() { shutdownParallelProcessing(); }

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

  SQLHDBC getMSSQLConnection(const std::string &connectionString) {
    // Validate connection string
    if (connectionString.empty()) {
      Logger::error(LogCategory::TRANSFER, "getMSSQLConnection",
                    "Empty connection string provided");
      return nullptr;
    }

    // Parse connection string to validate required parameters
    std::string server, database, uid, pwd, port;
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
      if (key == "SERVER")
        server = value;
      else if (key == "DATABASE")
        database = value;
      else if (key == "UID")
        uid = value;
      else if (key == "PWD")
        pwd = value;
      else if (key == "PORT")
        port = value;
    }

    // Validate required parameters
    if (server.empty() || database.empty() || uid.empty()) {
      Logger::error(
          LogCategory::TRANSFER, "getMSSQLConnection",
          "Missing required connection parameters (SERVER, DATABASE, or UID)");
      return nullptr;
    }

    // Validate port number if provided
    if (!port.empty()) {
      try {
        int portNum = std::stoi(port);
        if (portNum <= 0 || portNum > 65535) {
          Logger::warning(LogCategory::TRANSFER, "getMSSQLConnection",
                          "Invalid port number " + port +
                              ", using default 1433");
        }
      } catch (const std::exception &e) {
        Logger::warning(LogCategory::TRANSFER, "getMSSQLConnection",
                        "Could not parse port " + port + ": " +
                            std::string(e.what()) + ", using default 1433");
      }
    }

    // Crear nueva conexión para cada consulta para evitar "Connection is busy"
    SQLHENV tempEnv = nullptr;
    SQLHDBC tempConn = nullptr;
    SQLRETURN ret;

    // Crear nueva conexión
    ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &tempEnv);
    if (!SQL_SUCCEEDED(ret)) {
      Logger::error(LogCategory::TRANSFER, "getMSSQLConnection",
                    "Failed to allocate ODBC environment handle");
      return nullptr;
    }

    ret = SQLSetEnvAttr(tempEnv, SQL_ATTR_ODBC_VERSION,
                        (SQLPOINTER)SQL_OV_ODBC3, 0);
    if (!SQL_SUCCEEDED(ret)) {
      SQLFreeHandle(SQL_HANDLE_ENV, tempEnv);
      Logger::error(LogCategory::TRANSFER, "getMSSQLConnection",
                    "Failed to set ODBC version");
      return nullptr;
    }

    ret = SQLAllocHandle(SQL_HANDLE_DBC, tempEnv, &tempConn);
    if (!SQL_SUCCEEDED(ret)) {
      SQLFreeHandle(SQL_HANDLE_ENV, tempEnv);
      Logger::error(LogCategory::TRANSFER, "getMSSQLConnection",
                    "Failed to allocate ODBC connection handle");
      return nullptr;
    }

    // Set connection timeouts
    SQLSetConnectAttr(tempConn, SQL_ATTR_CONNECTION_TIMEOUT, (SQLPOINTER)30, 0);
    SQLSetConnectAttr(tempConn, SQL_ATTR_LOGIN_TIMEOUT, (SQLPOINTER)30, 0);

    SQLCHAR outConnStr[1024];
    SQLSMALLINT outConnStrLen;
    ret = SQLDriverConnect(
        tempConn, nullptr, (SQLCHAR *)connectionString.c_str(), SQL_NTS,
        outConnStr, sizeof(outConnStr), &outConnStrLen, SQL_DRIVER_NOPROMPT);
    if (!SQL_SUCCEEDED(ret)) {
      SQLCHAR sqlState[6], msg[SQL_MAX_MESSAGE_LENGTH];
      SQLINTEGER nativeError;
      SQLSMALLINT msgLen;
      SQLGetDiagRec(SQL_HANDLE_DBC, tempConn, 1, sqlState, &nativeError, msg,
                    sizeof(msg), &msgLen);
      SQLFreeHandle(SQL_HANDLE_DBC, tempConn);
      SQLFreeHandle(SQL_HANDLE_ENV, tempEnv);
      Logger::error(LogCategory::TRANSFER, "getMSSQLConnection",
                    "Failed to connect to MSSQL: " + std::string((char *)msg) +
                        " (server: " + server + ", database: " + database +
                        ", uid: " + uid + ")");
      return nullptr;
    }

    // Test connection with a simple query
    SQLHSTMT testStmt;
    ret = SQLAllocHandle(SQL_HANDLE_STMT, tempConn, &testStmt);
    if (ret == SQL_SUCCESS) {
      ret = SQLExecDirect(testStmt, (SQLCHAR *)"SELECT 1", SQL_NTS);
      if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
        SQLFreeHandle(SQL_HANDLE_STMT, testStmt);
      } else {
        SQLFreeHandle(SQL_HANDLE_STMT, testStmt);
        SQLFreeHandle(SQL_HANDLE_DBC, tempConn);
        SQLFreeHandle(SQL_HANDLE_ENV, tempEnv);
        Logger::error(LogCategory::TRANSFER, "getMSSQLConnection",
                      "Connection test failed");
        return nullptr;
      }
    }

    return tempConn;
  }

  void closeMSSQLConnection(SQLHDBC conn) {
    if (conn) {
      SQLDisconnect(conn);
      SQLFreeHandle(SQL_HANDLE_DBC, conn);
    }
  }

  std::vector<TableInfo> getActiveTables(pqxx::connection &pgConn) {
    std::vector<TableInfo> data;

    try {
      pqxx::work txn(pgConn);
      auto results = txn.exec(
          "SELECT schema_name, table_name, cluster_name, db_engine, "
          "connection_string, "
          "status, pk_strategy, "
          "pk_columns "
          "FROM metadata.catalog "
          "WHERE active=true AND db_engine='MSSQL' AND status != 'NO_DATA' "
          "AND schema_name != 'datasync_metadata' "
          "AND (cron_schedule IS NULL OR cron_schedule = '') "
          "ORDER BY schema_name, table_name;");
      txn.commit();

      Logger::info(LogCategory::TRANSFER, "getActiveTables",
                   "Query returned " + std::to_string(results.size()) +
                       " rows from catalog");

      for (const auto &row : results) {
        if (row.size() < 8) {
          Logger::warning(LogCategory::TRANSFER, "getActiveTables",
                          "Row has only " + std::to_string(row.size()) +
                              " columns, expected 8 - skipping");
          continue;
        }

        TableInfo t;
        t.schema_name = row[0].is_null() ? "" : row[0].as<std::string>();
        t.table_name = row[1].is_null() ? "" : row[1].as<std::string>();
        t.cluster_name = row[2].is_null() ? "" : row[2].as<std::string>();
        t.db_engine = row[3].is_null() ? "" : row[3].as<std::string>();
        t.connection_string = row[4].is_null() ? "" : row[4].as<std::string>();
        t.status = row[5].is_null() ? "" : row[5].as<std::string>();
        t.pk_strategy = row[6].is_null() ? "" : row[6].as<std::string>();
        t.pk_columns = row[7].is_null() ? "" : row[7].as<std::string>();
        std::vector<std::string> pkCols = parseJSONArray(t.pk_columns);
        t.has_pk = !pkCols.empty();
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

  void setupTableTargetMSSQLToPostgres() {
    Logger::info(LogCategory::TRANSFER, "Starting MSSQL table target setup");

    try {
      pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());

      if (!pgConn.is_open()) {
        Logger::error(LogCategory::TRANSFER, "setupTableTargetMSSQLToPostgres",
                      "CRITICAL ERROR: Cannot establish PostgreSQL connection "
                      "for MSSQL table setup");
        return;
      }

      Logger::info(LogCategory::TRANSFER,
                   "PostgreSQL connection established for MSSQL table setup");

      auto tables = getActiveTables(pgConn);

      if (tables.empty()) {
        Logger::info(LogCategory::TRANSFER,
                     "No active MSSQL tables found to setup");
        return;
      }

      std::set<std::string> processedDatabases;

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
                       " MSSQL tables in priority order");

      for (const auto &table : tables) {
        if (table.db_engine != "MSSQL") {
          Logger::warning(LogCategory::TRANSFER,
                          "Skipping non-MSSQL table: " + table.db_engine +
                              " - " + table.schema_name + "." +
                              table.table_name);
          continue;
        }

        std::string databaseName = extractDatabaseName(table.connection_string);

        if (processedDatabases.find(databaseName) == processedDatabases.end()) {
          SQLHDBC setupDbc = getMSSQLConnection(table.connection_string);
          if (!setupDbc) {
            Logger::error(
                LogCategory::TRANSFER, "setupTableTargetMSSQLToPostgres",
                "Failed to get MSSQL connection for database " + databaseName);
            continue;
          }

          std::string useQuery = "USE [" + databaseName + "];";
          executeQueryMSSQL(setupDbc, useQuery);

          std::string createSchemaQuery =
              "IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = "
              "'datasync_metadata') BEGIN EXEC('CREATE SCHEMA "
              "datasync_metadata') "
              "END;";
          executeQueryMSSQL(setupDbc, createSchemaQuery);

          std::string createTableQuery =
              "IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = "
              "OBJECT_ID(N'datasync_metadata.ds_change_log') AND type in "
              "(N'U')) BEGIN CREATE TABLE datasync_metadata.ds_change_log ("
              "change_id BIGINT IDENTITY(1,1) PRIMARY KEY, "
              "change_time DATETIME NOT NULL DEFAULT GETDATE(), "
              "operation CHAR(1) NOT NULL, "
              "schema_name NVARCHAR(255) NOT NULL, "
              "table_name NVARCHAR(255) NOT NULL, "
              "pk_values NVARCHAR(MAX) NOT NULL, "
              "row_data NVARCHAR(MAX) NOT NULL); "
              "CREATE INDEX idx_ds_change_log_table_time ON "
              "datasync_metadata.ds_change_log (schema_name, table_name, "
              "change_time); "
              "CREATE INDEX idx_ds_change_log_table_change ON "
              "datasync_metadata.ds_change_log (schema_name, table_name, "
              "change_id); END;";
          executeQueryMSSQL(setupDbc, createTableQuery);

          Logger::info(
              LogCategory::TRANSFER, "setupTableTargetMSSQLToPostgres",
              "Ensured datasync_metadata schema and ds_change_log table "
              "exist for database " +
                  databaseName);

          processedDatabases.insert(databaseName);
        }

        SQLHDBC dbc = getMSSQLConnection(table.connection_string);
        if (!dbc) {
          Logger::error(
              LogCategory::TRANSFER, "setupTableTargetMSSQLToPostgres",
              "CRITICAL ERROR: Failed to get MSSQL connection for table " +
                  table.schema_name + "." + table.table_name +
                  " - skipping table setup");
          continue;
        }

        // Primero cambiar a la base de datos correcta
        std::string useQuery = "USE [" + databaseName + "];";
        auto useResult = executeQueryMSSQL(dbc, useQuery);

        // Luego ejecutar la query sin prefijo de base de datos
        std::string query =
            "SELECT c.name AS COLUMN_NAME, tp.name AS DATA_TYPE, "
            "CASE WHEN c.is_nullable = 1 THEN 'YES' ELSE 'NO' END as "
            "IS_NULLABLE, "
            "CASE WHEN pk.column_id IS NOT NULL THEN 'YES' ELSE 'NO' END as "
            "IS_PRIMARY_KEY, "
            "c.max_length AS CHARACTER_MAXIMUM_LENGTH, "
            "c.precision AS NUMERIC_PRECISION, "
            "c.scale AS NUMERIC_SCALE, "
            "NULL AS COLUMN_DEFAULT "
            "FROM sys.columns c "
            "INNER JOIN sys.tables t ON c.object_id = t.object_id "
            "INNER JOIN sys.schemas s ON t.schema_id = s.schema_id "
            "INNER JOIN sys.types tp ON c.user_type_id = tp.user_type_id "
            "LEFT JOIN ( "
            "  SELECT ic.column_id, ic.object_id "
            "  FROM sys.indexes i "
            "  INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id "
            "AND i.index_id = ic.index_id "
            "  WHERE i.is_primary_key = 1 "
            ") pk ON c.column_id = pk.column_id AND t.object_id = pk.object_id "
            "WHERE s.name = '" +
            table.schema_name + "' AND t.name = '" + table.table_name +
            "' "
            "ORDER BY c.column_id;";

        std::vector<std::vector<std::string>> columns =
            executeQueryMSSQL(dbc, query);

        if (columns.empty()) {
          Logger::error(LogCategory::TRANSFER,
                        "setupTableTargetMSSQLToPostgres",
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

        std::string lowerTableName = table.table_name;
        std::transform(lowerTableName.begin(), lowerTableName.end(),
                       lowerTableName.begin(), ::tolower);
        std::string createQuery = "CREATE TABLE IF NOT EXISTS \"" +
                                  lowerSchema + "\".\"" + lowerTableName +
                                  "\" (";
        std::vector<std::string> primaryKeys;

        for (const std::vector<std::string> &col : columns) {
          if (col.size() < 8)
            continue;

          std::string colName = col[0];
          std::transform(colName.begin(), colName.end(), colName.begin(),
                         ::tolower);
          std::string dataType = col[1];
          std::string isNullable = col.size() > 2 ? col[2] : "YES";
          std::string isPrimaryKey = col[3];
          // Solo la PK debe ser NOT NULL, todas las demás columnas permiten
          // NULL
          std::string nullable = (isPrimaryKey == "YES") ? " NOT NULL" : "";
          std::string maxLength = col[4];
          std::string numericPrecision = col[5];
          std::string numericScale = col[6];
          std::string columnDefault = col[7];

          std::string pgType = "TEXT";
          if (dataType == "decimal" || dataType == "numeric") {
            // Para decimal/numeric, usar la precisión y escala de MSSQL
            if (!numericPrecision.empty() && numericPrecision != "NULL" &&
                !numericScale.empty() && numericScale != "NULL") {
              pgType = "NUMERIC(" + numericPrecision + "," + numericScale + ")";
            } else {
              pgType = "NUMERIC(18,4)";
            }
          } else if (dataType == "varchar" || dataType == "nvarchar") {
            pgType =
                (!maxLength.empty() && maxLength != "NULL" && maxLength != "-1")
                    ? "VARCHAR(" + maxLength + ")"
                    : "VARCHAR";
          } else if (dataType == "char" || dataType == "nchar") {
            pgType = "TEXT";
          } else if (dataTypeMap.count(dataType)) {
            pgType = dataTypeMap[dataType];
          }

          createQuery += "\"" + colName + "\" " + pgType + nullable;
          if (isPrimaryKey == "YES")
            primaryKeys.push_back(colName);
          createQuery += ", ";
        }

        // Check for duplicate PKs before creating table - if duplicates found,
        // don't create PK
        bool hasDuplicatePKs = false;
        if (!primaryKeys.empty()) {
          try {
            // Get a sample of data to check for duplicates
            std::string sampleQuery = "SELECT TOP 1000 ";
            for (size_t i = 0; i < primaryKeys.size(); ++i) {
              if (i > 0)
                sampleQuery += ", ";
              sampleQuery += "[" + primaryKeys[i] + "]";
            }
            sampleQuery +=
                " FROM [" + table.schema_name + "].[" + table.table_name + "]";

            std::vector<std::vector<std::string>> sampleData =
                executeQueryMSSQL(dbc, sampleQuery);
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
                    LogCategory::TRANSFER, "setupTableTargetMSSQLToPostgres",
                    "Duplicate PK values detected in sample data for " +
                        table.schema_name + "." + table.table_name +
                        " - creating table without PK constraint");
                break;
              }
              seenPKs.insert(pkKey);
            }
          } catch (const std::exception &e) {
            Logger::warning(
                LogCategory::TRANSFER, "setupTableTargetMSSQLToPostgres",
                "Error checking for duplicate PKs: " + std::string(e.what()) +
                    " - creating table without PK constraint");
            hasDuplicatePKs = true;
          }
        }

        if (!primaryKeys.empty() && !hasDuplicatePKs) {
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

        std::vector<std::string> pkColumns =
            getPrimaryKeyColumns(dbc, table.schema_name, table.table_name);

        std::string allColumnsQuery =
            "SELECT c.name FROM sys.columns c "
            "INNER JOIN sys.tables t ON c.object_id = t.object_id "
            "INNER JOIN sys.schemas s ON t.schema_id = s.schema_id "
            "WHERE s.name = '" +
            escapeSQL(table.schema_name) + "' AND t.name = '" +
            escapeSQL(table.table_name) + "' ORDER BY c.column_id";
        std::vector<std::vector<std::string>> allColumns =
            executeQueryMSSQL(dbc, allColumnsQuery);

        if (allColumns.empty()) {
          Logger::warning(
              LogCategory::TRANSFER, "setupTableTargetMSSQLToPostgres",
              "No columns found for " + table.schema_name + "." +
                  table.table_name + " - skipping trigger creation");
          closeMSSQLConnection(dbc);
          continue;
        }

        bool hasPK = !pkColumns.empty();
        std::string jsonObjectNew;
        std::string jsonObjectOld;

        if (hasPK) {
          jsonObjectNew = "CONCAT('{', ";
          for (size_t i = 0; i < pkColumns.size(); ++i) {
            if (i > 0) {
              jsonObjectNew += ", ',', ";
            }
            jsonObjectNew +=
                "'\"" + pkColumns[i] + "\":', " + "CASE WHEN INSERTED.[" +
                pkColumns[i] + "] IS NULL THEN 'null' ELSE CONCAT('\"', " +
                "REPLACE(REPLACE(REPLACE(CAST(INSERTED.[" + pkColumns[i] +
                "] AS NVARCHAR(MAX)), '\\', '\\\\'), '\"', '\\\"'), CHAR(13) + "
                "CHAR(10), '\\n'), '\"') END";
          }
          jsonObjectNew += ", '}')";

          jsonObjectOld = "CONCAT('{', ";
          for (size_t i = 0; i < pkColumns.size(); ++i) {
            if (i > 0) {
              jsonObjectOld += ", ',', ";
            }
            jsonObjectOld +=
                "'\"" + pkColumns[i] + "\":', " + "CASE WHEN DELETED.[" +
                pkColumns[i] + "] IS NULL THEN 'null' ELSE CONCAT('\"', " +
                "REPLACE(REPLACE(REPLACE(CAST(DELETED.[" + pkColumns[i] +
                "] AS NVARCHAR(MAX)), '\\', '\\\\'), '\"', '\\\"'), CHAR(13) + "
                "CHAR(10), '\\n'), '\"') END";
          }
          jsonObjectOld += ", '}')";
        } else {
          std::string concatFieldsNew = "CONCAT(";
          std::string concatFieldsOld = "CONCAT(";
          for (size_t i = 0; i < allColumns.size(); ++i) {
            if (i > 0) {
              concatFieldsNew += ", '|', ";
              concatFieldsOld += ", '|', ";
            }
            std::string colName = allColumns[i][0];
            concatFieldsNew += "COALESCE(CAST(INSERTED.[" + colName +
                               "] AS NVARCHAR(MAX)), '')";
            concatFieldsOld += "COALESCE(CAST(DELETED.[" + colName +
                               "] AS NVARCHAR(MAX)), '')";
          }
          concatFieldsNew += ")";
          concatFieldsOld += ")";
          jsonObjectNew = "CONCAT('{\"_hash\":\"', CONVERT(NVARCHAR(32), "
                          "HASHBYTES('MD5', " +
                          concatFieldsNew + "), 2), '\"}')";
          jsonObjectOld = "CONCAT('{\"_hash\":\"', CONVERT(NVARCHAR(32), "
                          "HASHBYTES('MD5', " +
                          concatFieldsOld + "), 2), '\"}')";
        }

        std::string rowDataNew = "CONCAT('{', ";
        for (size_t i = 0; i < allColumns.size(); ++i) {
          if (i > 0) {
            rowDataNew += ", ',', ";
          }
          std::string colName = allColumns[i][0];
          rowDataNew += "'\"" + colName + "\":', " + "CASE WHEN INSERTED.[" +
                        colName + "] IS NULL THEN 'null' ELSE CONCAT('\"', " +
                        "REPLACE(REPLACE(REPLACE(CAST(INSERTED.[" + colName +
                        "] AS NVARCHAR(MAX)), '\\', '\\\\'), '\"', '\\\"'), "
                        "CHAR(13) + CHAR(10), '\\n'), '\"') END";
        }
        rowDataNew += ", '}')";

        std::string rowDataOld = "CONCAT('{', ";
        for (size_t i = 0; i < allColumns.size(); ++i) {
          if (i > 0) {
            rowDataOld += ", ',', ";
          }
          std::string colName = allColumns[i][0];
          rowDataOld += "'\"" + colName + "\":', " + "CASE WHEN DELETED.[" +
                        colName + "] IS NULL THEN 'null' ELSE CONCAT('\"', " +
                        "REPLACE(REPLACE(REPLACE(CAST(DELETED.[" + colName +
                        "] AS NVARCHAR(MAX)), '\\', '\\\\'), '\"', '\\\"'), "
                        "CHAR(13) + CHAR(10), '\\n'), '\"') END";
        }
        rowDataOld += ", '}')";

        std::string triggerInsert =
            "ds_tr_" + table.schema_name + "_" + table.table_name + "_ai";
        std::string triggerUpdate =
            "ds_tr_" + table.schema_name + "_" + table.table_name + "_au";
        std::string triggerDelete =
            "ds_tr_" + table.schema_name + "_" + table.table_name + "_ad";

        std::string dropInsert = "IF EXISTS (SELECT * FROM sys.triggers WHERE "
                                 "name = '" +
                                 triggerInsert + "') DROP TRIGGER [" +
                                 table.schema_name + "].[" + triggerInsert +
                                 "];";
        std::string dropUpdate = "IF EXISTS (SELECT * FROM sys.triggers WHERE "
                                 "name = '" +
                                 triggerUpdate + "') DROP TRIGGER [" +
                                 table.schema_name + "].[" + triggerUpdate +
                                 "];";
        std::string dropDelete = "IF EXISTS (SELECT * FROM sys.triggers WHERE "
                                 "name = '" +
                                 triggerDelete + "') DROP TRIGGER [" +
                                 table.schema_name + "].[" + triggerDelete +
                                 "];";

        executeQueryMSSQL(dbc, dropInsert);
        executeQueryMSSQL(dbc, dropUpdate);
        executeQueryMSSQL(dbc, dropDelete);

        std::string createInsertTrigger =
            "CREATE TRIGGER [" + table.schema_name + "].[" + triggerInsert +
            "] ON [" + table.schema_name + "].[" + table.table_name +
            "] AFTER INSERT AS BEGIN INSERT INTO "
            "datasync_metadata.ds_change_log "
            "(operation, schema_name, table_name, pk_values, row_data) "
            "SELECT 'I', '" +
            table.schema_name + "', '" + table.table_name + "', " +
            jsonObjectNew + ", " + rowDataNew + " FROM INSERTED; END;";

        std::string createUpdateTrigger =
            "CREATE TRIGGER [" + table.schema_name + "].[" + triggerUpdate +
            "] ON [" + table.schema_name + "].[" + table.table_name +
            "] AFTER UPDATE AS BEGIN INSERT INTO "
            "datasync_metadata.ds_change_log "
            "(operation, schema_name, table_name, pk_values, row_data) "
            "SELECT 'U', '" +
            table.schema_name + "', '" + table.table_name + "', " +
            jsonObjectNew + ", " + rowDataNew + " FROM INSERTED; END;";

        std::string createDeleteTrigger =
            "CREATE TRIGGER [" + table.schema_name + "].[" + triggerDelete +
            "] ON [" + table.schema_name + "].[" + table.table_name +
            "] AFTER DELETE AS BEGIN INSERT INTO "
            "datasync_metadata.ds_change_log "
            "(operation, schema_name, table_name, pk_values, row_data) "
            "SELECT 'D', '" +
            table.schema_name + "', '" + table.table_name + "', " +
            jsonObjectOld + ", " + rowDataOld + " FROM DELETED; END;";

        executeQueryMSSQL(dbc, createInsertTrigger);
        executeQueryMSSQL(dbc, createUpdateTrigger);
        executeQueryMSSQL(dbc, createDeleteTrigger);

        Logger::info(LogCategory::TRANSFER, "setupTableTargetMSSQLToPostgres",
                     "Created CDC triggers for " + table.schema_name + "." +
                         table.table_name +
                         (hasPK ? " (with PK)" : " (no PK, using hash)"));

        // Cerrar conexión para evitar "Connection is busy"
        closeMSSQLConnection(dbc);
      }
    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER, "setupTableTargetMSSQLToPostgres",
                    "Error in setupTableTargetMSSQLToPostgres: " +
                        std::string(e.what()));
    }
  }

  void transferDataMSSQLToPostgres() {
    Logger::info(LogCategory::TRANSFER,
                 "Starting MSSQL to PostgreSQL data transfer");

    try {
      pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());

      if (!pgConn.is_open()) {
        Logger::error(LogCategory::TRANSFER, "transferDataMSSQLToPostgres",
                      "CRITICAL ERROR: Cannot establish PostgreSQL connection "
                      "for MSSQL data transfer");
        return;
      }

      Logger::info(LogCategory::TRANSFER,
                   "PostgreSQL connection established for MSSQL data transfer");

      auto tables = getActiveTables(pgConn);

      if (tables.empty()) {
        Logger::info(LogCategory::TRANSFER,
                     "No active MSSQL tables found for data transfer");
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
                       " MSSQL tables in priority order");
      // Removed individual table status logs to reduce noise

      for (auto &table : tables) {
        if (table.db_engine != "MSSQL") {
          Logger::warning(
              LogCategory::TRANSFER,
              "Skipping non-MSSQL table in transfer: " + table.db_engine +
                  " - " + table.schema_name + "." + table.table_name);
          continue;
        }

        SQLHDBC dbc = getMSSQLConnection(table.connection_string);
        if (!dbc) {
          Logger::error(
              LogCategory::TRANSFER, "transferDataMSSQLToPostgres",
              "CRITICAL ERROR: Failed to get MSSQL connection for table " +
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

        // Usar USE [database] para cambiar el contexto de base de datos
        std::string databaseName = extractDatabaseName(table.connection_string);
        std::string useQuery = "USE [" + databaseName + "];";
        auto useResult = executeQueryMSSQL(dbc, useQuery);

        auto countRes =
            executeQueryMSSQL(dbc, "SELECT COUNT(*) FROM [" + schema_name +
                                       "].[" + table_name + "];");
        size_t sourceCount = 0;
        if (!countRes.empty() && !countRes[0][0].empty()) {
          try {
            sourceCount = std::stoul(countRes[0][0]);
            Logger::info(LogCategory::TRANSFER,
                         "MSSQL source table " + schema_name + "." +
                             table_name + " has " +
                             std::to_string(sourceCount) + " records");
          } catch (const std::exception &e) {
            Logger::error(LogCategory::TRANSFER, "transferDataMSSQLToPostgres",
                          "ERROR parsing source count for MSSQL table " +
                              schema_name + "." + table_name + ": " +
                              std::string(e.what()));
            sourceCount = 0;
          }
        } else {
          Logger::error(LogCategory::TRANSFER, "transferDataMSSQLToPostgres",
                        "ERROR: Could not get source count for MSSQL table " +
                            schema_name + "." + table_name +
                            " - count query returned no results");
        }

        // Obtener conteo de registros en la tabla destino

        std::string lowerTableNamePG = table_name;
        std::transform(lowerTableNamePG.begin(), lowerTableNamePG.end(),
                       lowerTableNamePG.begin(), ::tolower);
        std::string targetCountQuery = "SELECT COUNT(*) FROM \"" +
                                       lowerSchemaName + "\".\"" +
                                       lowerTableNamePG + "\";";
        size_t targetCount = 0;
        try {
          pqxx::work txn(pgConn);
          auto targetResult = txn.exec(targetCountQuery);
          if (!targetResult.empty()) {
            targetCount = targetResult[0][0].as<size_t>();
            Logger::info(LogCategory::TRANSFER,
                         "MSSQL target table " + lowerSchemaName + "." +
                             table_name + " has " +
                             std::to_string(targetCount) + " records");
          } else {
            Logger::error(LogCategory::TRANSFER, "transferDataMSSQLToPostgres",
                          "ERROR: MSSQL target count query returned no results "
                          "for table " +
                              lowerSchemaName + "." + table_name);
          }
          txn.commit();
        } catch (const std::exception &e) {
          Logger::error(LogCategory::TRANSFER, "transferDataMSSQLToPostgres",
                        "ERROR getting MSSQL target count for table " +
                            lowerSchemaName + "." + table_name + ": " +
                            std::string(e.what()));
        }

        // Lógica simple basada en counts reales
        if (sourceCount == 0) {
          if (targetCount == 0) {
            updateStatus(pgConn, schema_name, table_name, "NO_DATA", 0);
          } else {
            Logger::warning(
                LogCategory::TRANSFER,
                "Source has no data but target has " +
                    std::to_string(targetCount) + " records for table " +
                    schema_name + "." + table_name +
                    ". This might indicate source table is empty or filtered.");
            updateStatus(pgConn, schema_name, table_name, "NO_DATA",
                         targetCount);
          }
          continue;
        }

        // Para FULL_LOAD, forzar la inserción de todos los datos
        bool forceFullLoad = (table.status == "FULL_LOAD");

        // Si sourceCount = targetCount, verificar si hay cambios incrementales
        if (sourceCount == targetCount) {
          // Si es una tabla FULL_LOAD que se completó, marcarla como
          // LISTENING_CHANGES
          if (table.status == "FULL_LOAD") {
            Logger::info(LogCategory::TRANSFER,
                         "FULL_LOAD completed for " + schema_name + "." +
                             table_name +
                             " (source: " + std::to_string(sourceCount) +
                             ", target: " + std::to_string(targetCount) +
                             ") - marking as LISTENING_CHANGES");
            updateStatus(pgConn, schema_name, table_name, "LISTENING_CHANGES",
                         targetCount);

            // Cerrar conexión MSSQL antes de continuar
            if (dbc) {
              closeMSSQLConnection(dbc);
              dbc = nullptr;
            }
            continue;
          }

          updateStatus(pgConn, schema_name, table_name, "LISTENING_CHANGES",
                       sourceCount);

          // IMPORTANTE: NO continuar con el procesamiento de datos si los
          // counts coinciden Solo procesar DELETEs si es necesario y luego
          // cerrar la conexión
          // Removed synchronized table log to reduce noise

          // Cerrar conexión MSSQL antes de continuar
          if (dbc) {
            closeMSSQLConnection(dbc);
            dbc = nullptr;
          }
          continue;
        }

        // Obtener estrategia de PK antes de procesar deletes
        std::string pkStrategy =
            getPKStrategyFromCatalog(pgConn, schema_name, table_name);

        // Si sourceCount < targetCount, hay registros eliminados en el origen
        // Procesar DELETEs según la estrategia
        if (sourceCount < targetCount && !forceFullLoad) {
          Logger::info(LogCategory::TRANSFER,
                       "Detected " + std::to_string(targetCount - sourceCount) +
                           " deleted records in " + schema_name + "." +
                           table_name + " - processing deletes");

          {
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
                               lowerTableNamePG + "\" CASCADE;");
              truncateTxn.commit();

              updateStatus(pgConn, schema_name, table_name, "FULL_LOAD", 0);

              Logger::info(
                  LogCategory::TRANSFER,
                  "OFFSET table truncated and reset for full resync: " +
                      schema_name + "." + table_name);
            } catch (const std::exception &e) {
              Logger::error(
                  LogCategory::TRANSFER, "transferDataMSSQLToPostgres",
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
                            "\".\"" + lowerTableNamePG + "\";");
          countTxn.commit();
          targetCount = newTargetCount[0][0].as<int>();
          Logger::info(LogCategory::TRANSFER,
                       "After deletes: source=" + std::to_string(sourceCount) +
                           ", target=" + std::to_string(targetCount));
        }

        // Para FULL_LOAD, forzar la inserción de todos los datos
        if (forceFullLoad) {
          Logger::info(LogCategory::TRANSFER,
                       "FULL_LOAD mode: forcing data insertion for " +
                           schema_name + "." + table_name);
        }

        // Luego ejecutar la query sin prefijo de base de datos
        std::vector<std::vector<std::string>> columns = executeQueryMSSQL(
            dbc,
            "SELECT c.name AS COLUMN_NAME, tp.name AS DATA_TYPE, "
            "CASE WHEN c.is_nullable = 1 THEN 'YES' ELSE 'NO' END as "
            "IS_NULLABLE, "
            "CASE WHEN pk.column_id IS NOT NULL THEN 'YES' ELSE 'NO' END as "
            "IS_PRIMARY_KEY, "
            "c.max_length AS CHARACTER_MAXIMUM_LENGTH, "
            "c.precision AS NUMERIC_PRECISION, "
            "c.scale AS NUMERIC_SCALE "
            "FROM sys.columns c "
            "INNER JOIN sys.tables t ON c.object_id = t.object_id "
            "INNER JOIN sys.schemas s ON t.schema_id = s.schema_id "
            "INNER JOIN sys.types tp ON c.user_type_id = tp.user_type_id "
            "LEFT JOIN ( "
            "  SELECT ic.column_id, ic.object_id "
            "  FROM sys.indexes i "
            "  INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id "
            "AND i.index_id = ic.index_id "
            "  WHERE i.is_primary_key = 1 "
            ") pk ON c.column_id = pk.column_id AND t.object_id = pk.object_id "
            "WHERE s.name = '" +
                schema_name + "' AND t.name = '" + table_name +
                "' "
                "ORDER BY c.column_id;");

        if (columns.empty()) {
          Logger::error(LogCategory::TRANSFER, "transferDataMSSQLToPostgres",
                        "No columns found for table " + schema_name + "." +
                            table_name +
                            ". This indicates the table structure could not be "
                            "retrieved from MSSQL.");
          updateStatus(pgConn, schema_name, table_name, "ERROR");
          continue;
        }

        std::vector<std::string> columnNames;
        std::vector<std::string> columnTypes;
        std::vector<bool> columnNullable;

        for (const std::vector<std::string> &col : columns) {
          if (col.size() < 7)
            continue;

          std::string colName = col[0];
          std::transform(colName.begin(), colName.end(), colName.begin(),
                         ::tolower);
          columnNames.push_back(colName);

          std::string dataType = col[1];
          std::string maxLength = col[4];
          std::string numericPrecision = col[5];
          std::string numericScale = col[6];

          std::string pgType = "TEXT";
          if (dataType == "varchar" || dataType == "nvarchar") {
            pgType =
                (!maxLength.empty() && maxLength != "NULL" && maxLength != "-1")
                    ? "VARCHAR(" + maxLength + ")"
                    : "VARCHAR";
          } else if (dataType == "decimal" || dataType == "numeric") {
            // Para decimal/numeric, usar la precisión y escala de MSSQL
            if (!numericPrecision.empty() && numericPrecision != "NULL" &&
                !numericScale.empty() && numericScale != "NULL") {
              pgType = "NUMERIC(" + numericPrecision + "," + numericScale + ")";
            } else {
              pgType = "NUMERIC(18,4)";
            }
          } else if (dataTypeMap.count(dataType)) {
            pgType = dataTypeMap[dataType];
          }

          columnTypes.push_back(pgType);
          columnNullable.push_back(col[2] == "YES");
        }

        if (columnNames.empty()) {
          Logger::error(
              LogCategory::TRANSFER, "transferDataMSSQLToPostgres",
              "No valid column names found for table " + schema_name + "." +
                  table_name +
                  ". This indicates a problem with column metadata parsing.");
          updateStatus(pgConn, schema_name, table_name, "ERROR");
          continue;
        }

        if (table.status == "FULL_LOAD") {
          Logger::info(LogCategory::TRANSFER,
                       "Truncating table: " + lowerSchemaName + "." +
                           table_name);
          pqxx::work txn(pgConn);
          txn.exec("TRUNCATE TABLE \"" + lowerSchemaName + "\".\"" +
                   lowerTableNamePG + "\" CASCADE;");
          txn.commit();
        } else if (table.status == "RESET") {
          Logger::info(LogCategory::TRANSFER,
                       "Processing RESET table: " + schema_name + "." +
                           table_name);
          pqxx::work txn(pgConn);
          txn.exec("TRUNCATE TABLE \"" + lowerSchemaName + "\".\"" +
                   lowerTableNamePG + "\" CASCADE;");
          txn.commit();

          updateStatus(pgConn, schema_name, table_name, "FULL_LOAD", 0);
          continue;
        }

        std::vector<std::string> pkColumns =
            getPKColumnsFromCatalog(pgConn, schema_name, table_name);

        bool hasMoreData = forceFullLoad || (sourceCount > targetCount);
        size_t chunkNumber = 0;
        size_t lastProcessedOffset = 0;
        const size_t CHUNK_SIZE = SyncConfig::getChunkSize();

        while (hasMoreData) {
          chunkNumber++;

          executeQueryMSSQL(dbc, "USE [" + databaseName + "];");

          std::string selectQuery =
              "SELECT * FROM [" + schema_name + "].[" + table_name + "]";

          if (!pkColumns.empty()) {
            selectQuery += " ORDER BY ";
            for (size_t i = 0; i < pkColumns.size(); ++i) {
              if (i > 0)
                selectQuery += ", ";
              selectQuery += "[" + pkColumns[i] + "]";
            }
            selectQuery += " OFFSET " + std::to_string(lastProcessedOffset) +
                           " ROWS FETCH NEXT " + std::to_string(CHUNK_SIZE) +
                           " ROWS ONLY;";
          } else {
            selectQuery += " ORDER BY (SELECT 0) OFFSET " +
                           std::to_string(lastProcessedOffset) +
                           " ROWS FETCH NEXT " + std::to_string(CHUNK_SIZE) +
                           " ROWS ONLY;";
          }

          std::vector<std::vector<std::string>> results =
              executeQueryMSSQL(dbc, selectQuery);

          if (results.empty()) {
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

            std::stringstream csvData;
            for (const std::vector<std::string> &row : results) {
              if (row.size() != columnNames.size()) {
                continue;
              }

              for (size_t i = 0; i < row.size(); ++i) {
                if (i > 0)
                  csvData << "|";

                std::string value = row[i];
                if (value == "NULL" || value.empty()) {
                  csvData << "\\N";
                } else {
                  // Con pipe como delimitador, solo necesitamos escapar pipes
                  // en los datos
                  std::string escapedValue = value;
                  size_t pos = 0;
                  while ((pos = escapedValue.find("|", pos)) !=
                         std::string::npos) {
                    escapedValue.replace(pos, 1, "\\|");
                    pos += 2;
                  }
                  csvData << escapedValue;
                }
              }
              csvData << "\n";
              rowsInserted++;
            }

            if (rowsInserted > 0) {
              try {
                std::string lowerTableNameForInsert = table_name;
                std::transform(lowerTableNameForInsert.begin(),
                               lowerTableNameForInsert.end(),
                               lowerTableNameForInsert.begin(), ::tolower);
                // Para tablas sin PK, usar INSERT directo para evitar
                // duplicados
                if (pkStrategy != "PK") {
                  performBulkInsert(pgConn, results, columnNames, columnTypes,
                                    lowerSchemaName, lowerTableNameForInsert);
                } else {
                  performBulkUpsert(pgConn, results, columnNames, columnTypes,
                                    lowerSchemaName, lowerTableNameForInsert,
                                    schema_name);
                }
                Logger::info(LogCategory::TRANSFER,
                             "Successfully processed " +
                                 std::to_string(rowsInserted) + " rows for " +
                                 schema_name + "." + table_name);
              } catch (const std::exception &e) {
                std::string errorMsg = e.what();
                Logger::error(LogCategory::TRANSFER,
                              "transferDataMSSQLToPostgres",
                              "Bulk upsert failed: " + errorMsg);

                // CRITICAL: Check for transaction abort errors that cause
                // infinite loops
                if (errorMsg.find("current transaction is aborted") !=
                        std::string::npos ||
                    errorMsg.find("previously aborted") != std::string::npos ||
                    errorMsg.find("aborted transaction") != std::string::npos) {
                  Logger::error(LogCategory::TRANSFER,
                                "transferDataMSSQLToPostgres",
                                "CRITICAL: Transaction abort detected - "
                                "breaking loop to prevent infinite hang");
                  hasMoreData = false;
                  break;
                }

                rowsInserted = 0;
              }
            }

          } catch (const std::exception &e) {
            std::string errorMsg = e.what();
            Logger::error(LogCategory::TRANSFER, "transferDataMSSQLToPostgres",
                          "Error processing data: " + errorMsg);

            // CRITICAL: Check for critical errors that require breaking the
            // loop
            if (errorMsg.find("current transaction is aborted") !=
                    std::string::npos ||
                errorMsg.find("previously aborted") != std::string::npos ||
                errorMsg.find("aborted transaction") != std::string::npos ||
                errorMsg.find("connection") != std::string::npos ||
                errorMsg.find("timeout") != std::string::npos) {
              Logger::error(LogCategory::TRANSFER,
                            "transferDataMSSQLToPostgres",
                            "CRITICAL: Critical error detected - breaking loop "
                            "to prevent infinite hang");
              hasMoreData = false;
              break;
            }
          }

          targetCount += rowsInserted;
          lastProcessedOffset += results.size();

          if (rowsInserted == 0 && !results.empty()) {
            targetCount += 1;
            Logger::info(LogCategory::TRANSFER,
                         "COPY failed, skipping problematic record for " +
                             schema_name + "." + table_name);
          }

          if (results.size() < CHUNK_SIZE || targetCount >= sourceCount) {
            hasMoreData = false;
          }
        }

        if (targetCount > 0) {
          Logger::info(LogCategory::TRANSFER,
                       "Table " + schema_name + "." + table_name +
                           " synchronized - LISTENING_CHANGES");
          updateStatus(pgConn, schema_name, table_name, "LISTENING_CHANGES",
                       targetCount);
        }

        // Cerrar conexión MSSQL con verificación
        if (dbc) {
          closeMSSQLConnection(dbc);
          dbc = nullptr;
        }
      }
    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER, "transferDataMSSQLToPostgres",
                    "Error in transferDataMSSQLToPostgres: " +
                        std::string(e.what()));
    }
  }

  // HYBRID PARALLEL PROCESSING METHODS
  void transferDataMSSQLToPostgresParallel() {
    Logger::info(LogCategory::TRANSFER,
                 "Starting HYBRID PARALLEL MSSQL to PostgreSQL data transfer");

    try {
      startParallelProcessing();

      pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());

      if (!pgConn.is_open()) {
        Logger::error(LogCategory::TRANSFER,
                      "transferDataMSSQLToPostgresParallel",
                      "CRITICAL ERROR: Cannot establish PostgreSQL connection "
                      "for parallel MSSQL data transfer");
        shutdownParallelProcessing();
        return;
      }

      Logger::info(LogCategory::TRANSFER, "PostgreSQL connection established "
                                          "for parallel MSSQL data transfer");

      auto tables = getActiveTables(pgConn);

      if (tables.empty()) {
        Logger::info(LogCategory::TRANSFER,
                     "No active MSSQL tables found for parallel data transfer");
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

      Logger::info(LogCategory::TRANSFER,
                   "Processing " + std::to_string(tables.size()) +
                       " MSSQL tables in HYBRID parallel mode");

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
        if (table.db_engine != "MSSQL") {
          Logger::warning(LogCategory::TRANSFER,
                          "Skipping non-MSSQL table in parallel transfer: " +
                              table.db_engine + " - " + table.schema_name +
                              "." + table.table_name);
          skipped++;
          continue;
        }

        try {
          MSSQLEngine engine(table.connection_string);
          std::vector<ColumnInfo> sourceColumns =
              engine.getTableColumns(table.schema_name, table.table_name);

          if (!sourceColumns.empty()) {
            SchemaSync::syncSchema(pgConn, table.schema_name, table.table_name,
                                   sourceColumns, "MSSQL");
          }
        } catch (const std::exception &e) {
          Logger::warning(
              LogCategory::TRANSFER, "transferDataMSSQLToPostgresParallel",
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
                       " MSSQL tables to thread pool (skipped " +
                       std::to_string(skipped) + ")");

      pool.waitForCompletion();

      Logger::info(LogCategory::TRANSFER,
                   "Thread pool completed - Completed: " +
                       std::to_string(pool.completedTasks()) +
                       " | Failed: " + std::to_string(pool.failedTasks()));

      shutdownParallelProcessing();

      Logger::info(LogCategory::TRANSFER,
                   "HYBRID PARALLEL MSSQL to PostgreSQL data "
                   "transfer completed successfully");
    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER,
                    "transferDataMSSQLToPostgresParallel",
                    "CRITICAL ERROR in transferDataMSSQLToPostgresParallel: " +
                        std::string(e.what()) +
                        " - Parallel MSSQL data transfer completely failed");
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
                      "processTableParallelWithConnection",
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

      SQLHDBC mssqlConn = getMSSQLConnection(table.connection_string);
      if (!mssqlConn) {
        Logger::error(LogCategory::TRANSFER, "processTableParallel",
                      "Failed to get MSSQL connection for parallel processing");
        updateStatus(pgConn, table.schema_name, table.table_name, "ERROR");
        return;
      }

      // Get table metadata
      std::string query =
          "SELECT c.name AS COLUMN_NAME, tp.name AS DATA_TYPE, "
          "CASE WHEN c.is_nullable = 1 THEN 'YES' ELSE 'NO' END as "
          "IS_NULLABLE, "
          "CASE WHEN pk.column_id IS NOT NULL THEN 'YES' ELSE 'NO' END as "
          "IS_PRIMARY_KEY, "
          "c.max_length AS CHARACTER_MAXIMUM_LENGTH, "
          "c.precision AS NUMERIC_PRECISION, "
          "c.scale AS NUMERIC_SCALE "
          "FROM sys.columns c "
          "INNER JOIN sys.tables t ON c.object_id = t.object_id "
          "INNER JOIN sys.schemas s ON t.schema_id = s.schema_id "
          "INNER JOIN sys.types tp ON c.user_type_id = tp.user_type_id "
          "LEFT JOIN ( "
          "  SELECT ic.column_id, ic.object_id "
          "  FROM sys.indexes i "
          "  INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id "
          "AND i.index_id = ic.index_id "
          "  WHERE i.is_primary_key = 1 "
          ") pk ON c.column_id = pk.column_id AND t.object_id = pk.object_id "
          "WHERE s.name = '" +
          table.schema_name + "' AND t.name = '" + table.table_name +
          "' "
          "ORDER BY c.column_id;";

      std::vector<std::vector<std::string>> columns =
          executeQueryMSSQL(mssqlConn, query);

      if (columns.empty()) {
        Logger::error(LogCategory::TRANSFER, "processTableParallel",
                      "No columns found for table " + table.schema_name + "." +
                          table.table_name);
        updateStatus(pgConn, table.schema_name, table.table_name, "ERROR");
        closeMSSQLConnection(mssqlConn);
        return;
      }

      std::vector<std::string> columnNames;
      std::vector<std::string> columnTypes;

      for (const std::vector<std::string> &col : columns) {
        if (col.size() < 7)
          continue;

        std::string colName = col[0];
        std::transform(colName.begin(), colName.end(), colName.begin(),
                       ::tolower);
        columnNames.push_back(colName);

        std::string dataType = col[1];
        std::string maxLength = col[4];
        std::string numericPrecision = col[5];
        std::string numericScale = col[6];

        std::string pgType = "TEXT";
        if (dataType == "varchar" || dataType == "nvarchar") {
          pgType =
              (!maxLength.empty() && maxLength != "NULL" && maxLength != "-1")
                  ? "VARCHAR(" + maxLength + ")"
                  : "VARCHAR";
        } else if (dataType == "decimal" || dataType == "numeric") {
          if (!numericPrecision.empty() && numericPrecision != "NULL" &&
              !numericScale.empty() && numericScale != "NULL") {
            pgType = "NUMERIC(" + numericPrecision + "," + numericScale + ")";
          } else {
            pgType = "NUMERIC(18,4)";
          }
        } else if (dataTypeMap.count(dataType)) {
          pgType = dataTypeMap[dataType];
        }

        columnTypes.push_back(pgType);
      }

      std::string lowerSchemaName = table.schema_name;
      std::transform(lowerSchemaName.begin(), lowerSchemaName.end(),
                     lowerSchemaName.begin(), ::tolower);
      std::string lowerTableName = table.table_name;
      std::transform(lowerTableName.begin(), lowerTableName.end(),
                     lowerTableName.begin(), ::tolower);

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
              " AND table_name = " + checkTxn.quote(lowerTableName) + ")");
          checkTxn.commit();
          return !result.empty() && result[0][0].as<bool>();
        }();

        if (!tableExists) {
          if (columns.empty()) {
            Logger::error(LogCategory::TRANSFER, "processTableParallel",
                          "Cannot create table - no columns found for " +
                              table.schema_name + "." + table.table_name);
            closeMSSQLConnection(mssqlConn);
            updateStatus(pgConn, table.schema_name, table.table_name, "ERROR");
            removeTableProcessingState(tableKey);
            return;
          }

          std::string createQuery = "CREATE TABLE IF NOT EXISTS \"" +
                                    lowerSchemaName + "\".\"" + lowerTableName +
                                    "\" (";
          std::vector<std::string> primaryKeys;
          size_t validColumns = 0;

          for (size_t i = 0; i < columns.size(); ++i) {
            if (columns[i].size() < 7)
              continue;

            std::string colName = columns[i][0];
            if (colName.empty())
              continue;
            std::transform(colName.begin(), colName.end(), colName.begin(),
                           ::tolower);
            std::string dataType = columns[i][1];
            if (dataType.empty())
              dataType = "nvarchar";
            std::string isPrimaryKey =
                columns[i].size() > 3 ? columns[i][3] : "NO";
            std::string maxLength = columns[i].size() > 4 ? columns[i][4] : "";
            std::string numericPrecision =
                columns[i].size() > 5 ? columns[i][5] : "";
            std::string numericScale =
                columns[i].size() > 6 ? columns[i][6] : "";

            std::string pgType = "TEXT";
            if (dataType == "decimal" || dataType == "numeric") {
              if (!numericPrecision.empty() && numericPrecision != "NULL" &&
                  !numericScale.empty() && numericScale != "NULL") {
                pgType =
                    "NUMERIC(" + numericPrecision + "," + numericScale + ")";
              } else {
                pgType = "NUMERIC(18,4)";
              }
            } else if (dataType == "varchar" || dataType == "nvarchar") {
              pgType = (!maxLength.empty() && maxLength != "NULL" &&
                        maxLength != "-1")
                           ? "VARCHAR(" + maxLength + ")"
                           : "VARCHAR";
            } else if (dataType == "char" || dataType == "nchar") {
              pgType = "TEXT";
            } else if (dataTypeMap.count(dataType)) {
              pgType = dataTypeMap[dataType];
            }

            // Solo la PK debe ser NOT NULL, todas las demás columnas permiten
            // NULL
            std::string nullable = (isPrimaryKey == "YES") ? " NOT NULL" : "";
            createQuery += "\"" + colName + "\" " + pgType + nullable;
            if (isPrimaryKey == "YES")
              primaryKeys.push_back(colName);
            createQuery += ", ";
            validColumns++;
          }

          if (validColumns == 0) {
            Logger::error(LogCategory::TRANSFER, "processTableParallel",
                          "No valid columns to create table for " +
                              table.schema_name + "." + table.table_name);
            closeMSSQLConnection(mssqlConn);
            updateStatus(pgConn, table.schema_name, table.table_name, "ERROR");
            removeTableProcessingState(tableKey);
            return;
          }

          // Check for duplicate PKs and NULLs before creating table - if
          // duplicates or NULLs found, don't create PK
          bool hasDuplicatePKs = false;
          bool hasNullPKs = false;
          if (!primaryKeys.empty()) {
            try {
              // Get a sample of data to check for duplicates
              std::string sampleQuery = "SELECT TOP 1000 ";
              for (size_t i = 0; i < primaryKeys.size(); ++i) {
                if (i > 0)
                  sampleQuery += ", ";
                sampleQuery += "[" + primaryKeys[i] + "]";
              }
              sampleQuery += " FROM [" + table.schema_name + "].[" +
                             table.table_name + "]";

              std::vector<std::vector<std::string>> sampleData =
                  executeQueryMSSQL(mssqlConn, sampleQuery);
              std::set<std::string> seenPKs;

              bool hasNullPKs = false;
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
              if (hasNullPKs) {
                Logger::warning(LogCategory::TRANSFER, "processTableParallel",
                                "NULL values detected in PK columns for " +
                                    table.schema_name + "." + table.table_name +
                                    " - creating table without PK constraint");
              }
            } catch (const std::exception &e) {
              Logger::warning(
                  LogCategory::TRANSFER, "processTableParallel",
                  "Error checking for duplicate PKs: " + std::string(e.what()) +
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
          }
          createQuery += ");";

          pqxx::work createTxn(pgConn);
          createTxn.exec(createQuery);
          createTxn.commit();

          Logger::info(LogCategory::TRANSFER, "processTableParallel",
                       "Created table " + lowerSchemaName + "." +
                           lowerTableName);
        }

        try {
          MSSQLEngine engine(table.connection_string);
          std::vector<ColumnInfo> sourceColumns =
              engine.getTableColumns(table.schema_name, table.table_name);
          if (!sourceColumns.empty()) {
            SchemaSync::syncSchema(pgConn, table.schema_name, table.table_name,
                                   sourceColumns, "MSSQL");
          }
        } catch (const std::exception &e) {
          Logger::warning(LogCategory::TRANSFER, "processTableParallel",
                          "Error syncing schema for " + table.schema_name +
                              "." + table.table_name + ": " +
                              std::string(e.what()) + " - continuing");
        }
      }

      {
        auto tableExistsFinal = [&]() {
          pqxx::work checkTxn(pgConn);
          auto result = checkTxn.exec(
              "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE "
              "table_schema = " +
              checkTxn.quote(lowerSchemaName) +
              " AND table_name = " + checkTxn.quote(lowerTableName) + ")");
          checkTxn.commit();
          return !result.empty() && result[0][0].as<bool>();
        }();

        if (!tableExistsFinal) {
          Logger::error(LogCategory::TRANSFER, "processTableParallel",
                        "Table " + table.schema_name + "." + table.table_name +
                            " does not exist after schema sync - skipping");
          closeMSSQLConnection(mssqlConn);
          updateStatus(pgConn, table.schema_name, table.table_name, "ERROR");
          removeTableProcessingState(tableKey);
          return;
        }
      }

      size_t sourceCount = 0;
      {
        std::string databaseName = extractDatabaseName(table.connection_string);
        std::string useQuery = "USE [" + databaseName + "];";
        executeQueryMSSQL(mssqlConn, useQuery);

        auto countRes = executeQueryMSSQL(
            mssqlConn, "SELECT COUNT(*) FROM [" + table.schema_name + "].[" +
                           table.table_name + "];");
        if (!countRes.empty() && !countRes[0][0].empty()) {
          try {
            sourceCount = std::stoul(countRes[0][0]);
          } catch (const std::exception &e) {
            Logger::error(LogCategory::TRANSFER, "processTableParallel",
                          "Error parsing source count: " +
                              std::string(e.what()));
            sourceCount = 0;
          }
        }
      }

      size_t targetCount = 0;
      try {
        pqxx::work txn(pgConn);
        auto targetResult =
            txn.exec("SELECT COUNT(*) FROM \"" + lowerSchemaName + "\".\"" +
                     lowerTableName + "\";");
        if (!targetResult.empty()) {
          targetCount = targetResult[0][0].as<size_t>();
        }
        txn.commit();
      } catch (const std::exception &e) {
        Logger::error(LogCategory::TRANSFER, "processTableParallel",
                      "Error getting target count: " + std::string(e.what()));
        targetCount = 0;
      }

      if (table.status == "FULL_LOAD" || table.status == "RESET") {
        Logger::info(
            LogCategory::TRANSFER, "processTableParallel",
            "FULL_LOAD/RESET detected - performing mandatory truncate for " +
                table.schema_name + "." + table.table_name);

        try {
          auto tableExistsForTruncate = [&]() {
            pqxx::work checkTxn(pgConn);
            auto result = checkTxn.exec(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE "
                "table_schema = " +
                checkTxn.quote(lowerSchemaName) +
                " AND table_name = " + checkTxn.quote(lowerTableName) + ")");
            checkTxn.commit();
            return !result.empty() && result[0][0].as<bool>();
          }();

          if (tableExistsForTruncate) {
            pqxx::work txn(pgConn);
            txn.exec("TRUNCATE TABLE \"" + lowerSchemaName + "\".\"" +
                     lowerTableName + "\" CASCADE;");
            txn.commit();
          }

          std::string pkStrategy = getPKStrategyFromCatalog(
              pgConn, table.schema_name, table.table_name);

          {
            pqxx::work updateTxn(pgConn);
            if (pkStrategy == "CDC") {
              updateTxn.exec(
                  "UPDATE metadata.catalog SET sync_metadata = "
                  "COALESCE(sync_metadata, '{}'::jsonb) || "
                  "jsonb_build_object('last_change_id', 0) WHERE schema_name=" +
                  updateTxn.quote(table.schema_name) + " AND table_name=" +
                  updateTxn.quote(table.table_name) + " AND db_engine='MSSQL'");
              Logger::info(LogCategory::TRANSFER, "processTableParallel",
                           "Reset last_change_id for CDC table " +
                               table.schema_name + "." + table.table_name);
            }
            updateTxn.commit();
          }

          targetCount = 0;
        } catch (const std::exception &e) {
          Logger::error(LogCategory::TRANSFER, "processTableParallel",
                        "Error truncating table: " + std::string(e.what()));
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
        processTableCDC(tableKey, mssqlConn, table, pgConn, columnNames,
                        columnTypes);

        size_t finalCount = 0;
        try {
          pqxx::work txn(pgConn);
          auto res = txn.exec("SELECT COUNT(*) FROM \"" + lowerSchemaName +
                              "\".\"" + lowerTableName + "\";");
          if (!res.empty())
            finalCount = res[0][0].as<size_t>();
          txn.commit();
        } catch (const std::exception &e) {
          Logger::error(LogCategory::TRANSFER, "processTableParallel",
                        "Error getting final count for CDC table: " +
                            std::string(e.what()));
        }

        updateStatus(pgConn, table.schema_name, table.table_name,
                     "LISTENING_CHANGES", finalCount);
        closeMSSQLConnection(mssqlConn);
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
        closeMSSQLConnection(mssqlConn);
        removeTableProcessingState(tableKey);
        return;
      }

      if (sourceCount == targetCount && table.status != "FULL_LOAD") {
        Logger::info(LogCategory::TRANSFER, "processTableParallel",
                     "Counts match (" + std::to_string(sourceCount) + ") for " +
                         table.schema_name + "." + table.table_name);
        updateStatus(pgConn, table.schema_name, table.table_name,
                     "LISTENING_CHANGES", targetCount);
        closeMSSQLConnection(mssqlConn);
        removeTableProcessingState(tableKey);
        return;
      }

      Logger::info(LogCategory::TRANSFER, "processTableParallel",
                   "Proceeding with FULL_LOAD for " + table.schema_name + "." +
                       table.table_name);

      std::thread dataFetcher(&MSSQLToPostgres::dataFetcherThread, this,
                              tableKey, mssqlConn, table, columnNames,
                              columnTypes);

      // Start batch preparers
      std::vector<std::thread> batchPreparers;
      for (size_t i = 0; i < MAX_BATCH_PREPARERS; ++i) {
        batchPreparers.emplace_back(&MSSQLToPostgres::batchPreparerThread, this,
                                    columnNames, columnTypes);
      }

      // Start multiple batch inserters
      std::vector<std::thread> batchInserters;
      for (size_t i = 0; i < MAX_BATCH_INSERTERS; ++i) {
        batchInserters.emplace_back(&MSSQLToPostgres::batchInserterThread, this,
                                    std::ref(pgConn));
      }

      // Wait for data fetcher to complete
      if (dataFetcher.joinable()) {
        dataFetcher.join();
      }

      setTableProcessingState(tableKey, false);

      // Signal end of data to ALL preparers (one signal per preparer)
      for (size_t i = 0; i < MAX_BATCH_PREPARERS; ++i) {
        DataChunk lastChunk;
        lastChunk.isLastChunk = true;
        rawDataQueue.push(std::move(lastChunk));
      }

      // Wait for all batch preparers to complete
      for (auto &preparer : batchPreparers) {
        if (preparer.joinable()) {
          preparer.join();
        }
      }

      // Signal end of batches to inserters
      for (size_t i = 0; i < MAX_BATCH_INSERTERS; ++i) {
        PreparedBatch lastBatch;
        lastBatch.batchSize = 0;
        preparedBatchQueue.push(std::move(lastBatch));
      }

      // Wait for all batch inserters to complete
      for (auto &inserter : batchInserters) {
        if (inserter.joinable()) {
          inserter.join();
        }
      }

      size_t finalTargetCount = 0;
      try {
        pqxx::work txn(pgConn);
        auto res = txn.exec("SELECT COUNT(*) FROM \"" + lowerSchemaName +
                            "\".\"" + lowerTableName + "\";");
        if (!res.empty())
          finalTargetCount = res[0][0].as<size_t>();
        txn.commit();
      } catch (const std::exception &e) {
        Logger::error(LogCategory::TRANSFER, "processTableParallel",
                      "Error getting final target count: " +
                          std::string(e.what()));
        finalTargetCount = 0;
      }
      updateStatus(pgConn, table.schema_name, table.table_name,
                   "LISTENING_CHANGES", finalTargetCount);

      closeMSSQLConnection(mssqlConn);
      removeTableProcessingState(tableKey);

      Logger::info(LogCategory::TRANSFER,
                   "Parallel processing completed for table " + tableKey);

    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER, "processTableParallel",
                    "Error in parallel table processing: " +
                        std::string(e.what()));
      updateStatus(pgConn, table.schema_name, table.table_name, "ERROR");
      removeTableProcessingState(tableKey);
    }
  }

  void dataFetcherThread(const std::string &tableKey, SQLHDBC mssqlConn,
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

      std::string databaseName = extractDatabaseName(table.connection_string);
      bool hasMoreData = true;
      size_t lastProcessedOffset = 0;

      while (hasMoreData) {
        chunkNumber++;

        executeQueryMSSQL(mssqlConn, "USE [" + databaseName + "];");

        std::string selectQuery = "SELECT ";
        for (size_t i = 0; i < columnNames.size(); ++i) {
          if (i > 0)
            selectQuery += ", ";
          selectQuery += "[" + columnNames[i] + "]";
        }
        selectQuery +=
            " FROM [" + table.schema_name + "].[" + table.table_name + "]";

        if (!pkColumns.empty()) {
          selectQuery += " ORDER BY ";
          for (size_t i = 0; i < pkColumns.size(); ++i) {
            if (i > 0)
              selectQuery += ", ";
            selectQuery += "[" + pkColumns[i] + "]";
          }
        } else {
          selectQuery += " ORDER BY (SELECT 0)";
        }

        selectQuery += " OFFSET " + std::to_string(lastProcessedOffset) +
                       " ROWS FETCH NEXT " + std::to_string(CHUNK_SIZE) +
                       " ROWS ONLY;";

        Logger::info(LogCategory::TRANSFER, "dataFetcherThread",
                     "Executing query for chunk " +
                         std::to_string(chunkNumber) + " on " +
                         table.schema_name + "." + table.table_name);

        std::vector<std::vector<std::string>> results =
            executeQueryMSSQL(mssqlConn, selectQuery);

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
          Logger::error(LogCategory::TRANSFER, "dataFetcherThread",
                        "Error processing chunk: " + std::string(e.what()));
          hasMoreData = false;
        }
      }

      Logger::info(LogCategory::TRANSFER, "Data fetcher thread completed for " +
                                              table.schema_name + "." +
                                              table.table_name);

    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER, "dataFetcherThread",
                    "Error in data fetcher thread: " + std::string(e.what()));
    }
  }

  void batchPreparerThread(const std::vector<std::string> &columnNames,
                           const std::vector<std::string> &columnTypes) {
    Logger::info(LogCategory::TRANSFER, "Batch preparer thread started");

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

          // Build batch query
          std::string lowerSchemaName = chunk.schemaName;
          std::string lowerTableName = chunk.tableName;

          PreparedBatch preparedBatch;
          preparedBatch.chunkNumber = chunk.chunkNumber;
          preparedBatch.schemaName = chunk.schemaName;
          preparedBatch.tableName = chunk.tableName;
          preparedBatch.batchSize = batchEnd - batchStart;

          // Get actual PostgreSQL columns to filter out non-existent columns
          pqxx::connection pgConn(
              DatabaseConfig::getPostgresConnectionString());
          std::vector<std::string> pgColumns = getPrimaryKeyColumnsFromPostgres(
              pgConn, lowerSchemaName, lowerTableName);

          // Get all PostgreSQL columns, not just PK
          std::set<std::string> pgColumnSet;
          try {
            pqxx::work txn(pgConn);
            auto pgColResult =
                txn.exec("SELECT column_name FROM information_schema.columns "
                         "WHERE table_schema = " +
                         txn.quote(lowerSchemaName) +
                         " AND table_name = " + txn.quote(lowerTableName) +
                         " ORDER BY ordinal_position");
            txn.commit();
            for (const auto &row : pgColResult) {
              std::string colName = row[0].as<std::string>();
              std::transform(colName.begin(), colName.end(), colName.begin(),
                             ::tolower);
              pgColumnSet.insert(colName);
            }
          } catch (const std::exception &e) {
            Logger::warning(
                LogCategory::TRANSFER, "batchPreparerThread",
                "Error getting PostgreSQL columns, using all source columns: " +
                    std::string(e.what()));
          }

          // Filter columnNames to only include columns that exist in PostgreSQL
          std::vector<std::string> validColumnNames;
          std::vector<std::string> validColumnTypes;
          for (size_t i = 0; i < columnNames.size(); ++i) {
            if (pgColumnSet.empty() ||
                pgColumnSet.find(columnNames[i]) != pgColumnSet.end()) {
              validColumnNames.push_back(columnNames[i]);
              validColumnTypes.push_back(columnTypes[i]);
            }
          }

          if (validColumnNames.empty()) {
            Logger::warning(LogCategory::TRANSFER, "batchPreparerThread",
                            "No valid columns found for " + lowerSchemaName +
                                "." + lowerTableName + ", skipping batch");
            continue;
          }

          std::vector<std::string> pkColumns = getPrimaryKeyColumnsFromPostgres(
              pgConn, lowerSchemaName, lowerTableName);

          if (!pkColumns.empty()) {
            preparedBatch.batchQuery = buildUpsertQuery(
                validColumnNames, pkColumns, lowerSchemaName, lowerTableName);
          } else {
            // Simple INSERT
            std::string insertQuery = "INSERT INTO \"" + lowerSchemaName +
                                      "\".\"" + lowerTableName + "\" (";
            for (size_t i = 0; i < validColumnNames.size(); ++i) {
              if (i > 0)
                insertQuery += ", ";
              insertQuery += "\"" + validColumnNames[i] + "\"";
            }
            insertQuery += ") VALUES ";
            preparedBatch.batchQuery = insertQuery;
          }

          // Build VALUES clause - row data already contains only valid columns
          std::string valuesClause;
          size_t validRowsCount = 0;
          for (size_t i = batchStart; i < batchEnd; ++i) {
            const auto &row = chunk.rawData[i];
            if (row.size() != validColumnNames.size())
              continue;

            if (validRowsCount > 0)
              valuesClause += ", ";

            valuesClause += "(";
            for (size_t j = 0; j < row.size() && j < validColumnNames.size();
                 ++j) {
              if (j > 0)
                valuesClause += ", ";

              if (row[j].empty()) {
                valuesClause += "NULL";
              } else {
                std::string cleanValue =
                    cleanValueForPostgres(row[j], validColumnTypes[j]);
                if (cleanValue == "NULL") {
                  valuesClause += "NULL";
                } else {
                  valuesClause += "'" + escapeSQL(cleanValue) + "'";
                }
              }
            }
            valuesClause += ")";
            validRowsCount++;
          }

          if (validRowsCount > 0 && !valuesClause.empty()) {
            preparedBatch.batchQuery += valuesClause;
            if (!pkColumns.empty()) {
              preparedBatch.batchQuery +=
                  buildUpsertConflictClause(validColumnNames, pkColumns);
            }
            preparedBatch.batchQuery += ";";
            preparedBatch.batchSize = validRowsCount;

            preparedBatchQueue.push(std::move(preparedBatch));
          }
        }

        Logger::info(LogCategory::TRANSFER,
                     "Prepared batches for chunk " +
                         std::to_string(chunk.chunkNumber) + " (" +
                         std::to_string(chunk.rawData.size()) + " rows)");
      }

      Logger::info(LogCategory::TRANSFER, "Batch preparer thread completed");

    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER, "batchPreparerThread",
                    "Error in batch preparer thread: " + std::string(e.what()));
    }
  }

  void updateStatus(pqxx::connection &pgConn, const std::string &schema_name,
                    const std::string &table_name, const std::string &status,
                    size_t /* rowCount */ = 0) {
    try {
      pqxx::work txn(pgConn);

      std::string updateQuery =
          "UPDATE metadata.catalog SET status='" + status + "'";

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

  void processTableCDC(const std::string &tableKey, SQLHDBC mssqlConn,
                       const TableInfo &table, pqxx::connection &pgConn,
                       const std::vector<std::string> &columnNames,
                       const std::vector<std::string> &columnTypes);

  std::vector<std::string> getPrimaryKeyColumns(SQLHDBC mssqlConn,
                                                const std::string &schema_name,
                                                const std::string &table_name) {
    std::vector<std::string> pkColumns;

    // Validate input parameters
    if (!mssqlConn) {
      Logger::error(LogCategory::TRANSFER, "getPrimaryKeyColumns",
                    "MSSQL connection is null");
      return pkColumns;
    }

    if (schema_name.empty() || table_name.empty()) {
      Logger::error(LogCategory::TRANSFER, "getPrimaryKeyColumns",
                    "Schema name or table name is empty");
      return pkColumns;
    }

    std::string query =
        "SELECT c.name AS COLUMN_NAME "
        "FROM sys.columns c "
        "INNER JOIN sys.tables t ON c.object_id = t.object_id "
        "INNER JOIN sys.schemas s ON t.schema_id = s.schema_id "
        "INNER JOIN sys.index_columns ic ON c.object_id = ic.object_id AND "
        "c.column_id = ic.column_id "
        "INNER JOIN sys.indexes i ON ic.object_id = i.object_id AND "
        "ic.index_id = i.index_id "
        "WHERE s.name = '" +
        escapeSQL(schema_name) + "' AND t.name = '" + escapeSQL(table_name) +
        "' "
        "AND i.is_primary_key = 1 "
        "ORDER BY ic.key_ordinal;";

    std::vector<std::vector<std::string>> results =
        executeQueryMSSQL(mssqlConn, query);

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

  std::string extractDatabaseName(const std::string &connectionString) {
    std::istringstream ss(connectionString);
    std::string token;
    while (std::getline(ss, token, ';')) {
      auto pos = token.find('=');
      if (pos == std::string::npos)
        continue;
      std::string key = token.substr(0, pos);
      std::string value = token.substr(pos + 1);
      if (key == "DATABASE") {
        return value;
      }
    }
    return "master"; // fallback
  }

  std::vector<std::vector<std::string>>
  executeQueryMSSQL(SQLHDBC conn, const std::string &query) {
    std::vector<std::vector<std::string>> results;
    if (!conn) {
      Logger::error(LogCategory::TRANSFER, "executeQueryMSSQL",
                    "No valid MSSQL connection");
      return results;
    }

    SQLHSTMT stmt;
    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, conn, &stmt);
    if (ret != SQL_SUCCESS) {
      Logger::error(LogCategory::TRANSFER, "executeQueryMSSQL",
                    "SQLAllocHandle(STMT) failed");
      return results;
    }

    ret = SQLExecDirect(stmt, (SQLCHAR *)query.c_str(), SQL_NTS);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
      SQLCHAR sqlState[6];
      SQLCHAR errorMsg[SQL_MAX_MESSAGE_LENGTH];
      SQLINTEGER nativeError;
      SQLSMALLINT msgLen;

      SQLGetDiagRec(SQL_HANDLE_STMT, stmt, 1, sqlState, &nativeError, errorMsg,
                    sizeof(errorMsg), &msgLen);

      Logger::error(
          LogCategory::TRANSFER, "executeQueryMSSQL",
          "SQLExecDirect failed - SQLState: " + std::string((char *)sqlState) +
              ", NativeError: " + std::to_string(nativeError) + ", Error: " +
              std::string((char *)errorMsg) + ", Query: " + query);
      SQLFreeHandle(SQL_HANDLE_STMT, stmt);
      return results;
    }

    // Get number of columns
    SQLSMALLINT numCols;
    SQLNumResultCols(stmt, &numCols);

    // Fetch rows
    while (SQLFetch(stmt) == SQL_SUCCESS) {
      std::vector<std::string> row;
      for (SQLSMALLINT i = 1; i <= numCols; i++) {
        std::string value;
        char buffer[1024];
        SQLLEN len;
        ret = SQLGetData(stmt, i, SQL_C_CHAR, buffer, sizeof(buffer) - 1, &len);
        if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
          if (len == SQL_NULL_DATA) {
            row.push_back("NULL");
            continue;
          }
          if (len > 0 && len < static_cast<SQLLEN>(sizeof(buffer) - 1)) {
            buffer[len] = '\0';
            value = std::string(buffer, len);
          } else if (len >= static_cast<SQLLEN>(sizeof(buffer) - 1)) {
            buffer[sizeof(buffer) - 1] = '\0';
            value = std::string(buffer, sizeof(buffer) - 1);
            SQLLEN remainingLen = len - (sizeof(buffer) - 1);
            while (remainingLen > 0 &&
                   (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO)) {
              SQLLEN chunkRead;
              ret = SQLGetData(stmt, i, SQL_C_CHAR, buffer, sizeof(buffer) - 1,
                               &chunkRead);
              if (chunkRead > 0 &&
                  chunkRead < static_cast<SQLLEN>(sizeof(buffer) - 1)) {
                buffer[chunkRead] = '\0';
                value += std::string(buffer, chunkRead);
                remainingLen -= chunkRead;
              } else if (chunkRead >= static_cast<SQLLEN>(sizeof(buffer) - 1)) {
                buffer[sizeof(buffer) - 1] = '\0';
                value += std::string(buffer, sizeof(buffer) - 1);
                remainingLen -= (sizeof(buffer) - 1);
              } else {
                break;
              }
            }
          }
          row.push_back(value);
        } else {
          row.push_back("NULL");
        }
      }
      results.push_back(row);
    }

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    return results;
  }
};

#endif
