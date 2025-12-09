#ifndef MSSQLTOPOSTGRES_H
#define MSSQLTOPOSTGRES_H

#include "catalog/catalog_manager.h"
#include "engines/database_engine.h"
#include "sync/DatabaseToPostgresSync.h"
#include "sync/TableProcessorThreadPool.h"

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

using namespace ParallelProcessing;

class MSSQLToPostgres : public DatabaseToPostgresSync {
public:
  MSSQLToPostgres() = default;
  ~MSSQLToPostgres() { shutdownParallelProcessing(); }

  static std::unordered_map<std::string, std::string> dataTypeMap;
  static std::unordered_map<std::string, std::string> collationMap;

  std::string cleanValueForPostgres(const std::string &value,
                                    const std::string &columnType) override;

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
          "connection_string, last_sync_time, last_sync_column, "
          "status, last_processed_pk, pk_strategy, "
          "pk_columns, has_pk "
          "FROM metadata.catalog "
          "WHERE active=true AND db_engine='MSSQL' AND status != 'NO_DATA' "
          "ORDER BY schema_name, table_name;");
      txn.commit();

      for (const auto &row : results) {
        if (row.size() < 13)
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

    SQLHDBC dbc = getMSSQLConnection(connection_string);
    if (!dbc) {
      Logger::error(LogCategory::TRANSFER, "syncIndexesAndConstraints",
                    "Failed to get MSSQL connection");
      return;
    }

    std::string query = "SELECT i.name AS index_name, "
                        "CASE WHEN i.is_unique = 1 THEN 'UNIQUE' ELSE "
                        "'NON_UNIQUE' END AS uniqueness, "
                        "c.name AS column_name "
                        "FROM sys.indexes i "
                        "INNER JOIN sys.index_columns ic ON i.object_id = "
                        "ic.object_id AND i.index_id = ic.index_id "
                        "INNER JOIN sys.columns c ON ic.object_id = "
                        "c.object_id AND ic.column_id = c.column_id "
                        "INNER JOIN sys.tables t ON i.object_id = t.object_id "
                        "INNER JOIN sys.schemas s ON t.schema_id = s.schema_id "
                        "WHERE s.name = '" +
                        schema_name + "' AND t.name = '" + table_name +
                        "' "
                        "AND i.name IS NOT NULL AND i.is_primary_key = 0 "
                        "ORDER BY i.name, ic.key_ordinal;";

    std::vector<std::vector<std::string>> results =
        executeQueryMSSQL(dbc, query);

    for (const auto &row : results) {
      if (row.size() < 3)
        continue;

      std::string indexName = row[0];
      std::string uniqueness = row[1];
      std::string columnName = row[2];
      std::transform(columnName.begin(), columnName.end(), columnName.begin(),
                     ::tolower);
      std::transform(indexName.begin(), indexName.end(), indexName.begin(),
                     ::tolower);
      std::string lowerTableName = table_name;
      std::transform(lowerTableName.begin(), lowerTableName.end(),
                     lowerTableName.begin(), ::tolower);

      std::string createQuery = "CREATE ";
      if (uniqueness == "UNIQUE")
        createQuery += "UNIQUE ";
      createQuery += "INDEX IF NOT EXISTS \"" + indexName + "\" ON \"" +
                     lowerSchemaName + "\".\"" + lowerTableName + "\" (\"" +
                     columnName + "\");";

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

      for (const auto &table : tables) {
        if (table.db_engine != "MSSQL") {
          Logger::warning(LogCategory::TRANSFER,
                          "Skipping non-MSSQL table: " + table.db_engine +
                              " - " + table.schema_name + "." +
                              table.table_name);
          continue;
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

        // Usar USE [database] para cambiar el contexto de base de datos
        std::string databaseName = extractDatabaseName(table.connection_string);

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
          std::string nullable =
              ""; // Siempre permitir NULL en todas las columnas
          std::string isPrimaryKey = col[3];
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
            pgType = (!maxLength.empty() && maxLength != "NULL")
                         ? "CHAR(" + maxLength + ")"
                         : "CHAR(1)";
          } else if (dataTypeMap.count(dataType)) {
            pgType = dataTypeMap[dataType];
          }

          createQuery += "\"" + colName + "\" " + pgType + nullable;
          if (isPrimaryKey == "YES")
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

          // Para tablas que NO son FULL_LOAD, procesar cambios incrementales
          // Procesar UPDATEs si hay columna de tiempo y last_sync_time
          if (!table.last_sync_column.empty() &&
              !table.last_sync_time.empty()) {
            Logger::info(LogCategory::TRANSFER,
                         "Processing updates for " + schema_name + "." +
                             table_name +
                             " using time column: " + table.last_sync_column +
                             " since: " + table.last_sync_time);
            processUpdatesByPrimaryKey(schema_name, table_name, dbc, pgConn,
                                       table.last_sync_column,
                                       table.last_sync_time);
          }

          updateStatus(pgConn, schema_name, table_name, "LISTENING_CHANGES",
                       sourceCount);

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
                maxPKQuery += "[" + pkColumns[i] + "]";
              }
              maxPKQuery +=
                  " FROM [" + schema_name + "].[" + table_name + "] ORDER BY ";
              for (size_t i = 0; i < pkColumns.size(); ++i) {
                if (i > 0)
                  maxPKQuery += ", ";
                maxPKQuery += "[" + pkColumns[i] + "]";
              }
              maxPKQuery += " DESC OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY;";

              std::vector<std::vector<std::string>> maxPKResults =
                  executeQueryMSSQL(dbc, maxPKQuery);

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
                            "transferDataMSSQLToPostgres",
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

          if (pkStrategy == "PK") {
            // Para tablas con PK, usar eliminación por PK
            try {
              processDeletesByPrimaryKey(schema_name, table_name, dbc, pgConn);
              Logger::info(LogCategory::TRANSFER,
                           "Delete processing completed for " + schema_name +
                               "." + table_name);
            } catch (const std::exception &e) {
              Logger::error(
                  LogCategory::TRANSFER, "transferDataMSSQLToPostgres",
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

        // Para tablas OFFSET, si sourceCount > targetCount, hay nuevos INSERTs
        // Necesitamos re-sincronizar completamente para mantener el orden
        // correcto
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
                             lowerTableNamePG + "\" CASCADE;");
            truncateTxn.commit();

            // Actualizar status a FULL_LOAD para procesamiento completo
            updateStatus(pgConn, schema_name, table_name, "FULL_LOAD", 0);

            Logger::info(LogCategory::TRANSFER,
                         "OFFSET table truncated and reset for full resync due "
                         "to new INSERTs: " +
                             schema_name + "." + table_name);
          } catch (const std::exception &e) {
            Logger::error(LogCategory::TRANSFER, "transferDataMSSQLToPostgres",
                          "ERROR truncating OFFSET table for new INSERTs " +
                              schema_name + "." + table_name + ": " +
                              std::string(e.what()));
          }
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
        std::string lastProcessedPK =
            getLastProcessedPKFromCatalog(pgConn, schema_name, table_name);

        bool hasMoreData = forceFullLoad || (sourceCount > targetCount);
        size_t chunkNumber = 0;

        // CRITICAL: Add timeout to prevent infinite loops
        auto startTime = std::chrono::steady_clock::now();
        const auto MAX_PROCESSING_TIME = std::chrono::hours(24);

        while (hasMoreData) {
          chunkNumber++;
          const size_t CHUNK_SIZE = SyncConfig::getChunkSize();

          auto currentTime = std::chrono::steady_clock::now();

          // Asegurar que estamos en la base de datos correcta
          executeQueryMSSQL(dbc, "USE [" + databaseName + "];");

          std::string selectQuery =
              "SELECT * FROM [" + schema_name + "].[" + table_name + "]";

          if (pkStrategy == "PK" && !pkColumns.empty()) {
            // CURSOR-BASED PAGINATION: Usar PK para paginación eficiente
            if (!lastProcessedPK.empty()) {
              selectQuery += " WHERE ";
              std::vector<std::string> lastPKValues =
                  parseLastPK(lastProcessedPK);

              if (pkColumns.size() == 1) {
                // Single PK: simple comparison
                selectQuery += "[" + pkColumns[0] + "] > '" +
                               escapeSQL(lastPKValues[0]) + "'";
              } else {
                // Composite PK: lexicographic ordering
                selectQuery += "(";
                for (size_t i = 0; i < pkColumns.size(); ++i) {
                  if (i > 0)
                    selectQuery += " OR ";
                  selectQuery += "(";
                  for (size_t j = 0; j <= i; ++j) {
                    if (j > 0)
                      selectQuery += " AND ";
                    if (j == i) {
                      selectQuery += "[" + pkColumns[j] + "] > '" +
                                     escapeSQL(lastPKValues[j]) + "'";
                    } else {
                      selectQuery += "[" + pkColumns[j] + "] = '" +
                                     escapeSQL(lastPKValues[j]) + "'";
                    }
                  }
                  selectQuery += ")";
                }
                selectQuery += ")";
              }
            }

            selectQuery += " ORDER BY ";
            for (size_t i = 0; i < pkColumns.size(); ++i) {
              if (i > 0)
                selectQuery += ", ";
              selectQuery += "[" + pkColumns[i] + "]";
            }
            selectQuery += " OFFSET 0 ROWS FETCH NEXT " +
                           std::to_string(CHUNK_SIZE) + " ROWS ONLY;";
          } else {
            selectQuery += " ORDER BY (SELECT 0) OFFSET 0 ROWS FETCH NEXT " +
                           std::to_string(CHUNK_SIZE) + " ROWS ONLY;";
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
                // Para tablas sin PK, usar INSERT directo para evitar
                // duplicados
                if (pkStrategy != "PK") {
                  performBulkInsert(pgConn, results, columnNames, columnTypes,
                                    lowerSchemaName, table_name);
                } else {
                  performBulkUpsert(pgConn, results, columnNames, columnTypes,
                                    lowerSchemaName, table_name, schema_name);
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

          if (rowsInserted == 0 && !results.empty()) {
            targetCount += 1;
            Logger::info(LogCategory::TRANSFER,
                         "COPY failed, skipping problematic record for " +
                             schema_name + "." + table_name);
          }

          // OPTIMIZED: Update last_processed_pk for cursor-based pagination
          if ((pkStrategy == "PK" && !pkColumns.empty()) && !results.empty()) {
            try {
              // Obtener el último PK del chunk procesado
              std::string lastPK =
                  getLastPKFromResults(results, pkColumns, columnNames);
              if (!lastPK.empty()) {
                updateLastProcessedPK(pgConn, schema_name, table_name, lastPK);
              }
            } catch (const std::exception &e) {
              Logger::error(
                  LogCategory::TRANSFER, "transferDataMSSQLToPostgres",
                  "Error updating last processed PK: " + std::string(e.what()));
            }
          }

          // Para OFFSET pagination, verificar si hemos procesado todos los
          // registros
          if (pkStrategy != "PK") {
            if (results.size() < CHUNK_SIZE) {
              hasMoreData = false;
            }
          } else {
            // Para PK pagination, usar el conteo de target
            if (targetCount >= sourceCount) {
              hasMoreData = false;
            }
          }
        }

        if (targetCount > 0) {
          // Simplificado: Siempre usar LISTENING_CHANGES para sincronización
          // incremental
          Logger::info(LogCategory::TRANSFER,
                       "Table " + schema_name + "." + table_name +
                           " synchronized - LISTENING_CHANGES");
          updateStatus(pgConn, schema_name, table_name, "LISTENING_CHANGES",
                       targetCount);

          // OPTIMIZED: Update last_processed_pk for completed transfer
          if (pkStrategy == "PK" && !pkColumns.empty()) {
            try {
              // Obtener el último PK de la tabla para marcar como procesada
              std::string maxPKQuery = "SELECT ";
              for (size_t i = 0; i < pkColumns.size(); ++i) {
                if (i > 0)
                  maxPKQuery += ", ";
                maxPKQuery += "[" + pkColumns[i] + "]";
              }
              maxPKQuery +=
                  " FROM [" + schema_name + "].[" + table_name + "] ORDER BY ";
              for (size_t i = 0; i < pkColumns.size(); ++i) {
                if (i > 0)
                  maxPKQuery += ", ";
                maxPKQuery += "[" + pkColumns[i] + "]";
              }
              maxPKQuery += " DESC OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY;";

              std::vector<std::vector<std::string>> maxPKResults =
                  executeQueryMSSQL(dbc, maxPKQuery);

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
                                 " for completed table " + schema_name + "." +
                                 table_name);
              }
            } catch (const std::exception &e) {
              Logger::error(LogCategory::TRANSFER,
                            "transferDataMSSQLToPostgres",
                            "ERROR: Failed to update last_processed_pk for "
                            "completed table " +
                                schema_name + "." + table_name + ": " +
                                std::string(e.what()));
            }
          }
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
      pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());

      if (!pgConn.is_open()) {
        Logger::error(LogCategory::TRANSFER,
                      "transferDataMSSQLToPostgresParallel",
                      "CRITICAL ERROR: Cannot establish PostgreSQL connection "
                      "for parallel MSSQL data transfer");
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

    try {
      startParallelProcessing();

      setTableProcessingState(tableKey, true);

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

      // Start parallel data processing
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

      Logger::info(LogCategory::TRANSFER,
                   "Updating table status to LISTENING_CHANGES for " +
                       tableKey);
      updateStatus(pgConn, table.schema_name, table.table_name,
                   "LISTENING_CHANGES", 0);

      closeMSSQLConnection(mssqlConn);
      shutdownParallelProcessing();

      removeTableProcessingState(tableKey);

      Logger::info(LogCategory::TRANSFER,
                   "Parallel processing completed for table " + tableKey);

    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER, "processTableParallel",
                    "Error in parallel table processing: " +
                        std::string(e.what()));
      removeTableProcessingState(tableKey);
      shutdownParallelProcessing();
    }
  }

  void dataFetcherThread(const std::string &tableKey, SQLHDBC mssqlConn,
                         const TableInfo &table,
                         const std::vector<std::string> &columnNames,
                         const std::vector<std::string> &columnTypes) {
    Logger::info(LogCategory::TRANSFER,
                 "Data fetcher thread started for " + tableKey);

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

      while (isTableProcessingActive(tableKey)) {
        chunkNumber++;

        // Build select query
        std::string selectQuery = "SELECT * FROM [" + table.schema_name +
                                  "].[" + table.table_name + "]";

        if (pkStrategy == "PK" && !pkColumns.empty()) {
          if (!lastProcessedPK.empty()) {
            selectQuery += " WHERE ";
            std::vector<std::string> lastPKValues =
                parseLastPK(lastProcessedPK);
            if (!lastPKValues.empty()) {
              selectQuery += "[" + pkColumns[0] + "] > '" +
                             escapeSQL(lastPKValues[0]) + "'";
            }
          } else {
            // Initialize PK cursor by starting from the first record
            Logger::info(LogCategory::TRANSFER,
                         "Initializing PK cursor for " + table.schema_name +
                             "." + table.table_name +
                             " - starting from first record");
          }
          selectQuery += " ORDER BY [" + pkColumns[0] + "]";
          selectQuery += " OFFSET 0 ROWS FETCH NEXT " +
                         std::to_string(CHUNK_SIZE) + " ROWS ONLY;";
        } else {
          selectQuery += " ORDER BY (SELECT 0) OFFSET 0 ROWS FETCH NEXT " +
                         std::to_string(CHUNK_SIZE) + " ROWS ONLY;";
        }

        Logger::info(LogCategory::TRANSFER,
                     "Executing MSSQL query: " + selectQuery);

        // Fetch data
        std::vector<std::vector<std::string>> results =
            executeQueryMSSQL(mssqlConn, selectQuery);

        Logger::info(LogCategory::TRANSFER,
                     "Query returned " + std::to_string(results.size()) +
                         " rows for " + table.schema_name + "." +
                         table.table_name);

        if (results.empty()) {
          Logger::info(LogCategory::TRANSFER, "No more data available for " +
                                                  table.schema_name + "." +
                                                  table.table_name);
          break;
        }

        // Create data chunk
        DataChunk chunk;
        chunk.rawData = std::move(results);
        chunk.chunkNumber = chunkNumber;
        chunk.schemaName = table.schema_name;
        chunk.tableName = table.table_name;
        chunk.isLastChunk = false;

        // Push to queue (with timeout to prevent blocking)
        auto start = std::chrono::steady_clock::now();
        while (isTableProcessingActive(tableKey) &&
               std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::steady_clock::now() - start)
                       .count() < 5000) {
          if (rawDataQueue.size() < MAX_QUEUE_SIZE) {
            rawDataQueue.push(std::move(chunk));
            break;
          }
          std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        // Note: offset will be updated after saving to database

        // Update last_processed_pk for PK strategy tables
        if (pkStrategy == "PK" && !pkColumns.empty()) {
          if (!results.empty()) {
            try {
              std::string lastPK =
                  getLastPKFromResults(results, pkColumns, columnNames);
              if (!lastPK.empty()) {
                updateLastProcessedPK(pgConn, table.schema_name,
                                      table.table_name, lastPK);
                lastProcessedPK =
                    lastPK; // Update local variable for next iteration
                Logger::info(LogCategory::TRANSFER,
                             "Updated last_processed_pk to " + lastPK +
                                 " for table " + table.schema_name + "." +
                                 table.table_name);
              }
            } catch (const std::exception &e) {
              Logger::error(LogCategory::TRANSFER, "dataFetcherThread",
                            "Error updating last_processed_pk: " +
                                std::string(e.what()));
            }
          } else {
            // No new data, but still update to maintain state
            Logger::info(LogCategory::TRANSFER,
                         "No new data for PK table " + table.schema_name + "." +
                             table.table_name +
                             " - maintaining current last_processed_pk state");
          }
        }

        // Check if we got less data than expected (end of table)
        if (results.size() < CHUNK_SIZE) {
          break;
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
          std::transform(lowerSchemaName.begin(), lowerSchemaName.end(),
                         lowerSchemaName.begin(), ::tolower);
          std::string lowerTableName = chunk.tableName;
          std::transform(lowerTableName.begin(), lowerTableName.end(),
                         lowerTableName.begin(), ::tolower);

          std::string batchQuery = "INSERT INTO \"" + lowerSchemaName +
                                   "\".\"" + lowerTableName + "\" (";
          for (size_t i = 0; i < columnNames.size(); ++i) {
            if (i > 0)
              batchQuery += ", ";
            batchQuery += "\"" + columnNames[i] + "\"";
          }
          batchQuery += ") VALUES ";

          std::vector<std::string> values;
          for (size_t i = batchStart; i < batchEnd; ++i) {
            const auto &row = chunk.rawData[i];
            if (row.size() != columnNames.size())
              continue;

            std::string rowValues = "(";
            for (size_t j = 0; j < row.size(); ++j) {
              if (j > 0)
                rowValues += ", ";
              if (row[j] == "NULL" || row[j].empty()) {
                rowValues += "NULL";
              } else {
                std::string cleanValue =
                    cleanValueForPostgres(row[j], columnTypes[j]);
                rowValues += "'" + escapeSQL(cleanValue) + "'";
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
            batchQuery += ";";

            PreparedBatch preparedBatch;
            preparedBatch.batchQuery = batchQuery;
            preparedBatch.batchSize = values.size();
            preparedBatch.chunkNumber = chunk.chunkNumber;
            preparedBatch.schemaName = chunk.schemaName;
            preparedBatch.tableName = chunk.tableName;

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

  std::string getLastSyncTimeOptimized(pqxx::connection &pgConn,
                                       const std::string &schema_name,
                                       const std::string &table_name,
                                       const std::string &lastSyncColumn) {
    try {
      if (lastSyncColumn.empty()) {
        return "";
      }

      // Usar una consulta optimizada con índice en la columna de tiempo
      std::string lowerSchema = schema_name;
      std::transform(lowerSchema.begin(), lowerSchema.end(),
                     lowerSchema.begin(), ::tolower);
      std::string lowerTable = table_name;
      std::transform(lowerTable.begin(), lowerTable.end(), lowerTable.begin(),
                     ::tolower);
      std::string lowerColumn = lastSyncColumn;
      std::transform(lowerColumn.begin(), lowerColumn.end(),
                     lowerColumn.begin(), ::tolower);
      std::string query = "SELECT MAX(\"" + lowerColumn + "\") FROM \"" +
                          lowerSchema + "\".\"" + lowerTable + "\";";

      pqxx::work txn(pgConn);
      auto result = txn.exec(query);
      txn.commit();

      if (!result.empty() && !result[0][0].is_null()) {
        return result[0][0].as<std::string>();
      }
    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER, "getLastSyncTimeOptimized",
                    "Error getting last sync time: " + std::string(e.what()));
    }
    return "";
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
          // Verificar el tipo de columna antes de hacer el cast
          auto columnTypeCheck =
              txn.exec("SELECT data_type FROM information_schema.columns "
                       "WHERE table_schema='" +
                       lowerSchemaName + "' AND table_name='" + lowerTableName +
                       "' AND column_name='" + lowerLastSyncColumn + "';");

          if (!columnTypeCheck.empty()) {
            std::string columnType = columnTypeCheck[0][0].as<std::string>();
            if (columnType == "time without time zone") {
              updateQuery += ", last_sync_time=NOW()";
            } else {
              updateQuery += ", last_sync_time=(SELECT MAX(\"" +
                             lowerLastSyncColumn + "\")::timestamp FROM \"" +
                             lowerSchemaName + "\".\"" + lowerTableName + "\")";
            }
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

  void processDeletesByPrimaryKey(const std::string &schema_name,
                                  const std::string &table_name,
                                  SQLHDBC mssqlConn, pqxx::connection &pgConn) {
    try {
      std::string lowerSchemaName = schema_name;
      std::transform(lowerSchemaName.begin(), lowerSchemaName.end(),
                     lowerSchemaName.begin(), ::tolower);

      // 1. Obtener columnas de primary key
      std::vector<std::string> pkColumns =
          getPrimaryKeyColumns(mssqlConn, schema_name, table_name);

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
          Logger::error(LogCategory::TRANSFER, "processDeletesByPrimaryKey",
                        "Error getting PKs from PostgreSQL: " +
                            std::string(e.what()));
          break;
        }

        if (pgPKs.empty()) {
          break; // No more data
        }

        // 3. Verificar cuáles PKs no existen en MSSQL
        std::vector<std::vector<std::string>> deletedPKs =
            findDeletedPrimaryKeys(mssqlConn, schema_name, table_name, pgPKs,
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
      Logger::error(LogCategory::TRANSFER, "processDeletesByPrimaryKey",
                    "Error processing deletes for " + schema_name + "." +
                        table_name + ": " + std::string(e.what()));
    }
  }

  void processUpdatesByPrimaryKey(const std::string &schema_name,
                                  const std::string &table_name,
                                  SQLHDBC mssqlConn, pqxx::connection &pgConn,
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
          getPrimaryKeyColumns(mssqlConn, schema_name, table_name);

      if (pkColumns.empty()) {
        return;
      }

      Logger::info(LogCategory::TRANSFER,
                   "Processing updates for " + schema_name + "." + table_name +
                       " using time column: " + timeColumn +
                       " since: " + lastSyncTime);

      // 2. Obtener registros modificados desde MSSQL
      std::string selectQuery = "SELECT * FROM [" + schema_name + "].[" +
                                table_name + "] WHERE [" + timeColumn +
                                "] > '" + escapeSQL(lastSyncTime) +
                                "' ORDER BY [" + timeColumn + "]";

      auto modifiedRecords = executeQueryMSSQL(mssqlConn, selectQuery);

      if (modifiedRecords.empty()) {
        return;
      }

      // 3. Obtener nombres de columnas de MSSQL
      std::string columnQuery =
          "SELECT c.name AS COLUMN_NAME "
          "FROM sys.columns c "
          "INNER JOIN sys.tables t ON c.object_id = t.object_id "
          "INNER JOIN sys.schemas s ON t.schema_id = s.schema_id "
          "WHERE s.name = '" +
          escapeSQL(schema_name) + "' AND t.name = '" + escapeSQL(table_name) +
          "' ORDER BY c.column_id";

      auto columnNames = executeQueryMSSQL(mssqlConn, columnQuery);
      if (columnNames.empty() || columnNames[0].empty()) {
        Logger::error(LogCategory::TRANSFER, "processUpdatesByPrimaryKey",
                      "Could not get column names for " + schema_name + "." +
                          table_name);
        return;
      }

      // 4. Procesar cada registro modificado con límite de seguridad
      size_t totalUpdated = 0;
      size_t processedRecords = 0;
      const size_t MAX_PROCESSED_RECORDS =
          10000; // Límite adicional de seguridad

      for (const auto &record : modifiedRecords) {
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
            std::string colName = columnNames[j][0];
            std::transform(colName.begin(), colName.end(), colName.begin(),
                           ::tolower);
            if (colName == pkColumns[i]) {
              pkIndex = j;
              break;
            }
          }

          if (i > 0)
            whereClause += " AND ";
          whereClause += "\"" + pkColumns[i] + "\" = " +
                         (record[pkIndex].empty()
                              ? "NULL"
                              : "'" + escapeSQL(record[pkIndex]) + "'");
        }

        // Verificar si el registro existe en PostgreSQL
        std::string lowerTableNamePG = table_name;
        std::transform(lowerTableNamePG.begin(), lowerTableNamePG.end(),
                       lowerTableNamePG.begin(), ::tolower);
        std::string checkQuery = "SELECT COUNT(*) FROM \"" + lowerSchemaName +
                                 "\".\"" + lowerTableNamePG + "\" WHERE " +
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
                     "Updated " + std::to_string(totalUpdated) + " out of " +
                         std::to_string(processedRecords) +
                         " processed records in " + schema_name + "." +
                         table_name);
      } else {
        Logger::info(
            LogCategory::TRANSFER,
            "No updates needed for " + std::to_string(processedRecords) +
                " processed records in " + schema_name + "." + table_name);
      }

    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER, "processUpdatesByPrimaryKey",
                    "Error processing updates for " + schema_name + "." +
                        table_name + ": " + std::string(e.what()));
    }
  }

private:
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

  std::vector<std::vector<std::string>>
  findDeletedPrimaryKeys(SQLHDBC mssqlConn, const std::string &schema_name,
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

      // Construir query para verificar existencia en MSSQL
      std::string checkQuery = "SELECT ";
      for (size_t i = 0; i < pkColumns.size(); ++i) {
        if (i > 0)
          checkQuery += ", ";
        checkQuery += "[" + pkColumns[i] + "]";
      }
      checkQuery += " FROM [" + schema_name + "].[" + table_name + "] WHERE (";

      for (size_t i = batchStart; i < batchEnd; ++i) {
        if (i > batchStart)
          checkQuery += " OR ";
        checkQuery += "(";
        for (size_t j = 0; j < pkColumns.size(); ++j) {
          if (j > 0)
            checkQuery += " AND ";
          std::string value = pgPKs[i][j];
          if (value == "NULL") {
            checkQuery += "[" + pkColumns[j] + "] IS NULL";
          } else {
            checkQuery += "[" + pkColumns[j] + "] = '" + escapeSQL(value) + "'";
          }
        }
        checkQuery += ")";
      }
      checkQuery += ");";

      // Ejecutar query en MSSQL
      auto existingResults = executeQueryMSSQL(mssqlConn, checkQuery);

      // Crear set de PKs que SÍ existen en MSSQL
      std::set<std::vector<std::string>> existingPKs;
      for (const auto &row : existingResults) {
        std::vector<std::string> pkValues;
        for (size_t i = 0; i < pkColumns.size() && i < row.size(); ++i) {
          pkValues.push_back(row[i]);
        }
        existingPKs.insert(pkValues);
      }

      // Encontrar PKs que NO existen en MSSQL (deleted)
      for (size_t i = batchStart; i < batchEnd; ++i) {
        if (existingPKs.find(pgPKs[i]) == existingPKs.end()) {
          deletedPKs.push_back(pgPKs[i]);
        }
      }
    }

    return deletedPKs;
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
        char buffer[1024];
        SQLLEN len;
        ret = SQLGetData(stmt, i, SQL_C_CHAR, buffer, sizeof(buffer), &len);
        if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
          if (len == SQL_NULL_DATA) {
            row.push_back("NULL");
          } else {
            row.push_back(std::string(buffer, len));
          }
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
