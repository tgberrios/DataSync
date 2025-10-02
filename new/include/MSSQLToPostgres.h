#ifndef MSSQLTOPOSTGRES_H
#define MSSQLTOPOSTGRES_H

#include "Config.h"
#include "catalog_manager.h"
#include "logger.h"

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <iostream>
#include <mutex>
#include <pqxx/pqxx>
#include <set>
#include <sql.h>
#include <sqlext.h>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

class MSSQLToPostgres {
private:
public:
  MSSQLToPostgres() = default;
  ~MSSQLToPostgres() = default;

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
      Logger::error(LogCategory::TRANSFER,
                    "Failed to allocate ODBC environment handle");
      return nullptr;
    }

    ret = SQLSetEnvAttr(tempEnv, SQL_ATTR_ODBC_VERSION,
                        (SQLPOINTER)SQL_OV_ODBC3, 0);
    if (!SQL_SUCCEEDED(ret)) {
      SQLFreeHandle(SQL_HANDLE_ENV, tempEnv);
      Logger::error(LogCategory::TRANSFER, "Failed to set ODBC version");
      return nullptr;
    }

    ret = SQLAllocHandle(SQL_HANDLE_DBC, tempEnv, &tempConn);
    if (!SQL_SUCCEEDED(ret)) {
      SQLFreeHandle(SQL_HANDLE_ENV, tempEnv);
      Logger::error(LogCategory::TRANSFER,
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
      Logger::error(LogCategory::TRANSFER,
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
          "status, last_offset, last_processed_pk, pk_strategy, "
          "pk_columns, candidate_columns, has_pk, table_size "
          "FROM metadata.catalog "
          "WHERE active=true AND db_engine='MSSQL' AND status != 'NO_DATA' "
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

      std::string createQuery = "CREATE ";
      if (uniqueness == "UNIQUE")
        createQuery += "UNIQUE ";
      createQuery += "INDEX IF NOT EXISTS \"" + indexName + "\" ON \"" +
                     lowerSchemaName + "\".\"" + table_name + "\" (\"" +
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
        Logger::error(LogCategory::TRANSFER,
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
              LogCategory::TRANSFER,
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
      Logger::error(LogCategory::TRANSFER,
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
        Logger::error(LogCategory::TRANSFER,
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
              LogCategory::TRANSFER,
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
            Logger::error(LogCategory::TRANSFER,
                          "ERROR parsing source count for MSSQL table " +
                              schema_name + "." + table_name + ": " +
                              std::string(e.what()));
            sourceCount = 0;
          }
        } else {
          Logger::error(LogCategory::TRANSFER,
                        "ERROR: Could not get source count for MSSQL table " +
                            schema_name + "." + table_name +
                            " - count query returned no results");
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
                         "MSSQL target table " + lowerSchemaName + "." +
                             table_name + " has " +
                             std::to_string(targetCount) + " records");
          } else {
            Logger::error(LogCategory::TRANSFER,
                          "ERROR: MSSQL target count query returned no results "
                          "for table " +
                              lowerSchemaName + "." + table_name);
          }
          txn.commit();
        } catch (const std::exception &e) {
          Logger::error(LogCategory::TRANSFER,
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

        // Si sourceCount = targetCount, verificar si hay cambios incrementales
        if (sourceCount == targetCount) {
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

        // Si sourceCount < targetCount, hay registros eliminados en el origen
        // Procesar DELETEs por Primary Key
        if (sourceCount < targetCount) {
          Logger::info(LogCategory::TRANSFER,
                       "Detected " + std::to_string(targetCount - sourceCount) +
                           " deleted records in " + schema_name + "." +
                           table_name + " - processing deletes");
          processDeletesByPrimaryKey(schema_name, table_name, dbc, pgConn);

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
          Logger::error(LogCategory::TRANSFER,
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
              LogCategory::TRANSFER,
              "No valid column names found for table " + schema_name + "." +
                  table_name +
                  ". This indicates a problem with column metadata parsing.");
          updateStatus(pgConn, schema_name, table_name, "ERROR");
          continue;
        }

        if (table.status == "FULL_LOAD") {
          pqxx::work txn(pgConn);
          auto offsetCheck = txn.exec(
              "SELECT last_offset FROM metadata.catalog WHERE schema_name='" +
              escapeSQL(schema_name) + "' AND table_name='" +
              escapeSQL(table_name) + "';");
          txn.commit();

          bool shouldTruncate = true;
          if (!offsetCheck.empty() && !offsetCheck[0][0].is_null()) {
            std::string currentOffset = offsetCheck[0][0].as<std::string>();
            if (currentOffset != "0" && !currentOffset.empty()) {
              shouldTruncate = false;
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

        // OPTIMIZED: Usar cursor-based pagination con primary key
        std::string pkStrategy =
            getPKStrategyFromCatalog(pgConn, schema_name, table_name);
        std::vector<std::string> pkColumns =
            getPKColumnsFromCatalog(pgConn, schema_name, table_name);
        std::vector<std::string> candidateColumns =
            getCandidateColumnsFromCatalog(pgConn, schema_name, table_name);
        std::string lastProcessedPK =
            getLastProcessedPKFromCatalog(pgConn, schema_name, table_name);

        // Transferir datos faltantes usando CURSOR-BASED PAGINATION
        bool hasMoreData = true;
        size_t currentOffset = 0; // Usar variable separada para OFFSET
        size_t chunkNumber = 0;

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
          } else if (pkStrategy == "TEMPORAL_PK" && !candidateColumns.empty()) {
            // CURSOR-BASED PAGINATION: Usar columnas candidatas para paginación
            // eficiente
            if (!lastProcessedPK.empty()) {
              selectQuery += " WHERE [" + candidateColumns[0] + "] > '" +
                             escapeSQL(lastProcessedPK) + "'";
            }

            // Ordenar por la primera columna candidata
            selectQuery += " ORDER BY [" + candidateColumns[0] + "]";
            selectQuery += " OFFSET 0 ROWS FETCH NEXT " +
                           std::to_string(CHUNK_SIZE) + " ROWS ONLY;";
          } else {
            // FALLBACK: Usar OFFSET pagination para tablas sin PK
            selectQuery += " ORDER BY (SELECT NULL) OFFSET " +
                           std::to_string(currentOffset) + " ROWS FETCH NEXT " +
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
                performBulkUpsert(pgConn, results, columnNames, columnTypes,
                                  lowerSchemaName, table_name, schema_name);
                Logger::info(LogCategory::TRANSFER,
                             "Successfully processed " +
                                 std::to_string(rowsInserted) + " rows for " +
                                 schema_name + "." + table_name);
              } catch (const std::exception &e) {
                std::string errorMsg = e.what();
                Logger::error(LogCategory::TRANSFER,
                              "Bulk upsert failed: " + errorMsg);

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
            }

          } catch (const std::exception &e) {
            std::string errorMsg = e.what();
            Logger::error(LogCategory::TRANSFER,
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
                            "CRITICAL: Critical error detected - breaking loop "
                            "to prevent infinite hang");
              hasMoreData = false;
              break;
            }
          }

          // Always update targetCount, but only update currentOffset for non-PK
          // tables
          targetCount += rowsInserted;

          // Solo incrementar currentOffset para tablas sin PK (OFFSET
          // pagination) Para tablas con PK o TEMPORAL_PK se usa cursor-based
          // pagination con last_processed_pk
          if (pkStrategy != "PK" && pkStrategy != "TEMPORAL_PK") {
            currentOffset += rowsInserted;
          }

          // If COPY failed but we have data, advance the offset by 1
          if (rowsInserted == 0 && !results.empty()) {
            targetCount += 1; // Advance by 1 to skip the problematic record
            // Solo avanzar currentOffset para tablas sin PK ni TEMPORAL_PK
            if (pkStrategy != "PK" && pkStrategy != "TEMPORAL_PK") {
              currentOffset +=
                  1; // Advance OFFSET by 1 to skip the problematic record
            }
            Logger::info(LogCategory::TRANSFER,
                         "COPY failed, advancing offset by 1 to skip "
                         "problematic record for " +
                             schema_name + "." + table_name);
          }

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
              }
            } catch (const std::exception &e) {
              Logger::error(LogCategory::TRANSFER, "updateLastProcessedPK",
                            "Error updating last processed PK: " +
                                std::string(e.what()));
            }
          }

          // Update last_offset in database solo para tablas sin PK (OFFSET
          // pagination) Para tablas con PK o TEMPORAL_PK se usa
          // last_processed_pk en lugar de last_offset
          if (pkStrategy != "PK" && pkStrategy != "TEMPORAL_PK") {
            try {
              pqxx::work updateTxn(pgConn);
              updateTxn.exec("UPDATE metadata.catalog SET last_offset='" +
                             std::to_string(currentOffset) +
                             "' WHERE schema_name='" + escapeSQL(schema_name) +
                             "' AND table_name='" + escapeSQL(table_name) +
                             "';");
              updateTxn.commit();
            } catch (const std::exception &e) {
              Logger::warning(LogCategory::TRANSFER,
                              "Failed to update last_offset: " +
                                  std::string(e.what()));
            }
          }

          if (targetCount >= sourceCount) {
            hasMoreData = false;
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
      Logger::error(LogCategory::TRANSFER,
                    "Error in transferDataMSSQLToPostgres: " +
                        std::string(e.what()));
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
      std::string query = "SELECT MAX(\"" + lastSyncColumn + "\") FROM \"" +
                          schema_name + "\".\"" + table_name + "\";";

      pqxx::work txn(pgConn);
      auto result = txn.exec(query);
      txn.commit();

      if (!result.empty() && !result[0][0].is_null()) {
        return result[0][0].as<std::string>();
      }
    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER,
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
          // Verificar el tipo de columna antes de hacer el cast
          auto columnTypeCheck =
              txn.exec("SELECT data_type FROM information_schema.columns "
                       "WHERE table_schema='" +
                       schema_name + "' AND table_name='" + table_name +
                       "' AND column_name='" + lastSyncColumn + "';");

          if (!columnTypeCheck.empty()) {
            std::string columnType = columnTypeCheck[0][0].as<std::string>();
            if (columnType == "time without time zone") {
              // Para columnas TIME, usar NOW() en lugar de MAX()
              updateQuery += ", last_sync_time=NOW()";
            } else {
              // Para columnas TIMESTAMP/DATE, usar MAX con cast apropiado
              updateQuery += ", last_sync_time=(SELECT MAX(\"" +
                             lastSyncColumn + "\")::timestamp FROM \"" +
                             schema_name + "\".\"" + table_name + "\")";
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
          Logger::error(LogCategory::TRANSFER,
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
      Logger::error(LogCategory::TRANSFER, "Error processing deletes for " +
                                               schema_name + "." + table_name +
                                               ": " + std::string(e.what()));
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
        Logger::error(LogCategory::TRANSFER, "Could not get column names for " +
                                                 schema_name + "." +
                                                 table_name);
        return;
      }

      // 4. Procesar cada registro modificado
      size_t totalUpdated = 0;
      for (const auto &record : modifiedRecords) {
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
          std::string lowerPkColumn = pkColumns[i];
          std::transform(lowerPkColumn.begin(), lowerPkColumn.end(), lowerPkColumn.begin(), ::tolower);
          whereClause += "\"" + lowerPkColumn + "\" = " +
                         (record[pkIndex].empty()
                              ? "NULL"
                              : "'" + escapeSQL(record[pkIndex]) + "'");
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
          std::string valueToSet;
          if (newValue.empty()) {
            valueToSet = "NULL";
          } else {
            // Usar cleanValueForPostgres para manejar fechas inválidas y otros valores problemáticos
            // TODO: Necesitamos obtener el tipo real de la columna, por ahora usar TEXT como fallback
            std::string cleanedValue = cleanValueForPostgres(newValue, "TEXT");
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
    const size_t CHECK_BATCH_SIZE =
        std::min(SyncConfig::getChunkSize() / 2, static_cast<size_t>(500));

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
      Logger::error(LogCategory::TRANSFER,
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

  // CURSOR-BASED PAGINATION HELPER FUNCTIONS
  std::string getPKStrategyFromCatalog(pqxx::connection &pgConn,
                                       const std::string &schema_name,
                                       const std::string &table_name) {
    try {
      pqxx::work txn(pgConn);
      auto result = txn.exec(
          "SELECT pk_strategy FROM metadata.catalog WHERE schema_name='" +
          escapeSQL(schema_name) + "' AND table_name='" +
          escapeSQL(table_name) + "';");
      txn.commit();

      if (!result.empty() && !result[0][0].is_null()) {
        return result[0][0].as<std::string>();
      }
    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER,
                    "Error getting PK strategy: " + std::string(e.what()));
    }
    return "OFFSET";
  }

  std::vector<std::string>
  getPKColumnsFromCatalog(pqxx::connection &pgConn,
                          const std::string &schema_name,
                          const std::string &table_name) {
    try {
      pqxx::work txn(pgConn);
      auto result = txn.exec(
          "SELECT pk_columns FROM metadata.catalog WHERE schema_name='" +
          escapeSQL(schema_name) + "' AND table_name='" +
          escapeSQL(table_name) + "';");
      txn.commit();

      if (!result.empty() && !result[0][0].is_null()) {
        std::string pkColumnsJson = result[0][0].as<std::string>();
        return parseJSONArray(pkColumnsJson);
      }
    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER,
                    "Error getting PK columns: " + std::string(e.what()));
    }
    return {};
  }

  std::vector<std::string>
  getCandidateColumnsFromCatalog(pqxx::connection &pgConn,
                                 const std::string &schema_name,
                                 const std::string &table_name) {
    try {
      pqxx::work txn(pgConn);
      auto result = txn.exec(
          "SELECT candidate_columns FROM metadata.catalog WHERE schema_name='" +
          escapeSQL(schema_name) + "' AND table_name='" +
          escapeSQL(table_name) + "';");
      txn.commit();

      if (!result.empty() && !result[0][0].is_null()) {
        std::string candidateColumnsJson = result[0][0].as<std::string>();
        return parseJSONArray(candidateColumnsJson);
      }
    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER, "Error getting candidate columns: " +
                                               std::string(e.what()));
    }
    return {};
  }

  std::string getLastProcessedPKFromCatalog(pqxx::connection &pgConn,
                                            const std::string &schema_name,
                                            const std::string &table_name) {
    try {
      pqxx::work txn(pgConn);
      auto result = txn.exec(
          "SELECT last_processed_pk FROM metadata.catalog WHERE schema_name='" +
          escapeSQL(schema_name) + "' AND table_name='" +
          escapeSQL(table_name) + "';");
      txn.commit();

      if (!result.empty() && !result[0][0].is_null()) {
        return result[0][0].as<std::string>();
      }
    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER, "Error getting last processed PK: " +
                                               std::string(e.what()));
    }
    return "";
  }

  std::vector<std::string> parseJSONArray(const std::string &jsonArray) {
    std::vector<std::string> result;
    if (jsonArray.empty() || jsonArray == "[]")
      return result;

    std::string content = jsonArray;
    if (content.front() == '[')
      content = content.substr(1);
    if (content.back() == ']')
      content = content.substr(0, content.length() - 1);

    std::istringstream ss(content);
    std::string item;
    while (std::getline(ss, item, ',')) {
      item = item.substr(item.find_first_not_of(" \t\""));
      item = item.substr(0, item.find_last_not_of(" \t\"") + 1);
      if (!item.empty()) {
        result.push_back(item);
      }
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
               escapeSQL(table_name) + "';");
      txn.commit();
    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER,
                    "Error updating last processed PK: " +
                        std::string(e.what()));
    }
  }

  std::string
  getLastPKFromResults(const std::vector<std::vector<std::string>> &results,
                       const std::vector<std::string> &pkColumns,
                       const std::vector<std::string> &columnNames) {
    if (results.empty())
      return "";

    const auto &lastRow = results.back();
    std::string lastPK;

    for (size_t i = 0; i < pkColumns.size(); ++i) {
      if (i > 0)
        lastPK += "|";

      // Find the index of this PK column in the results
      size_t pkIndex = 0;
      for (size_t j = 0; j < columnNames.size(); ++j) {
        if (columnNames[j] == pkColumns[i]) {
          pkIndex = j;
          break;
        }
      }

      if (pkIndex < lastRow.size()) {
        lastPK += lastRow[pkIndex];
      }
    }

    return lastPK;
  }

  std::vector<std::string> parseLastPK(const std::string &lastPK) {
    std::vector<std::string> result;
    if (lastPK.empty())
      return result;

    std::istringstream ss(lastPK);
    std::string item;
    while (std::getline(ss, item, '|')) {
      if (!item.empty()) {
        result.push_back(item);
      }
    }
    return result;
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
          batchQuery += conflictClause;

          try {
            txn.exec(batchQuery);
            totalProcessed += values.size();
          } catch (const std::exception &e) {
            std::string errorMsg = e.what();

            // Detectar transacción abortada
            if (errorMsg.find("current transaction is aborted") !=
                    std::string::npos ||
                errorMsg.find("previously aborted") != std::string::npos) {
              Logger::warning(LogCategory::TRANSFER, "performBulkUpsert",
                              "Transaction aborted detected, processing batch "
                              "individually");

              // Procesar registros individualmente con nuevas transacciones
              for (size_t i = batchStart; i < batchEnd; ++i) {
                try {
                  const auto &row = results[i];
                  if (row.size() != columnNames.size())
                    continue;

                  std::string singleRowValues = "(";
                  for (size_t j = 0; j < row.size(); ++j) {
                    if (j > 0)
                      singleRowValues += ", ";

                    if (row[j] == "NULL" || row[j].empty()) {
                      singleRowValues += "NULL";
                    } else {
                      std::string cleanValue =
                          cleanValueForPostgres(row[j], columnTypes[j]);
                      singleRowValues += "'" + escapeSQL(cleanValue) + "'";
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

                } catch (const std::exception &singleError) {
                  Logger::error(
                      LogCategory::TRANSFER, "performBulkUpsert",
                      "Skipping problematic record: " +
                          std::string(singleError.what()).substr(0, 100));
                }
              }
            } else {
              // Re-lanzar otros errores
              throw;
            }
          }
        }
      }

      // Solo hacer commit si la transacción no fue abortada
      try {
        txn.commit();
      } catch (const std::exception &commitError) {
        if (std::string(commitError.what()).find("previously aborted") !=
                std::string::npos ||
            std::string(commitError.what()).find("aborted transaction") !=
                std::string::npos) {
          Logger::warning(LogCategory::TRANSFER, "performBulkUpsert",
                          "Skipping commit for aborted transaction");
        } else {
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

    // Detectar valores NULL de MSSQL - SIMPLIFICADO
    bool isNull =
        (cleanValue.empty() || cleanValue == "NULL" || cleanValue == "null" ||
         cleanValue == "\\N" || cleanValue == "\\0" || cleanValue == "0" ||
         cleanValue.find("0000-") != std::string::npos ||
         cleanValue.find("1900-01-01") != std::string::npos ||
         cleanValue.find("1970-01-01") != std::string::npos);

    // Limpiar caracteres de control y caracteres problemáticos
    for (char &c : cleanValue) {
      if (static_cast<unsigned char>(c) > 127 || c < 32) {
        isNull = true;
        break;
      }
    }

    // Para fechas, cualquier valor que no sea una fecha válida = NULL
    if (upperType.find("TIMESTAMP") != std::string::npos ||
        upperType.find("DATETIME") != std::string::npos ||
        upperType.find("DATE") != std::string::npos) {
      if (cleanValue.length() < 10 ||
          cleanValue.find("-") == std::string::npos ||
          cleanValue.find("0000") != std::string::npos) {
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
        // Fallback para TEXT: devolver NULL para que PostgreSQL use el valor por defecto de la columna
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
      } else if (upperType.find("BOOLEAN") != std::string::npos ||
                 upperType.find("BOOL") != std::string::npos) {
        return "false"; // Valor por defecto para booleanos
      } else {
        return "DEFAULT"; // Valor por defecto genérico
      }
    }

    // Limpiar caracteres de control restantes
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
      if (cleanValue == "0" || cleanValue == "false" || cleanValue == "FALSE") {
        cleanValue = "false";
      } else if (cleanValue == "1" || cleanValue == "true" ||
                 cleanValue == "TRUE") {
        cleanValue = "true";
      }
    }

    return cleanValue;
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
      Logger::error(LogCategory::TRANSFER, "No valid MSSQL connection");
      return results;
    }

    SQLHSTMT stmt;
    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, conn, &stmt);
    if (ret != SQL_SUCCESS) {
      Logger::error(LogCategory::TRANSFER, "SQLAllocHandle(STMT) failed");
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
          "SQLExecDirect failed - SQLState: " + std::string((char *)sqlState) +
          ", NativeError: " + std::to_string(nativeError) +
          ", Error: " + std::string((char *)errorMsg) + ", Query: " + query);
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

// Definición de variables estáticas
std::unordered_map<std::string, std::string> MSSQLToPostgres::dataTypeMap = {
    {"int", "INTEGER"},
    {"bigint", "BIGINT"},
    {"smallint", "SMALLINT"},
    {"tinyint", "SMALLINT"},
    {"bit", "BOOLEAN"},
    {"decimal", "NUMERIC"},
    {"numeric", "NUMERIC"},
    {"float", "REAL"},
    {"real", "REAL"},
    {"money", "NUMERIC(19,4)"},
    {"smallmoney", "NUMERIC(10,4)"},
    {"varchar", "VARCHAR"},
    {"nvarchar", "VARCHAR"},
    {"char", "CHAR"},
    {"nchar", "CHAR"},
    {"text", "TEXT"},
    {"ntext", "TEXT"},
    {"datetime", "TIMESTAMP"},
    {"datetime2", "TIMESTAMP"},
    {"smalldatetime", "TIMESTAMP"},
    {"date", "DATE"},
    {"time", "TIME"},
    {"datetimeoffset", "TIMESTAMP WITH TIME ZONE"},
    {"uniqueidentifier", "UUID"},
    {"varbinary", "BYTEA"},
    {"image", "BYTEA"},
    {"binary", "BYTEA"},
    {"xml", "TEXT"},
    {"sql_variant", "TEXT"}};

std::unordered_map<std::string, std::string> MSSQLToPostgres::collationMap = {
    {"SQL_Latin1_General_CP1_CI_AS", "en_US.utf8"},
    {"Latin1_General_CI_AS", "en_US.utf8"},
    {"SQL_Latin1_General_CP1_CS_AS", "C"},
    {"Latin1_General_CS_AS", "C"}};

#endif
