#ifndef CATALOG_MANAGER_H
#define CATALOG_MANAGER_H

#include "Config.h"
#include "logger.h"

// ODBC handles structure
struct ODBCHandles {
  SQLHENV env;
  SQLHDBC dbc;
};
#include <algorithm>
#include <iostream>
#include <memory>
#include <mysql/mysql.h>
#include <pqxx/pqxx>
#include <set>
#include <sql.h>
#include <sqlext.h>
#include <sstream>
#include <string>
#include <vector>

class CatalogManager {
public:
  CatalogManager() = default;
  ~CatalogManager() = default;

  void cleanCatalog() {
    try {
      pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());

      // Limpiar tablas que no existen en PostgreSQL
      cleanNonExistentPostgresTables(pgConn);

      // Limpiar tablas que no existen en MariaDB
      cleanNonExistentMariaDBTables(pgConn);

      // Limpiar tablas que no existen en MSSQL
      cleanNonExistentMSSQLTables(pgConn);

      // Limpiar tablas huérfanas (sin conexión válida)
      cleanOrphanedTables(pgConn);

      // Actualizar cluster names después de la limpieza
      updateClusterNames();

      Logger::info("Catalog cleanup completed");
    } catch (const std::exception &e) {
      Logger::error("cleanCatalog",
                    "Error cleaning catalog: " + std::string(e.what()));
    }
  }

  void deactivateNoDataTables() {
    try {
      pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());

      pqxx::work txn(pgConn);

      // Contar tablas NO_DATA antes de desactivar
      auto countResult = txn.exec("SELECT COUNT(*) FROM metadata.catalog WHERE "
                                  "status = 'NO_DATA' AND active = true");
      int noDataCount = countResult[0][0].as<int>();

      if (noDataCount == 0) {
        txn.commit();
        return;
      }

      // Desactivar tablas NO_DATA
      auto updateResult =
          txn.exec("UPDATE metadata.catalog SET active = false WHERE status = "
                   "'NO_DATA' AND active = true");

      txn.commit();

      Logger::info("Deactivated " +
                   std::to_string(updateResult.affected_rows()) +
                   " NO_DATA tables");

    } catch (const std::exception &e) {
      Logger::error("deactivateNoDataTables",
                    "Error deactivating NO_DATA tables: " +
                        std::string(e.what()));
    }
  }

  void updateClusterNames() {
    try {
      pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());

      // Obtener todas las conexiones únicas que necesitan cluster_name
      pqxx::work txn(pgConn);
      auto results = txn.exec(
          "SELECT DISTINCT connection_string, db_engine FROM metadata.catalog "
          "WHERE (cluster_name IS NULL OR cluster_name = '') AND active = "
          "true");
      txn.commit();

      for (const auto &row : results) {
        std::string connectionString = row[0].as<std::string>();
        std::string dbEngine = row[1].as<std::string>();

        // Prefer resolving from the source engine itself; fallback to hostname
        // parsing
        std::string clusterName =
            resolveClusterName(connectionString, dbEngine);
        if (clusterName.empty()) {
          std::string hostname =
              extractHostnameFromConnection(connectionString, dbEngine);
          clusterName = getClusterNameFromHostname(hostname);
        }

        if (!clusterName.empty()) {
          // Actualizar cluster_name para todas las tablas con esta conexión
          pqxx::work updateTxn(pgConn);
          auto updateResult = updateTxn.exec(
              "UPDATE metadata.catalog SET cluster_name = '" +
              escapeSQL(clusterName) + "' WHERE connection_string = '" +
              escapeSQL(connectionString) + "' AND db_engine = '" +
              escapeSQL(dbEngine) + "'");
          updateTxn.commit();

          Logger::info("Updated cluster_name to '" + clusterName + "' for " +
                       std::to_string(updateResult.affected_rows()) +
                       " tables");
        }
      }

      Logger::info("Cluster name updates completed");
    } catch (const std::exception &e) {
      Logger::error("updateClusterNames",
                    "Error updating cluster names: " + std::string(e.what()));
    }
  }

  void syncCatalogMariaDBToPostgres() {
    try {
      pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());

      std::vector<std::string> mariaConnStrings;
      {
        pqxx::work txn(pgConn);
        auto results =
            txn.exec("SELECT connection_string FROM metadata.catalog "
                     "WHERE db_engine='MariaDB' AND active=true;");
        txn.commit();

        for (const auto &row : results) {
          if (row.size() >= 1) {
            mariaConnStrings.push_back(row[0].as<std::string>());
          }
        }
      }

      Logger::info("Found " + std::to_string(mariaConnStrings.size()) +
                   " MariaDB connections");
      if (mariaConnStrings.empty()) {
        Logger::warning("No MariaDB connections found in catalog");
        return;
      }

      for (const auto &connStr : mariaConnStrings) {
        {
          pqxx::work txn(pgConn);
          auto connectionCheck =
              txn.exec("SELECT COUNT(*) FROM metadata.catalog "
                       "WHERE connection_string='" +
                       escapeSQL(connStr) +
                       "' AND db_engine='MariaDB' AND active=true "
                       "AND last_sync_time > NOW() - INTERVAL '5 minutes';");
          txn.commit();

          if (!connectionCheck.empty() && !connectionCheck[0][0].is_null()) {
            int connectionCount = connectionCheck[0][0].as<int>();
            if (connectionCount > 0) {
              continue;
            }
          }
        }

        // Parse MariaDB connection string
        std::string host, user, password, db, port;
        std::istringstream ss(connStr);
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

        // Connect directly to MariaDB
        MYSQL *mariaConn = mysql_init(nullptr);
        if (!mariaConn) {
          Logger::error("syncCatalogMariaDBToPostgres", "mysql_init() failed");
          continue;
        }

        unsigned int portNum = 3306;
        if (!port.empty()) {
          try {
            portNum = std::stoul(port);
          } catch (...) {
            portNum = 3306;
          }
        }

        if (mysql_real_connect(mariaConn, host.c_str(), user.c_str(),
                               password.c_str(), db.c_str(), portNum, nullptr,
                               0) == nullptr) {
          Logger::error("syncCatalogMariaDBToPostgres",
                        "MariaDB connection failed: " +
                            std::string(mysql_error(mariaConn)));
          mysql_close(mariaConn);
          continue;
        }

        // Set connection timeouts for large tables
        {
          std::string timeoutQuery =
              "SET SESSION wait_timeout = " +
              std::to_string(SyncConfig::getConnectionTimeout()) +
              ", interactive_timeout = " +
              std::to_string(SyncConfig::getConnectionTimeout()) +
              ", net_read_timeout = 600" + ", net_write_timeout = 600";
          mysql_query(mariaConn, timeoutQuery.c_str());
        }

        std::string discoverQuery =
            "SELECT table_schema, table_name "
            "FROM information_schema.tables "
            "WHERE table_schema NOT IN ('information_schema', 'mysql', "
            "'performance_schema', 'sys') "
            "AND table_type = 'BASE TABLE' "
            "ORDER BY table_schema, table_name;";

        auto discoveredTables = executeQueryMariaDB(mariaConn, discoverQuery);

        for (const std::vector<std::string> &row : discoveredTables) {
          if (row.size() < 2)
            continue;

          std::string schemaName = row[0];
          std::string tableName = row[1];

          {
            // Detectar columna de tiempo con prioridad
            std::string timeColumn =
                detectTimeColumnMariaDB(mariaConn, schemaName, tableName);

            pqxx::work txn(pgConn);
            // Check if table already exists
            auto existingCheck =
                txn.exec("SELECT last_sync_column FROM metadata.catalog "
                         "WHERE schema_name='" +
                         escapeSQL(schemaName) + "' AND table_name='" +
                         escapeSQL(tableName) + "' AND db_engine='MariaDB';");

            if (!existingCheck.empty()) {
              // Table exists, only update if timeColumn changed
              std::string currentTimeColumn =
                  existingCheck[0][0].as<std::string>();
              if (currentTimeColumn != timeColumn) {
                txn.exec("UPDATE metadata.catalog SET last_sync_column = '" +
                         escapeSQL(timeColumn) + "' WHERE schema_name='" +
                         escapeSQL(schemaName) + "' AND table_name='" +
                         escapeSQL(tableName) + "' AND db_engine='MariaDB';");
              }
            } else {
              // New table, insert
              txn.exec("INSERT INTO metadata.catalog "
                       "(schema_name, table_name, cluster_name, db_engine, "
                       "connection_string, "
                       "last_sync_time, last_sync_column, status, "
                       "last_offset, active) "
                       "VALUES ('" +
                       escapeSQL(schemaName) + "', '" + escapeSQL(tableName) +
                       "', '', 'MariaDB', '" + escapeSQL(connStr) +
                       "', NOW(), '" + escapeSQL(timeColumn) +
                       "', 'PENDING', '0', false);");
            }
            txn.commit();
          }
        }

        // Close MariaDB connection
        mysql_close(mariaConn);
      }

      // Actualizar cluster names después de la sincronización
      updateClusterNames();
    } catch (const std::exception &e) {
    }
  }

  void syncCatalogMSSQLToPostgres() {
    try {
      pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());

      std::vector<std::string> mssqlConnStrings;
      {
        pqxx::work txn(pgConn);
        auto results =
            txn.exec("SELECT connection_string FROM metadata.catalog "
                     "WHERE db_engine='MSSQL' AND active=true;");
        txn.commit();

        for (const auto &row : results) {
          if (row.size() >= 1) {
            mssqlConnStrings.push_back(row[0].as<std::string>());
          }
        }
      }

      Logger::info("Found " + std::to_string(mssqlConnStrings.size()) +
                   " MSSQL connections");
      if (mssqlConnStrings.empty()) {
        Logger::warning("No MSSQL connections found in catalog");
        return;
      }

      for (const auto &connStr : mssqlConnStrings) {
        {
          pqxx::work txn(pgConn);
          auto connectionCheck =
              txn.exec("SELECT COUNT(*) FROM metadata.catalog "
                       "WHERE connection_string='" +
                       escapeSQL(connStr) +
                       "' AND db_engine='MSSQL' AND active=true "
                       "AND last_sync_time > NOW() - INTERVAL '5 minutes';");
          txn.commit();

          if (!connectionCheck.empty() && !connectionCheck[0][0].is_null()) {
            int connectionCount = connectionCheck[0][0].as<int>();
            if (connectionCount > 0) {
              continue;
            }
          }
        }

        // Connect directly to MSSQL using ODBC
        SQLHENV env;
        SQLHDBC dbc;
        SQLRETURN ret;

        ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
        if (!SQL_SUCCEEDED(ret)) {
          Logger::error("syncCatalogMSSQLToPostgres",
                        "Failed to allocate ODBC environment handle");
          continue;
        }

        ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION,
                            (SQLPOINTER)SQL_OV_ODBC3, 0);
        if (!SQL_SUCCEEDED(ret)) {
          SQLFreeHandle(SQL_HANDLE_ENV, env);
          Logger::error("syncCatalogMSSQLToPostgres",
                        "Failed to set ODBC version");
          continue;
        }

        ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
        if (!SQL_SUCCEEDED(ret)) {
          SQLFreeHandle(SQL_HANDLE_ENV, env);
          Logger::error("syncCatalogMSSQLToPostgres",
                        "Failed to allocate ODBC connection handle");
          continue;
        }

        SQLCHAR outConnStr[1024];
        SQLSMALLINT outConnStrLen;
        ret = SQLDriverConnect(dbc, nullptr, (SQLCHAR *)connStr.c_str(),
                               SQL_NTS, outConnStr, sizeof(outConnStr),
                               &outConnStrLen, SQL_DRIVER_NOPROMPT);
        if (!SQL_SUCCEEDED(ret)) {
          SQLCHAR sqlState[6], msg[SQL_MAX_MESSAGE_LENGTH];
          SQLINTEGER nativeError;
          SQLSMALLINT msgLen;
          SQLGetDiagRec(SQL_HANDLE_DBC, dbc, 1, sqlState, &nativeError, msg,
                        sizeof(msg), &msgLen);
          SQLFreeHandle(SQL_HANDLE_DBC, dbc);
          SQLFreeHandle(SQL_HANDLE_ENV, env);
          Logger::error("syncCatalogMSSQLToPostgres",
                        "Failed to connect to MSSQL: " +
                            std::string((char *)msg));
          continue;
        }

        std::string discoverQuery =
            "SELECT s.name AS table_schema, t.name AS table_name "
            "FROM sys.tables t "
            "INNER JOIN sys.schemas s ON t.schema_id = s.schema_id "
            "WHERE s.name NOT IN ('INFORMATION_SCHEMA', 'sys', 'guest') "
            "AND t.name NOT LIKE 'spt_%' "
            "AND t.name NOT LIKE 'MS%' "
            "AND t.name NOT LIKE 'sp_%' "
            "AND t.name NOT LIKE 'fn_%' "
            "AND t.name NOT LIKE 'xp_%' "
            "AND t.name NOT LIKE 'dt_%' "
            "ORDER BY s.name, t.name;";

        auto discoveredTables = executeQueryMSSQL(dbc, discoverQuery);
        Logger::info("Found " + std::to_string(discoveredTables.size()) +
                     " tables");

        for (const std::vector<std::string> &row : discoveredTables) {
          if (row.size() < 2)
            continue;

          std::string schemaName = row[0];
          std::string tableName = row[1];

          {
            // Detectar columna de tiempo con prioridad
            std::string timeColumn =
                detectTimeColumnMSSQL(dbc, schemaName, tableName);

            pqxx::work txn(pgConn);
            // Verificar si la tabla ya existe (sin importar connection_string)
            auto existingCheck =
                txn.exec("SELECT COUNT(*) FROM metadata.catalog "
                         "WHERE schema_name='" +
                         escapeSQL(schemaName) + "' AND table_name='" +
                         escapeSQL(tableName) + "' AND db_engine='MSSQL';");

            if (!existingCheck.empty() && existingCheck[0][0].as<int>() > 0) {
              // Tabla ya existe, verificar si timeColumn cambió
              auto timeColumnCheck =
                  txn.exec("SELECT last_sync_column FROM metadata.catalog "
                           "WHERE schema_name='" +
                           escapeSQL(schemaName) + "' AND table_name='" +
                           escapeSQL(tableName) + "' AND db_engine='MSSQL';");

              if (!timeColumnCheck.empty()) {
                std::string currentTimeColumn =
                    timeColumnCheck[0][0].as<std::string>();
                if (currentTimeColumn != timeColumn) {
                  txn.exec("UPDATE metadata.catalog SET last_sync_column = '" +
                           escapeSQL(timeColumn) + "' WHERE schema_name='" +
                           escapeSQL(schemaName) + "' AND table_name='" +
                           escapeSQL(tableName) + "' AND db_engine='MSSQL';");
                }
              }
            } else {
              // Tabla nueva, insertar
              txn.exec("INSERT INTO metadata.catalog "
                       "(schema_name, table_name, cluster_name, db_engine, "
                       "connection_string, "
                       "last_sync_time, last_sync_column, status, "
                       "last_offset, active) "
                       "VALUES ('" +
                       escapeSQL(schemaName) + "', '" + escapeSQL(tableName) +
                       "', '', 'MSSQL', '" + escapeSQL(connStr) +
                       "', NOW(), '" + escapeSQL(timeColumn) +
                       "', 'PENDING', '0', false);");
            }
            txn.commit();
          }
        }

        // Close MSSQL connection
        SQLDisconnect(dbc);
        SQLFreeHandle(SQL_HANDLE_DBC, dbc);
        SQLFreeHandle(SQL_HANDLE_ENV, env);
      }

      // Actualizar cluster names después de la sincronización
      updateClusterNames();
    } catch (const std::exception &e) {
      Logger::error("syncCatalogMSSQLToPostgres",
                    "Error in syncCatalogMSSQLToPostgres: " +
                        std::string(e.what()));
    }
  }

  void syncCatalogPostgresToPostgres() {
    try {
      pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());

      std::vector<std::string> pgConnStrings;
      {
        pqxx::work txn(pgConn);
        auto results =
            txn.exec("SELECT connection_string FROM metadata.catalog "
                     "WHERE db_engine='PostgreSQL' AND active=true;");
        txn.commit();

        for (const auto &row : results) {
          if (row.size() >= 1) {
            pgConnStrings.push_back(row[0].as<std::string>());
          }
        }
      }

      Logger::info("Found " + std::to_string(pgConnStrings.size()) +
                   " PostgreSQL source connections");
      if (pgConnStrings.empty()) {
        Logger::warning("No PostgreSQL source connections found in catalog");
        return;
      }

      for (const auto &connStr : pgConnStrings) {
        {
          pqxx::work txn(pgConn);
          auto connectionCheck =
              txn.exec("SELECT COUNT(*) FROM metadata.catalog "
                       "WHERE connection_string='" +
                       escapeSQL(connStr) +
                       "' AND db_engine='PostgreSQL' AND active=true "
                       "AND last_sync_time > NOW() - INTERVAL '5 minutes';");
          txn.commit();

          if (!connectionCheck.empty() && !connectionCheck[0][0].is_null()) {
            int connectionCount = connectionCheck[0][0].as<int>();
            if (connectionCount > 0) {
              continue;
            }
          }
        }

        // Connect directly to PostgreSQL
        pqxx::connection sourcePgConn(connStr);
        if (!sourcePgConn.is_open()) {
          Logger::error("syncCatalogPostgresToPostgres",
                        "Failed to connect to source PostgreSQL");
          continue;
        }

        std::string discoverQuery =
            "SELECT table_schema, table_name "
            "FROM information_schema.tables "
            "WHERE table_schema NOT IN ('information_schema', 'pg_catalog', "
            "'pg_toast', 'pg_temp_1', 'pg_toast_temp_1', 'metadata') "
            "AND table_type = 'BASE TABLE' "
            "ORDER BY table_schema, table_name;";

        pqxx::work sourceTxn(sourcePgConn);
        auto discoveredTables = sourceTxn.exec(discoverQuery);
        sourceTxn.commit();

        Logger::info("Found " + std::to_string(discoveredTables.size()) +
                     " tables");

        for (const auto &row : discoveredTables) {
          if (row.size() < 2)
            continue;

          std::string schemaName = row[0].as<std::string>();
          std::string tableName = row[1].as<std::string>();

          {
            // Detectar columna de tiempo con prioridad
            std::string timeColumn =
                detectTimeColumnPostgres(sourcePgConn, schemaName, tableName);

            pqxx::work txn(pgConn);
            // Check if table already exists
            auto existingCheck = txn.exec(
                "SELECT last_sync_column FROM metadata.catalog "
                "WHERE schema_name='" +
                escapeSQL(schemaName) + "' AND table_name='" +
                escapeSQL(tableName) + "' AND db_engine='PostgreSQL';");

            if (!existingCheck.empty()) {
              // Table exists, only update if timeColumn changed
              std::string currentTimeColumn =
                  existingCheck[0][0].as<std::string>();
              if (currentTimeColumn != timeColumn) {
                txn.exec("UPDATE metadata.catalog SET last_sync_column = '" +
                         escapeSQL(timeColumn) + "' WHERE schema_name='" +
                         escapeSQL(schemaName) + "' AND table_name='" +
                         escapeSQL(tableName) +
                         "' AND db_engine='PostgreSQL';");
              }
            } else {
              // New table, insert
              txn.exec("INSERT INTO metadata.catalog "
                       "(schema_name, table_name, cluster_name, db_engine, "
                       "connection_string, "
                       "last_sync_time, last_sync_column, status, "
                       "last_offset, active) "
                       "VALUES ('" +
                       escapeSQL(schemaName) + "', '" + escapeSQL(tableName) +
                       "', '', 'PostgreSQL', '" + escapeSQL(connStr) +
                       "', NOW(), '" + escapeSQL(timeColumn) +
                       "', 'PENDING', '0', false);");
            }
            txn.commit();
          }
        }

        // PostgreSQL connection closes automatically when sourcePgConn goes out
        // of scope
      }

      // Actualizar cluster names después de la sincronización
      updateClusterNames();
    } catch (const std::exception &e) {
      Logger::error("syncCatalogPostgresToPostgres",
                    "Error in syncCatalogPostgresToPostgres: " +
                        std::string(e.what()));
    }
  }

private:
  std::string detectTimeColumnMSSQL(SQLHDBC conn, const std::string &schema,
                                    const std::string &table) {
    try {
      std::string query =
          "SELECT c.name AS COLUMN_NAME "
          "FROM sys.columns c "
          "INNER JOIN sys.tables t ON c.object_id = t.object_id "
          "INNER JOIN sys.schemas s ON t.schema_id = s.schema_id "
          "WHERE s.name = '" +
          escapeSQL(schema) + "' AND t.name = '" + escapeSQL(table) +
          "' "
          "AND c.name IN ('updated_at', 'created_at', 'modified_at', "
          "'timestamp', 'last_modified', 'updated_time', 'created_time') "
          "ORDER BY CASE c.name "
          "  WHEN 'updated_at' THEN 1 "
          "  WHEN 'modified_at' THEN 2 "
          "  WHEN 'last_modified' THEN 3 "
          "  WHEN 'updated_time' THEN 4 "
          "  WHEN 'created_at' THEN 5 "
          "  WHEN 'created_time' THEN 6 "
          "  WHEN 'timestamp' THEN 7 "
          "  ELSE 8 END;";

      auto results = executeQueryMSSQL(conn, query);
      if (!results.empty() && !results[0][0].empty()) {
        return results[0][0];
      } else {
      }
    } catch (const std::exception &e) {
      Logger::error("detectTimeColumnMSSQL",
                    "Error detecting time column: " + std::string(e.what()));
    }
    return "";
  }

  std::string detectTimeColumnMariaDB(MYSQL *conn, const std::string &schema,
                                      const std::string &table) {
    try {
      std::string query =
          "SELECT COLUMN_NAME "
          "FROM information_schema.columns "
          "WHERE table_schema = '" +
          escapeSQL(schema) + "' AND table_name = '" + escapeSQL(table) +
          "' "
          "AND COLUMN_NAME IN ('updated_at', 'created_at', 'modified_at', "
          "'timestamp', 'last_modified', 'updated_time', 'created_time') "
          "ORDER BY FIELD(COLUMN_NAME, 'updated_at', 'modified_at', "
          "'last_modified', 'updated_time', 'created_at', 'created_time', "
          "'timestamp');";

      auto results = executeQueryMariaDB(conn, query);

      for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i].empty()) {
        }
      }

      if (!results.empty() && !results[0][0].empty()) {
        return results[0][0];
      } else {
      }
    } catch (const std::exception &e) {
      Logger::error("detectTimeColumnMariaDB",
                    "Error detecting time column: " + std::string(e.what()));
    }
    return "";
  }

  std::string detectTimeColumnPostgres(pqxx::connection &conn,
                                       const std::string &schema,
                                       const std::string &table) {
    try {
      std::string query =
          "SELECT column_name "
          "FROM information_schema.columns "
          "WHERE table_schema = '" +
          escapeSQL(schema) + "' AND table_name = '" + escapeSQL(table) +
          "' "
          "AND column_name IN ('updated_at', 'created_at', 'modified_at', "
          "'timestamp', 'last_modified', 'updated_time', 'created_time') "
          "ORDER BY CASE column_name "
          "  WHEN 'updated_at' THEN 1 "
          "  WHEN 'modified_at' THEN 2 "
          "  WHEN 'last_modified' THEN 3 "
          "  WHEN 'updated_time' THEN 4 "
          "  WHEN 'created_at' THEN 5 "
          "  WHEN 'created_time' THEN 6 "
          "  WHEN 'timestamp' THEN 7 "
          "  ELSE 8 END;";

      pqxx::work txn(conn);
      auto results = txn.exec(query);
      txn.commit();

      if (!results.empty() && !results[0][0].is_null()) {
        return results[0][0].as<std::string>();
      } else {
      }
    } catch (const std::exception &e) {
      Logger::error("detectTimeColumnPostgres",
                    "Error detecting time column: " + std::string(e.what()));
    }
    return "";
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
    Logger::warning(
        "No DATABASE found in connection string, using master fallback");
    return "master"; // fallback
  }

  std::string resolveClusterName(const std::string &connectionString,
                                 const std::string &dbEngine) {
    try {
      if (dbEngine == "MariaDB") {
        // Connect and get @@hostname
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

        MYSQL *conn = mysql_init(nullptr);
        if (!conn)
          return "";
        unsigned int portNum = 3306;
        if (!port.empty()) {
          try {
            portNum = std::stoul(port);
          } catch (...) {
            portNum = 3306;
          }
        }
        if (mysql_real_connect(conn, host.c_str(), user.c_str(),
                               password.c_str(), db.c_str(), portNum, nullptr,
                               0) == nullptr) {
          mysql_close(conn);
          return "";
        }

        // Set connection timeouts for large tables
        {
          std::string timeoutQuery =
              "SET SESSION wait_timeout = " +
              std::to_string(SyncConfig::getConnectionTimeout()) +
              ", interactive_timeout = " +
              std::to_string(SyncConfig::getConnectionTimeout()) +
              ", net_read_timeout = 600" + ", net_write_timeout = 600";
          mysql_query(conn, timeoutQuery.c_str());
        }
        auto res = executeQueryMariaDB(conn, "SELECT @@hostname;");
        mysql_close(conn);
        if (!res.empty() && !res[0].empty() && !res[0][0].empty()) {
          std::string name = res[0][0];
          std::transform(name.begin(), name.end(), name.begin(), ::toupper);
          return name;
        }
        return "";
      }

      if (dbEngine == "MSSQL") {
        // Connect via ODBC and query SERVERPROPERTY('MachineName')
        SQLHENV env;
        SQLHDBC dbc;
        SQLRETURN ret;
        ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
        if (!SQL_SUCCEEDED(ret))
          return "";
        ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION,
                            (SQLPOINTER)SQL_OV_ODBC3, 0);
        if (!SQL_SUCCEEDED(ret)) {
          SQLFreeHandle(SQL_HANDLE_ENV, env);
          return "";
        }
        ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
        if (!SQL_SUCCEEDED(ret)) {
          SQLFreeHandle(SQL_HANDLE_ENV, env);
          return "";
        }
        SQLCHAR outConnStr[1024];
        SQLSMALLINT outConnStrLen;
        ret =
            SQLDriverConnect(dbc, nullptr, (SQLCHAR *)connectionString.c_str(),
                             SQL_NTS, outConnStr, sizeof(outConnStr),
                             &outConnStrLen, SQL_DRIVER_NOPROMPT);
        if (!SQL_SUCCEEDED(ret)) {
          SQLFreeHandle(SQL_HANDLE_DBC, dbc);
          SQLFreeHandle(SQL_HANDLE_ENV, env);
          return "";
        }
        auto results =
            executeQueryMSSQL(dbc, "SELECT CAST(SERVERPROPERTY('MachineName') "
                                   "AS VARCHAR(128)) AS name;");
        if (results.empty() || results[0].empty() || results[0][0] == "NULL" ||
            results[0][0].empty()) {
          results = executeQueryMSSQL(
              dbc, "SELECT CAST(@@SERVERNAME AS VARCHAR(128)) AS name;");
        }
        SQLDisconnect(dbc);
        SQLFreeHandle(SQL_HANDLE_DBC, dbc);
        SQLFreeHandle(SQL_HANDLE_ENV, env);
        if (!results.empty() && !results[0].empty() &&
            results[0][0] != "NULL" && !results[0][0].empty()) {
          std::string name = results[0][0];
          std::transform(name.begin(), name.end(), name.begin(), ::toupper);
          return name;
        }
        return "";
      }

      if (dbEngine == "PostgreSQL") {
        try {
          pqxx::connection srcConn(connectionString);
          if (!srcConn.is_open())
            return "";
          pqxx::work txn(srcConn);
          // Prefer cluster_name GUC if set; else fallback to inet_server_addr()
          auto r1 = txn.exec("SELECT current_setting('cluster_name', true);");
          std::string name;
          if (!r1.empty() && !r1[0][0].is_null()) {
            name = r1[0][0].as<std::string>();
          }
          if (name.empty()) {
            auto r2 = txn.exec("SELECT inet_server_addr()::text;");
            if (!r2.empty() && !r2[0][0].is_null()) {
              name = r2[0][0].as<std::string>();
            }
          }
          txn.commit();
          std::transform(name.begin(), name.end(), name.begin(), ::toupper);
          return name;
        } catch (...) {
          return "";
        }
      }
    } catch (...) {
      return "";
    }
    return "";
  }

  std::string extractHostnameFromConnection(const std::string &connectionString,
                                            const std::string &dbEngine) {
    std::istringstream ss(connectionString);
    std::string token;

    while (std::getline(ss, token, ';')) {
      auto pos = token.find('=');
      if (pos == std::string::npos)
        continue;

      std::string key = token.substr(0, pos);
      std::string value = token.substr(pos + 1);

      // Limpiar espacios en blanco
      key.erase(0, key.find_first_not_of(" \t\r\n"));
      key.erase(key.find_last_not_of(" \t\r\n") + 1);
      value.erase(0, value.find_first_not_of(" \t\r\n"));
      value.erase(value.find_last_not_of(" \t\r\n") + 1);

      if (dbEngine == "MariaDB" && key == "host") {
        return value;
      } else if (dbEngine == "MSSQL" && key == "SERVER") {
        return value;
      } else if (dbEngine == "PostgreSQL" && key == "host") {
        return value;
      }
    }

    Logger::warning("No hostname found in connection string for " + dbEngine);
    return "";
  }

  std::string getClusterNameFromHostname(const std::string &hostname) {
    if (hostname.empty()) {
      return "";
    }

    // Mapeo de hostnames a cluster names
    // Puedes personalizar estos mapeos según tu infraestructura
    std::string lowerHostname = hostname;
    std::transform(lowerHostname.begin(), lowerHostname.end(),
                   lowerHostname.begin(), ::tolower);

    // Patrones comunes de hostnames
    if (lowerHostname.find("prod") != std::string::npos ||
        lowerHostname.find("production") != std::string::npos) {
      return "PRODUCTION";
    }
    if (lowerHostname.find("staging") != std::string::npos ||
        lowerHostname.find("stage") != std::string::npos) {
      return "STAGING";
    }
    if (lowerHostname.find("dev") != std::string::npos ||
        lowerHostname.find("development") != std::string::npos) {
      return "DEVELOPMENT";
    }
    if (lowerHostname.find("test") != std::string::npos ||
        lowerHostname.find("testing") != std::string::npos) {
      return "TESTING";
    }
    if (lowerHostname.find("local") != std::string::npos ||
        lowerHostname.find("localhost") != std::string::npos) {
      return "LOCAL";
    }
    if (lowerHostname.find("uat") != std::string::npos) {
      return "UAT";
    }
    if (lowerHostname.find("qa") != std::string::npos) {
      return "QA";
    }

    // Extraer cluster name del hostname (ej: db-cluster-01 -> CLUSTER-01)
    if (lowerHostname.find("cluster") != std::string::npos) {
      size_t clusterPos = lowerHostname.find("cluster");
      if (clusterPos != std::string::npos) {
        std::string clusterPart = lowerHostname.substr(clusterPos);
        std::transform(clusterPart.begin(), clusterPart.end(),
                       clusterPart.begin(), ::toupper);
        return clusterPart;
      }
    }

    // Extraer número de servidor (ej: db-01 -> DB-01)
    if (lowerHostname.find("db-") != std::string::npos) {
      size_t dbPos = lowerHostname.find("db-");
      if (dbPos != std::string::npos) {
        std::string dbPart = lowerHostname.substr(dbPos);
        std::transform(dbPart.begin(), dbPart.end(), dbPart.begin(), ::toupper);
        return dbPart;
      }
    }

    // Si no se encuentra patrón específico, usar el hostname completo en
    // mayúsculas
    std::string upperHostname = hostname;
    std::transform(upperHostname.begin(), upperHostname.end(),
                   upperHostname.begin(), ::toupper);
    return upperHostname;
  }

  void cleanNonExistentPostgresTables(pqxx::connection &pgConn) {
    try {
      pqxx::work txn(pgConn);

      // Obtener todas las tablas marcadas como PostgreSQL (solo destinos, no
      // fuentes)
      auto results =
          txn.exec("SELECT schema_name, table_name FROM metadata.catalog "
                   "WHERE db_engine='PostgreSQL';");

      for (const auto &row : results) {
        std::string schema_name = row[0].as<std::string>();
        std::string table_name = row[1].as<std::string>();

        // Verificar si la tabla existe en PostgreSQL
        auto checkResult =
            txn.exec("SELECT COUNT(*) FROM information_schema.tables "
                     "WHERE table_schema='" +
                     schema_name +
                     "' "
                     "AND table_name='" +
                     table_name + "';");

        if (!checkResult.empty() && checkResult[0][0].as<int>() == 0) {
          Logger::info("Removing non-existent PostgreSQL table: " +
                       schema_name + "." + table_name);

          txn.exec("DELETE FROM metadata.catalog WHERE schema_name='" +
                   schema_name + "' AND table_name='" + table_name +
                   "' AND db_engine='PostgreSQL';");
        }
      }

      txn.commit();
    } catch (const std::exception &e) {
      Logger::error("cleanNonExistentPostgresTables",
                    "Error cleaning PostgreSQL tables: " +
                        std::string(e.what()));
    }
  }

  void cleanNonExistentMariaDBTables(pqxx::connection &pgConn) {
    try {
      pqxx::work txn(pgConn);

      // Obtener connection_strings únicos de MariaDB
      auto connectionResults =
          txn.exec("SELECT DISTINCT connection_string FROM metadata.catalog "
                   "WHERE db_engine='MariaDB';");

      for (const auto &connRow : connectionResults) {
        std::string connection_string = connRow[0].as<std::string>();

        // Connect directly to MariaDB
        std::string host, user, password, db, port;
        std::istringstream ss(connection_string);
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

        MYSQL *mariadbConn = mysql_init(nullptr);
        if (!mariadbConn) {
          Logger::warning("cleanNonExistentMariaDBTables",
                          "mysql_init() failed");
          continue;
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
          Logger::warning("cleanNonExistentMariaDBTables",
                          "MariaDB connection failed: " +
                              std::string(mysql_error(mariadbConn)));
          mysql_close(mariadbConn);
          continue;
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

        // Obtener todas las tablas para este connection_string
        auto tableResults =
            txn.exec("SELECT schema_name, table_name FROM metadata.catalog "
                     "WHERE db_engine='MariaDB' AND connection_string='" +
                     escapeSQL(connection_string) + "';");

        if (tableResults.empty())
          continue;

        // Construir query batch para verificar todas las tablas de esta
        // conexión
        std::string batchQuery = "SELECT table_schema, table_name FROM "
                                 "information_schema.tables WHERE ";
        std::string whereConditions;

        for (size_t i = 0; i < tableResults.size(); ++i) {
          if (i > 0)
            whereConditions += " OR ";
          whereConditions += "(table_schema='" +
                             tableResults[i][0].as<std::string>() +
                             "' AND table_name='" +
                             tableResults[i][1].as<std::string>() + "')";
        }

        batchQuery += whereConditions;

        // Ejecutar verificación batch
        auto existingTables = executeQueryMariaDB(mariadbConn, batchQuery);

        // Crear set de tablas existentes para lookup rápido
        std::set<std::pair<std::string, std::string>> existingTableSet;
        for (const std::vector<std::string> &table : existingTables) {
          if (table.size() >= 2) {
            existingTableSet.insert(std::make_pair(table[0], table[1]));
          }
        }

        // Eliminar tablas que no existen
        for (const auto &tableRow : tableResults) {
          std::string schema_name = tableRow[0].as<std::string>();
          std::string table_name = tableRow[1].as<std::string>();

          if (existingTableSet.find({schema_name, table_name}) ==
              existingTableSet.end()) {
            Logger::info("Removing non-existent MariaDB table: " + schema_name +
                         "." + table_name);

            txn.exec("DELETE FROM metadata.catalog WHERE schema_name='" +
                     schema_name + "' AND table_name='" + table_name +
                     "' AND db_engine='MariaDB' AND connection_string='" +
                     escapeSQL(connection_string) + "';");
          }
        }

        // Close MariaDB connection
        mysql_close(mariadbConn);
      }

      txn.commit();
    } catch (const std::exception &e) {
      Logger::error("cleanNonExistentMariaDBTables",
                    "Error cleaning MariaDB tables: " + std::string(e.what()));
    }
  }

  void cleanNonExistentMSSQLTables(pqxx::connection &pgConn) {
    try {
      pqxx::work txn(pgConn);

      // Obtener connection_strings únicos de MSSQL
      auto connectionResults =
          txn.exec("SELECT DISTINCT connection_string FROM metadata.catalog "
                   "WHERE db_engine='MSSQL';");

      for (const auto &connRow : connectionResults) {
        std::string connection_string = connRow[0].as<std::string>();

        // Connect directly to MSSQL using ODBC
        SQLHENV env;
        SQLHDBC dbc;
        SQLRETURN ret;

        ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
        if (!SQL_SUCCEEDED(ret)) {
          Logger::warning("cleanNonExistentMSSQLTables",
                          "Failed to allocate ODBC environment handle");
          continue;
        }

        ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION,
                            (SQLPOINTER)SQL_OV_ODBC3, 0);
        if (!SQL_SUCCEEDED(ret)) {
          SQLFreeHandle(SQL_HANDLE_ENV, env);
          Logger::warning("cleanNonExistentMSSQLTables",
                          "Failed to set ODBC version");
          continue;
        }

        ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
        if (!SQL_SUCCEEDED(ret)) {
          SQLFreeHandle(SQL_HANDLE_ENV, env);
          Logger::warning("cleanNonExistentMSSQLTables",
                          "Failed to allocate ODBC connection handle");
          continue;
        }

        SQLCHAR outConnStr[1024];
        SQLSMALLINT outConnStrLen;
        ret =
            SQLDriverConnect(dbc, nullptr, (SQLCHAR *)connection_string.c_str(),
                             SQL_NTS, outConnStr, sizeof(outConnStr),
                             &outConnStrLen, SQL_DRIVER_NOPROMPT);
        if (!SQL_SUCCEEDED(ret)) {
          SQLCHAR sqlState[6], msg[SQL_MAX_MESSAGE_LENGTH];
          SQLINTEGER nativeError;
          SQLSMALLINT msgLen;
          SQLGetDiagRec(SQL_HANDLE_DBC, dbc, 1, sqlState, &nativeError, msg,
                        sizeof(msg), &msgLen);
          SQLFreeHandle(SQL_HANDLE_DBC, dbc);
          SQLFreeHandle(SQL_HANDLE_ENV, env);
          Logger::warning("cleanNonExistentMSSQLTables",
                          "Failed to connect to MSSQL: " +
                              std::string((char *)msg));
          continue;
        }

        // Obtener todas las tablas para este connection_string
        auto tableResults =
            txn.exec("SELECT schema_name, table_name FROM metadata.catalog "
                     "WHERE db_engine='MSSQL' AND connection_string='" +
                     escapeSQL(connection_string) + "';");

        if (tableResults.empty())
          continue;

        // Construir query batch para verificar todas las tablas de esta
        // conexión - usar sys.tables con base de datos específica
        std::string databaseName = extractDatabaseName(connection_string);
        std::string batchQuery =
            "SELECT s.name AS table_schema, t.name AS table_name FROM "
            "[" +
            databaseName +
            "].sys.tables t "
            "INNER JOIN [" +
            databaseName +
            "].sys.schemas s ON t.schema_id = s.schema_id "
            "WHERE s.name NOT IN ('INFORMATION_SCHEMA', 'sys', 'guest') "
            "AND t.name NOT LIKE 'spt_%' "
            "AND t.name NOT LIKE 'MS%' "
            "AND t.name NOT LIKE 'sp_%' "
            "AND t.name NOT LIKE 'fn_%' "
            "AND t.name NOT LIKE 'xp_%' "
            "AND t.name NOT LIKE 'dt_%' "
            "AND (";
        std::string whereConditions;

        for (size_t i = 0; i < tableResults.size(); ++i) {
          if (i > 0)
            whereConditions += " OR ";
          whereConditions +=
              "(s.name='" + tableResults[i][0].as<std::string>() +
              "' AND t.name='" + tableResults[i][1].as<std::string>() + "')";
        }

        batchQuery += whereConditions + ") ORDER BY s.name, t.name;";

        // Ejecutar verificación batch
        auto existingTables = executeQueryMSSQL(dbc, batchQuery);

        // Crear set de tablas existentes para lookup rápido
        std::set<std::pair<std::string, std::string>> existingTableSet;
        for (const std::vector<std::string> &table : existingTables) {
          if (table.size() >= 2) {
            existingTableSet.insert(std::make_pair(table[0], table[1]));
          }
        }

        // Eliminar tablas que no existen
        for (const auto &tableRow : tableResults) {
          std::string schema_name = tableRow[0].as<std::string>();
          std::string table_name = tableRow[1].as<std::string>();

          if (existingTableSet.find({schema_name, table_name}) ==
              existingTableSet.end()) {
            Logger::info("Removing non-existent MSSQL table: " + schema_name +
                         "." + table_name);

            txn.exec("DELETE FROM metadata.catalog WHERE schema_name='" +
                     schema_name + "' AND table_name='" + table_name +
                     "' AND db_engine='MSSQL' AND connection_string='" +
                     escapeSQL(connection_string) + "';");
          }
        }

        // Close MSSQL connection
        SQLDisconnect(dbc);
        SQLFreeHandle(SQL_HANDLE_DBC, dbc);
        SQLFreeHandle(SQL_HANDLE_ENV, env);
      }

      txn.commit();
    } catch (const std::exception &e) {
      Logger::error("cleanNonExistentMSSQLTables",
                    "Error cleaning MSSQL tables: " + std::string(e.what()));
    }
  }

  void cleanOrphanedTables(pqxx::connection &pgConn) {
    try {
      pqxx::work txn(pgConn);

      // Limpiar tablas con connection_string vacío o inválido
      txn.exec("DELETE FROM metadata.catalog WHERE connection_string IS NULL "
               "OR connection_string='';");

      // Limpiar tablas con db_engine inválido
      txn.exec("DELETE FROM metadata.catalog WHERE db_engine NOT IN "
               "('PostgreSQL', 'MariaDB', 'MSSQL');");

      // Limpiar tablas con schema_name o table_name vacío
      txn.exec("DELETE FROM metadata.catalog WHERE schema_name IS NULL OR "
               "schema_name='' OR table_name IS NULL OR table_name='';");

      txn.commit();
    } catch (const std::exception &e) {
      Logger::error("cleanOrphanedTables",
                    "Error cleaning orphaned tables: " + std::string(e.what()));
    }
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

  std::vector<std::vector<std::string>>
  executeQueryMSSQL(SQLHDBC conn, const std::string &query) {
    std::vector<std::vector<std::string>> results;
    if (!conn) {
      Logger::error("executeQueryMSSQL", "No valid MSSQL connection");
      return results;
    }

    SQLHSTMT stmt;
    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, conn, &stmt);
    if (ret != SQL_SUCCESS) {
      Logger::error("executeQueryMSSQL", "SQLAllocHandle(STMT) failed");
      return results;
    }

    ret = SQLExecDirect(stmt, (SQLCHAR *)query.c_str(), SQL_NTS);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
      Logger::error("executeQueryMSSQL", "SQLExecDirect failed");
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

#endif // CATALOG_MANAGER_H
