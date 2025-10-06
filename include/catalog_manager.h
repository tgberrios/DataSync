#ifndef CATALOG_MANAGER_H
#define CATALOG_MANAGER_H

#include "Config.h"
#include "logger.h"
#include <cstdint>

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

      // Limpiar valores de offset inválidos según la estrategia
      cleanInvalidOffsetValues();

      // Actualizar cluster names después de la limpieza
      updateClusterNames();

      Logger::info(LogCategory::DATABASE, "Catalog cleanup completed");
    } catch (const pqxx::sql_error &e) {
      Logger::error(LogCategory::DATABASE, "cleanCatalog",
                    "SQL ERROR cleaning catalog: " + std::string(e.what()) +
                        " [SQL State: " + e.sqlstate() + "]");
    } catch (const pqxx::broken_connection &e) {
      Logger::error(LogCategory::DATABASE, "cleanCatalog",
                    "CONNECTION ERROR cleaning catalog: " +
                        std::string(e.what()));
    } catch (const std::exception &e) {
      Logger::error(LogCategory::DATABASE, "cleanCatalog",
                    "ERROR cleaning catalog: " + std::string(e.what()));
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

      // Contar tablas inactivas que no son NO_DATA antes de marcar como SKIP
      auto inactiveCountResult =
          txn.exec("SELECT COUNT(*) FROM metadata.catalog WHERE "
                   "active = false AND status != 'NO_DATA'");
      int inactiveCount = inactiveCountResult[0][0].as<int>();

      if (noDataCount == 0 && inactiveCount == 0) {
        txn.commit();
        return;
      }

      // Desactivar tablas NO_DATA
      if (noDataCount > 0) {
        auto updateResult = txn.exec(
            "UPDATE metadata.catalog SET active = false WHERE status = "
            "'NO_DATA' AND active = true");
        Logger::info(LogCategory::DATABASE,
                     "Deactivated " +
                         std::to_string(updateResult.affected_rows()) +
                         " NO_DATA tables");
      }

      // Marcar tablas inactivas como SKIP y resetear valores
      if (inactiveCount > 0) {
        auto skipResult =
            txn.exec("UPDATE metadata.catalog SET "
                     "status = 'SKIP', "
                     "last_offset = 0, "
                     "last_processed_pk = 0 "
                     "WHERE active = false AND status != 'NO_DATA'");
        Logger::info(LogCategory::DATABASE,
                     "Marked " + std::to_string(skipResult.affected_rows()) +
                         " inactive tables as SKIP with reset values");
      }

      txn.commit();

    } catch (const pqxx::sql_error &e) {
      Logger::error(
          LogCategory::DATABASE, "deactivateNoDataTables",
          "SQL ERROR deactivating NO_DATA tables: " + std::string(e.what()) +
              " [SQL State: " + e.sqlstate() + "]");
    } catch (const pqxx::broken_connection &e) {
      Logger::error(LogCategory::DATABASE, "deactivateNoDataTables",
                    "CONNECTION ERROR deactivating NO_DATA tables: " +
                        std::string(e.what()));
    } catch (const std::exception &e) {
      Logger::error(LogCategory::DATABASE, "deactivateNoDataTables",
                    "ERROR deactivating NO_DATA tables: " +
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

          Logger::info(LogCategory::DATABASE,
                       "Updated cluster_name to '" + clusterName + "' for " +
                           std::to_string(updateResult.affected_rows()) +
                           " tables");
        }
      }

      Logger::info(LogCategory::DATABASE, "Cluster name updates completed");
    } catch (const pqxx::sql_error &e) {
      Logger::error(
          LogCategory::DATABASE, "updateClusterNames",
          "SQL ERROR updating cluster names: " + std::string(e.what()) +
              " [SQL State: " + e.sqlstate() + "]");
    } catch (const pqxx::broken_connection &e) {
      Logger::error(LogCategory::DATABASE, "updateClusterNames",
                    "CONNECTION ERROR updating cluster names: " +
                        std::string(e.what()));
    } catch (const std::exception &e) {
      Logger::error(LogCategory::DATABASE, "updateClusterNames",
                    "ERROR updating cluster names: " + std::string(e.what()));
    }
  }

  void validateSchemaConsistency() {
    try {
      pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());

      Logger::info(LogCategory::DATABASE,
                   "Starting schema consistency validation");

      pqxx::work txn(pgConn);
      auto results = txn.exec("SELECT schema_name, table_name, db_engine, "
                              "connection_string, status "
                              "FROM metadata.catalog "
                              "WHERE active = true AND status IN "
                              "('LISTENING_CHANGES', 'FULL_LOAD') "
                              "ORDER BY db_engine, schema_name, table_name");
      txn.commit();

      int totalTables = results.size();
      int validatedTables = 0;
      int resetTables = 0;

      Logger::info(LogCategory::DATABASE, "Found " +
                                              std::to_string(totalTables) +
                                              " tables to validate");

      for (const auto &row : results) {
        std::string schemaName = row[0].as<std::string>();
        std::string tableName = row[1].as<std::string>();
        std::string dbEngine = row[2].as<std::string>();
        std::string connectionString = row[3].as<std::string>();
        std::string status = row[4].as<std::string>();

        Logger::info(LogCategory::DATABASE, "Validating schema: " + schemaName +
                                                "." + tableName + " [" +
                                                dbEngine + "]");

        bool needsReset = false;
        int sourceColumnCount = 0;
        int targetColumnCount = 0;

        if (dbEngine == "MariaDB") {
          auto counts =
              getColumnCountsMariaDB(connectionString, schemaName, tableName);
          sourceColumnCount = counts.first;
          targetColumnCount = counts.second;
        } else if (dbEngine == "MSSQL") {
          auto counts =
              getColumnCountsMSSQL(connectionString, schemaName, tableName);
          sourceColumnCount = counts.first;
          targetColumnCount = counts.second;

          needsReset = (sourceColumnCount != targetColumnCount);

          if (needsReset) {
            Logger::warning(
                LogCategory::DATABASE,
                "SCHEMA MISMATCH: " + schemaName + "." + tableName +
                    " - Source columns: " + std::to_string(sourceColumnCount) +
                    ", Target columns: " + std::to_string(targetColumnCount) +
                    " - Dropping and resetting table");

            pqxx::work resetTxn(pgConn);

            // Drop the target table in PostgreSQL
            std::string lowerSchemaName = schemaName;
            std::transform(lowerSchemaName.begin(), lowerSchemaName.end(),
                           lowerSchemaName.begin(), ::tolower);
            std::string lowerTableName = tableName;
            std::transform(lowerTableName.begin(), lowerTableName.end(),
                           lowerTableName.begin(), ::tolower);
            resetTxn.exec("DROP TABLE IF EXISTS \"" + lowerSchemaName +
                          "\".\"" + lowerTableName + "\"");

            // Reset catalog entry
            resetTxn.exec("UPDATE metadata.catalog SET "
                          "status = 'FULL_LOAD', "
                          "last_offset = 0, "
                          "last_processed_pk = 0 "
                          "WHERE schema_name = '" +
                          escapeSQL(schemaName) +
                          "' "
                          "AND table_name = '" +
                          escapeSQL(tableName) +
                          "' "
                          "AND db_engine = '" +
                          escapeSQL(dbEngine) + "'");
            resetTxn.commit();
            resetTables++;
          } else {
            Logger::info(
                LogCategory::DATABASE,
                "SCHEMA VALID: " + schemaName + "." + tableName +
                    " - Columns match: " + std::to_string(sourceColumnCount));
            validatedTables++;
          }
        }

        Logger::info(LogCategory::DATABASE,
                     "Schema validation completed - Validated: " +
                         std::to_string(validatedTables) +
                         ", Reset: " + std::to_string(resetTables) +
                         ", Total: " + std::to_string(totalTables));
      }
      catch (const pqxx::sql_error &e) {
        Logger::error(
            LogCategory::DATABASE, "validateSchemaConsistency",
            "SQL ERROR in schema validation: " + std::string(e.what()) +
                " [SQL State: " + e.sqlstate() + "]");
      }
      catch (const pqxx::broken_connection &e) {
        Logger::error(LogCategory::DATABASE, "validateSchemaConsistency",
                      "CONNECTION ERROR in schema validation: " +
                          std::string(e.what()));
      }
      catch (const std::exception &e) {
        Logger::error(LogCategory::DATABASE, "validateSchemaConsistency",
                      "ERROR in schema validation: " + std::string(e.what()));
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

        Logger::info(LogCategory::DATABASE,
                     "Found " + std::to_string(mariaConnStrings.size()) +
                         " MariaDB connection(s)");
        if (mariaConnStrings.empty()) {
          Logger::warning(LogCategory::DATABASE,
                          "No MariaDB connections found in catalog");
          return;
        }

        for (const auto &connStr : mariaConnStrings) {
          {
            pqxx::work txn(pgConn);
            // REMOVED: Time-based filter that was preventing PK detection
            // updates All connections will be processed to ensure PK
            // information is up to date
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

          // Validate connection parameters
          if (host.empty() || user.empty() || db.empty()) {
            Logger::error(
                LogCategory::DATABASE, "syncCatalogMariaDBToPostgres",
                "Missing required connection parameters (host, user, db)");
            continue;
          }

          // Connect directly to MariaDB
          MYSQL *mariaConn = mysql_init(nullptr);
          if (!mariaConn) {
            Logger::error(LogCategory::DATABASE, "syncCatalogMariaDBToPostgres",
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

          if (mysql_real_connect(mariaConn, host.c_str(), user.c_str(),
                                 password.c_str(), db.c_str(), portNum, nullptr,
                                 0) == nullptr) {
            Logger::error(LogCategory::DATABASE, "syncCatalogMariaDBToPostgres",
                          "MariaDB connection failed: " +
                              std::string(mysql_error(mariaConn)));
            mysql_close(mariaConn);
            continue;
          }

          // Set connection timeouts for large tables - OPTIMIZED
          {
            std::string timeoutQuery =
                "SET SESSION wait_timeout = 600" +                // 10 minutos
                std::string(", interactive_timeout = 600") +      // 10 minutos
                std::string(", net_read_timeout = 600") +         // 10 minutos
                std::string(", net_write_timeout = 600") +        // 10 minutos
                std::string(", innodb_lock_wait_timeout = 600") + // 10 minutos
                std::string(", lock_wait_timeout = 600");         // 10 minutos
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

              // Detectar PK
              Logger::info(LogCategory::DATABASE,
                           "Detecting PK for table: " + schemaName + "." +
                               tableName);
              std::vector<std::string> pkColumns =
                  detectPrimaryKeyColumns(mariaConn, schemaName, tableName);
              std::string pkStrategy = determinePKStrategy(pkColumns);
              bool hasPK = !pkColumns.empty();

              Logger::info(LogCategory::DATABASE,
                           "PK Detection Results for " + schemaName + "." +
                               tableName +
                               ": hasPK=" + (hasPK ? "true" : "false") +
                               ", pkStrategy=" + pkStrategy +
                               ", pkColumns=" + columnsToJSON(pkColumns));

              pqxx::work txn(pgConn);

              // Obtener tamaño de la tabla para ordenamiento usando pg_class
              int64_t tableSize = 0;
              try {
                std::string lowerSchemaName = schemaName;
                std::transform(lowerSchemaName.begin(), lowerSchemaName.end(),
                               lowerSchemaName.begin(), ::tolower);
                std::string lowerTableName = tableName;
                std::transform(lowerTableName.begin(), lowerTableName.end(),
                               lowerTableName.begin(), ::tolower);
                std::string sizeQuery =
                    "SELECT COALESCE(reltuples::bigint, 0) FROM pg_class "
                    "WHERE relname = '" +
                    escapeSQL(lowerTableName) +
                    "' AND "
                    "relnamespace = (SELECT oid FROM pg_namespace WHERE "
                    "nspname "
                    "= '" +
                    escapeSQL(lowerSchemaName) + "')";
                auto sizeResult = txn.exec(sizeQuery);
                if (!sizeResult.empty() && !sizeResult[0][0].is_null()) {
                  tableSize = sizeResult[0][0].as<int64_t>();
                }
              } catch (const std::exception &e) {
                // Silently continue without table size - not critical for
                // functionality
                tableSize = 0;
              }
              // Check if table already exists
              auto existingCheck =
                  txn.exec("SELECT last_sync_column, pk_columns, pk_strategy, "
                           "has_pk, table_size FROM metadata.catalog "
                           "WHERE schema_name='" +
                           escapeSQL(schemaName) + "' AND table_name='" +
                           escapeSQL(tableName) + "' AND db_engine='MariaDB';");

              if (!existingCheck.empty()) {
                // Table exists, update if any column information changed
                std::string currentTimeColumn =
                    existingCheck[0][0].is_null()
                        ? ""
                        : existingCheck[0][0].as<std::string>();
                std::string currentPKColumns =
                    existingCheck[0][1].is_null()
                        ? ""
                        : existingCheck[0][1].as<std::string>();
                std::string currentPKStrategy =
                    existingCheck[0][2].is_null()
                        ? ""
                        : existingCheck[0][2].as<std::string>();
                bool currentHasPK = existingCheck[0][3].is_null()
                                        ? false
                                        : existingCheck[0][3].as<bool>();

                bool needsUpdate = false;
                std::string updateQuery = "UPDATE metadata.catalog SET ";

                if (currentTimeColumn != timeColumn) {
                  updateQuery +=
                      "last_sync_column = '" + escapeSQL(timeColumn) + "'";
                  needsUpdate = true;
                }

                if (currentPKColumns != columnsToJSON(pkColumns)) {
                  if (needsUpdate)
                    updateQuery += ", ";
                  updateQuery += "pk_columns = '" +
                                 escapeSQL(columnsToJSON(pkColumns)) + "'";
                  needsUpdate = true;
                }

                if (currentPKStrategy != pkStrategy) {
                  if (needsUpdate)
                    updateQuery += ", ";
                  updateQuery +=
                      "pk_strategy = '" + escapeSQL(pkStrategy) + "'";
                  needsUpdate = true;
                }

                if (currentHasPK != hasPK) {
                  if (needsUpdate)
                    updateQuery += ", ";
                  updateQuery +=
                      "has_pk = " + std::string(hasPK ? "true" : "false");
                  needsUpdate = true;
                }

                // NUEVO: Actualizar table_size siempre para mantenerlo
                // sincronizado
                if (needsUpdate)
                  updateQuery += ", ";
                updateQuery += "table_size = " + std::to_string(tableSize);
                needsUpdate = true;

                if (needsUpdate) {
                  updateQuery += " WHERE schema_name='" +
                                 escapeSQL(schemaName) + "' AND table_name='" +
                                 escapeSQL(tableName) +
                                 "' AND db_engine='MariaDB'";
                  txn.exec(updateQuery);
                }
              } else {
                // New table, insert with all information
                txn.exec(
                    "INSERT INTO metadata.catalog "
                    "(schema_name, table_name, cluster_name, db_engine, "
                    "connection_string, last_sync_time, last_sync_column, "
                    "status, last_offset, active, pk_columns, pk_strategy, "
                    "has_pk, table_size) "
                    "VALUES ('" +
                    escapeSQL(schemaName) + "', '" + escapeSQL(tableName) +
                    "', '', 'MariaDB', '" + escapeSQL(connStr) + "', NOW(), '" +
                    escapeSQL(timeColumn) + "', 'PENDING', '0', false, '" +
                    escapeSQL(columnsToJSON(pkColumns)) + "', '" +
                    escapeSQL(pkStrategy) + "', " +
                    std::string(hasPK ? "true" : "false") + ", '" +
                    std::to_string(tableSize) + ");");
              }
              txn.commit();
            }
          }

          // Close MariaDB connection
          mysql_close(mariaConn);
        }

        // Actualizar cluster names después de la sincronización
        updateClusterNames();
      } catch (const pqxx::sql_error &e) {
        Logger::error(LogCategory::DATABASE, "syncCatalogMariaDBToPostgres",
                      "SQL ERROR in syncCatalogMariaDBToPostgres: " +
                          std::string(e.what()) +
                          " [SQL State: " + e.sqlstate() + "]");
      } catch (const pqxx::broken_connection &e) {
        Logger::error(LogCategory::DATABASE, "syncCatalogMariaDBToPostgres",
                      "CONNECTION ERROR in syncCatalogMariaDBToPostgres: " +
                          std::string(e.what()));
      } catch (const std::exception &e) {
        Logger::error(LogCategory::DATABASE, "syncCatalogMariaDBToPostgres",
                      "ERROR in syncCatalogMariaDBToPostgres: " +
                          std::string(e.what()));
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

        Logger::info(LogCategory::DATABASE,
                     "Found " + std::to_string(mssqlConnStrings.size()) +
                         " MSSQL connections");
        if (mssqlConnStrings.empty()) {
          Logger::warning(LogCategory::DATABASE,
                          "No MSSQL connections found in catalog");
          return;
        }

        for (const auto &connStr : mssqlConnStrings) {
          {
            pqxx::work txn(pgConn);
            // REMOVED: Time-based filter that was preventing PK detection
            // updates All connections will be processed to ensure PK
            // information is up to date
          }

          // Connect directly to MSSQL using ODBC
          SQLHENV env;
          SQLHDBC dbc;
          SQLRETURN ret;

          ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
          if (!SQL_SUCCEEDED(ret)) {
            Logger::error(LogCategory::DATABASE, "syncCatalogMSSQLToPostgres",
                          "Failed to allocate ODBC environment handle");
            continue;
          }

          ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION,
                              (SQLPOINTER)SQL_OV_ODBC3, 0);
          if (!SQL_SUCCEEDED(ret)) {
            SQLFreeHandle(SQL_HANDLE_ENV, env);
            Logger::error(LogCategory::DATABASE, "syncCatalogMSSQLToPostgres",
                          "Failed to set ODBC version");
            continue;
          }

          ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
          if (!SQL_SUCCEEDED(ret)) {
            SQLFreeHandle(SQL_HANDLE_ENV, env);
            Logger::error(LogCategory::DATABASE, "syncCatalogMSSQLToPostgres",
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
            Logger::error(LogCategory::DATABASE, "syncCatalogMSSQLToPostgres",
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
          Logger::info(LogCategory::DATABASE,
                       "Found " + std::to_string(discoveredTables.size()) +
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

              // Detectar PK para MSSQL
              std::vector<std::string> pkColumns =
                  detectPrimaryKeyColumnsMSSQL(dbc, schemaName, tableName);
              std::string pkStrategy = determinePKStrategy(pkColumns);
              bool hasPK = !pkColumns.empty();

              // Obtener tamaño de la tabla para ordenamiento
              int64_t tableSize = 0;
              try {
                std::string sizeQuery =
                    "SELECT SUM(rows) FROM sys.dm_db_partition_stats WHERE "
                    "object_id = OBJECT_ID('" +
                    schemaName + "." + tableName + "') AND index_id IN (0,1)";
                // Para MSSQL, usamos una query diferente
                tableSize = 1000; // Valor por defecto para MSSQL
              } catch (const std::exception &e) {
                Logger::warning(LogCategory::DATABASE,
                                "Could not get table size for " + schemaName +
                                    "." + tableName + ": " +
                                    std::string(e.what()));
              }

              pqxx::work txn(pgConn);
              // Verificar si la tabla ya existe (sin importar
              // connection_string)
              auto existingCheck =
                  txn.exec("SELECT last_sync_column, pk_columns, pk_strategy, "
                           "has_pk, table_size FROM metadata.catalog "
                           "WHERE schema_name='" +
                           escapeSQL(schemaName) + "' AND table_name='" +
                           escapeSQL(tableName) + "' AND db_engine='MSSQL';");

              if (!existingCheck.empty()) {
                // Tabla ya existe, verificar si timeColumn cambió
                std::string currentTimeColumn =
                    existingCheck[0][0].is_null()
                        ? ""
                        : existingCheck[0][0].as<std::string>();
                std::string currentPKColumns =
                    existingCheck[0][1].is_null()
                        ? ""
                        : existingCheck[0][1].as<std::string>();
                std::string currentPKStrategy =
                    existingCheck[0][2].is_null()
                        ? ""
                        : existingCheck[0][2].as<std::string>();
                bool currentHasPK = existingCheck[0][3].is_null()
                                        ? false
                                        : existingCheck[0][3].as<bool>();

                bool needsUpdate = false;
                std::string updateQuery = "UPDATE metadata.catalog SET ";

                if (currentTimeColumn != timeColumn) {
                  updateQuery +=
                      "last_sync_column = '" + escapeSQL(timeColumn) + "'";
                  needsUpdate = true;
                }

                if (currentPKColumns != columnsToJSON(pkColumns)) {
                  if (needsUpdate)
                    updateQuery += ", ";
                  updateQuery += "pk_columns = '" +
                                 escapeSQL(columnsToJSON(pkColumns)) + "'";
                  needsUpdate = true;
                }

                if (currentPKStrategy != pkStrategy) {
                  if (needsUpdate)
                    updateQuery += ", ";
                  updateQuery +=
                      "pk_strategy = '" + escapeSQL(pkStrategy) + "'";
                  needsUpdate = true;
                }

                if (currentHasPK != hasPK) {
                  if (needsUpdate)
                    updateQuery += ", ";
                  updateQuery +=
                      "has_pk = " + std::string(hasPK ? "true" : "false");
                  needsUpdate = true;
                }

                // NUEVO: Actualizar table_size siempre para mantenerlo
                // sincronizado
                if (needsUpdate)
                  updateQuery += ", ";
                updateQuery += "table_size = " + std::to_string(tableSize);
                needsUpdate = true;

                if (needsUpdate) {
                  updateQuery += " WHERE schema_name='" +
                                 escapeSQL(schemaName) + "' AND table_name='" +
                                 escapeSQL(tableName) +
                                 "' AND db_engine='MSSQL'";
                  txn.exec(updateQuery);
                }
              } else {
                // Tabla nueva, insertar con toda la información
                txn.exec(
                    "INSERT INTO metadata.catalog "
                    "(schema_name, table_name, cluster_name, db_engine, "
                    "connection_string, last_sync_time, last_sync_column, "
                    "status, last_offset, active, pk_columns, pk_strategy, "
                    "has_pk, table_size) "
                    "VALUES ('" +
                    escapeSQL(schemaName) + "', '" + escapeSQL(tableName) +
                    "', '', 'MSSQL', '" + escapeSQL(connStr) + "', NOW(), '" +
                    escapeSQL(timeColumn) + "', 'PENDING', '0', false, '" +
                    escapeSQL(columnsToJSON(pkColumns)) + "', '" +
                    escapeSQL(pkStrategy) + "', " +
                    std::string(hasPK ? "true" : "false") + ", '" +
                    std::to_string(tableSize) + ");");
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
      } catch (const pqxx::sql_error &e) {
        Logger::error(LogCategory::DATABASE, "syncCatalogMSSQLToPostgres",
                      "SQL ERROR in syncCatalogMSSQLToPostgres: " +
                          std::string(e.what()) +
                          " [SQL State: " + e.sqlstate() + "]");
      } catch (const pqxx::broken_connection &e) {
        Logger::error(LogCategory::DATABASE, "syncCatalogMSSQLToPostgres",
                      "CONNECTION ERROR in syncCatalogMSSQLToPostgres: " +
                          std::string(e.what()));
      } catch (const std::exception &e) {
        Logger::error(LogCategory::DATABASE, "syncCatalogMSSQLToPostgres",
                      "ERROR in syncCatalogMSSQLToPostgres: " +
                          std::string(e.what()));
      }
    }

  private:
    // FUNCIONES PARA DETECCIÓN DE PK
    // NUEVAS FUNCIONES PARA OBTENER TAMAÑO DE TABLAS
    // NUEVAS FUNCIONES PARA DISASTER RECOVERY

    std::vector<std::string> detectPrimaryKeyColumns(
        MYSQL * conn, const std::string &schema, const std::string &table) {
      std::vector<std::string> pkColumns;
      try {
        std::string query = "SELECT COLUMN_NAME "
                            "FROM information_schema.KEY_COLUMN_USAGE "
                            "WHERE TABLE_SCHEMA = '" +
                            escapeSQL(schema) +
                            "' "
                            "AND TABLE_NAME = '" +
                            escapeSQL(table) +
                            "' "
                            "AND CONSTRAINT_NAME = 'PRIMARY' "
                            "ORDER BY ORDINAL_POSITION;";

        Logger::info(LogCategory::DATABASE,
                     "Executing PK detection query: " + query);
        auto results = executeQueryMariaDB(conn, query);
        Logger::info(LogCategory::DATABASE, "PK detection query returned " +
                                                std::to_string(results.size()) +
                                                " rows");

        for (const auto &row : results) {
          if (!row.empty() && !row[0].empty()) {
            Logger::info(LogCategory::DATABASE, "Found PK column: " + row[0]);
            pkColumns.push_back(row[0]);
          }
        }
      } catch (const std::exception &e) {
        Logger::error(LogCategory::DATABASE, "detectPrimaryKeyColumns",
                      "ERROR detecting primary key columns: " +
                          std::string(e.what()));
      }
      return pkColumns;
    }

    std::string determinePKStrategy(const std::vector<std::string> &pkColumns) {
      if (!pkColumns.empty()) {
        return "PK"; // Tabla tiene PK real
      } else {
        return "OFFSET"; // Último recurso: OFFSET pagination
      }
    }

    std::string columnsToJSON(const std::vector<std::string> &columns) {
      if (columns.empty())
        return "[]";

      std::string json = "[";
      for (size_t i = 0; i < columns.size(); ++i) {
        if (i > 0)
          json += ",";
        json += "\"" + escapeSQL(columns[i]) + "\"";
      }
      json += "]";
      return json;
    }

    // FUNCIONES ESPECÍFICAS PARA MSSQL
    std::vector<std::string> detectPrimaryKeyColumnsMSSQL(
        SQLHDBC conn, const std::string &schema, const std::string &table) {
      std::vector<std::string> pkColumns;
      try {
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
            escapeSQL(schema) +
            "' "
            "AND t.name = '" +
            escapeSQL(table) +
            "' "
            "AND i.is_primary_key = 1 "
            "ORDER BY ic.key_ordinal;";

        auto results = executeQueryMSSQL(conn, query);
        for (const auto &row : results) {
          if (!row.empty() && !row[0].empty()) {
            pkColumns.push_back(row[0]);
          }
        }
      } catch (const std::exception &e) {
        Logger::error(LogCategory::DATABASE, "detectPrimaryKeyColumnsMSSQL",
                      "ERROR detecting primary key columns: " +
                          std::string(e.what()));
      }
      return pkColumns;
    }

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
        Logger::error(LogCategory::DATABASE, "detectTimeColumnMSSQL",
                      "ERROR detecting time column: " + std::string(e.what()));
      }
      return "";
    }

    std::string detectTimeColumnMariaDB(MYSQL * conn, const std::string &schema,
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
        Logger::error(LogCategory::DATABASE, "detectTimeColumnMariaDB",
                      "ERROR detecting time column: " + std::string(e.what()));
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

          // Set connection timeouts for large tables - OPTIMIZED
          {
            std::string timeoutQuery =
                "SET SESSION wait_timeout = 600" +                // 10 minutos
                std::string(", interactive_timeout = 600") +      // 10 minutos
                std::string(", net_read_timeout = 600") +         // 10 minutos
                std::string(", net_write_timeout = 600") +        // 10 minutos
                std::string(", innodb_lock_wait_timeout = 600") + // 10 minutos
                std::string(", lock_wait_timeout = 600");         // 10 minutos
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
          ret = SQLDriverConnect(dbc, nullptr,
                                 (SQLCHAR *)connectionString.c_str(), SQL_NTS,
                                 outConnStr, sizeof(outConnStr), &outConnStrLen,
                                 SQL_DRIVER_NOPROMPT);
          if (!SQL_SUCCEEDED(ret)) {
            SQLFreeHandle(SQL_HANDLE_DBC, dbc);
            SQLFreeHandle(SQL_HANDLE_ENV, env);
            return "";
          }
          auto results = executeQueryMSSQL(
              dbc, "SELECT CAST(SERVERPROPERTY('MachineName') "
                   "AS VARCHAR(128)) AS name;");
          if (results.empty() || results[0].empty() ||
              results[0][0] == "NULL" || results[0][0].empty()) {
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

      } catch (...) {
        return "";
      }
      return "";
    }

    std::string extractHostnameFromConnection(
        const std::string &connectionString, const std::string &dbEngine) {
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
        }
      }

      Logger::warning(LogCategory::DATABASE,
                      "No hostname found in connection string for " + dbEngine);
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
          std::transform(dbPart.begin(), dbPart.end(), dbPart.begin(),
                         ::toupper);
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

    void cleanNonExistentPostgresTables(pqxx::connection & pgConn) {
      try {
        pqxx::work txn(pgConn);

        // Limpiar tablas huérfanas que apunten a PostgreSQL como fuente (ya no
        // soportado)
        auto results = txn.exec(
            "DELETE FROM metadata.catalog WHERE db_engine='PostgreSQL';");

        Logger::info(LogCategory::DATABASE,
                     "Removed " + std::to_string(results.affected_rows()) +
                         " PostgreSQL source tables (no longer supported)");

        txn.commit();
      } catch (const pqxx::sql_error &e) {
        Logger::error(
            LogCategory::DATABASE, "cleanNonExistentPostgresTables",
            "SQL ERROR cleaning PostgreSQL tables: " + std::string(e.what()) +
                " [SQL State: " + e.sqlstate() + "]");
      } catch (const pqxx::broken_connection &e) {
        Logger::error(LogCategory::DATABASE, "cleanNonExistentPostgresTables",
                      "CONNECTION ERROR cleaning PostgreSQL tables: " +
                          std::string(e.what()));
      } catch (const std::exception &e) {
        Logger::error(LogCategory::DATABASE, "cleanNonExistentPostgresTables",
                      "ERROR cleaning PostgreSQL tables: " +
                          std::string(e.what()));
      }
    }

    void cleanNonExistentMariaDBTables(pqxx::connection & pgConn) {
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
            Logger::warning(LogCategory::DATABASE,
                            "cleanNonExistentMariaDBTables",
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
            Logger::warning(LogCategory::DATABASE,
                            "cleanNonExistentMariaDBTables",
                            "MariaDB connection failed: " +
                                std::string(mysql_error(mariadbConn)));
            mysql_close(mariadbConn);
            continue;
          }

          // Set connection timeouts for large tables - OPTIMIZED
          {
            std::string timeoutQuery =
                "SET SESSION wait_timeout = 600" +                // 10 minutos
                std::string(", interactive_timeout = 600") +      // 10 minutos
                std::string(", net_read_timeout = 600") +         // 10 minutos
                std::string(", net_write_timeout = 600") +        // 10 minutos
                std::string(", innodb_lock_wait_timeout = 600") + // 10 minutos
                std::string(", lock_wait_timeout = 600");         // 10 minutos
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
              Logger::info(LogCategory::DATABASE,
                           "Removing non-existent MariaDB table: " +
                               schema_name + "." + table_name);

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
      } catch (const pqxx::sql_error &e) {
        Logger::error(
            LogCategory::DATABASE, "cleanNonExistentMariaDBTables",
            "SQL ERROR cleaning MariaDB tables: " + std::string(e.what()) +
                " [SQL State: " + e.sqlstate() + "]");
      } catch (const pqxx::broken_connection &e) {
        Logger::error(LogCategory::DATABASE, "cleanNonExistentMariaDBTables",
                      "CONNECTION ERROR cleaning MariaDB tables: " +
                          std::string(e.what()));
      } catch (const std::exception &e) {
        Logger::error(LogCategory::DATABASE, "cleanNonExistentMariaDBTables",
                      "ERROR cleaning MariaDB tables: " +
                          std::string(e.what()));
      }
    }

    void cleanNonExistentMSSQLTables(pqxx::connection & pgConn) {
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
            Logger::warning(LogCategory::DATABASE,
                            "cleanNonExistentMSSQLTables",
                            "Failed to allocate ODBC environment handle");
            continue;
          }

          ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION,
                              (SQLPOINTER)SQL_OV_ODBC3, 0);
          if (!SQL_SUCCEEDED(ret)) {
            SQLFreeHandle(SQL_HANDLE_ENV, env);
            Logger::warning(LogCategory::DATABASE,
                            "cleanNonExistentMSSQLTables",
                            "Failed to set ODBC version");
            continue;
          }

          ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
          if (!SQL_SUCCEEDED(ret)) {
            SQLFreeHandle(SQL_HANDLE_ENV, env);
            Logger::warning(LogCategory::DATABASE,
                            "cleanNonExistentMSSQLTables",
                            "Failed to allocate ODBC connection handle");
            continue;
          }

          SQLCHAR outConnStr[1024];
          SQLSMALLINT outConnStrLen;
          ret = SQLDriverConnect(dbc, nullptr,
                                 (SQLCHAR *)connection_string.c_str(), SQL_NTS,
                                 outConnStr, sizeof(outConnStr), &outConnStrLen,
                                 SQL_DRIVER_NOPROMPT);
          if (!SQL_SUCCEEDED(ret)) {
            SQLCHAR sqlState[6], msg[SQL_MAX_MESSAGE_LENGTH];
            SQLINTEGER nativeError;
            SQLSMALLINT msgLen;
            SQLGetDiagRec(SQL_HANDLE_DBC, dbc, 1, sqlState, &nativeError, msg,
                          sizeof(msg), &msgLen);
            SQLFreeHandle(SQL_HANDLE_DBC, dbc);
            SQLFreeHandle(SQL_HANDLE_ENV, env);
            Logger::warning(
                LogCategory::DATABASE, "cleanNonExistentMSSQLTables",
                "Failed to connect to MSSQL: " + std::string((char *)msg));
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
              Logger::info(LogCategory::DATABASE,
                           "Removing non-existent MSSQL table: " + schema_name +
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
      } catch (const pqxx::sql_error &e) {
        Logger::error(
            LogCategory::DATABASE, "cleanNonExistentMSSQLTables",
            "SQL ERROR cleaning MSSQL tables: " + std::string(e.what()) +
                " [SQL State: " + e.sqlstate() + "]");
      } catch (const pqxx::broken_connection &e) {
        Logger::error(LogCategory::DATABASE, "cleanNonExistentMSSQLTables",
                      "CONNECTION ERROR cleaning MSSQL tables: " +
                          std::string(e.what()));
      } catch (const std::exception &e) {
        Logger::error(LogCategory::DATABASE, "cleanNonExistentMSSQLTables",
                      "ERROR cleaning MSSQL tables: " + std::string(e.what()));
      }
    }

    void cleanOrphanedTables(pqxx::connection & pgConn) {
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
      } catch (const pqxx::sql_error &e) {
        Logger::error(
            LogCategory::DATABASE, "cleanOrphanedTables",
            "SQL ERROR cleaning orphaned tables: " + std::string(e.what()) +
                " [SQL State: " + e.sqlstate() + "]");
      } catch (const pqxx::broken_connection &e) {
        Logger::error(LogCategory::DATABASE, "cleanOrphanedTables",
                      "CONNECTION ERROR cleaning orphaned tables: " +
                          std::string(e.what()));
      } catch (const std::exception &e) {
        Logger::error(LogCategory::DATABASE, "cleanOrphanedTables",
                      "ERROR cleaning orphaned tables: " +
                          std::string(e.what()));
      }
    }

    void cleanInvalidOffsetValues() {
      try {
        pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());
        pqxx::work txn(pgConn);

        // Limpiar last_offset en tablas con estrategia PK (deberían usar
        // last_processed_pk)
        auto pkResult =
            txn.exec("UPDATE metadata.catalog SET last_offset = NULL "
                     "WHERE pk_strategy = 'PK' AND last_offset IS NOT NULL;");

        // Limpiar last_processed_pk en tablas con estrategia OFFSET (deberían
        // usar last_offset)
        auto offsetResult = txn.exec(
            "UPDATE metadata.catalog SET last_processed_pk = NULL "
            "WHERE pk_strategy = 'OFFSET' AND last_processed_pk IS NOT NULL;");

        txn.commit();

        Logger::info(LogCategory::DATABASE,
                     "Cleaned " + std::to_string(pkResult.affected_rows()) +
                         " PK strategy tables with invalid last_offset values");
        Logger::info(LogCategory::DATABASE,
                     "Cleaned " + std::to_string(offsetResult.affected_rows()) +
                         " OFFSET strategy tables with invalid "
                         "last_processed_pk values");

      } catch (const pqxx::sql_error &e) {
        Logger::error(LogCategory::DATABASE, "cleanInvalidOffsetValues",
                      "SQL ERROR cleaning invalid offset values: " +
                          std::string(e.what()) +
                          " [SQL State: " + e.sqlstate() + "]");
      } catch (const pqxx::broken_connection &e) {
        Logger::error(LogCategory::DATABASE, "cleanInvalidOffsetValues",
                      "CONNECTION ERROR cleaning invalid offset values: " +
                          std::string(e.what()));
      } catch (const std::exception &e) {
        Logger::error(LogCategory::DATABASE, "cleanInvalidOffsetValues",
                      "ERROR cleaning invalid offset values: " +
                          std::string(e.what()));
      }
    }

    std::vector<std::vector<std::string>> executeQueryMariaDB(
        MYSQL * conn, const std::string &query) {
      std::vector<std::vector<std::string>> results;
      if (!conn) {
        Logger::error(LogCategory::DATABASE, "executeQueryMariaDB",
                      "No valid MariaDB connection");
        return results;
      }

      if (mysql_query(conn, query.c_str())) {
        Logger::error(LogCategory::DATABASE, "executeQueryMariaDB",
                      "Query execution failed: " +
                          std::string(mysql_error(conn)));
        return results;
      }

      MYSQL_RES *res = mysql_store_result(conn);
      if (!res) {
        if (mysql_field_count(conn) > 0) {
          Logger::error(LogCategory::DATABASE, "executeQueryMariaDB",
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

    std::vector<std::vector<std::string>> executeQueryMSSQL(
        SQLHDBC conn, const std::string &query) {
      std::vector<std::vector<std::string>> results;
      if (!conn) {
        Logger::error(LogCategory::DATABASE, "executeQueryMSSQL",
                      "No valid MSSQL connection");
        return results;
      }

      SQLHSTMT stmt;
      SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, conn, &stmt);
      if (ret != SQL_SUCCESS) {
        Logger::error(LogCategory::DATABASE, "executeQueryMSSQL",
                      "SQLAllocHandle(STMT) failed");
        return results;
      }

      ret = SQLExecDirect(stmt, (SQLCHAR *)query.c_str(), SQL_NTS);
      if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        Logger::error(LogCategory::DATABASE, "executeQueryMSSQL",
                      "SQLExecDirect failed");
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

    std::pair<int, int> getColumnCountsMariaDB(
        const std::string &connectionString, const std::string &schema,
        const std::string &table) {
      try {
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
          return {0, 0};

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
          return {0, 0};
        }

        std::string sourceQuery =
            "SELECT COUNT(*) FROM information_schema.columns "
            "WHERE table_schema = '" +
            escapeSQL(schema) +
            "' "
            "AND table_name = '" +
            escapeSQL(table) + "'";

        auto sourceResults = executeQueryMariaDB(conn, sourceQuery);
        mysql_close(conn);

        int sourceCount = 0;
        if (!sourceResults.empty() && !sourceResults[0].empty()) {
          try {
            sourceCount = std::stoi(sourceResults[0][0]);
          } catch (...) {
            sourceCount = 0;
          }
        }

        pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());
        pqxx::work txn(pgConn);
        std::string lowerSchema = schema;
        std::transform(lowerSchema.begin(), lowerSchema.end(),
                       lowerSchema.begin(), ::tolower);
        std::string lowerTable = table;
        std::transform(lowerTable.begin(), lowerTable.end(), lowerTable.begin(),
                       ::tolower);
        std::string targetQuery =
            "SELECT COUNT(*) FROM information_schema.columns "
            "WHERE table_schema = '" +
            escapeSQL(lowerSchema) +
            "' "
            "AND table_name = '" +
            escapeSQL(lowerTable) + "'";
        auto targetResults = txn.exec(targetQuery);
        txn.commit();

        int targetCount = 0;
        if (!targetResults.empty() && !targetResults[0][0].is_null()) {
          targetCount = targetResults[0][0].as<int>();
        }

        return {sourceCount, targetCount};
      } catch (const std::exception &e) {
        Logger::error(LogCategory::DATABASE, "getColumnCountsMariaDB",
                      "ERROR getting column counts: " + std::string(e.what()));
        return {0, 0};
      }
    }

    std::pair<int, int> getColumnCountsMSSQL(
        const std::string &connectionString, const std::string &schema,
        const std::string &table) {
      try {
        SQLHENV env;
        SQLHDBC dbc;
        SQLRETURN ret;

        ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
        if (!SQL_SUCCEEDED(ret))
          return {0, 0};

        ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION,
                            (SQLPOINTER)SQL_OV_ODBC3, 0);
        if (!SQL_SUCCEEDED(ret)) {
          SQLFreeHandle(SQL_HANDLE_ENV, env);
          return {0, 0};
        }

        ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
        if (!SQL_SUCCEEDED(ret)) {
          SQLFreeHandle(SQL_HANDLE_ENV, env);
          return {0, 0};
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
          return {0, 0};
        }

        std::string sourceQuery =
            "SELECT COUNT(*) FROM sys.columns c "
            "INNER JOIN sys.tables t ON c.object_id = t.object_id "
            "INNER JOIN sys.schemas s ON t.schema_id = s.schema_id "
            "WHERE s.name = '" +
            escapeSQL(schema) +
            "' "
            "AND t.name = '" +
            escapeSQL(table) + "'";

        auto sourceResults = executeQueryMSSQL(dbc, sourceQuery);
        SQLDisconnect(dbc);
        SQLFreeHandle(SQL_HANDLE_DBC, dbc);
        SQLFreeHandle(SQL_HANDLE_ENV, env);

        int sourceCount = 0;
        if (!sourceResults.empty() && !sourceResults[0].empty()) {
          try {
            sourceCount = std::stoi(sourceResults[0][0]);
          } catch (...) {
            sourceCount = 0;
          }
        }

        pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());
        pqxx::work txn(pgConn);
        std::string lowerSchema = schema;
        std::transform(lowerSchema.begin(), lowerSchema.end(),
                       lowerSchema.begin(), ::tolower);
        std::string lowerTable = table;
        std::transform(lowerTable.begin(), lowerTable.end(), lowerTable.begin(),
                       ::tolower);
        std::string targetQuery =
            "SELECT COUNT(*) FROM information_schema.columns "
            "WHERE table_schema = '" +
            escapeSQL(lowerSchema) +
            "' "
            "AND table_name = '" +
            escapeSQL(lowerTable) + "'";
        auto targetResults = txn.exec(targetQuery);
        txn.commit();

        int targetCount = 0;
        if (!targetResults.empty() && !targetResults[0][0].is_null()) {
          targetCount = targetResults[0][0].as<int>();
        }

        return {sourceCount, targetCount};
      } catch (const std::exception &e) {
        Logger::error(LogCategory::DATABASE, "getColumnCountsMSSQL",
                      "ERROR getting column counts: " + std::string(e.what()));
        return {0, 0};
      }
    }
  };

#endif // CATALOG_MANAGER_H
