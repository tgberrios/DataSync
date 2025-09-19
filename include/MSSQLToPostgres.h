#ifndef MSSQLTOPOSTGRES_H
#define MSSQLTOPOSTGRES_H

#include "Config.h"
#include "ConnectionPool.h"
#include "SyncReporter.h"
#include "logger.h"
#include <algorithm>
#include <atomic>
#include <cctype>
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

class MSSQLToPostgres {
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
  };

  std::vector<TableInfo> getActiveTables(pqxx::connection &pgConn) {
    std::vector<TableInfo> data;

    try {
      pqxx::work txn(pgConn);
      auto results =
          txn.exec("SELECT schema_name, table_name, cluster_name, db_engine, "
                   "connection_string, last_sync_time, last_sync_column, "
                   "status, last_offset "
                   "FROM metadata.catalog "
                   "WHERE active=true AND db_engine='MSSQL' "
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
    ConnectionGuard mssqlGuard(g_connectionPool.get(), DatabaseType::MSSQL);
    auto mssqlConn = mssqlGuard.get<ODBCHandles>();
    if (!mssqlConn) {
      Logger::error("syncIndexesAndConstraints",
                    "Failed to connect to MSSQL for " + schema_name + "." +
                        table_name);
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

    auto results = executeQueryMSSQL(mssqlConn->dbc, query);

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
      } catch (const std::exception &e) {
        Logger::error("syncIndexesAndConstraints",
                      "Error creating index '" + indexName +
                          "': " + std::string(e.what()));
      }
    }
  }

  void setupTableTargetMSSQLToPostgres() {
    try {
      pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());
      auto tables = getActiveTables(pgConn);

      for (const auto &table : tables) {
        if (table.db_engine != "MSSQL")
          continue;

        ConnectionGuard mssqlGuard(g_connectionPool.get(), DatabaseType::MSSQL);
        auto mssqlConn = mssqlGuard.get<ODBCHandles>();
        if (!mssqlConn) {
          Logger::error("setupTableTargetMSSQLToPostgres",
                        "Failed to connect to MSSQL for " + table.schema_name +
                            "." + table.table_name);
          continue;
        }

        // Usar USE [database] para cambiar el contexto de base de datos
        std::string databaseName = extractDatabaseName(table.connection_string);
        Logger::debug("setupTableTargetMSSQLToPostgres",
                      "Processing table " + table.schema_name + "." +
                          table.table_name + " with database: " + databaseName +
                          " from connection: " + table.connection_string);

        // Primero cambiar a la base de datos correcta
        std::string useQuery = "USE [" + databaseName + "];";
        auto useResult = executeQueryMSSQL(mssqlConn->dbc, useQuery);

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

        auto columns = executeQueryMSSQL(mssqlConn->dbc, query);

        if (columns.empty()) {
          Logger::error("setupTableTargetMSSQLToPostgres",
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
          if (dataType == "int") {
            pgType = "INTEGER";
          } else if (dataType == "bigint") {
            pgType = "BIGINT";
          } else if (dataType == "smallint") {
            pgType = "SMALLINT";
          } else if (dataType == "tinyint") {
            pgType = "SMALLINT";
          } else if (dataType == "bit") {
            pgType = "BOOLEAN";
          } else if (dataType == "decimal") {
            // Para decimal, usar la precisión y escala de MSSQL
            if (!numericPrecision.empty() && numericPrecision != "NULL" &&
                !numericScale.empty() && numericScale != "NULL") {
              pgType = "NUMERIC(" + numericPrecision + "," + numericScale + ")";
            } else {
              pgType = "NUMERIC(18,4)";
            }
          } else if (dataType == "numeric") {
            // Para numeric, usar la precisión y escala de MSSQL
            if (!numericPrecision.empty() && numericPrecision != "NULL" &&
                !numericScale.empty() && numericScale != "NULL") {
              pgType = "NUMERIC(" + numericPrecision + "," + numericScale + ")";
            } else {
              pgType = "NUMERIC(18,4)";
            }
          } else if (dataType == "float") {
            pgType = "REAL";
          } else if (dataType == "real") {
            pgType = "REAL";
          } else if (dataType == "money") {
            pgType = "NUMERIC(19,4)";
          } else if (dataType == "smallmoney") {
            pgType = "NUMERIC(10,4)";
          } else if (dataType == "varchar") {
            pgType =
                (!maxLength.empty() && maxLength != "NULL" && maxLength != "-1")
                    ? "VARCHAR(" + maxLength + ")"
                    : "VARCHAR";
          } else if (dataType == "nvarchar") {
            pgType =
                (!maxLength.empty() && maxLength != "NULL" && maxLength != "-1")
                    ? "VARCHAR(" + maxLength + ")"
                    : "VARCHAR";
          } else if (dataType == "char") {
            pgType = (!maxLength.empty() && maxLength != "NULL")
                         ? "CHAR(" + maxLength + ")"
                         : "CHAR(1)";
          } else if (dataType == "nchar") {
            pgType = (!maxLength.empty() && maxLength != "NULL")
                         ? "CHAR(" + maxLength + ")"
                         : "CHAR(1)";
          } else if (dataType == "text") {
            pgType = "TEXT";
          } else if (dataType == "ntext") {
            pgType = "TEXT";
          } else if (dataType == "datetime") {
            pgType = "TIMESTAMP";
          } else if (dataType == "datetime2") {
            pgType = "TIMESTAMP";
          } else if (dataType == "smalldatetime") {
            pgType = "TIMESTAMP";
          } else if (dataType == "date") {
            pgType = "DATE";
          } else if (dataType == "time") {
            pgType = "TIME";
          } else if (dataType == "datetimeoffset") {
            pgType = "TIMESTAMP WITH TIME ZONE";
          } else if (dataType == "uniqueidentifier") {
            pgType = "UUID";
          } else if (dataType == "varbinary") {
            pgType = "BYTEA";
          } else if (dataType == "image") {
            pgType = "BYTEA";
          } else if (dataType == "binary") {
            pgType = "BYTEA";
          } else if (dataType == "xml") {
            pgType = "TEXT";
          } else if (dataType == "sql_variant") {
            pgType = "TEXT";
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
      }
    } catch (const std::exception &e) {
      Logger::error("setupTableTargetMSSQLToPostgres",
                    "Error in setupTableTargetMSSQLToPostgres: " +
                        std::string(e.what()));
    }
  }

  void transferDataMSSQLToPostgres() {
    try {
      pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());
      auto tables = getActiveTables(pgConn);

      for (auto &table : tables) {
        if (table.db_engine != "MSSQL")
          continue;

        // Actualizar tabla actualmente procesando para el dashboard
        SyncReporter::currentProcessingTable = table.schema_name + "." +
                                               table.table_name + " (" +
                                               table.status + ")";

        ConnectionGuard mssqlGuard(g_connectionPool.get(), DatabaseType::MSSQL);
        auto mssqlConn = mssqlGuard.get<ODBCHandles>();
        if (!mssqlConn) {
          Logger::error("transferDataMSSQLToPostgres",
                        "Failed to connect to MSSQL for " + table.schema_name +
                            "." + table.table_name);
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
        auto useResult = executeQueryMSSQL(mssqlConn->dbc, useQuery);

        auto countRes = executeQueryMSSQL(
            mssqlConn->dbc,
            "SELECT COUNT(*) FROM [" + schema_name + "].[" + table_name + "];");
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
          Logger::debug("transferDataMSSQLToPostgres",
                        "Target table might not exist yet: " +
                            std::string(e.what()));
        }

        // Lógica simple basada en counts reales
        if (sourceCount == 0) {
          if (targetCount == 0) {
            updateStatus(pgConn, schema_name, table_name, "NO_DATA", 0);
          } else {
            Logger::warning(
                "transferDataMSSQLToPostgres",
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
            Logger::info("transferDataMSSQLToPostgres",
                         "Processing updates for " + schema_name + "." +
                             table_name +
                             " using time column: " + table.last_sync_column +
                             " since: " + table.last_sync_time);
            processUpdatesByPrimaryKey(schema_name, table_name, mssqlConn->dbc,
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
          Logger::info("transferDataMSSQLToPostgres",
                       "Detected " + std::to_string(targetCount - sourceCount) +
                           " deleted records in " + schema_name + "." +
                           table_name + " - processing deletes");
          processDeletesByPrimaryKey(schema_name, table_name, mssqlConn->dbc,
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
          Logger::info("transferDataMSSQLToPostgres",
                       "After deletes: source=" + std::to_string(sourceCount) +
                           ", target=" + std::to_string(targetCount));
        }

        // Luego ejecutar la query sin prefijo de base de datos
        auto columns = executeQueryMSSQL(
            mssqlConn->dbc,
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
          Logger::error("transferDataMSSQLToPostgres",
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

        for (const auto &col : columns) {
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
              "transferDataMSSQLToPostgres",
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
            Logger::info("transferDataMSSQLToPostgres",
                         "Truncating table: " + lowerSchemaName + "." +
                             table_name);
            pqxx::work txn(pgConn);
            txn.exec("TRUNCATE TABLE \"" + lowerSchemaName + "\".\"" +
                     table_name + "\" CASCADE;");
            txn.commit();
            Logger::debug("transferDataMSSQLToPostgres",
                          "Table truncated successfully");
          }
        } else if (table.status == "RESET") {
          Logger::info("transferDataMSSQLToPostgres",
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

        // Transferir datos faltantes usando OFFSET
        bool hasMoreData = true;
        while (hasMoreData) {
          const size_t CHUNK_SIZE = SyncConfig::getChunkSize();

          // Asegurar que estamos en la base de datos correcta
          executeQueryMSSQL(mssqlConn->dbc, "USE [" + databaseName + "];");

          std::string selectQuery =
              "SELECT * FROM [" + schema_name + "].[" + table_name + "]";

          // Usar last_offset para paginación simple y eficiente
          selectQuery += " ORDER BY 1 OFFSET " + std::to_string(targetCount) +
                         " ROWS FETCH NEXT " + std::to_string(CHUNK_SIZE) +
                         " ROWS ONLY;";

          auto results = executeQueryMSSQL(mssqlConn->dbc, selectQuery);

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
            for (const auto &row : results) {
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
                pqxx::work txn(pgConn);
                std::string tableName =
                    "\"" + lowerSchemaName + "\".\"" + table_name + "\"";
                pqxx::stream_to stream(txn, tableName);

                for (const auto &row : results) {
                  if (row.size() == columnNames.size()) {
                    std::vector<std::optional<std::string>> values;
                    for (size_t i = 0; i < row.size(); ++i) {
                      if (row[i] == "NULL" || row[i].empty()) {
                        // Para columnas que podrían ser NOT NULL, usar valores
                        // por defecto apropiados
                        std::string defaultValue = "NO_DATA";
                        std::string columnType = columnTypes[i];
                        std::transform(columnType.begin(), columnType.end(),
                                       columnType.begin(), ::toupper);

                        if (columnType.find("TIMESTAMP") != std::string::npos ||
                            columnType.find("DATETIME") != std::string::npos) {
                          defaultValue = "1970-01-01 00:00:00";
                        } else if (columnType.find("DATE") !=
                                   std::string::npos) {
                          defaultValue = "1970-01-01";
                        } else if (columnType.find("TIME") !=
                                   std::string::npos) {
                          defaultValue = "00:00:00";
                        } else if (columnType.find("INT") !=
                                       std::string::npos ||
                                   columnType.find("BIGINT") !=
                                       std::string::npos ||
                                   columnType.find("SMALLINT") !=
                                       std::string::npos ||
                                   columnType.find("TINYINT") !=
                                       std::string::npos) {
                          defaultValue = "0";
                        } else if (columnType.find("DECIMAL") !=
                                       std::string::npos ||
                                   columnType.find("NUMERIC") !=
                                       std::string::npos ||
                                   columnType.find("FLOAT") !=
                                       std::string::npos ||
                                   columnType.find("DOUBLE") !=
                                       std::string::npos) {
                          defaultValue = "0.0";
                        } else if (columnType.find("BOOLEAN") !=
                                       std::string::npos ||
                                   columnType.find("BOOL") !=
                                       std::string::npos) {
                          defaultValue = "false";
                        }

                        values.push_back(defaultValue);
                      } else {

                        std::string cleanValue = row[i];

                        // Transformar fechas inválidas de MSSQL a fechas
                        // válidas para PostgreSQL
                        if (columnTypes[i].find("TIMESTAMP") !=
                                std::string::npos ||
                            columnTypes[i].find("DATETIME") !=
                                std::string::npos ||
                            columnTypes[i].find("DATE") != std::string::npos) {
                          if (cleanValue == "0000-00-00 00:00:00" ||
                              cleanValue == "0000-00-00") {
                            cleanValue = "1970-01-01 00:00:00";
                          } else if (cleanValue.find("0000-00-00") !=
                                     std::string::npos) {
                            cleanValue = "1970-01-01 00:00:00";
                          }
                        }

                        for (char &c : cleanValue) {
                          if (static_cast<unsigned char>(c) > 127) {
                            c = '?';
                          }
                        }

                        cleanValue.erase(
                            std::remove_if(cleanValue.begin(), cleanValue.end(),
                                           [](unsigned char c) {
                                             return c < 32 && c != 9 &&
                                                    c != 10 && c != 13;
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
              } catch (const std::exception &e) {
                Logger::error("transferDataMSSQLToPostgres",
                              "COPY failed: " + std::string(e.what()));
                rowsInserted = 0;
              }
            }

          } catch (const std::exception &e) {
            Logger::error("transferDataMSSQLToPostgres",
                          "Error processing data: " + std::string(e.what()));
          }

          // Always update targetCount and last_offset, even if COPY failed
          targetCount += rowsInserted;

          // If COPY failed but we have data, advance the offset by 1
          if (rowsInserted == 0 && !results.empty()) {
            targetCount += 1; // Advance by 1 to skip the problematic record
            Logger::info("transferDataMSSQLToPostgres",
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
            Logger::debug("transferDataMSSQLToPostgres",
                          "Updated last_offset to " +
                              std::to_string(targetCount) + " for " +
                              schema_name + "." + table_name);
          } catch (const std::exception &e) {
            Logger::warning("transferDataMSSQLToPostgres",
                            "Failed to update last_offset: " +
                                std::string(e.what()));
          }

          if (targetCount >= sourceCount) {
            hasMoreData = false;
          }
        }

        if (targetCount > 0) {
          if (targetCount >= sourceCount) {
            Logger::info("transferDataMSSQLToPostgres",
                         "Table " + schema_name + "." + table_name +
                             " synchronized - PERFECT_MATCH");
            updateStatus(pgConn, schema_name, table_name, "PERFECT_MATCH",
                         targetCount);
          } else {
            Logger::info("transferDataMSSQLToPostgres",
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
      Logger::error("transferDataMSSQLToPostgres",
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
      Logger::error("getLastSyncTimeOptimized",
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
    } catch (const std::exception &e) {
      Logger::error("updateStatus",
                    "Error updating status: " + std::string(e.what()));
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

        // 3. Verificar cuáles PKs no existen en MSSQL
        std::vector<std::vector<std::string>> deletedPKs =
            findDeletedPrimaryKeys(mssqlConn, schema_name, table_name, pgPKs,
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
                                  SQLHDBC mssqlConn, pqxx::connection &pgConn,
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
          getPrimaryKeyColumns(mssqlConn, schema_name, table_name);

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

      // 2. Obtener registros modificados desde MSSQL
      std::string selectQuery = "SELECT * FROM [" + schema_name + "].[" +
                                table_name + "] WHERE [" + timeColumn +
                                "] > '" + escapeSQL(lastSyncTime) +
                                "' ORDER BY [" + timeColumn + "]";

      auto modifiedRecords = executeQueryMSSQL(mssqlConn, selectQuery);
      Logger::debug("processUpdatesByPrimaryKey",
                    "Found " + std::to_string(modifiedRecords.size()) +
                        " modified records in MSSQL");

      if (modifiedRecords.empty()) {
        Logger::debug("processUpdatesByPrimaryKey",
                      "No modified records found for " + schema_name + "." +
                          table_name);
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
        std::transform(columnName.begin(), columnName.end(), columnName.begin(),
                       ::tolower);
        std::string newValue = newRecord[i];

        // Obtener valor actual de PostgreSQL
        std::string currentValue =
            currentRow[i].is_null() ? "" : currentRow[i].as<std::string>();

        // Comparar valores (normalizar para comparación)
        if (currentValue != newValue) {
          updateFields.push_back(
              "\"" + columnName + "\" = " +
              (newValue.empty() ? "NULL" : "'" + escapeSQL(newValue) + "'"));
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

private:
  std::vector<std::string> getPrimaryKeyColumns(SQLHDBC mssqlConn,
                                                const std::string &schema_name,
                                                const std::string &table_name) {
    std::vector<std::string> pkColumns;

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

    Logger::debug("getPrimaryKeyColumns", "Executing query: " + query +
                                              " for " + schema_name + "." +
                                              table_name);
    auto results = executeQueryMSSQL(mssqlConn, query);
    Logger::debug("getPrimaryKeyColumns",
                  "Query returned " + std::to_string(results.size()) +
                      " rows for " + schema_name + "." + table_name);

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
    const size_t CHECK_BATCH_SIZE = 500;

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
        for (size_t i = 0; i < pkColumns.size(); ++i) {
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
      SQLCHAR sqlState[6];
      SQLCHAR errorMsg[SQL_MAX_MESSAGE_LENGTH];
      SQLINTEGER nativeError;
      SQLSMALLINT msgLen;

      SQLGetDiagRec(SQL_HANDLE_STMT, stmt, 1, sqlState, &nativeError, errorMsg,
                    sizeof(errorMsg), &msgLen);

      Logger::error(
          "executeQueryMSSQL",
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
