#ifndef POSTGRESTOPOSTGRES_H
#define POSTGRESTOPOSTGRES_H

#include "Config.h"
#include "logger.h"
#include <pqxx/pqxx>
#include <string>
#include <vector>

class PostgresToPostgres {
public:
  PostgresToPostgres() = default;
  ~PostgresToPostgres() = default;

  void setupTableTargetPostgresToPostgres() {
    try {
      Logger::info("setupTableTargetPostgresToPostgres",
                   "Starting PostgreSQL target table setup");
      pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());

      pqxx::work txn(pgConn);
      auto results = txn.exec("SELECT schema_name, table_name, "
                              "connection_string FROM metadata.catalog "
                              "WHERE db_engine='PostgreSQL' AND active=true;");

      for (const auto &row : results) {
        if (row.size() < 3)
          continue;

        std::string schemaName = row[0].as<std::string>();
        std::string tableName = row[1].as<std::string>();
        std::string sourceConnStr = row[2].as<std::string>();

        Logger::debug("setupTableTargetPostgresToPostgres",
                      "Setting up table: " + schemaName + "." + tableName);

        try {
          auto sourceConn = connectPostgres(sourceConnStr);
          if (!sourceConn) {
            Logger::error("setupTableTargetPostgresToPostgres",
                          "Failed to connect to source PostgreSQL");
            continue;
          }

          std::string lowerSchemaName = toLowerCase(schemaName);
          createSchemaIfNotExists(txn, lowerSchemaName);

          std::string createTableQuery = buildCreateTableQuery(
              *sourceConn, schemaName, tableName, lowerSchemaName);
          if (!createTableQuery.empty()) {
            txn.exec(createTableQuery);
            Logger::info("setupTableTargetPostgresToPostgres",
                         "Created target table: " + lowerSchemaName + "." +
                             tableName);
          }

        } catch (const std::exception &e) {
          Logger::error("setupTableTargetPostgresToPostgres",
                        "Error setting up table " + schemaName + "." +
                            tableName + ": " + e.what());
        }
      }

      txn.commit();
      Logger::info("setupTableTargetPostgresToPostgres",
                   "Target table setup completed");
    } catch (const std::exception &e) {
      Logger::error("setupTableTargetPostgresToPostgres",
                    "Error in setupTableTargetPostgresToPostgres: " +
                        std::string(e.what()));
    }
  }

  void transferDataPostgresToPostgres() {
    try {
      pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());

      {
        pqxx::work txn(pgConn);
        auto results =
            txn.exec("SELECT schema_name, table_name, connection_string, "
                     "last_offset, status, last_sync_column, last_sync_time "
                     "FROM metadata.catalog "
                     "WHERE db_engine='PostgreSQL' AND active=true AND status "
                     "!= 'NO_DATA';");

        for (const auto &row : results) {
          if (row.size() < 7)
            continue;

          std::string schemaName = row[0].as<std::string>();
          std::string tableName = row[1].as<std::string>();
          std::string sourceConnStr = row[2].as<std::string>();
          std::string lastOffset = row[3].as<std::string>();
          std::string status = row[4].as<std::string>();
          std::string lastSyncColumn =
              row[5].is_null() ? "" : row[5].as<std::string>();
          std::string lastSyncTime =
              row[6].is_null() ? "" : row[6].as<std::string>();

          Logger::debug("transferDataPostgresToPostgres",
                        "Processing table: " + schemaName + "." + tableName +
                            " (status: " + status + ")");

          try {
            processTableWithDeltas(pgConn, schemaName, tableName, sourceConnStr,
                                   lastOffset, status, lastSyncColumn,
                                   lastSyncTime);
          } catch (const std::exception &e) {
            Logger::error("transferDataPostgresToPostgres",
                          "Error processing table " + schemaName + "." +
                              tableName + ": " + e.what());
            updateStatus(pgConn, schemaName, tableName, "ERROR", 0);
          }
        }

        txn.commit();
      }
    } catch (const std::exception &e) {
      Logger::error("transferDataPostgresToPostgres",
                    "Error in transferDataPostgresToPostgres: " +
                        std::string(e.what()));
    }
  }

private:
  std::unique_ptr<pqxx::connection>
  connectPostgres(const std::string &connStr) {
    try {
      auto conn = std::make_unique<pqxx::connection>(connStr);
      if (conn->is_open()) {
        return conn;
      } else {
        Logger::error("connectPostgres",
                      "Failed to open PostgreSQL connection");
        return nullptr;
      }
    } catch (const std::exception &e) {
      Logger::error("connectPostgres",
                    "Connection failed: " + std::string(e.what()));
      return nullptr;
    }
  }

  std::string toLowerCase(const std::string &str) {
    std::string result = str;
    std::transform(result.begin(), result.end(), result.begin(), ::tolower);
    return result;
  }

  void createSchemaIfNotExists(pqxx::work &txn, const std::string &schemaName) {
    txn.exec("CREATE SCHEMA IF NOT EXISTS \"" + schemaName + "\";");
  }

  std::string buildCreateTableQuery(pqxx::connection &sourceConn,
                                    const std::string &sourceSchema,
                                    const std::string &tableName,
                                    const std::string &targetSchema) {
    try {
      pqxx::work sourceTxn(sourceConn);
      auto results = sourceTxn.exec(
          "SELECT column_name, data_type, is_nullable, column_default "
          "FROM information_schema.columns "
          "WHERE table_schema = '" +
          sourceSchema + "' AND table_name = '" + tableName +
          "' "
          "ORDER BY ordinal_position;");

      if (results.empty()) {
        Logger::warning("buildCreateTableQuery", "No columns found for table " +
                                                     sourceSchema + "." +
                                                     tableName);
        return "";
      }

      std::string createQuery = "CREATE TABLE IF NOT EXISTS \"" + targetSchema +
                                "\".\"" + tableName + "\" (";
      std::vector<std::string> columns;

      for (const auto &row : results) {
        std::string colName = row[0].as<std::string>();
        std::string dataType = row[1].as<std::string>();
        std::string isNullable = row[2].as<std::string>();
        std::string defaultValue =
            row[3].is_null() ? "" : row[3].as<std::string>();

        std::string columnDef = "\"" + colName + "\" " + dataType;

        // Siempre permitir NULL en todas las columnas
        // if (isNullable == "NO") {
        //   columnDef += " NOT NULL";
        // }

        if (!defaultValue.empty() && defaultValue != "NULL") {
          // Handle sequences - replace with SERIAL for auto-increment columns
          if (defaultValue.find("nextval") != std::string::npos) {
            // This is a sequence, we'll handle it differently
            if (dataType == "integer") {
              columnDef = "\"" + colName + "\" SERIAL";
            } else if (dataType == "bigint") {
              columnDef = "\"" + colName + "\" BIGSERIAL";
            } else if (dataType == "smallint") {
              columnDef = "\"" + colName + "\" SMALLSERIAL";
            }
          } else {
            columnDef += " DEFAULT " + defaultValue;
          }
        }

        columns.push_back(columnDef);
      }

      createQuery += columns[0];
      for (size_t i = 1; i < columns.size(); ++i) {
        createQuery += ", " + columns[i];
      }
      createQuery += ");";

      return createQuery;
    } catch (const std::exception &e) {
      Logger::error("buildCreateTableQuery",
                    "Error building create table query: " +
                        std::string(e.what()));
      return "";
    }
  }

  void processTableWithDeltas(
      pqxx::connection &pgConn, const std::string &schemaName,
      const std::string &tableName, const std::string &sourceConnStr,
      const std::string &lastOffset, const std::string &status,
      const std::string &lastSyncColumn, const std::string &lastSyncTime) {
    if (status == "RESET") {
      Logger::info("processTableWithDeltas",
                   "Processing RESET table: " + schemaName + "." + tableName);
      {
        pqxx::work txn(pgConn);
        std::string lowerSchemaName = toLowerCase(schemaName);
        txn.exec("TRUNCATE TABLE \"" + lowerSchemaName + "\".\"" + tableName +
                 "\" CASCADE;");
        txn.exec(
            "UPDATE metadata.catalog SET last_offset='0' WHERE schema_name='" +
            escapeSQL(schemaName) + "' AND table_name='" +
            escapeSQL(tableName) + "';");
        txn.commit();
      }
      updateStatus(pgConn, schemaName, tableName, "FULL_LOAD", 0);
      return;
    }

    if (status == "FULL_LOAD") {
      Logger::info("processTableWithDeltas", "Processing FULL_LOAD table: " +
                                                 schemaName + "." + tableName);

      {
        pqxx::work txn(pgConn);
        auto offsetCheck = txn.exec(
            "SELECT last_offset FROM metadata.catalog WHERE schema_name='" +
            escapeSQL(schemaName) + "' AND table_name='" +
            escapeSQL(tableName) + "';");

        bool shouldTruncate = true;
        if (!offsetCheck.empty() && !offsetCheck[0][0].is_null()) {
          std::string currentOffset = offsetCheck[0][0].as<std::string>();
          if (currentOffset != "0" && !currentOffset.empty()) {
            shouldTruncate = false;
          }
        }

        if (shouldTruncate) {
          Logger::info("processTableWithDeltas",
                       "Truncating table: " + toLowerCase(schemaName) + "." +
                           tableName);
          txn.exec("TRUNCATE TABLE \"" + toLowerCase(schemaName) + "\".\"" +
                   tableName + "\" CASCADE;");
          Logger::debug("processTableWithDeltas",
                        "Table truncated successfully");
        }
        txn.commit();
      }
    }

    auto sourceConn = connectPostgres(sourceConnStr);
    if (!sourceConn) {
      updateStatus(pgConn, schemaName, tableName, "ERROR", 0);
      return;
    }

    // Obtener conteo total de registros en la fuente (sin filtros)
    int sourceCount = getSourceCount(*sourceConn, schemaName, tableName);
    int targetCount = getTargetCount(pgConn, schemaName, tableName);

    Logger::debug("processTableWithDeltas",
                  "Table " + schemaName + "." + tableName +
                      " - Source: " + std::to_string(sourceCount) +
                      ", Target: " + std::to_string(targetCount));

    if (sourceCount == 0) {
      updateStatus(pgConn, schemaName, tableName, "NO_DATA", 0);
    } else if (sourceCount == targetCount) {
      // Procesar UPDATEs si hay columna de tiempo y last_sync_time
      if (!lastSyncColumn.empty() && !lastSyncTime.empty()) {
        Logger::info("processTableWithDeltas",
                     "Processing updates for " + schemaName + "." + tableName +
                         " using time column: " + lastSyncColumn +
                         " since: " + lastSyncTime);
        processUpdatesByPrimaryKey(schemaName, tableName, *sourceConn, pgConn,
                                   lastSyncColumn, lastSyncTime);
      }

      // Verificar si hay datos nuevos usando last_offset
      size_t lastOffsetNum = 0;
      try {
        pqxx::work txn(pgConn);
        auto offsetRes = txn.exec(
            "SELECT last_offset FROM metadata.catalog WHERE schema_name='" +
            escapeSQL(schemaName) + "' AND table_name='" +
            escapeSQL(tableName) + "';");
        txn.commit();

        if (!offsetRes.empty() && !offsetRes[0][0].is_null()) {
          lastOffsetNum = std::stoul(offsetRes[0][0].as<std::string>());
        }
      } catch (...) {
        lastOffsetNum = 0;
      }

      if (lastOffsetNum >= sourceCount) {
        updateStatus(pgConn, schemaName, tableName, "PERFECT_MATCH",
                     targetCount);
      } else {
        updateStatus(pgConn, schemaName, tableName, "LISTENING_CHANGES",
                     targetCount);
      }
    } else if (sourceCount < targetCount) {
      // Hay registros eliminados en el origen - procesar DELETEs por Primary
      // Key
      Logger::info("processTableWithDeltas",
                   "Detected " + std::to_string(targetCount - sourceCount) +
                       " deleted records in " + schemaName + "." + tableName +
                       " - processing deletes");
      processDeletesByPrimaryKey(schemaName, tableName, *sourceConn, pgConn);

      // Después de procesar DELETEs, verificar el nuevo conteo
      pqxx::connection countConn(DatabaseConfig::getPostgresConnectionString());
      pqxx::work countTxn(countConn);
      auto newTargetCount =
          countTxn.exec("SELECT COUNT(*) FROM \"" + toLowerCase(schemaName) +
                        "\".\"" + tableName + "\";");
      countTxn.commit();
      targetCount = newTargetCount[0][0].as<int>();
      Logger::info("processTableWithDeltas",
                   "After deletes: source=" + std::to_string(sourceCount) +
                       ", target=" + std::to_string(targetCount));
    } else {
      // sourceCount > targetCount - hay datos nuevos para insertar
      performDataTransfer(pgConn, *sourceConn, schemaName, tableName,
                          lastOffset, sourceCount);
    }
  }

  void processTable(pqxx::connection &pgConn, const std::string &schemaName,
                    const std::string &tableName,
                    const std::string &sourceConnStr,
                    const std::string &lastOffset, const std::string &status) {

    if (status == "RESET") {
      Logger::info("processTable",
                   "Processing RESET table: " + schemaName + "." + tableName);
      {
        pqxx::work txn(pgConn);
        std::string lowerSchemaName = toLowerCase(schemaName);
        txn.exec("TRUNCATE TABLE \"" + lowerSchemaName + "\".\"" + tableName +
                 "\" CASCADE;");
        txn.exec(
            "UPDATE metadata.catalog SET last_offset='0' WHERE schema_name='" +
            escapeSQL(schemaName) + "' AND table_name='" +
            escapeSQL(tableName) + "';");
        txn.commit();
      }
      updateStatus(pgConn, schemaName, tableName, "FULL_LOAD", 0);
      return;
    }

    if (status == "FULL_LOAD") {
      Logger::info("processTable", "Processing FULL_LOAD table: " + schemaName +
                                       "." + tableName);

      pqxx::work txn(pgConn);
      auto offsetCheck = txn.exec(
          "SELECT last_offset FROM metadata.catalog WHERE schema_name='" +
          escapeSQL(schemaName) + "' AND table_name='" + escapeSQL(tableName) +
          "';");

      bool shouldTruncate = true;
      if (!offsetCheck.empty() && !offsetCheck[0][0].is_null()) {
        std::string currentOffset = offsetCheck[0][0].as<std::string>();
        if (currentOffset != "0" && !currentOffset.empty()) {
          shouldTruncate = false;
        }
      }

      if (shouldTruncate) {
        Logger::info("processTable",
                     "Truncating table: " + toLowerCase(schemaName) + "." +
                         tableName);
        txn.exec("TRUNCATE TABLE \"" + toLowerCase(schemaName) + "\".\"" +
                 tableName + "\" CASCADE;");
        Logger::debug("processTable", "Table truncated successfully");
      }
      txn.commit();
    }

    auto sourceConn = connectPostgres(sourceConnStr);
    if (!sourceConn) {
      updateStatus(pgConn, schemaName, tableName, "ERROR", 0);
      return;
    }

    std::string timeColumn =
        detectTimeColumn(*sourceConn, schemaName, tableName);
    if (timeColumn.empty()) {
      Logger::warning("processTable", "No time column detected for " +
                                          schemaName + "." + tableName);
    }

    int sourceCount = getSourceCount(*sourceConn, schemaName, tableName);
    int targetCount = getTargetCount(pgConn, schemaName, tableName);

    Logger::debug("processTable",
                  "Table " + schemaName + "." + tableName +
                      " - Source: " + std::to_string(sourceCount) +
                      ", Target: " + std::to_string(targetCount));

    if (sourceCount == targetCount) {
      updateStatus(pgConn, schemaName, tableName, "PERFECT_MATCH", sourceCount);
    } else if (sourceCount == 0) {
      updateStatus(pgConn, schemaName, tableName, "NO_DATA", 0);
    } else if (sourceCount < targetCount) {
      updateStatus(pgConn, schemaName, tableName, "ERROR", sourceCount);
    } else {
      performDataTransfer(pgConn, *sourceConn, schemaName, tableName,
                          lastOffset, sourceCount);
    }
  }

  std::string detectTimeColumn(pqxx::connection &sourceConn,
                               const std::string &schemaName,
                               const std::string &tableName) {
    try {
      pqxx::work txn(sourceConn);
      auto results = txn.exec(
          "SELECT column_name, data_type FROM information_schema.columns "
          "WHERE table_schema = '" +
          schemaName + "' AND table_name = '" + tableName +
          "' "
          "AND data_type IN ('timestamp', 'timestamp without time zone', "
          "'timestamp with time zone', 'date') "
          "ORDER BY column_name;");

      std::string detectedTimeColumn;
      for (const auto &row : results) {
        std::string colName = row[0].as<std::string>();
        std::string dataType = row[1].as<std::string>();

        if (colName == "updated_at") {
          detectedTimeColumn = colName;
          break;
        } else if (colName == "created_at" &&
                   detectedTimeColumn != "updated_at") {
          detectedTimeColumn = colName;
        } else if (colName.find("_at") != std::string::npos &&
                   detectedTimeColumn != "updated_at" &&
                   detectedTimeColumn != "created_at") {
          detectedTimeColumn = colName;
        } else if (colName.find("fecha_") != std::string::npos &&
                   detectedTimeColumn != "updated_at" &&
                   detectedTimeColumn != "created_at") {
          detectedTimeColumn = colName;
        }
      }

      if (!detectedTimeColumn.empty()) {
        Logger::debug("detectTimeColumn",
                      "Detected time column: " + detectedTimeColumn + " for " +
                          schemaName + "." + tableName);
      }

      return detectedTimeColumn;
    } catch (const std::exception &e) {
      Logger::error("detectTimeColumn",
                    "Error detecting time column: " + std::string(e.what()));
      return "";
    }
  }

  int getSourceCount(pqxx::connection &sourceConn,
                     const std::string &schemaName,
                     const std::string &tableName) {
    try {
      pqxx::work txn(sourceConn);
      std::string query =
          "SELECT COUNT(*) FROM \"" + schemaName + "\".\"" + tableName + "\"";

      auto result = txn.exec(query);
      if (!result.empty()) {
        return result[0][0].as<int>();
      }
      return 0;
    } catch (const std::exception &e) {
      Logger::error("getSourceCount",
                    "Error getting source count: " + std::string(e.what()));
      return 0;
    }
  }

  int getTargetCount(pqxx::connection &pgConn, const std::string &schemaName,
                     const std::string &tableName) {
    try {
      std::string lowerSchemaName = toLowerCase(schemaName);
      pqxx::connection countConn(DatabaseConfig::getPostgresConnectionString());
      pqxx::work txn(countConn);
      auto result = txn.exec("SELECT COUNT(*) FROM \"" + lowerSchemaName +
                             "\".\"" + tableName + "\"");
      txn.commit();
      if (!result.empty()) {
        return result[0][0].as<int>();
      }
      return 0;
    } catch (const std::exception &e) {
      Logger::error("getTargetCount",
                    "Error getting target count: " + std::string(e.what()));
      return 0;
    }
  }

  void performDataTransfer(pqxx::connection &pgConn,
                           pqxx::connection &sourceConn,
                           const std::string &schemaName,
                           const std::string &tableName,
                           const std::string &lastOffset, int sourceCount) {
    try {
      Logger::info("performDataTransfer",
                   "Transferring data for " + schemaName + "." + tableName);

      std::string lowerSchemaName = toLowerCase(schemaName);

      // Usar last_offset para paginación (como en MariaDBToPostgres.h)
      size_t currentOffset = 0;
      try {
        currentOffset = std::stoul(lastOffset);
      } catch (...) {
        currentOffset = 0;
      }

      const size_t CHUNK_SIZE = 1000; // Tamaño de chunk fijo
      size_t totalProcessed = currentOffset;
      bool hasMoreData = true;

      while (hasMoreData) {
        pqxx::work sourceTxn(sourceConn);
        std::string selectQuery = "SELECT * FROM \"" + schemaName + "\".\"" +
                                  tableName + "\" LIMIT " +
                                  std::to_string(CHUNK_SIZE) + " OFFSET " +
                                  std::to_string(totalProcessed) + ";";

        auto sourceResult = sourceTxn.exec(selectQuery);
        sourceTxn.commit();

        if (sourceResult.empty()) {
          hasMoreData = false;
          break;
        }

        Logger::info("performDataTransfer",
                     "Processing chunk of " +
                         std::to_string(sourceResult.size()) + " rows for " +
                         schemaName + "." + tableName);

        // Insertar datos en el destino usando UPSERT
        {
          pqxx::connection targetConn(
              DatabaseConfig::getPostgresConnectionString());

          // Convertir pqxx::result a vector<vector<string>> para usar con
          // performBulkUpsert
          std::vector<std::vector<std::string>> results;
          std::vector<std::string> columnNames;

          // Obtener nombres de columnas
          if (!sourceResult.empty()) {
            for (size_t i = 0; i < sourceResult[0].size(); ++i) {
              columnNames.push_back(sourceResult[0][i].name());
            }
          }

          // Convertir datos
          for (const auto &row : sourceResult) {
            std::vector<std::string> rowData;
            for (size_t i = 0; i < row.size(); ++i) {
              rowData.push_back(row[i].is_null() ? "NULL"
                                                 : row[i].as<std::string>());
            }
            results.push_back(rowData);
          }

          // Obtener tipos de columnas (asumir TEXT por defecto)
          std::vector<std::string> columnTypes(columnNames.size(), "TEXT");

          performBulkUpsert(targetConn, results, columnNames, columnTypes,
                            lowerSchemaName, tableName, schemaName);
        }

        totalProcessed += sourceResult.size();

        // Actualizar last_offset en la base de datos
        try {
          pqxx::work updateTxn(pgConn);
          updateTxn.exec("UPDATE metadata.catalog SET last_offset='" +
                         std::to_string(totalProcessed) +
                         "' WHERE schema_name='" + escapeSQL(schemaName) +
                         "' AND table_name='" + escapeSQL(tableName) + "';");
          updateTxn.commit();
          Logger::debug("performDataTransfer",
                        "Updated last_offset to " +
                            std::to_string(totalProcessed) + " for " +
                            schemaName + "." + tableName);
        } catch (const std::exception &e) {
          Logger::warning("performDataTransfer",
                          "Failed to update last_offset: " +
                              std::string(e.what()));
        }

        // Si obtuvimos menos registros que el chunk size, hemos terminado
        if (sourceResult.size() < CHUNK_SIZE) {
          hasMoreData = false;
        }
      }

      updateStatus(pgConn, schemaName, tableName, "PERFECT_MATCH",
                   totalProcessed);
      Logger::info("performDataTransfer", "Successfully transferred " +
                                              std::to_string(totalProcessed) +
                                              " records for " + schemaName +
                                              "." + tableName);

    } catch (const std::exception &e) {
      Logger::error("performDataTransfer",
                    "Error transferring data: " + std::string(e.what()));
      updateStatus(pgConn, schemaName, tableName, "ERROR", 0);
    }
  }

  void updateStatus(pqxx::connection &pgConn, const std::string &schemaName,
                    const std::string &tableName, const std::string &status,
                    int count) {
    try {
      pqxx::connection updateConn(
          DatabaseConfig::getPostgresConnectionString());
      pqxx::work txn(updateConn);
      txn.exec("UPDATE metadata.catalog SET status='" + status +
               "' "
               "WHERE schema_name='" +
               escapeSQL(schemaName) + "' AND table_name='" +
               escapeSQL(tableName) + "';");
      txn.commit();
    } catch (const std::exception &e) {
      Logger::error("updateStatus",
                    "Error updating status: " + std::string(e.what()));
    }
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

  void processDeletesByPrimaryKey(const std::string &schema_name,
                                  const std::string &table_name,
                                  pqxx::connection &sourceConn,
                                  pqxx::connection &pgConn) {
    try {
      std::string lowerSchemaName = toLowerCase(schema_name);

      std::vector<std::string> pkColumns =
          getPrimaryKeyColumns(sourceConn, schema_name, table_name);

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

      const size_t BATCH_SIZE = 1000;
      size_t offset = 0;
      size_t totalDeleted = 0;

      while (true) {
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

        std::vector<std::vector<std::string>> pgPKs;
        try {
          pqxx::connection pkConn(
              DatabaseConfig::getPostgresConnectionString());
          pqxx::work txn(pkConn);
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
          break;
        }

        std::vector<std::vector<std::string>> deletedPKs =
            findDeletedPrimaryKeys(sourceConn, schema_name, table_name, pgPKs,
                                   pkColumns);

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
                                  pqxx::connection &sourceConn,
                                  pqxx::connection &pgConn,
                                  const std::string &timeColumn,
                                  const std::string &lastSyncTime) {
    try {
      if (timeColumn.empty() || lastSyncTime.empty()) {
        Logger::debug("processUpdatesByPrimaryKey",
                      "No time column or sync time for " + schema_name + "." +
                          table_name + " - skipping updates");
        return;
      }

      std::string lowerSchemaName = toLowerCase(schema_name);

      std::vector<std::string> pkColumns =
          getPrimaryKeyColumns(sourceConn, schema_name, table_name);

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

      std::string selectQuery = "SELECT * FROM \"" + schema_name + "\".\"" +
                                table_name + "\" WHERE \"" + timeColumn +
                                "\" > '" + escapeSQL(lastSyncTime) +
                                "' ORDER BY \"" + timeColumn + "\"";

      pqxx::work sourceTxn(sourceConn);
      auto modifiedRecords = sourceTxn.exec(selectQuery);
      sourceTxn.commit();

      Logger::debug("processUpdatesByPrimaryKey",
                    "Found " + std::to_string(modifiedRecords.size()) +
                        " modified records in " + schema_name + "." +
                        table_name);

      if (modifiedRecords.empty()) {
        return;
      }

      std::vector<std::string> columnNames;
      for (size_t i = 0; i < modifiedRecords[0].size(); ++i) {
        columnNames.push_back(modifiedRecords[0][i].name());
      }

      size_t totalUpdated = 0;
      for (const auto &record : modifiedRecords) {
        if (record.size() != columnNames.size()) {
          Logger::warning("processUpdatesByPrimaryKey",
                          "Record size mismatch for " + schema_name + "." +
                              table_name + " - skipping record");
          continue;
        }

        std::string whereClause = "";
        for (size_t i = 0; i < pkColumns.size(); ++i) {
          size_t pkIndex = 0;
          for (size_t j = 0; j < columnNames.size(); ++j) {
            if (columnNames[j] == pkColumns[i]) {
              pkIndex = j;
              break;
            }
          }

          if (i > 0)
            whereClause += " AND ";
          whereClause +=
              "\"" + pkColumns[i] + "\" = " +
              (record[pkIndex].is_null()
                   ? "NULL"
                   : "'" + escapeSQL(record[pkIndex].as<std::string>()) + "'");
        }

        std::string checkQuery = "SELECT COUNT(*) FROM \"" + lowerSchemaName +
                                 "\".\"" + table_name + "\" WHERE " +
                                 whereClause;

        pqxx::connection checkConn(
            DatabaseConfig::getPostgresConnectionString());
        pqxx::work txn(checkConn);
        auto result = txn.exec(checkQuery);
        txn.commit();

        if (result[0][0].as<int>() > 0) {
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
      }

    } catch (const std::exception &e) {
      Logger::error("processUpdatesByPrimaryKey",
                    "Error processing updates for " + schema_name + "." +
                        table_name + ": " + std::string(e.what()));
    }
  }

  std::vector<std::string> getPrimaryKeyColumns(pqxx::connection &sourceConn,
                                                const std::string &schema_name,
                                                const std::string &table_name) {
    std::vector<std::string> pkColumns;

    std::string query = "SELECT kcu.column_name "
                        "FROM information_schema.table_constraints tc "
                        "JOIN information_schema.key_column_usage kcu "
                        "ON tc.constraint_name = kcu.constraint_name "
                        "AND tc.table_schema = kcu.table_schema "
                        "WHERE tc.constraint_type = 'PRIMARY KEY' "
                        "AND tc.table_schema = '" +
                        escapeSQL(schema_name) +
                        "' "
                        "AND tc.table_name = '" +
                        escapeSQL(table_name) +
                        "' "
                        "ORDER BY kcu.ordinal_position;";

    try {
      pqxx::work txn(sourceConn);
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
      Logger::error("getPrimaryKeyColumns",
                    "Error getting primary key columns: " +
                        std::string(e.what()));
    }

    return pkColumns;
  }

  std::vector<std::vector<std::string>>
  findDeletedPrimaryKeys(pqxx::connection &sourceConn,
                         const std::string &schema_name,
                         const std::string &table_name,
                         const std::vector<std::vector<std::string>> &pgPKs,
                         const std::vector<std::string> &pkColumns) {

    std::vector<std::vector<std::string>> deletedPKs;

    if (pgPKs.empty() || pkColumns.empty()) {
      return deletedPKs;
    }

    const size_t CHECK_BATCH_SIZE = 500;

    for (size_t batchStart = 0; batchStart < pgPKs.size();
         batchStart += CHECK_BATCH_SIZE) {
      size_t batchEnd = std::min(batchStart + CHECK_BATCH_SIZE, pgPKs.size());

      std::string checkQuery = "SELECT ";
      for (size_t i = 0; i < pkColumns.size(); ++i) {
        if (i > 0)
          checkQuery += ", ";
        checkQuery += "\"" + pkColumns[i] + "\"";
      }
      checkQuery +=
          " FROM \"" + schema_name + "\".\"" + table_name + "\" WHERE (";

      for (size_t i = batchStart; i < batchEnd; ++i) {
        if (i > batchStart)
          checkQuery += " OR ";
        checkQuery += "(";
        for (size_t j = 0; j < pkColumns.size(); ++j) {
          if (j > 0)
            checkQuery += " AND ";
          std::string value = pgPKs[i][j];
          if (value == "NULL") {
            checkQuery += "\"" + pkColumns[j] + "\" IS NULL";
          } else {
            checkQuery +=
                "\"" + pkColumns[j] + "\" = '" + escapeSQL(value) + "'";
          }
        }
        checkQuery += ")";
      }
      checkQuery += ");";

      try {
        pqxx::work txn(sourceConn);
        auto existingResults = txn.exec(checkQuery);
        txn.commit();

        std::set<std::vector<std::string>> existingPKs;
        for (const auto &row : existingResults) {
          std::vector<std::string> pkValues;
          for (size_t i = 0; i < pkColumns.size(); ++i) {
            pkValues.push_back(row[i].is_null() ? "NULL"
                                                : row[i].as<std::string>());
          }
          existingPKs.insert(pkValues);
        }

        for (size_t i = batchStart; i < batchEnd; ++i) {
          if (existingPKs.find(pgPKs[i]) == existingPKs.end()) {
            deletedPKs.push_back(pgPKs[i]);
          }
        }
      } catch (const std::exception &e) {
        Logger::error("findDeletedPrimaryKeys",
                      "Error checking deleted primary keys: " +
                          std::string(e.what()));
        break;
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
      pqxx::connection deleteConn(
          DatabaseConfig::getPostgresConnectionString());
      pqxx::work txn(deleteConn);

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

      auto result = txn.exec(deleteQuery);
      txn.commit();
      deletedCount = result.affected_rows();

    } catch (const std::exception &e) {
      Logger::error("deleteRecordsByPrimaryKey",
                    "Error deleting records: " + std::string(e.what()));
    }

    return deletedCount;
  }

  bool compareAndUpdateRecord(pqxx::connection &pgConn,
                              const std::string &lowerSchemaName,
                              const std::string &table_name,
                              const pqxx::row &newRecord,
                              const std::vector<std::string> &columnNames,
                              const std::string &whereClause) {
    try {
      std::string selectQuery = "SELECT * FROM \"" + lowerSchemaName + "\".\"" +
                                table_name + "\" WHERE " + whereClause;

      pqxx::connection selectConn(
          DatabaseConfig::getPostgresConnectionString());
      pqxx::work txn(selectConn);
      auto result = txn.exec(selectQuery);
      txn.commit();

      if (result.empty()) {
        return false;
      }

      const auto &existingRecord = result[0];
      bool hasChanges = false;

      for (size_t i = 0; i < columnNames.size(); ++i) {
        std::string newValue =
            newRecord[i].is_null() ? "NULL" : newRecord[i].as<std::string>();
        std::string existingValue = existingRecord[i].is_null()
                                        ? "NULL"
                                        : existingRecord[i].as<std::string>();

        if (newValue != existingValue) {
          hasChanges = true;
          break;
        }
      }

      if (hasChanges) {
        std::string updateQuery =
            "UPDATE \"" + lowerSchemaName + "\".\"" + table_name + "\" SET ";
        std::vector<std::string> setClauses;

        for (size_t i = 0; i < columnNames.size(); ++i) {
          std::string setClause = "\"" + columnNames[i] + "\" = ";
          if (newRecord[i].is_null()) {
            setClause += "NULL";
          } else {
            setClause += "'" + escapeSQL(newRecord[i].as<std::string>()) + "'";
          }
          setClauses.push_back(setClause);
        }

        updateQuery += setClauses[0];
        for (size_t i = 1; i < setClauses.size(); ++i) {
          updateQuery += ", " + setClauses[i];
        }
        updateQuery += " WHERE " + whereClause;

        pqxx::connection updateConn(
            DatabaseConfig::getPostgresConnectionString());
        pqxx::work updateTxn(updateConn);
        updateTxn.exec(updateQuery);
        updateTxn.commit();

        Logger::debug("compareAndUpdateRecord", "Updated record in " +
                                                    lowerSchemaName + "." +
                                                    table_name);
      }

      return hasChanges;

    } catch (const std::exception &e) {
      Logger::error("compareAndUpdateRecord",
                    "Error comparing/updating record: " +
                        std::string(e.what()));
      return false;
    }
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
      std::string conflictClause = buildUpsertConflictClause(columnNames, pkColumns);

      pqxx::work txn(pgConn);
      txn.exec("SET statement_timeout = '300s'");

      // Procesar en batches para evitar queries muy largas
      const size_t BATCH_SIZE = 500;
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
      const size_t BATCH_SIZE = 1000;
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

  std::string buildUpsertConflictClause(const std::vector<std::string> &columnNames,
                                       const std::vector<std::string> &pkColumns) {
    std::string conflictClause = " ON CONFLICT (";
    
    for (size_t i = 0; i < pkColumns.size(); ++i) {
      if (i > 0) conflictClause += ", ";
      conflictClause += "\"" + pkColumns[i] + "\"";
    }
    conflictClause += ") DO UPDATE SET ";

    // Construir SET clause para UPDATE
    for (size_t i = 0; i < columnNames.size(); ++i) {
      if (i > 0) conflictClause += ", ";
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

    return cleanValue;
  }
};

#endif // POSTGRESTOPOSTGRES_H
