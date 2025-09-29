#ifndef POSTGRESTOPOSTGRES_H
#define POSTGRESTOPOSTGRES_H

#include "Config.h"
#include "catalog_manager.h"
#include "logger.h"
#include <chrono>
#include <mutex>
#include <pqxx/pqxx>
#include <string>
#include <unordered_map>
#include <vector>

class PostgresToPostgres {
private:
public:
  PostgresToPostgres() = default;

  static std::unordered_map<std::string, std::string> dataTypeMap;
  ~PostgresToPostgres() = default;

  pqxx::connection *getPostgresConnection(const std::string &connectionString) {
    // Validate input parameters
    if (connectionString.empty()) {
      Logger::error(LogCategory::TRANSFER, "getPostgresConnection",
                    "Connection string is empty");
      return nullptr;
    }

    // Check for required parameters
    if (connectionString.find("host=") == std::string::npos ||
        connectionString.find("dbname=") == std::string::npos ||
        connectionString.find("user=") == std::string::npos) {
      Logger::error(
          LogCategory::TRANSFER, "getPostgresConnection",
          "Missing required connection parameters (host, dbname, user)");
      return nullptr;
    }

    // Crear nueva conexión directa para cada consulta
    try {
      auto *conn = new pqxx::connection(connectionString);
      if (conn->is_open()) {
        // Test connection with a simple query
        pqxx::work testTxn(*conn);
        testTxn.exec("SELECT 1");
        testTxn.commit();
        return conn;
      } else {
        Logger::error(LogCategory::TRANSFER, "getPostgresConnection",
                      "Failed to open PostgreSQL connection");
        delete conn;
        return nullptr;
      }
    } catch (const pqxx::sql_error &e) {
      Logger::error(LogCategory::TRANSFER, "getPostgresConnection",
                    "SQL ERROR: " + std::string(e.what()) +
                        " [SQL State: " + e.sqlstate() + "]");
      return nullptr;
    } catch (const pqxx::broken_connection &e) {
      Logger::error(LogCategory::TRANSFER, "getPostgresConnection",
                    "CONNECTION ERROR: " + std::string(e.what()));
      return nullptr;
    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER, "getPostgresConnection",
                    "ERROR: " + std::string(e.what()));
      return nullptr;
    }
  }

  void setupTableTargetPostgresToPostgres() {
    Logger::info(LogCategory::TRANSFER,
                 "Starting PostgreSQL table target setup");

    try {
      pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());

      if (!pgConn.is_open()) {
        Logger::error(LogCategory::TRANSFER,
                      "CRITICAL ERROR: Cannot establish PostgreSQL connection "
                      "for PostgreSQL table setup");
        return;
      }

      pqxx::work txn(pgConn);
      auto results = txn.exec(
          "SELECT schema_name, table_name, "
          "connection_string, status, table_size FROM metadata.catalog "
          "WHERE db_engine='PostgreSQL' AND active=true "
          "ORDER BY table_size ASC, schema_name, table_name;");

      Logger::info(LogCategory::TRANSFER,
                   "PostgreSQL catalog query executed - found " +
                       std::to_string(results.size()) +
                       " active PostgreSQL tables");

      // Sort tables by priority: FULL_LOAD, RESET, LISTENING_CHANGES
      std::vector<
          std::tuple<std::string, std::string, std::string, std::string>>
          tables;
      for (const auto &row : results) {
        if (row.size() < 5)
          continue;
        tables.emplace_back(row[0].as<std::string>(), // schema_name
                            row[1].as<std::string>(), // table_name
                            row[2].as<std::string>(), // connection_string
                            row[3].as<std::string>()  // status
        );
      }

      std::sort(tables.begin(), tables.end(), [](const auto &a, const auto &b) {
        std::string statusA = std::get<3>(a);
        std::string statusB = std::get<3>(b);
        if (statusA == "FULL_LOAD" && statusB != "FULL_LOAD")
          return true;
        if (statusA != "FULL_LOAD" && statusB == "FULL_LOAD")
          return false;
        if (statusA == "RESET" && statusB != "RESET")
          return true;
        if (statusA != "RESET" && statusB == "RESET")
          return false;
        if (statusA == "LISTENING_CHANGES" && statusB != "LISTENING_CHANGES")
          return true;
        if (statusA != "LISTENING_CHANGES" && statusB == "LISTENING_CHANGES")
          return false;
        return false;
      });

      Logger::info(LogCategory::TRANSFER,
                   "Processing " + std::to_string(tables.size()) +
                       " PostgreSQL tables in priority order");
      // Removed individual table status logs to reduce noise

      for (const auto &table : tables) {
        std::string schemaName = std::get<0>(table);
        std::string tableName = std::get<1>(table);
        std::string sourceConnStr = std::get<2>(table);

        try {
          auto sourceConn = getPostgresConnection(sourceConnStr);
          if (!sourceConn) {
            Logger::error(LogCategory::TRANSFER,
                          "Failed to connect to source PostgreSQL");
            continue;
          }

          std::string lowerSchemaName = toLowerCase(schemaName);
          createSchemaIfNotExists(txn, lowerSchemaName);

          std::string createTableQuery = buildCreateTableQuery(
              *sourceConn, schemaName, tableName, lowerSchemaName);
          if (!createTableQuery.empty()) {
            txn.exec(createTableQuery);
            Logger::info(LogCategory::TRANSFER,
                         "Created target table: " + lowerSchemaName + "." +
                             tableName);
          }

        } catch (const std::exception &e) {
          Logger::error(LogCategory::TRANSFER, "Error setting up table " +
                                                   schemaName + "." +
                                                   tableName + ": " + e.what());
        }
      }

      txn.commit();
      Logger::info(LogCategory::TRANSFER, "Target table setup completed");
    } catch (const pqxx::sql_error &e) {
      Logger::error(LogCategory::TRANSFER, "setupTableTargetPostgresToPostgres",
                    "SQL ERROR: " + std::string(e.what()) +
                        " [SQL State: " + e.sqlstate() + "]");
    } catch (const pqxx::broken_connection &e) {
      Logger::error(LogCategory::TRANSFER, "setupTableTargetPostgresToPostgres",
                    "CONNECTION ERROR: " + std::string(e.what()));
    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER, "setupTableTargetPostgresToPostgres",
                    "ERROR: " + std::string(e.what()));
    }
  }

  void transferDataPostgresToPostgres() {
    Logger::info(LogCategory::TRANSFER,
                 "Starting PostgreSQL to PostgreSQL data transfer");

    try {
      pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());

      if (!pgConn.is_open()) {
        Logger::error(LogCategory::TRANSFER,
                      "CRITICAL ERROR: Cannot establish PostgreSQL connection "
                      "for PostgreSQL data transfer");
        return;
      }

      {
        pqxx::work txn(pgConn);
        auto results =
            txn.exec("SELECT schema_name, table_name, connection_string, "
                     "last_offset, status, last_sync_column, last_sync_time "
                     "FROM metadata.catalog "
                     "WHERE db_engine='PostgreSQL' AND active=true AND status "
                     "!= 'NO_DATA';");

        Logger::info(LogCategory::TRANSFER,
                     "PostgreSQL catalog query executed - found " +
                         std::to_string(results.size()) +
                         " active PostgreSQL tables for transfer");

        // Sort tables by priority: FULL_LOAD, RESET, LISTENING_CHANGES
        std::vector<
            std::tuple<std::string, std::string, std::string, std::string,
                       std::string, std::string, std::string>>
            tables;
        for (const auto &row : results) {
          if (row.size() < 7)
            continue;
          tables.emplace_back(
              row[0].as<std::string>(), // schema_name
              row[1].as<std::string>(), // table_name
              row[2].as<std::string>(), // connection_string
              row[3].as<std::string>(), // last_offset
              row[4].as<std::string>(), // status
              row[5].is_null() ? ""
                               : row[5].as<std::string>(), // last_sync_column
              row[6].is_null() ? "" : row[6].as<std::string>() // last_sync_time
          );
        }

        std::sort(tables.begin(), tables.end(),
                  [](const auto &a, const auto &b) {
                    std::string statusA = std::get<4>(a);
                    std::string statusB = std::get<4>(b);
                    if (statusA == "FULL_LOAD" && statusB != "FULL_LOAD")
                      return true;
                    if (statusA != "FULL_LOAD" && statusB == "FULL_LOAD")
                      return false;
                    if (statusA == "RESET" && statusB != "RESET")
                      return true;
                    if (statusA != "RESET" && statusB == "RESET")
                      return false;
                    if (statusA == "LISTENING_CHANGES" &&
                        statusB != "LISTENING_CHANGES")
                      return true;
                    if (statusA != "LISTENING_CHANGES" &&
                        statusB == "LISTENING_CHANGES")
                      return false;
                    return false;
                  });

        Logger::info(LogCategory::TRANSFER, "transferDataPostgresToPostgres",
                     "Processing " + std::to_string(tables.size()) +
                         " PostgreSQL tables in priority order");
        for (size_t i = 0; i < tables.size(); ++i) {
          Logger::info(LogCategory::TRANSFER, "transferDataPostgresToPostgres",
                       "[" + std::to_string(i + 1) + "/" +
                           std::to_string(tables.size()) + "] " +
                           std::get<0>(tables[i]) + "." +
                           std::get<1>(tables[i]) +
                           " (status: " + std::get<4>(tables[i]) + ")");
        }

        for (const auto &table : tables) {
          std::string schemaName = std::get<0>(table);
          std::string tableName = std::get<1>(table);
          std::string sourceConnStr = std::get<2>(table);
          std::string lastOffset = std::get<3>(table);
          std::string status = std::get<4>(table);
          std::string lastSyncColumn = std::get<5>(table);
          std::string lastSyncTime = std::get<6>(table);

          Logger::debug("transferDataPostgresToPostgres",
                        "Processing table: " + schemaName + "." + tableName +
                            " (status: " + status + ")");

          try {
            processTableWithDeltas(pgConn, schemaName, tableName, sourceConnStr,
                                   lastOffset, status, lastSyncColumn,
                                   lastSyncTime);
          } catch (const std::exception &e) {
            Logger::error(LogCategory::TRANSFER,
                          "transferDataPostgresToPostgres",
                          "Error processing table " + schemaName + "." +
                              tableName + ": " + e.what());
            updateStatus(pgConn, schemaName, tableName, "ERROR", 0);
          }
        }

        txn.commit();
      }

      Logger::info(
          LogCategory::TRANSFER, "transferDataPostgresToPostgres",
          "PostgreSQL to PostgreSQL data transfer completed successfully");
    } catch (const pqxx::sql_error &e) {
      Logger::error(LogCategory::TRANSFER, "transferDataPostgresToPostgres",
                    "SQL ERROR: " + std::string(e.what()) +
                        " [SQL State: " + e.sqlstate() + "]");
    } catch (const pqxx::broken_connection &e) {
      Logger::error(LogCategory::TRANSFER, "transferDataPostgresToPostgres",
                    "CONNECTION ERROR: " + std::string(e.what()));
    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER, "transferDataPostgresToPostgres",
                    "CRITICAL ERROR: " + std::string(e.what()) +
                        " - PostgreSQL data transfer completely failed");
    }
  }

private:
  // connectPostgres method removed - using getPostgresConnection instead

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
        Logger::warning(LogCategory::TRANSFER, "buildCreateTableQuery",
                        "No columns found for table " + sourceSchema + "." +
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
    } catch (const pqxx::sql_error &e) {
      Logger::error(
          LogCategory::TRANSFER, "buildCreateTableQuery",
          "SQL ERROR building create table query: " + std::string(e.what()) +
              " [SQL State: " + e.sqlstate() + "]");
      return "";
    } catch (const pqxx::broken_connection &e) {
      Logger::error(LogCategory::TRANSFER, "buildCreateTableQuery",
                    "CONNECTION ERROR building create table query: " +
                        std::string(e.what()));
      return "";
    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER, "buildCreateTableQuery",
                    "ERROR building create table query: " +
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
      Logger::info(LogCategory::TRANSFER, "processTableWithDeltas",
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
      Logger::info(LogCategory::TRANSFER, "processTableWithDeltas",
                   "Processing FULL_LOAD table: " + schemaName + "." +
                       tableName);

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
          Logger::info(LogCategory::TRANSFER, "processTableWithDeltas",
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

    auto sourceConn = getPostgresConnection(sourceConnStr);
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
        Logger::info(LogCategory::TRANSFER, "processTableWithDeltas",
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
        updateStatus(pgConn, schemaName, tableName, "LISTENING_CHANGES",
                     sourceCount);

        // OPTIMIZED: Update last_processed_pk for LISTENING_CHANGES tables
        std::string pkStrategy =
            getPKStrategyFromCatalog(pgConn, schemaName, tableName);
        std::vector<std::string> pkColumns =
            getPKColumnsFromCatalog(pgConn, schemaName, tableName);

        if (pkStrategy == "PK" && !pkColumns.empty()) {
          try {
            // Obtener el último PK de la tabla para marcar como
            // completamente procesada
            std::string maxPKQuery = "SELECT ";
            for (size_t i = 0; i < pkColumns.size(); ++i) {
              if (i > 0)
                maxPKQuery += ", ";
              maxPKQuery += "\"" + pkColumns[i] + "\"";
            }
            maxPKQuery +=
                " FROM \"" + schemaName + "\".\"" + tableName + "\" ORDER BY ";
            for (size_t i = 0; i < pkColumns.size(); ++i) {
              if (i > 0)
                maxPKQuery += ", ";
              maxPKQuery += "\"" + pkColumns[i] + "\"";
            }
            maxPKQuery += " DESC LIMIT 1;";

            pqxx::work sourceTxn(*sourceConn);
            auto maxPKResults = sourceTxn.exec(maxPKQuery);
            sourceTxn.commit();

            if (!maxPKResults.empty() && maxPKResults[0].size() > 0) {
              std::string lastPK;
              for (size_t i = 0; i < maxPKResults[0].size(); ++i) {
                if (i > 0)
                  lastPK += "|";
                lastPK += maxPKResults[0][i].is_null()
                              ? "NULL"
                              : maxPKResults[0][i].as<std::string>();
              }

              updateLastProcessedPK(pgConn, schemaName, tableName, lastPK);
              Logger::info(LogCategory::TRANSFER,
                           "Updated last_processed_pk to " + lastPK +
                               " for LISTENING_CHANGES table " + schemaName +
                               "." + tableName);
            }
          } catch (const std::exception &e) {
            Logger::error(LogCategory::TRANSFER,
                          "ERROR: Failed to update last_processed_pk for "
                          "LISTENING_CHANGES table " +
                              schemaName + "." + tableName + ": " +
                              std::string(e.what()));
          }
        }
      } else {
        updateStatus(pgConn, schemaName, tableName, "LISTENING_CHANGES",
                     sourceCount);

        // Actualizar last_processed_pk para tablas ya sincronizadas
        std::string pkStrategy =
            getPKStrategyFromCatalog(pgConn, schemaName, tableName);
        std::vector<std::string> pkColumns =
            getPKColumnsFromCatalog(pgConn, schemaName, tableName);

        if (pkStrategy == "PK" && !pkColumns.empty()) {
          try {
            // Obtener el último PK de la tabla para marcar como procesada
            std::string maxPKQuery = "SELECT ";
            for (size_t i = 0; i < pkColumns.size(); ++i) {
              if (i > 0)
                maxPKQuery += ", ";
              maxPKQuery += "\"" + pkColumns[i] + "\"";
            }
            maxPKQuery +=
                " FROM \"" + schemaName + "\".\"" + tableName + "\" ORDER BY ";
            for (size_t i = 0; i < pkColumns.size(); ++i) {
              if (i > 0)
                maxPKQuery += ", ";
              maxPKQuery += "\"" + pkColumns[i] + "\"";
            }
            maxPKQuery += " DESC LIMIT 1;";

            pqxx::work sourceTxn(*sourceConn);
            auto maxPKResults = sourceTxn.exec(maxPKQuery);
            sourceTxn.commit();

            if (!maxPKResults.empty() && maxPKResults[0].size() > 0) {
              std::string lastPK;
              for (size_t i = 0; i < maxPKResults[0].size(); ++i) {
                if (i > 0)
                  lastPK += "|";
                lastPK += maxPKResults[0][i].is_null()
                              ? "NULL"
                              : maxPKResults[0][i].as<std::string>();
              }

              updateLastProcessedPK(pgConn, schemaName, tableName, lastPK);
              Logger::info(LogCategory::TRANSFER,
                           "Updated last_processed_pk to " + lastPK +
                               " for synchronized table " + schemaName + "." +
                               tableName);
            } else {
              Logger::warning(
                  LogCategory::TRANSFER,
                  "No PK data found for synchronized table " + schemaName +
                      "." + tableName + " - maxPKResults.empty()=" +
                      (maxPKResults.empty() ? "true" : "false") +
                      ", first row empty=" +
                      (!maxPKResults.empty() && maxPKResults[0].size() == 0
                           ? "true"
                           : "false"));
            }
          } catch (const std::exception &e) {
            Logger::error(LogCategory::TRANSFER,
                          "ERROR: Failed to update last_processed_pk for "
                          "synchronized table " +
                              schemaName + "." + tableName + ": " +
                              std::string(e.what()));
          }
        } else {
          Logger::warning(LogCategory::TRANSFER,
                          "DEBUG: Skipping last_processed_pk update for " +
                              schemaName + "." + tableName + " - pkStrategy: " +
                              pkStrategy + ", pkColumns empty: " +
                              (pkColumns.empty() ? "true" : "false"));
        }
      }
    } else if (sourceCount < targetCount) {
      // Hay registros eliminados en el origen - procesar DELETEs por Primary
      // Key
      Logger::info(LogCategory::TRANSFER, "processTableWithDeltas",
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
      Logger::info(LogCategory::TRANSFER, "processTableWithDeltas",
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
      Logger::info(LogCategory::TRANSFER, "processTable",
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
      Logger::info(LogCategory::TRANSFER, "processTable",
                   "Processing FULL_LOAD table: " + schemaName + "." +
                       tableName);

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
        Logger::info(LogCategory::TRANSFER, "processTable",
                     "Truncating table: " + toLowerCase(schemaName) + "." +
                         tableName);
        txn.exec("TRUNCATE TABLE \"" + toLowerCase(schemaName) + "\".\"" +
                 tableName + "\" CASCADE;");
        Logger::debug("processTable", "Table truncated successfully");
      }
      txn.commit();
    }

    auto sourceConn = getPostgresConnection(sourceConnStr);
    if (!sourceConn) {
      updateStatus(pgConn, schemaName, tableName, "ERROR", 0);
      return;
    }

    std::string timeColumn =
        detectTimeColumn(*sourceConn, schemaName, tableName);
    if (timeColumn.empty()) {
      Logger::warning(LogCategory::TRANSFER, "processTable",
                      "No time column detected for " + schemaName + "." +
                          tableName);
    }

    int sourceCount = getSourceCount(*sourceConn, schemaName, tableName);
    int targetCount = getTargetCount(pgConn, schemaName, tableName);

    Logger::debug("processTable",
                  "Table " + schemaName + "." + tableName +
                      " - Source: " + std::to_string(sourceCount) +
                      ", Target: " + std::to_string(targetCount));

    if (sourceCount == targetCount) {
      updateStatus(pgConn, schemaName, tableName, "LISTENING_CHANGES",
                   sourceCount);
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
      Logger::error(LogCategory::TRANSFER, "detectTimeColumn",
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
    } catch (const pqxx::sql_error &e) {
      Logger::error(LogCategory::TRANSFER, "getSourceCount",
                    "SQL ERROR getting source count: " + std::string(e.what()) +
                        " [SQL State: " + e.sqlstate() + "]");
      return 0;
    } catch (const pqxx::broken_connection &e) {
      Logger::error(LogCategory::TRANSFER, "getSourceCount",
                    "CONNECTION ERROR getting source count: " +
                        std::string(e.what()));
      return 0;
    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER, "getSourceCount",
                    "ERROR getting source count: " + std::string(e.what()));
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
    } catch (const pqxx::sql_error &e) {
      Logger::error(LogCategory::TRANSFER, "getTargetCount",
                    "SQL ERROR getting target count: " + std::string(e.what()) +
                        " [SQL State: " + e.sqlstate() + "]");
      return 0;
    } catch (const pqxx::broken_connection &e) {
      Logger::error(LogCategory::TRANSFER, "getTargetCount",
                    "CONNECTION ERROR getting target count: " +
                        std::string(e.what()));
      return 0;
    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER, "getTargetCount",
                    "ERROR getting target count: " + std::string(e.what()));
      return 0;
    }
  }

  void performDataTransfer(pqxx::connection &pgConn,
                           pqxx::connection &sourceConn,
                           const std::string &schemaName,
                           const std::string &tableName,
                           const std::string &lastOffset, int sourceCount) {
    // Validate input parameters
    if (schemaName.empty() || tableName.empty()) {
      Logger::error(LogCategory::TRANSFER, "performDataTransfer",
                    "Schema name or table name is empty");
      return;
    }

    try {
      Logger::info(LogCategory::TRANSFER, "performDataTransfer",
                   "Transferring data for " + schemaName + "." + tableName);

      std::string lowerSchemaName = toLowerCase(schemaName);

      // OPTIMIZED: Usar cursor-based pagination con primary key
      std::string pkStrategy =
          getPKStrategyFromCatalog(pgConn, schemaName, tableName);
      std::vector<std::string> pkColumns =
          getPKColumnsFromCatalog(pgConn, schemaName, tableName);
      std::vector<std::string> candidateColumns =
          getCandidateColumnsFromCatalog(pgConn, schemaName, tableName);
      std::string lastProcessedPK =
          getLastProcessedPKFromCatalog(pgConn, schemaName, tableName);

      const size_t CHUNK_SIZE = SyncConfig::getChunkSize();
      size_t totalProcessed = 0;
      bool hasMoreData = true;
      size_t chunkNumber = 0;

      // CRITICAL: Add timeout to prevent infinite loops
      auto startTime = std::chrono::steady_clock::now();
      const auto MAX_PROCESSING_TIME =
          std::chrono::hours(2); // 2 hours max per table

      while (hasMoreData) {
        chunkNumber++;
        std::string selectQuery =
            "SELECT * FROM \"" + schemaName + "\".\"" + tableName + "\"";

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
                  " minutes) for table " + schemaName + "." + tableName +
                  " - breaking to prevent infinite loop");
          hasMoreData = false;
          break;
        }

        // CRITICAL: Add maximum chunk limit to prevent infinite loops
        if (chunkNumber > 10000) {
          Logger::error(LogCategory::TRANSFER,
                        "CRITICAL: Maximum chunk limit reached (" +
                            std::to_string(chunkNumber) + ") for table " +
                            schemaName + "." + tableName +
                            " - breaking to prevent infinite loop");
          hasMoreData = false;
          break;
        }

        if (pkStrategy == "PK" && !pkColumns.empty()) {
          // CURSOR-BASED PAGINATION: Usar PK para paginación eficiente
          if (!lastProcessedPK.empty()) {
            selectQuery += " WHERE ";
            std::vector<std::string> lastPKValues =
                parseLastPK(lastProcessedPK);

            if (pkColumns.size() == 1) {
              // Single PK: simple comparison
              selectQuery += "\"" + pkColumns[0] + "\" > '" +
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
                    selectQuery += "\"" + pkColumns[j] + "\" > '" +
                                   escapeSQL(lastPKValues[j]) + "'";
                  } else {
                    selectQuery += "\"" + pkColumns[j] + "\" = '" +
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
            selectQuery += "\"" + pkColumns[i] + "\"";
          }
          selectQuery += " LIMIT " + std::to_string(CHUNK_SIZE) + ";";
        } else if (pkStrategy == "TEMPORAL_PK" && !candidateColumns.empty()) {
          // CURSOR-BASED PAGINATION: Usar columnas candidatas para paginación
          // eficiente
          if (!lastProcessedPK.empty()) {
            selectQuery += " WHERE \"" + candidateColumns[0] + "\" > '" +
                           escapeSQL(lastProcessedPK) + "'";
          }

          // Ordenar por la primera columna candidata
          selectQuery += " ORDER BY \"" + candidateColumns[0] + "\"";
          selectQuery += " LIMIT " + std::to_string(CHUNK_SIZE) + ";";
        } else {
          // FALLBACK: Usar OFFSET pagination para tablas sin PK
          selectQuery += " ORDER BY (SELECT NULL) LIMIT " +
                         std::to_string(CHUNK_SIZE) + " OFFSET " +
                         std::to_string(totalProcessed) + ";";
        }

        pqxx::work sourceTxn(sourceConn);
        auto sourceResult = sourceTxn.exec(selectQuery);
        sourceTxn.commit();

        if (sourceResult.empty()) {
          hasMoreData = false;
          break;
        }

        Logger::info(LogCategory::TRANSFER, "performDataTransfer",
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

          try {
            performBulkUpsert(targetConn, results, columnNames, columnTypes,
                              lowerSchemaName, tableName, schemaName);
          } catch (const std::exception &e) {
            std::string errorMsg = e.what();
            Logger::error(LogCategory::TRANSFER,
                          "Bulk upsert failed: " + errorMsg);

            // CRITICAL: Check for transaction abort errors that cause infinite
            // loops
            if (errorMsg.find("current transaction is aborted") !=
                    std::string::npos ||
                errorMsg.find("previously aborted") != std::string::npos ||
                errorMsg.find("aborted transaction") != std::string::npos) {
              Logger::error(LogCategory::TRANSFER,
                            "CRITICAL: Transaction abort detected - breaking "
                            "loop to prevent infinite hang");
              hasMoreData = false;
              break;
            }
          }
        }

        totalProcessed += sourceResult.size();

        // OPTIMIZED: Update last_processed_pk for cursor-based pagination
        if (pkStrategy == "PK" && !pkColumns.empty() && !sourceResult.empty()) {
          // Convert pqxx::result to vector<vector<string>> for
          // getLastPKFromResults
          std::vector<std::vector<std::string>> resultsVector;
          std::vector<std::string> columnNames;

          // Get column names
          if (!sourceResult.empty()) {
            for (size_t i = 0; i < sourceResult[0].size(); ++i) {
              columnNames.push_back(sourceResult[0][i].name());
            }
          }

          // Convert results
          for (const auto &row : sourceResult) {
            std::vector<std::string> rowData;
            for (size_t i = 0; i < row.size(); ++i) {
              rowData.push_back(row[i].is_null() ? "NULL"
                                                 : row[i].as<std::string>());
            }
            resultsVector.push_back(rowData);
          }

          // OPTIMIZED: Update last_processed_pk for cursor-based pagination
          if (((pkStrategy == "PK" && !pkColumns.empty()) ||
               (pkStrategy == "TEMPORAL_PK" && !candidateColumns.empty())) &&
              !resultsVector.empty()) {
            try {
              // Obtener el último PK del chunk procesado
              std::vector<std::string> columnsToUse =
                  (pkStrategy == "PK") ? pkColumns : candidateColumns;
              std::string lastPK = getLastPKFromResults(
                  resultsVector, columnsToUse, columnNames);
              if (!lastPK.empty()) {
                updateLastProcessedPK(pgConn, schemaName, tableName, lastPK);
              }
            } catch (const std::exception &e) {
              Logger::error(LogCategory::TRANSFER, "updateLastProcessedPK",
                            "Error updating last processed PK: " +
                                std::string(e.what()));
            }
          }
        }

        // Actualizar last_offset en la base de datos solo para tablas sin PK
        // (OFFSET pagination) Para tablas con PK o TEMPORAL_PK se usa
        // last_processed_pk en lugar de last_offset
        if (pkStrategy != "PK" && pkStrategy != "TEMPORAL_PK") {
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
            Logger::warning(LogCategory::TRANSFER, "performDataTransfer",
                            "Failed to update last_offset: " +
                                std::string(e.what()));
          }
        }

        // Si obtuvimos menos registros que el chunk size, hemos terminado
        if (sourceResult.size() < CHUNK_SIZE) {
          hasMoreData = false;
        }
      }

      updateStatus(pgConn, schemaName, tableName, "LISTENING_CHANGES",
                   sourceCount);

      // OPTIMIZED: Update last_processed_pk for completed transfer (even if
      // single chunk)
      if (pkStrategy == "PK" && !pkColumns.empty() && totalProcessed > 0) {
        try {
          // Obtener el último PK de la tabla para marcar como completamente
          // procesada
          std::string maxPKQuery = "SELECT ";
          for (size_t i = 0; i < pkColumns.size(); ++i) {
            if (i > 0)
              maxPKQuery += ", ";
            maxPKQuery += "\"" + pkColumns[i] + "\"";
          }
          maxPKQuery +=
              " FROM \"" + schemaName + "\".\"" + tableName + "\" ORDER BY ";
          for (size_t i = 0; i < pkColumns.size(); ++i) {
            if (i > 0)
              maxPKQuery += ", ";
            maxPKQuery += "\"" + pkColumns[i] + "\"";
          }
          maxPKQuery += " DESC LIMIT 1;";

          pqxx::work sourceTxn(sourceConn);
          auto maxPKResults = sourceTxn.exec(maxPKQuery);
          sourceTxn.commit();

          if (!maxPKResults.empty() && !maxPKResults[0].empty()) {
            std::string lastPK;
            for (size_t i = 0; i < maxPKResults[0].size(); ++i) {
              if (i > 0)
                lastPK += "|";
              lastPK += maxPKResults[0][i].is_null()
                            ? "NULL"
                            : maxPKResults[0][i].as<std::string>();
            }

            updateLastProcessedPK(pgConn, schemaName, tableName, lastPK);
            Logger::info(LogCategory::TRANSFER,
                         "Updated last_processed_pk to " + lastPK +
                             " for completed table " + schemaName + "." +
                             tableName);
          }
        } catch (const std::exception &e) {
          Logger::error(
              LogCategory::TRANSFER,
              "ERROR: Failed to update last_processed_pk for completed table " +
                  schemaName + "." + tableName + ": " + std::string(e.what()));
        }
      }

      Logger::info(LogCategory::TRANSFER, "performDataTransfer",
                   "Successfully transferred " +
                       std::to_string(totalProcessed) + " records for " +
                       schemaName + "." + tableName);

    } catch (const pqxx::sql_error &e) {
      Logger::error(LogCategory::TRANSFER, "performDataTransfer",
                    "SQL ERROR transferring data: " + std::string(e.what()) +
                        " [SQL State: " + e.sqlstate() + "]");
      updateStatus(pgConn, schemaName, tableName, "ERROR", 0);
    } catch (const pqxx::broken_connection &e) {
      Logger::error(LogCategory::TRANSFER, "performDataTransfer",
                    "CONNECTION ERROR transferring data: " +
                        std::string(e.what()));
      updateStatus(pgConn, schemaName, tableName, "ERROR", 0);
    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER, "performDataTransfer",
                    "ERROR transferring data: " + std::string(e.what()));
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

      std::string updateQuery =
          "UPDATE metadata.catalog SET status='" + status + "'";

      // Actualizar last_offset para todos los status que requieren tracking
      if (status == "FULL_LOAD" || status == "RESET" ||
          status == "LISTENING_CHANGES") {
        updateQuery += ", last_offset='" + std::to_string(count) + "'";
      }

      updateQuery += " WHERE schema_name='" + escapeSQL(schemaName) +
                     "' AND table_name='" + escapeSQL(tableName) + "';";

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
    } catch (const pqxx::sql_error &e) {
      Logger::error(LogCategory::TRANSFER, "getPKStrategyFromCatalog",
                    "SQL ERROR getting PK strategy: " + std::string(e.what()) +
                        " [SQL State: " + e.sqlstate() + "]");
    } catch (const pqxx::broken_connection &e) {
      Logger::error(LogCategory::TRANSFER, "getPKStrategyFromCatalog",
                    "CONNECTION ERROR getting PK strategy: " +
                        std::string(e.what()));
    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER, "getPKStrategyFromCatalog",
                    "ERROR getting PK strategy: " + std::string(e.what()));
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
    } catch (const pqxx::sql_error &e) {
      Logger::error(LogCategory::TRANSFER, "getPKColumnsFromCatalog",
                    "SQL ERROR getting PK columns: " + std::string(e.what()) +
                        " [SQL State: " + e.sqlstate() + "]");
    } catch (const pqxx::broken_connection &e) {
      Logger::error(LogCategory::TRANSFER, "getPKColumnsFromCatalog",
                    "CONNECTION ERROR getting PK columns: " +
                        std::string(e.what()));
    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER, "getPKColumnsFromCatalog",
                    "ERROR getting PK columns: " + std::string(e.what()));
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
    } catch (const pqxx::sql_error &e) {
      Logger::error(
          LogCategory::TRANSFER, "getCandidateColumnsFromCatalog",
          "SQL ERROR getting candidate columns: " + std::string(e.what()) +
              " [SQL State: " + e.sqlstate() + "]");
    } catch (const pqxx::broken_connection &e) {
      Logger::error(LogCategory::TRANSFER, "getCandidateColumnsFromCatalog",
                    "CONNECTION ERROR getting candidate columns: " +
                        std::string(e.what()));
    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER, "getCandidateColumnsFromCatalog",
                    "ERROR getting candidate columns: " +
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
    } catch (const pqxx::sql_error &e) {
      Logger::error(
          LogCategory::TRANSFER, "getLastProcessedPKFromCatalog",
          "SQL ERROR getting last processed PK: " + std::string(e.what()) +
              " [SQL State: " + e.sqlstate() + "]");
    } catch (const pqxx::broken_connection &e) {
      Logger::error(LogCategory::TRANSFER, "getLastProcessedPKFromCatalog",
                    "CONNECTION ERROR getting last processed PK: " +
                        std::string(e.what()));
    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER, "getLastProcessedPKFromCatalog",
                    "ERROR getting last processed PK: " +
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
    } catch (const pqxx::sql_error &e) {
      Logger::error(
          LogCategory::TRANSFER, "updateLastProcessedPK",
          "SQL ERROR updating last processed PK: " + std::string(e.what()) +
              " [SQL State: " + e.sqlstate() + "]");
    } catch (const pqxx::broken_connection &e) {
      Logger::error(LogCategory::TRANSFER, "updateLastProcessedPK",
                    "CONNECTION ERROR updating last processed PK: " +
                        std::string(e.what()));
    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER, "updateLastProcessedPK",
                    "ERROR updating last processed PK: " +
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

    } catch (const pqxx::sql_error &e) {
      Logger::error(LogCategory::TRANSFER, "performBulkUpsert",
                    "SQL ERROR in bulk upsert: " + std::string(e.what()) +
                        " [SQL State: " + e.sqlstate() + "]");
      throw;
    } catch (const pqxx::broken_connection &e) {
      Logger::error(LogCategory::TRANSFER, "performBulkUpsert",
                    "CONNECTION ERROR in bulk upsert: " +
                        std::string(e.what()));
      throw;
    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER, "performBulkUpsert",
                    "ERROR in bulk upsert: " + std::string(e.what()));
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

    } catch (const pqxx::sql_error &e) {
      Logger::error(LogCategory::TRANSFER, "performBulkInsert",
                    "SQL ERROR in bulk insert: " + std::string(e.what()) +
                        " [SQL State: " + e.sqlstate() + "]");
      throw;
    } catch (const pqxx::broken_connection &e) {
      Logger::error(LogCategory::TRANSFER, "performBulkInsert",
                    "CONNECTION ERROR in bulk insert: " +
                        std::string(e.what()));
      throw;
    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER, "performBulkInsert",
                    "ERROR in bulk insert: " + std::string(e.what()));
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

      const size_t BATCH_SIZE = SyncConfig::getChunkSize();
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
          Logger::error(LogCategory::TRANSFER, "processDeletesByPrimaryKey",
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

          Logger::info(LogCategory::TRANSFER, "processDeletesByPrimaryKey",
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
        Logger::info(LogCategory::TRANSFER, "processDeletesByPrimaryKey",
                     "Total deleted records: " + std::to_string(totalDeleted) +
                         " from " + schema_name + "." + table_name);
      }

    } catch (const pqxx::sql_error &e) {
      Logger::error(LogCategory::TRANSFER, "processDeletesByPrimaryKey",
                    "SQL ERROR processing deletes for " + schema_name + "." +
                        table_name + ": " + std::string(e.what()) +
                        " [SQL State: " + e.sqlstate() + "]");
    } catch (const pqxx::broken_connection &e) {
      Logger::error(LogCategory::TRANSFER, "processDeletesByPrimaryKey",
                    "CONNECTION ERROR processing deletes for " + schema_name +
                        "." + table_name + ": " + std::string(e.what()));
    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER, "processDeletesByPrimaryKey",
                    "ERROR processing deletes for " + schema_name + "." +
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

      Logger::info(LogCategory::TRANSFER, "processUpdatesByPrimaryKey",
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
          Logger::warning(LogCategory::TRANSFER, "processUpdatesByPrimaryKey",
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
          std::string lowerPkColumn = pkColumns[i];
          std::transform(lowerPkColumn.begin(), lowerPkColumn.end(), lowerPkColumn.begin(), ::tolower);
          whereClause +=
              "\"" + lowerPkColumn + "\" = " +
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
        Logger::info(LogCategory::TRANSFER, "processUpdatesByPrimaryKey",
                     "Updated " + std::to_string(totalUpdated) +
                         " records in " + schema_name + "." + table_name);
      }

    } catch (const pqxx::sql_error &e) {
      Logger::error(LogCategory::TRANSFER, "processUpdatesByPrimaryKey",
                    "SQL ERROR processing updates for " + schema_name + "." +
                        table_name + ": " + std::string(e.what()) +
                        " [SQL State: " + e.sqlstate() + "]");
    } catch (const pqxx::broken_connection &e) {
      Logger::error(LogCategory::TRANSFER, "processUpdatesByPrimaryKey",
                    "CONNECTION ERROR processing updates for " + schema_name +
                        "." + table_name + ": " + std::string(e.what()));
    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER, "processUpdatesByPrimaryKey",
                    "ERROR processing updates for " + schema_name + "." +
                        table_name + ": " + std::string(e.what()));
    }
  }

  std::vector<std::string> getPrimaryKeyColumns(pqxx::connection &sourceConn,
                                                const std::string &schema_name,
                                                const std::string &table_name) {
    std::vector<std::string> pkColumns;

    // Validate input parameters
    if (schema_name.empty() || table_name.empty()) {
      Logger::error(LogCategory::TRANSFER, "getPrimaryKeyColumns",
                    "Schema name or table name is empty");
      return pkColumns;
    }

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
    } catch (const pqxx::sql_error &e) {
      Logger::error(
          LogCategory::TRANSFER, "getPrimaryKeyColumns",
          "SQL ERROR getting primary key columns: " + std::string(e.what()) +
              " [SQL State: " + e.sqlstate() + "]");
    } catch (const pqxx::broken_connection &e) {
      Logger::error(LogCategory::TRANSFER, "getPrimaryKeyColumns",
                    "CONNECTION ERROR getting primary key columns: " +
                        std::string(e.what()));
    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER, "getPrimaryKeyColumns",
                    "ERROR getting primary key columns: " +
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

    const size_t CHECK_BATCH_SIZE =
        std::min(SyncConfig::getChunkSize() / 2, static_cast<size_t>(500));

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
      } catch (const pqxx::sql_error &e) {
        Logger::error(LogCategory::TRANSFER, "findDeletedPrimaryKeys",
                      "SQL ERROR checking deleted primary keys: " +
                          std::string(e.what()) +
                          " [SQL State: " + e.sqlstate() + "]");
        break;
      } catch (const pqxx::broken_connection &e) {
        Logger::error(LogCategory::TRANSFER, "findDeletedPrimaryKeys",
                      "CONNECTION ERROR checking deleted primary keys: " +
                          std::string(e.what()));
        break;
      } catch (const std::exception &e) {
        Logger::error(LogCategory::TRANSFER, "findDeletedPrimaryKeys",
                      "ERROR checking deleted primary keys: " +
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

    } catch (const pqxx::sql_error &e) {
      Logger::error(LogCategory::TRANSFER, "deleteRecordsByPrimaryKey",
                    "SQL ERROR deleting records: " + std::string(e.what()) +
                        " [SQL State: " + e.sqlstate() + "]");
    } catch (const pqxx::broken_connection &e) {
      Logger::error(LogCategory::TRANSFER, "deleteRecordsByPrimaryKey",
                    "CONNECTION ERROR deleting records: " +
                        std::string(e.what()));
    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER, "deleteRecordsByPrimaryKey",
                    "ERROR deleting records: " + std::string(e.what()));
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
          std::string columnName = columnNames[i];
          std::transform(columnName.begin(), columnName.end(), columnName.begin(), ::tolower);
          std::string setClause = "\"" + columnName + "\" = ";
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

    } catch (const pqxx::sql_error &e) {
      Logger::error(
          LogCategory::TRANSFER, "compareAndUpdateRecord",
          "SQL ERROR comparing/updating record: " + std::string(e.what()) +
              " [SQL State: " + e.sqlstate() + "]");
      return false;
    } catch (const pqxx::broken_connection &e) {
      Logger::error(LogCategory::TRANSFER, "compareAndUpdateRecord",
                    "CONNECTION ERROR comparing/updating record: " +
                        std::string(e.what()));
      return false;
    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER, "compareAndUpdateRecord",
                    "ERROR comparing/updating record: " +
                        std::string(e.what()));
      return false;
    }
  }

  std::string cleanValueForPostgres(const std::string &value,
                                    const std::string &columnType) {
    std::string cleanValue = value;
    std::string upperType = columnType;
    std::transform(upperType.begin(), upperType.end(), upperType.begin(),
                   ::toupper);

    // Detectar valores NULL de PostgreSQL - SIMPLIFICADO
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
};

// Definición de variables estáticas
std::unordered_map<std::string, std::string> PostgresToPostgres::dataTypeMap = {
    {"int4", "INTEGER"},
    {"int8", "BIGINT"},
    {"int2", "SMALLINT"},
    {"serial", "INTEGER"},
    {"bigserial", "BIGINT"},
    {"smallserial", "SMALLINT"},
    {"numeric", "NUMERIC"},
    {"decimal", "NUMERIC"},
    {"real", "REAL"},
    {"float4", "REAL"},
    {"double precision", "DOUBLE PRECISION"},
    {"float8", "DOUBLE PRECISION"},
    {"money", "NUMERIC(19,4)"},
    {"varchar", "VARCHAR"},
    {"character varying", "VARCHAR"},
    {"char", "CHAR"},
    {"character", "CHAR"},
    {"text", "TEXT"},
    {"bytea", "BYTEA"},
    {"timestamp", "TIMESTAMP"},
    {"timestamp without time zone", "TIMESTAMP"},
    {"timestamp with time zone", "TIMESTAMP WITH TIME ZONE"},
    {"timestamptz", "TIMESTAMP WITH TIME ZONE"},
    {"date", "DATE"},
    {"time", "TIME"},
    {"time without time zone", "TIME"},
    {"time with time zone", "TIME WITH TIME ZONE"},
    {"timetz", "TIME WITH TIME ZONE"},
    {"interval", "INTERVAL"},
    {"boolean", "BOOLEAN"},
    {"bool", "BOOLEAN"},
    {"bit", "BIT"},
    {"bit varying", "BIT VARYING"},
    {"varbit", "BIT VARYING"},
    {"uuid", "UUID"},
    {"xml", "TEXT"},
    {"json", "JSON"},
    {"jsonb", "JSONB"},
    {"array", "TEXT"},
    {"inet", "INET"},
    {"cidr", "CIDR"},
    {"macaddr", "MACADDR"},
    {"point", "POINT"},
    {"line", "LINE"},
    {"lseg", "LSEG"},
    {"box", "BOX"},
    {"path", "PATH"},
    {"polygon", "POLYGON"},
    {"circle", "CIRCLE"}};

#endif // POSTGRESTOPOSTGRES_H
