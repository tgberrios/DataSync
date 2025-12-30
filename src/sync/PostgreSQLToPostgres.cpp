#include "sync/PostgreSQLToPostgres.h"
#include "core/Config.h"
#include "core/database_config.h"
#include "engines/postgres_engine.h"
#include "third_party/json.hpp"
#include <algorithm>
#include <pqxx/pqxx>
#include <set>

using json = nlohmann::json;

// For PostgreSQL to PostgreSQL, no value cleaning needed - same database type
std::string
PostgreSQLToPostgres::cleanValueForPostgres(const std::string &value,
                                            const std::string &columnType) {
  // PostgreSQL to PostgreSQL - no cleaning needed, just return as-is
  return value;
}

std::unique_ptr<pqxx::connection> PostgreSQLToPostgres::getPostgreSQLConnection(
    const std::string &connectionString) {
  try {
    if (connectionString.empty()) {
      Logger::error(LogCategory::TRANSFER, "getPostgreSQLConnection",
                    "Empty connection string provided");
      return nullptr;
    }

    std::unique_ptr<pqxx::connection> conn =
        std::make_unique<pqxx::connection>(connectionString);

    if (!conn->is_open()) {
      Logger::error(LogCategory::TRANSFER, "getPostgreSQLConnection",
                    "PostgreSQL connection failed");
      return nullptr;
    }

    // Test connection
    pqxx::work testTxn(*conn);
    testTxn.exec("SELECT 1");
    testTxn.commit();

    Logger::info(LogCategory::TRANSFER, "getPostgreSQLConnection",
                 "PostgreSQL connection established successfully");
    return conn;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "getPostgreSQLConnection",
                  "Error creating PostgreSQL connection: " +
                      std::string(e.what()));
    return nullptr;
  }
}

std::vector<DatabaseToPostgresSync::TableInfo>
PostgreSQLToPostgres::getActiveTables(pqxx::connection &pgConn) {
  std::vector<TableInfo> tables;
  try {
    pqxx::work txn(pgConn);
    std::string query = "SELECT schema_name, table_name, connection_string, "
                        "COALESCE(pk_strategy, '') as pk_strategy, "
                        "COALESCE(status, '') as status, pk_columns "
                        "FROM metadata.catalog "
                        "WHERE db_engine = 'PostgreSQL' "
                        "AND active = true "
                        "AND status IN ('FULL_LOAD', 'SYNC', 'IN_PROGRESS', "
                        "'LISTENING_CHANGES', 'RESET') "
                        "ORDER BY "
                        "CASE status "
                        "  WHEN 'FULL_LOAD' THEN 1 "
                        "  WHEN 'RESET' THEN 2 "
                        "  WHEN 'SYNC' THEN 3 "
                        "  WHEN 'IN_PROGRESS' THEN 4 "
                        "  WHEN 'LISTENING_CHANGES' THEN 5 "
                        "  ELSE 6 "
                        "END";

    auto result = txn.exec(query);
    txn.commit();

    for (const auto &row : result) {
      TableInfo table;
      table.schema_name = row[0].as<std::string>();
      table.table_name = row[1].as<std::string>();
      table.connection_string = row[2].as<std::string>();
      table.pk_strategy = row[3].is_null() ? "" : row[3].as<std::string>();
      table.status = row[4].is_null() ? "" : row[4].as<std::string>();
      table.pk_columns = row[5].is_null() ? "" : row[5].as<std::string>();

      std::vector<std::string> pkCols = parseJSONArray(table.pk_columns);
      table.has_pk = !pkCols.empty();
      table.db_engine = "PostgreSQL";

      tables.push_back(table);
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "getActiveTables",
                  "Error getting active PostgreSQL tables: " +
                      std::string(e.what()));
  }
  return tables;
}

void PostgreSQLToPostgres::setupTableTargetPostgreSQLToPostgres() {
  Logger::info(LogCategory::TRANSFER,
               "Starting PostgreSQL to PostgreSQL table setup");

  try {
    pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());

    if (!pgConn.is_open()) {
      Logger::error(LogCategory::TRANSFER,
                    "setupTableTargetPostgreSQLToPostgres",
                    "CRITICAL ERROR: Cannot establish PostgreSQL connection");
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
                return false;
              });

    Logger::info(LogCategory::TRANSFER, "Setting up " +
                                            std::to_string(tables.size()) +
                                            " PostgreSQL tables");

    // Create datasync_metadata schema and ds_change_log table
    {
      pqxx::work setupTxn(pgConn);
      setupTxn.exec("CREATE SCHEMA IF NOT EXISTS datasync_metadata");
      setupTxn.exec(
          "CREATE TABLE IF NOT EXISTS datasync_metadata.ds_change_log ("
          "change_id BIGSERIAL PRIMARY KEY,"
          "change_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,"
          "operation CHAR(1) NOT NULL,"
          "schema_name VARCHAR(255) NOT NULL,"
          "table_name VARCHAR(255) NOT NULL,"
          "pk_values JSONB NOT NULL,"
          "row_data JSONB NOT NULL"
          ")");
      setupTxn.exec(
          "CREATE INDEX IF NOT EXISTS idx_ds_change_log_table_time ON "
          "datasync_metadata.ds_change_log(schema_name, table_name, "
          "change_time)");
      setupTxn.exec(
          "CREATE INDEX IF NOT EXISTS idx_ds_change_log_table_change ON "
          "datasync_metadata.ds_change_log(schema_name, table_name, "
          "change_id)");
      setupTxn.commit();
    }

    for (const auto &table : tables) {
      if (table.db_engine != "PostgreSQL")
        continue;

      auto sourceConn = getPostgreSQLConnection(table.connection_string);
      if (!sourceConn) {
        Logger::error(LogCategory::TRANSFER,
                      "setupTableTargetPostgreSQLToPostgres",
                      "Failed to get PostgreSQL connection for table " +
                          table.schema_name + "." + table.table_name);
        continue;
      }

      std::vector<std::string> pkColumns = getPrimaryKeyColumns(
          sourceConn.get(), table.schema_name, table.table_name);

      // Get all columns
      pqxx::work colTxn(*sourceConn);
      std::string colQuery =
          "SELECT column_name, data_type, is_nullable, column_default "
          "FROM information_schema.columns "
          "WHERE table_schema = " +
          colTxn.quote(table.schema_name) +
          " AND table_name = " + colTxn.quote(table.table_name) +
          " ORDER BY ordinal_position";
      auto colResult = colTxn.exec(colQuery);
      colTxn.commit();

      if (colResult.empty()) {
        Logger::warning(LogCategory::TRANSFER,
                        "setupTableTargetPostgreSQLToPostgres",
                        "No columns found for " + table.schema_name + "." +
                            table.table_name);
        continue;
      }

      bool hasPK = !pkColumns.empty();
      std::string jsonObjectNew;
      std::string jsonObjectOld;
      std::string rowDataNew;
      std::string rowDataOld;

      // Create triggers
      std::string triggerInsert =
          "ds_tr_" + table.schema_name + "_" + table.table_name + "_ai";
      std::string triggerUpdate =
          "ds_tr_" + table.schema_name + "_" + table.table_name + "_au";
      std::string triggerDelete =
          "ds_tr_" + table.schema_name + "_" + table.table_name + "_ad";

      pqxx::work triggerTxn(*sourceConn);

      // Drop existing triggers
      triggerTxn.exec("DROP TRIGGER IF EXISTS " +
                      triggerTxn.quote_name(triggerInsert) + " ON " +
                      triggerTxn.quote_name(table.schema_name) + "." +
                      triggerTxn.quote_name(table.table_name));
      triggerTxn.exec("DROP TRIGGER IF EXISTS " +
                      triggerTxn.quote_name(triggerUpdate) + " ON " +
                      triggerTxn.quote_name(table.schema_name) + "." +
                      triggerTxn.quote_name(table.table_name));
      triggerTxn.exec("DROP TRIGGER IF EXISTS " +
                      triggerTxn.quote_name(triggerDelete) + " ON " +
                      triggerTxn.quote_name(table.schema_name) + "." +
                      triggerTxn.quote_name(table.table_name));

      // Create function for triggers
      std::string functionName =
          "ds_fn_" + table.schema_name + "_" + table.table_name;
      triggerTxn.exec("DROP FUNCTION IF EXISTS " +
                      triggerTxn.quote_name(functionName) + "() CASCADE");

      // Build function body
      std::string fnBody = "CREATE OR REPLACE FUNCTION " +
                           triggerTxn.quote_name(functionName) +
                           "() "
                           "RETURNS TRIGGER AS $$ "
                           "DECLARE "
                           "  pk_vals JSONB; "
                           "  row_vals JSONB; "
                           "BEGIN ";

      if (hasPK) {
        fnBody += "pk_vals := jsonb_build_object(";
        for (size_t i = 0; i < pkColumns.size(); ++i) {
          if (i > 0)
            fnBody += ", ";
          std::string pkColQuoted = triggerTxn.quote(pkColumns[i]);
          std::string pkColName = triggerTxn.quote_name(pkColumns[i]);
          fnBody += pkColQuoted + ", ";
          fnBody += "CASE WHEN TG_OP = 'DELETE' THEN to_jsonb(OLD." +
                    pkColName +
                    ") "
                    "ELSE to_jsonb(NEW." +
                    pkColName + ") END";
        }
        fnBody += "); ";
      } else {
        fnBody += "pk_vals := jsonb_build_object('_hash', "
                  "md5(CASE WHEN TG_OP = 'DELETE' THEN row_to_json(OLD)::text "
                  "ELSE row_to_json(NEW)::text END)); ";
      }

      fnBody +=
          "row_vals := CASE WHEN TG_OP = 'DELETE' THEN row_to_json(OLD)::jsonb "
          "ELSE row_to_json(NEW)::jsonb END; "
          "INSERT INTO datasync_metadata.ds_change_log "
          "(operation, schema_name, table_name, pk_values, row_data) "
          "VALUES (TG_OP::char(1), TG_TABLE_SCHEMA, TG_TABLE_NAME, pk_vals, "
          "row_vals); "
          "RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END; "
          "END; "
          "$$ LANGUAGE plpgsql;";

      triggerTxn.exec(fnBody);

      // Create triggers using the function
      triggerTxn.exec("CREATE TRIGGER " + triggerTxn.quote_name(triggerInsert) +
                      " AFTER INSERT ON " +
                      triggerTxn.quote_name(table.schema_name) + "." +
                      triggerTxn.quote_name(table.table_name) +
                      " FOR EACH ROW EXECUTE FUNCTION " +
                      triggerTxn.quote_name(functionName) + "()");

      triggerTxn.exec("CREATE TRIGGER " + triggerTxn.quote_name(triggerUpdate) +
                      " AFTER UPDATE ON " +
                      triggerTxn.quote_name(table.schema_name) + "." +
                      triggerTxn.quote_name(table.table_name) +
                      " FOR EACH ROW EXECUTE FUNCTION " +
                      triggerTxn.quote_name(functionName) + "()");

      triggerTxn.exec("CREATE TRIGGER " + triggerTxn.quote_name(triggerDelete) +
                      " AFTER DELETE ON " +
                      triggerTxn.quote_name(table.schema_name) + "." +
                      triggerTxn.quote_name(table.table_name) +
                      " FOR EACH ROW EXECUTE FUNCTION " +
                      triggerTxn.quote_name(functionName) + "()");

      triggerTxn.commit();

      Logger::info(
          LogCategory::TRANSFER, "setupTableTargetPostgreSQLToPostgres",
          "Created triggers for " + table.schema_name + "." + table.table_name +
              (hasPK ? " (with PK)" : " (no PK, using hash)"));

      // Create target table in datalake
      std::string lowerSchema = table.schema_name;
      std::transform(lowerSchema.begin(), lowerSchema.end(),
                     lowerSchema.begin(), ::tolower);
      std::string lowerTableName = table.table_name;
      std::transform(lowerTableName.begin(), lowerTableName.end(),
                     lowerTableName.begin(), ::tolower);

      {
        pqxx::work targetTxn(pgConn);
        targetTxn.exec("CREATE SCHEMA IF NOT EXISTS \"" + lowerSchema + "\"");

        // Get column definitions from source
        pqxx::work sourceTxn(*sourceConn);
        std::string createQuery = "CREATE TABLE IF NOT EXISTS \"" +
                                  lowerSchema + "\".\"" + lowerTableName +
                                  "\" (";

        std::vector<std::string> primaryKeys;
        std::vector<std::string> columnDefinitions;

        for (const auto &col : colResult) {
          std::string colName = col[0].as<std::string>();
          std::transform(colName.begin(), colName.end(), colName.begin(),
                         ::tolower);
          std::string dataType = col[1].as<std::string>();
          std::string isNullable = col[2].as<std::string>();
          std::string defaultValue =
              col[3].is_null() ? "" : col[3].as<std::string>();

          // Check if it's a primary key
          bool isPK = std::find(pkColumns.begin(), pkColumns.end(), colName) !=
                      pkColumns.end();
          if (isPK) {
            primaryKeys.push_back(colName);
          }

          std::string nullable = (isNullable == "NO") ? " NOT NULL" : "";
          std::string def = "";
          if (!defaultValue.empty() && defaultValue != "NULL") {
            def = " DEFAULT " + defaultValue;
          }

          std::string columnDef =
              "\"" + colName + "\" " + dataType + nullable + def;
          columnDefinitions.push_back(columnDef);
        }

        if (columnDefinitions.empty()) {
          Logger::error(LogCategory::TRANSFER,
                        "setupTableTargetPostgreSQLToPostgres",
                        "No columns found for table " + table.schema_name +
                            "." + table.table_name);
          continue;
        }

        for (size_t i = 0; i < columnDefinitions.size(); ++i) {
          if (i > 0)
            createQuery += ", ";
          createQuery += columnDefinitions[i];
        }

        // Check for duplicate PKs
        bool hasDuplicatePKs = false;
        if (!primaryKeys.empty()) {
          try {
            std::string sampleQuery = "SELECT ";
            for (size_t i = 0; i < primaryKeys.size(); ++i) {
              if (i > 0)
                sampleQuery += ", ";
              sampleQuery += sourceTxn.quote_name(primaryKeys[i]);
            }
            sampleQuery += " FROM " + sourceTxn.quote_name(table.schema_name) +
                           "." + sourceTxn.quote_name(table.table_name) +
                           " LIMIT 1000";

            auto sampleResult = sourceTxn.exec(sampleQuery);
            std::set<std::string> seenPKs;

            for (const auto &row : sampleResult) {
              std::string pkKey;
              for (size_t i = 0; i < primaryKeys.size(); ++i) {
                if (i > 0)
                  pkKey += "|";
                std::string pkValue =
                    row[i].is_null() ? "<NULL>" : row[i].as<std::string>();
                pkKey += pkValue;
              }
              if (seenPKs.find(pkKey) != seenPKs.end()) {
                hasDuplicatePKs = true;
                Logger::warning(LogCategory::TRANSFER,
                                "setupTableTargetPostgreSQLToPostgres",
                                "Duplicate PK values detected for " +
                                    table.schema_name + "." + table.table_name);
                break;
              }
              seenPKs.insert(pkKey);
            }
          } catch (const std::exception &e) {
            Logger::warning(LogCategory::TRANSFER,
                            "setupTableTargetPostgreSQLToPostgres",
                            "Error checking PKs: " + std::string(e.what()));
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
        createQuery += ")";

        targetTxn.exec(createQuery);
        targetTxn.commit();

        Logger::info(
            LogCategory::TRANSFER, "setupTableTargetPostgreSQLToPostgres",
            "Created target table " + lowerSchema + "." + lowerTableName);
      }
    }

    Logger::info(LogCategory::TRANSFER,
                 "PostgreSQL to PostgreSQL table setup completed successfully");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "setupTableTargetPostgreSQLToPostgres",
                  "Error: " + std::string(e.what()));
  }
}

void PostgreSQLToPostgres::transferDataPostgreSQLToPostgres() {
  transferDataPostgreSQLToPostgresParallel();
}

void PostgreSQLToPostgres::transferDataPostgreSQLToPostgresParallel() {
  Logger::info(LogCategory::TRANSFER,
               "Starting parallel PostgreSQL to PostgreSQL data transfer");

  try {
    startParallelProcessing();

    pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());

    if (!pgConn.is_open()) {
      Logger::error(LogCategory::TRANSFER,
                    "transferDataPostgreSQLToPostgresParallel",
                    "CRITICAL ERROR: Cannot establish PostgreSQL connection");
      shutdownParallelProcessing();
      return;
    }

    auto tables = getActiveTables(pgConn);

    if (tables.empty()) {
      Logger::info(
          LogCategory::TRANSFER,
          "No active PostgreSQL tables found - skipping transfer cycle");
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
                return false;
              });

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
                     " tables");

    size_t skipped = 0;
    for (const auto &table : tables) {
      if (table.db_engine != "PostgreSQL") {
        skipped++;
        continue;
      }

      try {
        PostgreSQLEngine engine(table.connection_string);
        std::vector<ColumnInfo> sourceColumns =
            engine.getTableColumns(table.schema_name, table.table_name);

        if (!sourceColumns.empty()) {
          SchemaSync::syncSchema(pgConn, table.schema_name, table.table_name,
                                 sourceColumns, "PostgreSQL");
        }
      } catch (const std::exception &e) {
        Logger::warning(LogCategory::TRANSFER,
                        "transferDataPostgreSQLToPostgresParallel",
                        "Error syncing schema: " + std::string(e.what()));
      }

      pool.submitTask(table,
                      [this](const DatabaseToPostgresSync::TableInfo &t) {
                        this->processTableParallelWithConnection(t);
                      });
    }

    Logger::info(LogCategory::TRANSFER,
                 "Submitted " + std::to_string(tables.size() - skipped) +
                     " PostgreSQL tables to thread pool");

    pool.waitForCompletion();

    Logger::info(LogCategory::TRANSFER,
                 "Thread pool completed - Completed: " +
                     std::to_string(pool.completedTasks()) +
                     " | Failed: " + std::to_string(pool.failedTasks()));

    shutdownParallelProcessing();

  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER,
                  "transferDataPostgreSQLToPostgresParallel",
                  "CRITICAL ERROR: " + std::string(e.what()));
    shutdownParallelProcessing();
  }
}

void PostgreSQLToPostgres::processTableParallelWithConnection(
    const TableInfo &table) {
  try {
    pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());
    if (!pgConn.is_open()) {
      Logger::error(LogCategory::TRANSFER, "processTableParallelWithConnection",
                    "Failed to establish PostgreSQL connection");
      return;
    }

    processTableParallel(table, pgConn);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "processTableParallelWithConnection",
                  "Error: " + std::string(e.what()));
  }
}

void PostgreSQLToPostgres::processTableParallel(const TableInfo &table,
                                                pqxx::connection &pgConn) {
  std::string tableKey = table.schema_name + "." + table.table_name;

  Logger::info(LogCategory::TRANSFER,
               "Starting parallel processing for table " + tableKey);

  try {
    setTableProcessingState(tableKey, true);
    updateStatus(pgConn, table.schema_name, table.table_name, "IN_PROGRESS");

    auto sourceConn = getPostgreSQLConnection(table.connection_string);
    if (!sourceConn) {
      Logger::error(LogCategory::TRANSFER, "processTableParallel",
                    "Failed to get PostgreSQL connection");
      updateStatus(pgConn, table.schema_name, table.table_name, "ERROR");
      return;
    }

    // Get table metadata
    pqxx::work metaTxn(*sourceConn);
    std::string query = "SELECT column_name, data_type, is_nullable "
                        "FROM information_schema.columns "
                        "WHERE table_schema = " +
                        metaTxn.quote(table.schema_name) +
                        " AND table_name = " + metaTxn.quote(table.table_name) +
                        " ORDER BY ordinal_position";

    auto colResult = metaTxn.exec(query);
    metaTxn.commit();

    if (colResult.empty()) {
      Logger::error(LogCategory::TRANSFER, "processTableParallel",
                    "No columns found for table " + tableKey);
      return;
    }

    // Prepare column information
    std::vector<std::string> columnNames;
    std::vector<std::string> columnTypes;
    for (const auto &col : colResult) {
      std::string colName = col[0].as<std::string>();
      std::transform(colName.begin(), colName.end(), colName.begin(),
                     ::tolower);
      columnNames.push_back(colName);
      columnTypes.push_back(col[1].as<std::string>());
    }

    std::string lowerSchemaName = table.schema_name;
    std::transform(lowerSchemaName.begin(), lowerSchemaName.end(),
                   lowerSchemaName.begin(), ::tolower);
    std::string lowerTableNamePG = table.table_name;
    std::transform(lowerTableNamePG.begin(), lowerTableNamePG.end(),
                   lowerTableNamePG.begin(), ::tolower);

    {
      pqxx::work schemaTxn(pgConn);
      schemaTxn.exec("CREATE SCHEMA IF NOT EXISTS \"" + lowerSchemaName + "\"");
      schemaTxn.commit();
    }

    // Check if table exists, create if not
    {
      pqxx::work checkTxn(pgConn);
      auto existsResult = checkTxn.exec(
          "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE "
          "table_schema = " +
          checkTxn.quote(lowerSchemaName) +
          " AND table_name = " + checkTxn.quote(lowerTableNamePG) + ")");
      checkTxn.commit();

      if (!existsResult.empty() && !existsResult[0][0].as<bool>()) {
        // Create table - copy structure from source
        pqxx::work createTxn(*sourceConn);
        std::string createQuery =
            "SELECT 'CREATE TABLE IF NOT EXISTS \"" + lowerSchemaName +
            "\".\"" + lowerTableNamePG +
            "\" (' || "
            "string_agg(column_def, ', ') || ')' "
            "FROM ("
            "SELECT '\"' || lower(column_name) || '\" ' || "
            "data_type || "
            "CASE WHEN is_nullable = 'NO' THEN ' NOT NULL' ELSE '' END as "
            "column_def "
            "FROM information_schema.columns "
            "WHERE table_schema = " +
            createTxn.quote(table.schema_name) +
            " AND table_name = " + createTxn.quote(table.table_name) +
            " ORDER BY ordinal_position"
            ") sub";

        // Actually, let's create it directly
        std::string directCreate = "CREATE TABLE IF NOT EXISTS \"" +
                                   lowerSchemaName + "\".\"" +
                                   lowerTableNamePG + "\" (";

        std::vector<std::string> primaryKeys;
        for (const auto &col : colResult) {
          std::string colName = col[0].as<std::string>();
          std::transform(colName.begin(), colName.end(), colName.begin(),
                         ::tolower);
          std::string dataType = col[1].as<std::string>();
          std::string isNullable = col[2].as<std::string>();

          bool isPK =
              std::find(columnNames.begin(), columnNames.end(), colName) !=
                  columnNames.end() &&
              getPrimaryKeyColumns(sourceConn.get(), table.schema_name,
                                   table.table_name)
                      .size() > 0 &&
              std::find(
                  getPrimaryKeyColumns(sourceConn.get(), table.schema_name,
                                       table.table_name)
                      .begin(),
                  getPrimaryKeyColumns(sourceConn.get(), table.schema_name,
                                       table.table_name)
                      .end(),
                  colName) != getPrimaryKeyColumns(sourceConn.get(),
                                                   table.schema_name,
                                                   table.table_name)
                                  .end();

          if (isPK) {
            primaryKeys.push_back(colName);
          }

          std::string nullable = (isNullable == "NO") ? " NOT NULL" : "";
          directCreate += "\"" + colName + "\" " + dataType + nullable;
          directCreate += ", ";
        }

        // Remove trailing comma
        if (directCreate.size() > 2) {
          directCreate.erase(directCreate.size() - 2, 2);
        }

        if (!primaryKeys.empty()) {
          directCreate += ", PRIMARY KEY (";
          for (size_t i = 0; i < primaryKeys.size(); ++i) {
            if (i > 0)
              directCreate += ", ";
            directCreate += "\"" + primaryKeys[i] + "\"";
          }
          directCreate += ")";
        }
        directCreate += ")";

        pqxx::work createTargetTxn(pgConn);
        createTargetTxn.exec(directCreate);
        createTargetTxn.commit();

        Logger::info(LogCategory::TRANSFER, "processTableParallel",
                     "Created table " + lowerSchemaName + "." +
                         lowerTableNamePG);
      }
    }

    // Count source and target
    size_t sourceCount = 0;
    {
      pqxx::work countTxn(*sourceConn);
      auto countResult = countTxn.exec(
          "SELECT COUNT(*) FROM " + countTxn.quote_name(table.schema_name) +
          "." + countTxn.quote_name(table.table_name));
      if (!countResult.empty()) {
        sourceCount = countResult[0][0].as<size_t>();
      }
      countTxn.commit();
    }

    size_t targetCount = 0;
    {
      pqxx::work countTxn(pgConn);
      auto countResult =
          countTxn.exec("SELECT COUNT(*) FROM \"" + lowerSchemaName + "\".\"" +
                        lowerTableNamePG + "\"");
      if (!countResult.empty()) {
        targetCount = countResult[0][0].as<size_t>();
      }
      countTxn.commit();
    }

    // Handle FULL_LOAD/RESET
    if (table.status == "FULL_LOAD" || table.status == "RESET") {
      pqxx::work truncateTxn(pgConn);
      truncateTxn.exec("TRUNCATE TABLE \"" + lowerSchemaName + "\".\"" +
                       lowerTableNamePG + "\" CASCADE");

      std::string pkStrategy =
          getPKStrategyFromCatalog(pgConn, table.schema_name, table.table_name);
      if (pkStrategy == "CDC") {
        truncateTxn.exec(
            "UPDATE metadata.catalog SET sync_metadata = "
            "COALESCE(sync_metadata, '{}'::jsonb) || "
            "jsonb_build_object('last_change_id', 0) WHERE schema_name=" +
            truncateTxn.quote(table.schema_name) +
            " AND table_name=" + truncateTxn.quote(table.table_name) +
            " AND db_engine='PostgreSQL'");
      }
      truncateTxn.commit();
      targetCount = 0;
    }

    std::string pkStrategy =
        getPKStrategyFromCatalog(pgConn, table.schema_name, table.table_name);

    Logger::info(LogCategory::TRANSFER, "processTableParallel",
                 "Counts for " + tableKey +
                     ": source=" + std::to_string(sourceCount) +
                     ", target=" + std::to_string(targetCount) +
                     ", pkStrategy=" + pkStrategy + ", status=" + table.status);

    // Handle CDC
    if (pkStrategy == "CDC" && table.status != "FULL_LOAD") {
      Logger::info(LogCategory::TRANSFER, "processTableParallel",
                   "CDC strategy detected for " + tableKey);
      processTableCDC(tableKey, sourceConn.get(), table, pgConn, columnNames,
                      columnTypes);

      size_t finalCount = 0;
      {
        pqxx::work txn(pgConn);
        auto res = txn.exec("SELECT COUNT(*) FROM \"" + lowerSchemaName +
                            "\".\"" + lowerTableNamePG + "\"");
        if (!res.empty())
          finalCount = res[0][0].as<size_t>();
        txn.commit();
      }

      updateStatus(pgConn, table.schema_name, table.table_name,
                   "LISTENING_CHANGES", finalCount);
      removeTableProcessingState(tableKey);
      return;
    }

    if (sourceCount == 0) {
      if (targetCount == 0) {
        updateStatus(pgConn, table.schema_name, table.table_name, "NO_DATA", 0);
      } else {
        updateStatus(pgConn, table.schema_name, table.table_name,
                     "LISTENING_CHANGES", targetCount);
      }
      removeTableProcessingState(tableKey);
      return;
    }

    if (sourceCount == targetCount && table.status != "FULL_LOAD") {
      Logger::info(LogCategory::TRANSFER, "processTableParallel",
                   "Counts match (" + std::to_string(sourceCount) + ") for " +
                       tableKey);
      updateStatus(pgConn, table.schema_name, table.table_name,
                   "LISTENING_CHANGES", targetCount);
      removeTableProcessingState(tableKey);
      return;
    }

    // Proceed with FULL_LOAD
    Logger::info(LogCategory::TRANSFER, "processTableParallel",
                 "Proceeding with FULL_LOAD for " + tableKey);
    dataFetcherThread(tableKey, sourceConn.get(), table, columnNames,
                      columnTypes);

    size_t finalTargetCount = 0;
    {
      pqxx::work txn(pgConn);
      auto res = txn.exec("SELECT COUNT(*) FROM \"" + lowerSchemaName +
                          "\".\"" + lowerTableNamePG + "\"");
      if (!res.empty())
        finalTargetCount = res[0][0].as<size_t>();
      txn.commit();
    }

    updateStatus(pgConn, table.schema_name, table.table_name,
                 "LISTENING_CHANGES", finalTargetCount);
    removeTableProcessingState(tableKey);

    Logger::info(LogCategory::TRANSFER,
                 "Parallel processing completed for table " + tableKey);

  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "processTableParallel",
                  "Error: " + std::string(e.what()));
    updateStatus(pgConn, table.schema_name, table.table_name, "ERROR");
    removeTableProcessingState(tableKey);
  }
}

void PostgreSQLToPostgres::dataFetcherThread(
    const std::string &tableKey, pqxx::connection *sourceConn,
    const TableInfo &table, const std::vector<std::string> &columnNames,
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

      pqxx::work selectTxn(*sourceConn);
      std::string selectQuery = "SELECT * FROM " +
                                selectTxn.quote_name(table.schema_name) + "." +
                                selectTxn.quote_name(table.table_name);

      if (!pkColumns.empty()) {
        selectQuery += " ORDER BY ";
        for (size_t i = 0; i < pkColumns.size(); ++i) {
          if (i > 0)
            selectQuery += ", ";
          selectQuery += selectTxn.quote_name(pkColumns[i]);
        }
      }

      selectQuery += " LIMIT " + std::to_string(CHUNK_SIZE) + " OFFSET " +
                     std::to_string(lastProcessedOffset);

      auto results = selectTxn.exec(selectQuery);
      selectTxn.commit();

      if (results.empty()) {
        hasMoreData = false;
        break;
      }

      // Convert results to vector format
      std::vector<std::vector<std::string>> rows;
      for (const auto &row : results) {
        std::vector<std::string> rowData;
        for (size_t i = 0; i < row.size(); ++i) {
          rowData.push_back(row[i].is_null() ? "" : row[i].as<std::string>());
        }
        rows.push_back(rowData);
      }

      try {
        std::string lowerSchemaName = table.schema_name;
        std::transform(lowerSchemaName.begin(), lowerSchemaName.end(),
                       lowerSchemaName.begin(), ::tolower);

        performBulkUpsert(pgConn, rows, columnNames, columnTypes,
                          lowerSchemaName, table.table_name, table.schema_name);

        Logger::info(LogCategory::TRANSFER,
                     "Successfully processed chunk " +
                         std::to_string(chunkNumber) + " with " +
                         std::to_string(rows.size()) + " rows");

        lastProcessedOffset += rows.size();

        if (rows.size() < CHUNK_SIZE) {
          hasMoreData = false;
        }
      } catch (const std::exception &e) {
        Logger::error(LogCategory::TRANSFER, "dataFetcherThread",
                      "Bulk upsert failed: " + std::string(e.what()));
        hasMoreData = false;
        break;
      }
    }

  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "dataFetcherThread",
                  "Error: " + std::string(e.what()));
  }
}

void PostgreSQLToPostgres::processTableCDC(
    const std::string &tableKey, pqxx::connection *sourceConn,
    const TableInfo &table, pqxx::connection &pgConn,
    const std::vector<std::string> &columnNames,
    const std::vector<std::string> &columnTypes) {
  try {
    const size_t CHUNK_SIZE = SyncConfig::getChunkSize();
    long long lastChangeId = 0;

    // Get last change ID
    {
      pqxx::work txn(pgConn);
      std::string query =
          "SELECT sync_metadata->>'last_change_id' FROM metadata.catalog "
          "WHERE schema_name=" +
          txn.quote(table.schema_name) +
          " AND table_name=" + txn.quote(table.table_name) +
          " AND db_engine='PostgreSQL'";
      auto res = txn.exec(query);
      txn.commit();

      if (!res.empty() && !res[0][0].is_null()) {
        std::string value = res[0][0].as<std::string>();
        if (!value.empty() && value.size() <= 20) {
          try {
            lastChangeId = std::stoll(value);
          } catch (const std::exception &e) {
            Logger::error(LogCategory::TRANSFER, "processTableCDC",
                          "Failed to parse last_change_id: " +
                              std::string(e.what()));
            lastChangeId = 0;
          }
        }
      }
    }

    std::vector<std::string> pkColumns =
        getPKColumnsFromCatalog(pgConn, table.schema_name, table.table_name);
    bool hasPK = !pkColumns.empty();

    bool hasMore = true;
    size_t batchNumber = 0;

    while (hasMore) {
      batchNumber++;

      pqxx::work changeTxn(*sourceConn);
      std::string query =
          "SELECT change_id, operation, pk_values, row_data "
          "FROM datasync_metadata.ds_change_log WHERE "
          "schema_name=" +
          changeTxn.quote(table.schema_name) +
          " AND table_name=" + changeTxn.quote(table.table_name) +
          " AND change_id > " + std::to_string(lastChangeId) +
          " ORDER BY change_id LIMIT " + std::to_string(CHUNK_SIZE);

      auto changeResult = changeTxn.exec(query);
      changeTxn.commit();

      if (changeResult.empty()) {
        hasMore = false;
        break;
      }

      long long maxChangeId = lastChangeId;
      std::vector<std::vector<std::string>> deletedPKs;
      std::vector<std::vector<std::string>> recordsToUpsert;

      for (const auto &row : changeResult) {
        if (row.size() < 3)
          continue;

        std::string changeIdStr = row[0].as<std::string>();
        std::string op = row[1].as<std::string>();
        std::string pkJson = row[2].as<std::string>();

        try {
          if (!changeIdStr.empty()) {
            long long cid = std::stoll(changeIdStr);
            if (cid > maxChangeId) {
              maxChangeId = cid;
            }
          }
        } catch (const std::exception &e) {
          Logger::error(LogCategory::TRANSFER, "processTableCDC",
                        "Failed to parse change_id: " + std::string(e.what()));
        }

        try {
          json pkObject = json::parse(pkJson);
          bool isNoPKTable = !hasPK && pkObject.contains("_hash");

          if (isNoPKTable) {
            std::string hashValue = pkObject["_hash"].get<std::string>();

            if (op == "D") {
              std::vector<std::string> deleteRecord;
              deleteRecord.push_back(hashValue);
              deletedPKs.push_back(deleteRecord);
            } else if (op == "I" || op == "U") {
              if (row.size() >= 4 && !row[3].is_null()) {
                json rowData = json::parse(row[3].as<std::string>());
                std::vector<std::string> record;
                for (const auto &colName : columnNames) {
                  if (rowData.contains(colName) &&
                      !rowData[colName].is_null()) {
                    if (rowData[colName].is_string()) {
                      record.push_back(rowData[colName].get<std::string>());
                    } else {
                      record.push_back(rowData[colName].dump());
                    }
                  } else {
                    record.push_back("");
                  }
                }
                if (record.size() == columnNames.size()) {
                  recordsToUpsert.push_back(record);
                }
              }
            }
          } else {
            if (!hasPK) {
              continue;
            }

            std::vector<std::string> pkValues;
            for (const auto &pkCol : pkColumns) {
              if (pkObject.contains(pkCol) && !pkObject[pkCol].is_null()) {
                if (pkObject[pkCol].is_string()) {
                  pkValues.push_back(pkObject[pkCol].get<std::string>());
                } else {
                  pkValues.push_back(pkObject[pkCol].dump());
                }
              } else {
                pkValues.push_back("NULL");
              }
            }

            if (pkValues.size() != pkColumns.size()) {
              continue;
            }

            if (op == "D") {
              deletedPKs.push_back(pkValues);
            } else if (op == "I" || op == "U") {
              if (row.size() >= 4 && !row[3].is_null()) {
                json rowData = json::parse(row[3].as<std::string>());
                std::vector<std::string> record;
                for (const auto &colName : columnNames) {
                  if (rowData.contains(colName) &&
                      !rowData[colName].is_null()) {
                    if (rowData[colName].is_string()) {
                      record.push_back(rowData[colName].get<std::string>());
                    } else {
                      record.push_back(rowData[colName].dump());
                    }
                  } else {
                    record.push_back("");
                  }
                }
                if (record.size() == columnNames.size()) {
                  recordsToUpsert.push_back(record);
                }
              } else {
                // Fetch from source
                pqxx::work fetchTxn(*sourceConn);
                std::string whereClause;
                for (size_t i = 0; i < pkColumns.size(); ++i) {
                  if (i > 0)
                    whereClause += " AND ";
                  whereClause += fetchTxn.quote_name(pkColumns[i]) + " = " +
                                 fetchTxn.quote(pkValues[i]);
                }

                std::string selectQuery =
                    "SELECT * FROM " + fetchTxn.quote_name(table.schema_name) +
                    "." + fetchTxn.quote_name(table.table_name) + " WHERE " +
                    whereClause + " LIMIT 1";

                auto recordResult = fetchTxn.exec(selectQuery);
                fetchTxn.commit();

                if (!recordResult.empty() &&
                    recordResult[0].size() == columnNames.size()) {
                  std::vector<std::string> record;
                  for (size_t i = 0; i < recordResult[0].size(); ++i) {
                    record.push_back(
                        recordResult[0][i].is_null()
                            ? ""
                            : recordResult[0][i].as<std::string>());
                  }
                  recordsToUpsert.push_back(record);
                }
              }
            }
          }
        } catch (const std::exception &e) {
          Logger::error(LogCategory::TRANSFER, "processTableCDC",
                        "Failed to process change: " + std::string(e.what()));
        }
      }

      // Process deletes
      size_t deletedCount = 0;
      if (!deletedPKs.empty()) {
        std::string lowerSchemaName = table.schema_name;
        std::transform(lowerSchemaName.begin(), lowerSchemaName.end(),
                       lowerSchemaName.begin(), ::tolower);

        if (!hasPK && !deletedPKs.empty()) {
          deletedCount =
              deleteRecordsByHash(pgConn, lowerSchemaName, table.table_name,
                                  deletedPKs, columnNames);
        } else if (hasPK && !deletedPKs.empty()) {
          deletedCount = deleteRecordsByPrimaryKey(
              pgConn, lowerSchemaName, table.table_name, deletedPKs, pkColumns);
        }
      }

      // Process upserts
      size_t upsertedCount = 0;
      if (!recordsToUpsert.empty()) {
        std::string lowerSchemaName = table.schema_name;
        std::transform(lowerSchemaName.begin(), lowerSchemaName.end(),
                       lowerSchemaName.begin(), ::tolower);

        performBulkUpsert(pgConn, recordsToUpsert, columnNames, columnTypes,
                          lowerSchemaName, table.table_name, table.schema_name);
        upsertedCount = recordsToUpsert.size();
      }

      // Update last change ID
      {
        pqxx::work updateTxn(pgConn);
        updateTxn.exec("UPDATE metadata.catalog SET sync_metadata = "
                       "COALESCE(sync_metadata, '{}'::jsonb) || "
                       "jsonb_build_object('last_change_id', " +
                       std::to_string(maxChangeId) + ") WHERE schema_name=" +
                       updateTxn.quote(table.schema_name) +
                       " AND table_name=" + updateTxn.quote(table.table_name) +
                       " AND db_engine='PostgreSQL'");
        updateTxn.commit();
      }

      Logger::info(LogCategory::TRANSFER, "processTableCDC",
                   "Processed batch " + std::to_string(batchNumber) + " for " +
                       tableKey + ": " + std::to_string(upsertedCount) +
                       " upserted, " + std::to_string(deletedCount) +
                       " deleted");

      if (changeResult.size() < CHUNK_SIZE) {
        hasMore = false;
      }
    }

  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "processTableCDC",
                  "Error processing CDC: " + std::string(e.what()));
  }
}

std::vector<std::string>
PostgreSQLToPostgres::getPrimaryKeyColumns(pqxx::connection *conn,
                                           const std::string &schema_name,
                                           const std::string &table_name) {
  std::vector<std::string> pkColumns;

  if (!conn || !conn->is_open()) {
    Logger::error(LogCategory::TRANSFER, "getPrimaryKeyColumns",
                  "PostgreSQL connection is null or closed");
    return pkColumns;
  }

  if (schema_name.empty() || table_name.empty()) {
    Logger::error(LogCategory::TRANSFER, "getPrimaryKeyColumns",
                  "Schema name or table name is empty");
    return pkColumns;
  }

  try {
    pqxx::work txn(*conn);
    std::string query =
        "SELECT column_name "
        "FROM information_schema.key_column_usage "
        "WHERE table_schema = " +
        txn.quote(schema_name) + " AND table_name = " + txn.quote(table_name) +
        " AND constraint_name IN ("
        "  SELECT constraint_name FROM information_schema.table_constraints "
        "  WHERE table_schema = " +
        txn.quote(schema_name) + "  AND table_name = " + txn.quote(table_name) +
        "  AND constraint_type = 'PRIMARY KEY'"
        ") "
        "ORDER BY ordinal_position";

    auto result = txn.exec(query);
    txn.commit();

    for (const auto &row : result) {
      if (!row[0].is_null()) {
        std::string colName = row[0].as<std::string>();
        std::transform(colName.begin(), colName.end(), colName.begin(),
                       ::tolower);
        pkColumns.push_back(colName);
      }
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "getPrimaryKeyColumns",
                  "Error: " + std::string(e.what()));
  }

  return pkColumns;
}

std::vector<std::vector<std::string>>
PostgreSQLToPostgres::executeQueryPostgreSQL(pqxx::connection *conn,
                                             const std::string &query) {
  std::vector<std::vector<std::string>> results;
  try {
    if (!conn || !conn->is_open()) {
      return results;
    }

    pqxx::work txn(*conn);
    auto result = txn.exec(query);
    txn.commit();

    for (const auto &row : result) {
      std::vector<std::string> rowData;
      for (size_t i = 0; i < row.size(); ++i) {
        rowData.push_back(row[i].is_null() ? "" : row[i].as<std::string>());
      }
      results.push_back(rowData);
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "executeQueryPostgreSQL",
                  "Query execution failed: " + std::string(e.what()));
  }
  return results;
}

std::string PostgreSQLToPostgres::escapeSQL(const std::string &value) {
  if (value.empty()) {
    return value;
  }
  std::string escaped = value;
  size_t pos = 0;
  while ((pos = escaped.find('\'', pos)) != std::string::npos) {
    escaped.replace(pos, 1, "''");
    pos += 2;
  }
  return escaped;
}

void PostgreSQLToPostgres::updateStatus(pqxx::connection &pgConn,
                                        const std::string &schema_name,
                                        const std::string &table_name,
                                        const std::string &status,
                                        size_t rowCount) {
  try {
    std::lock_guard<std::mutex> lock(metadataUpdateMutex);

    pqxx::work txn(pgConn);
    std::string updateQuery =
        "UPDATE metadata.catalog SET status=" + txn.quote(status) +
        " WHERE schema_name=" + txn.quote(schema_name) +
        " AND table_name=" + txn.quote(table_name);
    txn.exec(updateQuery);
    txn.commit();

  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "updateStatus",
                  "Error updating status: " + std::string(e.what()));
  }
}
