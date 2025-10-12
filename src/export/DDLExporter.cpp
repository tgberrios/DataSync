#include "export/DDLExporter.h"
#include <algorithm>
#include <chrono>
#include <fstream>
#include <iostream>
#include <mysql/mysql.h>
#include <pqxx/pqxx>
#include <sstream>

void DDLExporter::exportAllDDL() {
  auto startTime = std::chrono::high_resolution_clock::now();

  try {
    createFolderStructure();
    getSchemasFromCatalog();

    size_t successCount = 0;
    size_t errorCount = 0;

    for (size_t i = 0; i < schemas.size(); ++i) {
      const auto &schema = schemas[i];
      try {

        exportSchemaDDL(schema);
        successCount++;

      } catch (const std::exception &e) {
        errorCount++;
        Logger::error(LogCategory::DDL_EXPORT, "exportAllDDL",
                      "Error exporting schema " + schema.schema_name + ": " +
                          std::string(e.what()));
      }
    }

    auto endTime = std::chrono::high_resolution_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::seconds>(endTime - startTime);

  } catch (const std::exception &e) {
    auto endTime = std::chrono::high_resolution_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::seconds>(endTime - startTime);

    Logger::error(LogCategory::DDL_EXPORT, "exportAllDDL",
                  "Error in DDL export process after " +
                      std::to_string(duration.count()) +
                      " seconds: " + std::string(e.what()));
  }
}

void DDLExporter::createFolderStructure() {
  try {
    std::filesystem::create_directories(exportPath);

  } catch (const std::exception &e) {
    Logger::error(LogCategory::DDL_EXPORT, "createFolderStructure",
                  "Error creating folder structure: " + std::string(e.what()));
  }
}

void DDLExporter::getSchemasFromCatalog() {
  try {
    pqxx::connection conn(DatabaseConfig::getPostgresConnectionString());
    pqxx::work txn(conn);

    std::string query =
        "SELECT DISTINCT schema_name, db_engine, connection_string, "
        "cluster_name "
        "FROM metadata.catalog "
        "WHERE db_engine IS NOT NULL AND cluster_name IS NOT NULL "
        "ORDER BY cluster_name, db_engine, schema_name;";

    auto result = txn.exec(query);
    txn.commit();

    schemas.clear();
    for (const auto &row : result) {
      SchemaInfo schema;
      schema.schema_name = row[0].as<std::string>();
      schema.db_engine = row[1].as<std::string>();
      schema.database_name = schema.schema_name;
      schema.connection_string = row[2].as<std::string>();
      schema.cluster_name = row[3].as<std::string>();
      schemas.push_back(schema);
    }

  } catch (const std::exception &e) {
    Logger::error(LogCategory::DDL_EXPORT, "getSchemasFromCatalog",
                  "Error getting schemas from catalog: " +
                      std::string(e.what()));
  }
}

void DDLExporter::exportSchemaDDL(const SchemaInfo &schema) {
  try {
    createClusterFolder(schema.cluster_name);
    createEngineFolder(schema.cluster_name, schema.db_engine);
    createDatabaseFolder(schema.cluster_name, schema.db_engine,
                         schema.database_name);
    createSchemaFolder(schema.cluster_name, schema.db_engine,
                       schema.database_name, schema.schema_name);

    if (schema.db_engine == "MariaDB") {
      exportMariaDBDDL(schema);
    } else if (schema.db_engine == "MSSQL") {
      exportMSSQLDDL(schema);
    } else {
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DDL_EXPORT, "exportSchemaDDL",
                  "Error exporting schema DDL: " + std::string(e.what()));
  }
}

void DDLExporter::createClusterFolder(const std::string &cluster) {
  try {
    std::string clusterPath = exportPath + "/" + sanitizeFileName(cluster);
    std::filesystem::create_directories(clusterPath);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DDL_EXPORT, "createClusterFolder",
                  "Error creating cluster folder: " + std::string(e.what()));
  }
}

void DDLExporter::createEngineFolder(const std::string &cluster,
                                     const std::string &engine) {
  try {
    std::string enginePath = exportPath + "/" + sanitizeFileName(cluster) +
                             "/" + sanitizeFileName(engine);
    std::filesystem::create_directories(enginePath);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DDL_EXPORT, "createEngineFolder",
                  "Error creating engine folder: " + std::string(e.what()));
  }
}

void DDLExporter::createDatabaseFolder(const std::string &cluster,
                                       const std::string &engine,
                                       const std::string &database) {
  try {
    std::string dbPath = exportPath + "/" + sanitizeFileName(cluster) + "/" +
                         sanitizeFileName(engine) + "/" +
                         sanitizeFileName(database);
    std::filesystem::create_directories(dbPath);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DDL_EXPORT, "createDatabaseFolder",
                  "Error creating database folder: " + std::string(e.what()));
  }
}

void DDLExporter::createSchemaFolder(const std::string &cluster,
                                     const std::string &engine,
                                     const std::string &database,
                                     const std::string &schema) {
  try {
    std::string schemaPath = exportPath + "/" + sanitizeFileName(cluster) +
                             "/" + sanitizeFileName(engine) + "/" +
                             sanitizeFileName(database) + "/" +
                             sanitizeFileName(schema);
    std::filesystem::create_directories(schemaPath + "/tables");
    std::filesystem::create_directories(schemaPath + "/indexes");
    std::filesystem::create_directories(schemaPath + "/constraints");
    std::filesystem::create_directories(schemaPath + "/functions");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DDL_EXPORT, "createSchemaFolder",
                  "Error creating schema folder: " + std::string(e.what()));
  }
}

void DDLExporter::exportMariaDBDDL(const SchemaInfo &schema) {
  try {
    const unsigned int timeout_seconds = 30;
    std::string connStr = getConnectionString(schema);

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

      // Trim whitespace from key and value
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
      Logger::error(LogCategory::DDL_EXPORT, "exportMariaDBDDL",
                    "MariaDB connection parameters are incomplete");
      return;
    }

    // Connect to MariaDB using MySQL library
    MYSQL *conn = mysql_init(nullptr);
    if (!conn) {
      Logger::error(LogCategory::DDL_EXPORT, "exportMariaDBDDL",
                    "mysql_init() failed");
      return;
    }

    // Set connection timeout
    mysql_options(conn, MYSQL_OPT_CONNECT_TIMEOUT,
                  (const char *)&timeout_seconds);
    mysql_options(conn, MYSQL_OPT_READ_TIMEOUT, (const char *)&timeout_seconds);
    mysql_options(conn, MYSQL_OPT_WRITE_TIMEOUT,
                  (const char *)&timeout_seconds);

    unsigned int portNum = 3306;
    if (!port.empty()) {
      try {
        portNum = std::stoul(port);
        if (portNum == 0 || portNum > 65535) {
          Logger::error(LogCategory::DDL_EXPORT, "exportMariaDBDDL",
                        "Invalid port number: " + port);
          mysql_close(conn);
          return;
        }
      } catch (const std::exception &e) {
        Logger::error(LogCategory::DDL_EXPORT, "exportMariaDBDDL",
                      "Invalid port format: " + port + " - " +
                          std::string(e.what()));
        mysql_close(conn);
        return;
      }
    }

    if (mysql_real_connect(conn, host.c_str(), user.c_str(), password.c_str(),
                           db.c_str(), portNum, nullptr, 0) == nullptr) {
      Logger::error(LogCategory::DDL_EXPORT, "exportMariaDBDDL",
                    "MariaDB connection failed: " +
                        std::string(mysql_error(conn)));
      mysql_close(conn);
      return;
    }

    std::string tablesQuery =
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = '" +
        escapeSQL(schema.schema_name) +
        "' "
        "AND table_type = 'BASE TABLE';";

    if (mysql_query(conn, tablesQuery.c_str())) {
      Logger::error(LogCategory::DDL_EXPORT, "exportMariaDBDDL",
                    "Query failed: " + std::string(mysql_error(conn)));
      mysql_close(conn);
      return;
    }

    MYSQL_RES *tablesResult = mysql_store_result(conn);
    if (!tablesResult) {
      Logger::error(LogCategory::DDL_EXPORT, "exportMariaDBDDL",
                    "mysql_store_result() failed: " +
                        std::string(mysql_error(conn)));
      mysql_close(conn);
      return;
    }

    MYSQL_ROW tableRow;
    while ((tableRow = mysql_fetch_row(tablesResult))) {
      std::string tableName = tableRow[0] ? tableRow[0] : "";

      std::string createTableQuery = "SHOW CREATE TABLE `" +
                                     escapeSQL(schema.schema_name) + "`.`" +
                                     escapeSQL(tableName) + "`;";

      if (mysql_query(conn, createTableQuery.c_str())) {
        Logger::error(LogCategory::DDL_EXPORT, "exportMariaDBDDL",
                      "SHOW CREATE TABLE failed: " +
                          std::string(mysql_error(conn)));
        continue;
      }

      MYSQL_RES *createResult = mysql_store_result(conn);
      if (createResult && mysql_num_rows(createResult) > 0) {
        MYSQL_ROW createRow = mysql_fetch_row(createResult);
        if (createRow && createRow[1]) {
          std::string ddl = createRow[1];
          saveTableDDL(schema.cluster_name, schema.db_engine,
                       schema.database_name, schema.schema_name, tableName,
                       ddl);
        }
      }
      mysql_free_result(createResult);

      std::string indexesQuery = "SHOW INDEX FROM `" +
                                 escapeSQL(schema.schema_name) + "`.`" +
                                 escapeSQL(tableName) + "`;";

      if (mysql_query(conn, indexesQuery.c_str())) {
        Logger::error(LogCategory::DDL_EXPORT, "exportMariaDBDDL",
                      "SHOW INDEX failed: " + std::string(mysql_error(conn)));
        continue;
      }

      MYSQL_RES *indexesResult = mysql_store_result(conn);
      if (indexesResult) {
        MYSQL_ROW indexRow;
        while ((indexRow = mysql_fetch_row(indexesResult))) {
          if (indexRow[2] && indexRow[4] && indexRow[1]) {
            std::string indexName = indexRow[2];
            std::string columnName = indexRow[4];
            std::string nonUnique = indexRow[1];

            std::string indexDDL = "CREATE ";
            if (nonUnique == "0") {
              indexDDL += "UNIQUE ";
            }
            indexDDL += "INDEX `" + indexName + "` ON `" + schema.schema_name +
                        "`.`" + tableName + "` (`" + columnName + "`);";

            saveIndexDDL(schema.cluster_name, schema.db_engine,
                         schema.database_name, schema.schema_name, tableName,
                         indexDDL);
          }
        }
        mysql_free_result(indexesResult);
      }
    }

    mysql_free_result(tablesResult);

    // Export Views
    exportMariaDBViews(conn, schema);

    // Export Stored Procedures
    exportMariaDBProcedures(conn, schema);

    // Export Functions
    exportMariaDBFunctions(conn, schema);

    // Export Triggers
    exportMariaDBTriggers(conn, schema);

    // Export Constraints
    exportMariaDBConstraints(conn, schema);

    // Export Events
    exportMariaDBEvents(conn, schema);

    mysql_close(conn);

  } catch (const std::exception &e) {
    Logger::error(LogCategory::DDL_EXPORT, "FUNCTION_NAME",
                  "Error exporting MariaDB DDL: " + std::string(e.what()));
  }
}

void DDLExporter::exportMariaDBViews(MYSQL *conn, const SchemaInfo &schema) {
  try {
    std::string viewsQuery = "SHOW FULL TABLES FROM `" +
                             escapeSQL(schema.schema_name) +
                             "` WHERE Table_type = 'VIEW';";

    // Logger::debug(LogCategory::DDL_EXPORT, "DDLExporter", "Querying for views
    // in schema: " + schema.schema_name);

    if (mysql_query(conn, viewsQuery.c_str())) {
      return;
    }

    MYSQL_RES *viewsResult = mysql_store_result(conn);
    if (!viewsResult) {
      return;
    }

    int viewCount = mysql_num_rows(viewsResult);
    // Logger::debug(LogCategory::DDL_EXPORT, "DDLExporter", "Found " +
    // std::to_string(viewCount) + " views in schema: " + schema.schema_name);

    MYSQL_ROW viewRow;
    while ((viewRow = mysql_fetch_row(viewsResult))) {
      if (viewRow[0]) {
        std::string viewName = viewRow[0];
        // Logger::debug(LogCategory::DDL_EXPORT, "DDLExporter", "Processing
        // view: " + viewName);

        std::string createViewQuery = "SHOW CREATE VIEW `" +
                                      escapeSQL(schema.schema_name) + "`.`" +
                                      escapeSQL(viewName) + "`;";

        if (mysql_query(conn, createViewQuery.c_str())) {
          Logger::error(LogCategory::DDL_EXPORT, "exportMariaDBDDL",
                        "SHOW CREATE VIEW failed for " + viewName + ": " +
                            std::string(mysql_error(conn)));
          continue;
        }

        MYSQL_RES *createResult = mysql_store_result(conn);
        if (createResult && mysql_num_rows(createResult) > 0) {
          MYSQL_ROW createRow = mysql_fetch_row(createResult);
          if (createRow && createRow[1]) {
            std::string ddl = createRow[1];
            saveTableDDL(schema.cluster_name, schema.db_engine,
                         schema.database_name, schema.schema_name, viewName,
                         ddl);
            // Logger::debug(LogCategory::DDL_EXPORT, "DDLExporter",
            // "Successfully exported view: " + viewName);
          } else {
          }
        } else {
        }
        mysql_free_result(createResult);
      }
    }

    mysql_free_result(viewsResult);
    // Logger::debug(LogCategory::DDL_EXPORT, "DDLExporter", "Exported MariaDB
    // views for schema: " + schema.schema_name);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DDL_EXPORT, "exportMariaDBViews",
                  "Error exporting MariaDB views: " + std::string(e.what()));
  }
}

void DDLExporter::exportMariaDBProcedures(MYSQL *conn,
                                          const SchemaInfo &schema) {
  try {
    std::string proceduresQuery =
        "SELECT routine_name FROM information_schema.routines "
        "WHERE routine_schema = '" +
        escapeSQL(schema.schema_name) +
        "' "
        "AND routine_type = 'PROCEDURE';";

    if (mysql_query(conn, proceduresQuery.c_str())) {
      return;
    }

    MYSQL_RES *proceduresResult = mysql_store_result(conn);
    if (!proceduresResult)
      return;

    MYSQL_ROW procRow;
    while ((procRow = mysql_fetch_row(proceduresResult))) {
      std::string procName = procRow[0] ? procRow[0] : "";

      std::string createProcQuery = "SHOW CREATE PROCEDURE `" +
                                    escapeSQL(schema.schema_name) + "`.`" +
                                    escapeSQL(procName) + "`;";

      if (mysql_query(conn, createProcQuery.c_str())) {
        Logger::error(LogCategory::DDL_EXPORT, "exportMariaDBDDL",
                      "SHOW CREATE PROCEDURE failed: " +
                          std::string(mysql_error(conn)));
        continue;
      }

      MYSQL_RES *createResult = mysql_store_result(conn);
      if (createResult && mysql_num_rows(createResult) > 0) {
        MYSQL_ROW createRow = mysql_fetch_row(createResult);
        if (createRow && createRow[2]) {
          std::string ddl = createRow[2];
          saveFunctionDDL(schema.cluster_name, schema.db_engine,
                          schema.database_name, schema.schema_name, procName,
                          ddl);
        }
      }
      mysql_free_result(createResult);
    }

    mysql_free_result(proceduresResult);
    // Logger::debug(LogCategory::DDL_EXPORT, "DDLExporter", "Exported MariaDB
    // procedures for schema: " + schema.schema_name);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DDL_EXPORT, "exportMariaDBProcedures",
                  "Error exporting MariaDB procedures: " +
                      std::string(e.what()));
  }
}

void DDLExporter::exportMariaDBFunctions(MYSQL *conn,
                                         const SchemaInfo &schema) {
  try {
    std::string functionsQuery =
        "SELECT routine_name FROM information_schema.routines "
        "WHERE routine_schema = '" +
        escapeSQL(schema.schema_name) +
        "' "
        "AND routine_type = 'FUNCTION';";

    if (mysql_query(conn, functionsQuery.c_str())) {
      return;
    }

    MYSQL_RES *functionsResult = mysql_store_result(conn);
    if (!functionsResult)
      return;

    MYSQL_ROW funcRow;
    while ((funcRow = mysql_fetch_row(functionsResult))) {
      std::string funcName = funcRow[0] ? funcRow[0] : "";

      std::string createFuncQuery = "SHOW CREATE FUNCTION `" +
                                    escapeSQL(schema.schema_name) + "`.`" +
                                    escapeSQL(funcName) + "`;";

      if (mysql_query(conn, createFuncQuery.c_str())) {
        Logger::error(LogCategory::DDL_EXPORT, "exportMariaDBDDL",
                      "SHOW CREATE FUNCTION failed: " +
                          std::string(mysql_error(conn)));
        continue;
      }

      MYSQL_RES *createResult = mysql_store_result(conn);
      if (createResult && mysql_num_rows(createResult) > 0) {
        MYSQL_ROW createRow = mysql_fetch_row(createResult);
        if (createRow && createRow[2]) {
          std::string ddl = createRow[2];
          saveFunctionDDL(schema.cluster_name, schema.db_engine,
                          schema.database_name, schema.schema_name, funcName,
                          ddl);
        }
      }
      mysql_free_result(createResult);
    }

    mysql_free_result(functionsResult);
    // Logger::debug(LogCategory::DDL_EXPORT, "DDLExporter", "Exported MariaDB
    // functions for schema: " + schema.schema_name);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DDL_EXPORT, "exportMariaDBFunctions",
                  "Error exporting MariaDB functions: " +
                      std::string(e.what()));
  }
}

void DDLExporter::exportMariaDBTriggers(MYSQL *conn, const SchemaInfo &schema) {
  try {
    std::string triggersQuery =
        "SELECT trigger_name FROM information_schema.triggers "
        "WHERE trigger_schema = '" +
        escapeSQL(schema.schema_name) + "';";

    if (mysql_query(conn, triggersQuery.c_str())) {
      return;
    }

    MYSQL_RES *triggersResult = mysql_store_result(conn);
    if (!triggersResult)
      return;

    MYSQL_ROW triggerRow;
    while ((triggerRow = mysql_fetch_row(triggersResult))) {
      std::string triggerName = triggerRow[0] ? triggerRow[0] : "";

      std::string createTriggerQuery = "SHOW CREATE TRIGGER `" +
                                       escapeSQL(schema.schema_name) + "`.`" +
                                       escapeSQL(triggerName) + "`;";

      if (mysql_query(conn, createTriggerQuery.c_str())) {
        Logger::error(LogCategory::DDL_EXPORT, "exportMariaDBDDL",
                      "SHOW CREATE TRIGGER failed: " +
                          std::string(mysql_error(conn)));
        continue;
      }

      MYSQL_RES *createResult = mysql_store_result(conn);
      if (createResult && mysql_num_rows(createResult) > 0) {
        MYSQL_ROW createRow = mysql_fetch_row(createResult);
        if (createRow && createRow[2]) {
          std::string ddl = createRow[2];
          saveFunctionDDL(schema.cluster_name, schema.db_engine,
                          schema.database_name, schema.schema_name, triggerName,
                          ddl);
        }
      }
      mysql_free_result(createResult);
    }

    mysql_free_result(triggersResult);
    // Logger::debug(LogCategory::DDL_EXPORT, "DDLExporter", "Exported MariaDB
    // triggers for schema: " + schema.schema_name);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DDL_EXPORT, "exportMariaDBTriggers",
                  "Error exporting MariaDB triggers: " + std::string(e.what()));
  }
}

void DDLExporter::exportMariaDBConstraints(MYSQL *conn,
                                           const SchemaInfo &schema) {
  try {
    std::string constraintsQuery =
        "SELECT table_name, constraint_name, constraint_type "
        "FROM information_schema.table_constraints "
        "WHERE table_schema = '" +
        escapeSQL(schema.schema_name) +
        "' "
        "AND constraint_type IN ('FOREIGN KEY', 'CHECK', 'UNIQUE');";

    if (mysql_query(conn, constraintsQuery.c_str())) {
      return;
    }

    MYSQL_RES *constraintsResult = mysql_store_result(conn);
    if (!constraintsResult)
      return;

    MYSQL_ROW constraintRow;
    while ((constraintRow = mysql_fetch_row(constraintsResult))) {
      std::string tableName = constraintRow[0] ? constraintRow[0] : "";
      std::string constraintName = constraintRow[1] ? constraintRow[1] : "";
      std::string constraintType = constraintRow[2] ? constraintRow[2] : "";

      // For now, save constraint info as a simple DDL
      std::string ddl = "-- " + constraintType +
                        " constraint: " + constraintName + " on table " +
                        tableName;

      saveConstraintDDL(schema.cluster_name, schema.db_engine,
                        schema.database_name, schema.schema_name, tableName,
                        ddl);
    }

    mysql_free_result(constraintsResult);
    // Logger::debug(LogCategory::DDL_EXPORT, "DDLExporter", "Exported MariaDB
    // constraints for schema: "
    // + schema.schema_name);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DDL_EXPORT, "exportMariaDBConstraints",
                  "Error exporting MariaDB constraints: " +
                      std::string(e.what()));
  }
}

void DDLExporter::exportMariaDBEvents(MYSQL *conn, const SchemaInfo &schema) {
  try {
    std::string eventsQuery =
        "SELECT event_name FROM information_schema.events "
        "WHERE event_schema = '" +
        escapeSQL(schema.schema_name) + "';";

    if (mysql_query(conn, eventsQuery.c_str())) {
      return;
    }

    MYSQL_RES *eventsResult = mysql_store_result(conn);
    if (!eventsResult)
      return;

    MYSQL_ROW eventRow;
    while ((eventRow = mysql_fetch_row(eventsResult))) {
      std::string eventName = eventRow[0] ? eventRow[0] : "";

      std::string createEventQuery = "SHOW CREATE EVENT `" +
                                     escapeSQL(schema.schema_name) + "`.`" +
                                     escapeSQL(eventName) + "`;";

      if (mysql_query(conn, createEventQuery.c_str())) {
        Logger::error(LogCategory::DDL_EXPORT, "exportMariaDBDDL",
                      "SHOW CREATE EVENT failed: " +
                          std::string(mysql_error(conn)));
        continue;
      }

      MYSQL_RES *createResult = mysql_store_result(conn);
      if (createResult && mysql_num_rows(createResult) > 0) {
        MYSQL_ROW createRow = mysql_fetch_row(createResult);
        if (createRow && createRow[3]) {
          std::string ddl = createRow[3];
          saveFunctionDDL(schema.cluster_name, schema.db_engine,
                          schema.database_name, schema.schema_name, eventName,
                          ddl);
        }
      }
      mysql_free_result(createResult);
    }

    mysql_free_result(eventsResult);
    // Logger::debug(LogCategory::DDL_EXPORT, "DDLExporter", "Exported MariaDB
    // events for schema: " + schema.schema_name);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DDL_EXPORT, "exportMariaDBEvents",
                  "Error exporting MariaDB events: " + std::string(e.what()));
  }
}

void DDLExporter::exportMSSQLDDL(const SchemaInfo &schema) {
  try {
    std::string connStr = getConnectionString(schema);

    // Validate connection string is not empty
    if (connStr.empty()) {
      return;
    }

    // Parse MSSQL connection string
    std::string server, database, username, password, port;
    std::istringstream ss(connStr);
    std::string token;
    while (std::getline(ss, token, ';')) {
      auto pos = token.find('=');
      if (pos == std::string::npos)
        continue;
      std::string key = token.substr(0, pos);
      std::string value = token.substr(pos + 1);

      // Trim whitespace
      key.erase(0, key.find_first_not_of(" \t\r\n"));
      key.erase(key.find_last_not_of(" \t\r\n") + 1);
      value.erase(0, value.find_first_not_of(" \t\r\n"));
      value.erase(value.find_last_not_of(" \t\r\n") + 1);

      if (key == "server" || key == "SERVER")
        server = value;
      else if (key == "database" || key == "DATABASE")
        database = value;
      else if (key == "username" || key == "UID")
        username = value;
      else if (key == "password" || key == "PWD")
        password = value;
      else if (key == "port" || key == "PORT")
        port = value;
    }

    // Validate connection parameters
    if (server.empty()) {
      return;
    }

    if (database.empty()) {
      database = schema.schema_name;
    }

    if (port.empty()) {
      port = "1433";
    } else {
      try {
        int portNum = std::stoi(port);
        if (portNum <= 0 || portNum > 65535) {
          Logger::error(LogCategory::DDL_EXPORT, "exportMariaDBDDL",
                        "Invalid port number: " + port);
          return;
        }
      } catch (const std::exception &e) {
        Logger::error(LogCategory::DDL_EXPORT, "exportMariaDBDDL",
                      "Invalid port format: " + port + " - " + e.what());
        return;
      }
    }

    // Get MSSQL connection string from configuration
    std::string odbcConnStr = getConnectionString(schema);

    // Initialize ODBC
    SQLHENV env;
    SQLHDBC conn;
    SQLRETURN ret;

    ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    if (ret != SQL_SUCCESS) {
      Logger::error(LogCategory::DDL_EXPORT, "exportMariaDBDDL",
                    "Failed to allocate ODBC environment");
      return;
    }

    ret =
        SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    if (ret != SQL_SUCCESS) {
      Logger::error(LogCategory::DDL_EXPORT, "exportMariaDBDDL",
                    "Failed to set ODBC version");
      SQLFreeHandle(SQL_HANDLE_ENV, env);
      return;
    }

    ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);
    if (ret != SQL_SUCCESS) {
      Logger::error(LogCategory::DDL_EXPORT, "exportMariaDBDDL",
                    "Failed to allocate ODBC connection");
      SQLFreeHandle(SQL_HANDLE_ENV, env);
      return;
    }

    // Set connection timeout
    SQLSetConnectAttr(conn, SQL_ATTR_CONNECTION_TIMEOUT, (SQLPOINTER)30, 0);
    SQLSetConnectAttr(conn, SQL_ATTR_LOGIN_TIMEOUT, (SQLPOINTER)30, 0);

    ret = SQLDriverConnect(conn, nullptr, (SQLCHAR *)odbcConnStr.c_str(),
                           SQL_NTS, nullptr, 0, nullptr, SQL_DRIVER_NOPROMPT);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
      SQLCHAR sqlstate[SQL_SQLSTATE_SIZE + 1];
      SQLCHAR message[SQL_MAX_MESSAGE_LENGTH + 1];
      SQLSMALLINT i = 1;
      SQLINTEGER native_error;
      SQLSMALLINT msg_len;
      std::string error_details;

      while (SQLGetDiagRec(SQL_HANDLE_DBC, conn, i++, sqlstate, &native_error,
                           message, sizeof(message), &msg_len) == SQL_SUCCESS) {
        error_details += "SQLSTATE: " + std::string((char *)sqlstate) +
                         ", Native Error: " + std::to_string(native_error) +
                         ", Message: " + std::string((char *)message) + "\n";
      }

      Logger::error(LogCategory::DDL_EXPORT, "exportMariaDBDDL",
                    "Failed to connect to MSSQL server. Connection string: " +
                        odbcConnStr + "\nDetailed errors:\n" + error_details);
      SQLFreeHandle(SQL_HANDLE_DBC, conn);
      SQLFreeHandle(SQL_HANDLE_ENV, env);
      return;
    }

    // Test the connection with a simple query
    SQLHSTMT testStmt;
    ret = SQLAllocHandle(SQL_HANDLE_STMT, conn, &testStmt);
    if (ret == SQL_SUCCESS) {
      ret = SQLExecDirect(testStmt, (SQLCHAR *)"SELECT 1", SQL_NTS);
      if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        Logger::error(LogCategory::DDL_EXPORT, "exportMariaDBDDL",
                      "Connection test failed");
        SQLFreeHandle(SQL_HANDLE_STMT, testStmt);
        SQLFreeHandle(SQL_HANDLE_DBC, conn);
        SQLFreeHandle(SQL_HANDLE_ENV, env);
        return;
      }
      SQLFreeHandle(SQL_HANDLE_STMT, testStmt);
    }

    // Export Tables (already implemented in existing code)
    // Export Views
    exportMSSQLViews(conn, schema);

    // Export Procedures
    exportMSSQLProcedures(conn, schema);

    // Export Functions
    exportMSSQLFunctions(conn, schema);

    // Export Triggers
    exportMSSQLTriggers(conn, schema);

    // Export Constraints
    exportMSSQLConstraints(conn, schema);

    SQLDisconnect(conn);
    SQLFreeHandle(SQL_HANDLE_DBC, conn);
    SQLFreeHandle(SQL_HANDLE_ENV, env);

  } catch (const std::exception &e) {
    Logger::error(LogCategory::DDL_EXPORT, "exportMSSQLDDL",
                  "Error exporting MSSQL DDL: " + std::string(e.what()));
  }
}

void DDLExporter::exportMSSQLViews(SQLHDBC conn, const SchemaInfo &schema) {
  try {
    SQLHSTMT stmt;
    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, conn, &stmt);
    if (ret != SQL_SUCCESS) {
      Logger::error(LogCategory::DDL_EXPORT, "exportMariaDBDDL",
                    "Failed to allocate statement handle for views");
      return;
    }

    std::string viewsQuery =
        "SELECT name, definition FROM sys.views v "
        "JOIN sys.sql_modules m ON v.object_id = m.object_id "
        "WHERE SCHEMA_NAME(v.schema_id) = '" +
        escapeSQL(schema.schema_name) + "'";

    ret = SQLExecDirect(stmt, (SQLCHAR *)viewsQuery.c_str(), SQL_NTS);
    if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
      SQLCHAR viewName[256];
      SQLCHAR definition[4000];
      SQLLEN nameLen, defLen;

      while (SQLFetch(stmt) == SQL_SUCCESS) {
        SQLGetData(stmt, 1, SQL_C_CHAR, viewName, sizeof(viewName), &nameLen);
        SQLGetData(stmt, 2, SQL_C_CHAR, definition, sizeof(definition),
                   &defLen);

        std::string ddl =
            "-- MSSQL View: " + std::string((char *)viewName) + "\n";
        ddl += "-- Schema: " + schema.schema_name + "\n";
        ddl += "-- Database: " + schema.database_name + "\n\n";
        ddl += "CREATE VIEW [" + schema.schema_name + "].[" +
               std::string((char *)viewName) + "] AS\n";
        ddl += std::string((char *)definition) + "\n";

        saveTableDDL(schema.cluster_name, schema.db_engine,
                     schema.database_name, schema.schema_name,
                     std::string((char *)viewName), ddl);
      }
    }

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);

  } catch (const std::exception &e) {
    Logger::error(LogCategory::DDL_EXPORT, "exportMSSQLViews",
                  "Error exporting MSSQL views: " + std::string(e.what()));
  }
}

void DDLExporter::exportMSSQLProcedures(SQLHDBC conn,
                                        const SchemaInfo &schema) {
  try {
    SQLHSTMT stmt;
    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, conn, &stmt);
    if (ret != SQL_SUCCESS) {
      Logger::error(LogCategory::DDL_EXPORT, "exportMariaDBDDL",
                    "Failed to allocate statement handle for procedures");
      return;
    }

    std::string proceduresQuery =
        "SELECT name, definition FROM sys.procedures p "
        "JOIN sys.sql_modules m ON p.object_id = m.object_id "
        "WHERE SCHEMA_NAME(p.schema_id) = '" +
        escapeSQL(schema.schema_name) + "'";

    ret = SQLExecDirect(stmt, (SQLCHAR *)proceduresQuery.c_str(), SQL_NTS);
    if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
      SQLCHAR procName[256];
      SQLCHAR definition[4000];
      SQLLEN nameLen, defLen;

      while (SQLFetch(stmt) == SQL_SUCCESS) {
        SQLGetData(stmt, 1, SQL_C_CHAR, procName, sizeof(procName), &nameLen);
        SQLGetData(stmt, 2, SQL_C_CHAR, definition, sizeof(definition),
                   &defLen);

        std::string ddl =
            "-- MSSQL Procedure: " + std::string((char *)procName) + "\n";
        ddl += "-- Schema: " + schema.schema_name + "\n";
        ddl += "-- Database: " + schema.database_name + "\n\n";
        ddl += std::string((char *)definition) + "\n";

        saveFunctionDDL(schema.cluster_name, schema.db_engine,
                        schema.database_name, schema.schema_name,
                        std::string((char *)procName), ddl);
      }
    }

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);

  } catch (const std::exception &e) {
    Logger::error(LogCategory::DDL_EXPORT, "exportMSSQLProcedures",
                  "Error exporting MSSQL procedures: " + std::string(e.what()));
  }
}

void DDLExporter::exportMSSQLFunctions(SQLHDBC conn, const SchemaInfo &schema) {
  try {
    SQLHSTMT stmt;
    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, conn, &stmt);
    if (ret != SQL_SUCCESS) {
      Logger::error(LogCategory::DDL_EXPORT, "exportMariaDBDDL",
                    "Failed to allocate statement handle for functions");
      return;
    }

    std::string functionsQuery =
        "SELECT name, definition FROM sys.objects o "
        "JOIN sys.sql_modules m ON o.object_id = m.object_id "
        "WHERE o.type IN ('FN', 'IF', 'TF') "
        "AND SCHEMA_NAME(o.schema_id) = '" +
        escapeSQL(schema.schema_name) + "'";

    ret = SQLExecDirect(stmt, (SQLCHAR *)functionsQuery.c_str(), SQL_NTS);
    if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
      SQLCHAR funcName[256];
      SQLCHAR definition[4000];
      SQLLEN nameLen, defLen;

      while (SQLFetch(stmt) == SQL_SUCCESS) {
        SQLGetData(stmt, 1, SQL_C_CHAR, funcName, sizeof(funcName), &nameLen);
        SQLGetData(stmt, 2, SQL_C_CHAR, definition, sizeof(definition),
                   &defLen);

        std::string ddl =
            "-- MSSQL Function: " + std::string((char *)funcName) + "\n";
        ddl += "-- Schema: " + schema.schema_name + "\n";
        ddl += "-- Database: " + schema.database_name + "\n\n";
        ddl += std::string((char *)definition) + "\n";

        saveFunctionDDL(schema.cluster_name, schema.db_engine,
                        schema.database_name, schema.schema_name,
                        std::string((char *)funcName), ddl);
      }
    }

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);

  } catch (const std::exception &e) {
    Logger::error(LogCategory::DDL_EXPORT, "exportMSSQLFunctions",
                  "Error exporting MSSQL functions: " + std::string(e.what()));
  }
}

void DDLExporter::exportMSSQLTriggers(SQLHDBC conn, const SchemaInfo &schema) {
  try {
    SQLHSTMT stmt;
    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, conn, &stmt);
    if (ret != SQL_SUCCESS) {
      Logger::error(LogCategory::DDL_EXPORT, "exportMariaDBDDL",
                    "Failed to allocate statement handle for triggers");
      return;
    }

    std::string triggersQuery =
        "SELECT t.name, t.definition, o.name as table_name "
        "FROM sys.triggers t "
        "JOIN sys.objects o ON t.parent_id = o.object_id "
        "WHERE SCHEMA_NAME(t.schema_id) = '" +
        escapeSQL(schema.schema_name) + "'";

    ret = SQLExecDirect(stmt, (SQLCHAR *)triggersQuery.c_str(), SQL_NTS);
    if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
      SQLCHAR triggerName[256];
      SQLCHAR definition[4000];
      SQLCHAR tableName[256];
      SQLLEN nameLen, defLen, tableLen;

      while (SQLFetch(stmt) == SQL_SUCCESS) {
        SQLGetData(stmt, 1, SQL_C_CHAR, triggerName, sizeof(triggerName),
                   &nameLen);
        SQLGetData(stmt, 2, SQL_C_CHAR, definition, sizeof(definition),
                   &defLen);
        SQLGetData(stmt, 3, SQL_C_CHAR, tableName, sizeof(tableName),
                   &tableLen);

        std::string ddl =
            "-- MSSQL Trigger: " + std::string((char *)triggerName) + "\n";
        ddl += "-- Table: " + std::string((char *)tableName) + "\n";
        ddl += "-- Schema: " + schema.schema_name + "\n";
        ddl += "-- Database: " + schema.database_name + "\n\n";
        ddl += std::string((char *)definition) + "\n";

        saveFunctionDDL(schema.cluster_name, schema.db_engine,
                        schema.database_name, schema.schema_name,
                        std::string((char *)triggerName), ddl);
      }
    }

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);

  } catch (const std::exception &e) {
    Logger::error(LogCategory::DDL_EXPORT, "exportMSSQLTriggers",
                  "Error exporting MSSQL triggers: " + std::string(e.what()));
  }
}

void DDLExporter::exportMSSQLConstraints(SQLHDBC conn,
                                         const SchemaInfo &schema) {
  try {
    SQLHSTMT stmt;
    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, conn, &stmt);
    if (ret != SQL_SUCCESS) {
      Logger::error(LogCategory::DDL_EXPORT, "exportMariaDBDDL",
                    "Failed to allocate statement handle for constraints");
      return;
    }

    std::string constraintsQuery =
        "SELECT tc.table_name, tc.constraint_name, tc.constraint_type, "
        "cc.definition "
        "FROM information_schema.table_constraints tc "
        "LEFT JOIN sys.check_constraints cc ON tc.constraint_name = cc.name "
        "WHERE tc.table_schema = '" +
        escapeSQL(schema.schema_name) +
        "' "
        "AND tc.constraint_type IN ('FOREIGN KEY', 'CHECK', 'UNIQUE', 'PRIMARY "
        "KEY')";

    ret = SQLExecDirect(stmt, (SQLCHAR *)constraintsQuery.c_str(), SQL_NTS);
    if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
      SQLCHAR tableName[256];
      SQLCHAR constraintName[256];
      SQLCHAR constraintType[50];
      SQLCHAR definition[4000];
      SQLLEN tableLen, nameLen, typeLen, defLen;

      while (SQLFetch(stmt) == SQL_SUCCESS) {
        SQLGetData(stmt, 1, SQL_C_CHAR, tableName, sizeof(tableName),
                   &tableLen);
        SQLGetData(stmt, 2, SQL_C_CHAR, constraintName, sizeof(constraintName),
                   &nameLen);
        SQLGetData(stmt, 3, SQL_C_CHAR, constraintType, sizeof(constraintType),
                   &typeLen);
        SQLGetData(stmt, 4, SQL_C_CHAR, definition, sizeof(definition),
                   &defLen);

        std::string ddl =
            "-- " + std::string((char *)constraintType) +
            " constraint: " + std::string((char *)constraintName) +
            " on table " + std::string((char *)tableName);
        if (defLen > 0) {
          ddl += "\n" + std::string((char *)definition);
        }

        saveConstraintDDL(schema.cluster_name, schema.db_engine,
                          schema.database_name, schema.schema_name,
                          std::string((char *)tableName), ddl);
      }
    }

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);

  } catch (const std::exception &e) {
    Logger::error(LogCategory::DDL_EXPORT, "exportMSSQLConstraints",
                  "Error exporting MSSQL constraints: " +
                      std::string(e.what()));
  }
}

void DDLExporter::saveTableDDL(const std::string &cluster,
                               const std::string &engine,
                               const std::string &database,
                               const std::string &schema,
                               const std::string &table_name,
                               const std::string &ddl) {
  try {
    std::string filePath = exportPath + "/" + sanitizeFileName(cluster) + "/" +
                           sanitizeFileName(engine) + "/" +
                           sanitizeFileName(database) + "/" +
                           sanitizeFileName(schema) + "/tables/" +
                           sanitizeFileName(table_name) + ".sql";

    // Validate file path length
    if (filePath.length() > 260) {
      Logger::error(LogCategory::DDL_EXPORT, "exportMariaDBDDL",
                    "File path too long: " + filePath);
      return;
    }

    // Check if parent directory exists and is writable
    std::filesystem::path parentPath =
        std::filesystem::path(filePath).parent_path();
    if (!std::filesystem::exists(parentPath)) {
      try {
        std::filesystem::create_directories(parentPath);
      } catch (const std::filesystem::filesystem_error &e) {
        Logger::error(LogCategory::DDL_EXPORT, "exportMariaDBDDL",
                      "Failed to create directory: " + parentPath.string() +
                          " - " + e.what());
        return;
      }
    }

    // Check available disk space
    std::error_code ec;
    auto space = std::filesystem::space(parentPath, ec);
    if (!ec && space.available < 1024 * 1024) { // Less than 1MB available
    }

    std::ofstream file(filePath);
    if (file.is_open()) {
      file << "-- Table DDL for " << schema << "." << table_name << std::endl;
      file << "-- Engine: " << engine << std::endl;
      file << "-- Database: " << database << std::endl;
      file << "-- Generated: " << std::time(nullptr) << std::endl;
      file << std::endl;
      file << ddl << std::endl;

      if (file.good()) {
        file.close();
      } else {
        Logger::error(LogCategory::DDL_EXPORT, "exportMariaDBDDL",
                      "Error writing to file: " + filePath);
        file.close();
        // Try to remove the partial file
        std::filesystem::remove(filePath);
      }
    } else {
      Logger::error(LogCategory::DDL_EXPORT, "exportMariaDBDDL",
                    "Failed to open file for writing: " + filePath);
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DDL_EXPORT, "saveTableDDL",
                  "Error saving table DDL: " + std::string(e.what()));
  }
}

void DDLExporter::saveIndexDDL(const std::string &cluster,
                               const std::string &engine,
                               const std::string &database,
                               const std::string &schema,
                               const std::string &table_name,
                               const std::string &index_ddl) {
  try {
    std::string filePath = exportPath + "/" + sanitizeFileName(cluster) + "/" +
                           sanitizeFileName(engine) + "/" +
                           sanitizeFileName(database) + "/" +
                           sanitizeFileName(schema) + "/indexes/" +
                           sanitizeFileName(table_name) + "_indexes.sql";

    // Create directory if it doesn't exist
    std::filesystem::create_directories(
        std::filesystem::path(filePath).parent_path());

    std::ofstream file(filePath, std::ios::app);
    if (file.is_open()) {
      file << index_ddl << std::endl;
      file.close();

      // Logger::debug(LogCategory::DDL_EXPORT, "DDLExporter", "Saved index DDL:
      // " + filePath);
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DDL_EXPORT, "saveIndexDDL",
                  "Error saving index DDL: " + std::string(e.what()));
  }
}

void DDLExporter::saveConstraintDDL(const std::string &cluster,
                                    const std::string &engine,
                                    const std::string &database,
                                    const std::string &schema,
                                    const std::string &table_name,
                                    const std::string &constraint_ddl) {
  try {
    std::string filePath = exportPath + "/" + sanitizeFileName(cluster) + "/" +
                           sanitizeFileName(engine) + "/" +
                           sanitizeFileName(database) + "/" +
                           sanitizeFileName(schema) + "/constraints/" +
                           sanitizeFileName(table_name) + "_constraints.sql";

    // Create directory if it doesn't exist
    std::filesystem::create_directories(
        std::filesystem::path(filePath).parent_path());

    std::ofstream file(filePath, std::ios::app);
    if (file.is_open()) {
      file << constraint_ddl << std::endl;
      file.close();

      // Logger::debug(LogCategory::DDL_EXPORT, "DDLExporter", "Saved constraint
      // DDL: " + filePath);
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DDL_EXPORT, "saveConstraintDDL",
                  "Error saving constraint DDL: " + std::string(e.what()));
  }
}

void DDLExporter::saveFunctionDDL(const std::string &cluster,
                                  const std::string &engine,
                                  const std::string &database,
                                  const std::string &schema,
                                  const std::string &function_name,
                                  const std::string &function_ddl) {
  try {
    std::string filePath = exportPath + "/" + sanitizeFileName(cluster) + "/" +
                           sanitizeFileName(engine) + "/" +
                           sanitizeFileName(database) + "/" +
                           sanitizeFileName(schema) + "/functions/" +
                           sanitizeFileName(function_name) + ".sql";

    // Create directory if it doesn't exist
    std::filesystem::create_directories(
        std::filesystem::path(filePath).parent_path());

    std::ofstream file(filePath);
    if (file.is_open()) {
      file << "-- Function DDL for " << schema << "." << function_name
           << std::endl;
      file << "-- Engine: " << engine << std::endl;
      file << "-- Database: " << database << std::endl;
      file << "-- Generated: " << std::time(nullptr) << std::endl;
      file << std::endl;
      file << function_ddl << std::endl;
      file.close();

      // Logger::debug(LogCategory::DDL_EXPORT, "DDLExporter", "Saved function
      // DDL: " + filePath);
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DDL_EXPORT, "saveFunctionDDL",
                  "Error saving function DDL: " + std::string(e.what()));
  }
}

std::string DDLExporter::getConnectionString(const SchemaInfo &schema) {
  return schema.connection_string;
}

std::string DDLExporter::escapeSQL(const std::string &value) {
  std::string escaped = value;
  size_t pos = 0;
  while ((pos = escaped.find("'", pos)) != std::string::npos) {
    escaped.replace(pos, 1, "''");
    pos += 2;
  }
  return escaped;
}

std::string DDLExporter::sanitizeFileName(const std::string &name) {
  std::string sanitized = name;
  std::replace(sanitized.begin(), sanitized.end(), ' ', '_');
  std::replace(sanitized.begin(), sanitized.end(), '/', '_');
  std::replace(sanitized.begin(), sanitized.end(), '\\', '_');
  std::replace(sanitized.begin(), sanitized.end(), ':', '_');
  std::replace(sanitized.begin(), sanitized.end(), '*', '_');
  std::replace(sanitized.begin(), sanitized.end(), '?', '_');
  std::replace(sanitized.begin(), sanitized.end(), '"', '_');
  std::replace(sanitized.begin(), sanitized.end(), '<', '_');
  std::replace(sanitized.begin(), sanitized.end(), '>', '_');
  std::replace(sanitized.begin(), sanitized.end(), '|', '_');
  return sanitized;
}
