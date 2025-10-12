#include "export/DDLExporter.h"
#include "engines/database_engine.h"
#include "engines/mariadb_engine.h"
#include "engines/mssql_engine.h"
#include "utils/connection_utils.h"
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
    std::string connStr = getConnectionString(schema);

    auto params = ConnectionStringParser::parse(connStr);
    if (!params) {
      Logger::error(LogCategory::DDL_EXPORT, "exportMariaDBDDL",
                    "Failed to parse MariaDB connection string");
      return;
    }

    MySQLConnection conn(*params);
    if (!conn.isValid()) {
      Logger::error(LogCategory::DDL_EXPORT, "exportMariaDBDDL",
                    "MariaDB connection failed");
      return;
    }

    std::string tablesQuery =
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = '" +
        escapeSQL(schema.schema_name) +
        "' "
        "AND table_type = 'BASE TABLE';";

    if (mysql_query(conn.get(), tablesQuery.c_str())) {
      Logger::error(LogCategory::DDL_EXPORT, "exportMariaDBDDL",
                    "Query failed: " + std::string(mysql_error(conn.get())));
      return;
    }

    MYSQL_RES *tablesResult = mysql_store_result(conn.get());
    if (!tablesResult) {
      Logger::error(LogCategory::DDL_EXPORT, "exportMariaDBDDL",
                    "mysql_store_result() failed: " +
                        std::string(mysql_error(conn.get())));
      return;
    }

    MYSQL_ROW tableRow;
    while ((tableRow = mysql_fetch_row(tablesResult))) {
      std::string tableName = tableRow[0] ? tableRow[0] : "";

      std::string createTableQuery = "SHOW CREATE TABLE `" +
                                     escapeSQL(schema.schema_name) + "`.`" +
                                     escapeSQL(tableName) + "`;";

      if (mysql_query(conn.get(), createTableQuery.c_str())) {
        Logger::error(LogCategory::DDL_EXPORT, "exportMariaDBDDL",
                      "SHOW CREATE TABLE failed: " +
                          std::string(mysql_error(conn.get())));
        continue;
      }

      MYSQL_RES *createResult = mysql_store_result(conn.get());
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

      if (mysql_query(conn.get(), indexesQuery.c_str())) {
        Logger::error(LogCategory::DDL_EXPORT, "exportMariaDBDDL",
                      "SHOW INDEX failed: " +
                          std::string(mysql_error(conn.get())));
        continue;
      }

      MYSQL_RES *indexesResult = mysql_store_result(conn.get());
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
    exportMariaDBViews(conn.get(), schema);

    // Export Stored Procedures
    exportMariaDBProcedures(conn.get(), schema);

    // Export Functions
    exportMariaDBFunctions(conn.get(), schema);

    // Export Triggers
    exportMariaDBTriggers(conn.get(), schema);

    // Export Constraints
    exportMariaDBConstraints(conn.get(), schema);

    // Export Events
    exportMariaDBEvents(conn.get(), schema);

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

    if (connStr.empty()) {
      Logger::error(LogCategory::DDL_EXPORT, "exportMSSQLDDL",
                    "MSSQL connection string is empty");
      return;
    }

    ODBCConnection conn(connStr);
    if (!conn.isValid()) {
      Logger::error(LogCategory::DDL_EXPORT, "exportMSSQLDDL",
                    "MSSQL connection failed");
      return;
    }

    // Export Views
    exportMSSQLViews(conn.getDbc(), schema);

    // Export Procedures
    exportMSSQLProcedures(conn.getDbc(), schema);

    // Export Functions
    exportMSSQLFunctions(conn.getDbc(), schema);

    // Export Triggers
    exportMSSQLTriggers(conn.getDbc(), schema);

    // Export Constraints
    exportMSSQLConstraints(conn.getDbc(), schema);

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

void DDLExporter::saveDDLToFile(
    const std::string &cluster, const std::string &engine,
    const std::string &database, const std::string &schema,
    const std::string &objectName, const std::string &ddlContent,
    const std::string &subfolder, const std::string &fileSuffix,
    bool appendMode, bool includeHeader) {
  try {
    std::filesystem::path filePath =
        std::filesystem::path(exportPath) / sanitizeFileName(cluster) /
        sanitizeFileName(engine) / sanitizeFileName(database) /
        sanitizeFileName(schema) / subfolder /
        (sanitizeFileName(objectName) + fileSuffix);

    std::filesystem::create_directories(filePath.parent_path());

    std::ios_base::openmode mode = appendMode ? std::ios::app : std::ios::out;
    std::ofstream file(filePath, mode);

    if (file.is_open()) {
      if (includeHeader && !appendMode) {
        file << "-- " << subfolder << " DDL for " << schema << "." << objectName
             << "\n"
             << "-- Engine: " << engine << "\n"
             << "-- Database: " << database << "\n"
             << "-- Generated: " << std::time(nullptr) << "\n\n";
      }
      file << ddlContent << "\n";
      file.close();
    } else {
      Logger::error(LogCategory::DDL_EXPORT, "saveDDLToFile",
                    "Failed to open file: " + filePath.string());
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DDL_EXPORT, "saveDDLToFile",
                  "Error saving DDL: " + std::string(e.what()));
  }
}

void DDLExporter::saveTableDDL(const std::string &cluster,
                               const std::string &engine,
                               const std::string &database,
                               const std::string &schema,
                               const std::string &table_name,
                               const std::string &ddl) {
  saveDDLToFile(cluster, engine, database, schema, table_name, ddl, "tables",
                ".sql", false, true);
}

void DDLExporter::saveIndexDDL(const std::string &cluster,
                               const std::string &engine,
                               const std::string &database,
                               const std::string &schema,
                               const std::string &table_name,
                               const std::string &index_ddl) {
  saveDDLToFile(cluster, engine, database, schema, table_name, index_ddl,
                "indexes", "_indexes.sql", true, false);
}

void DDLExporter::saveConstraintDDL(const std::string &cluster,
                                    const std::string &engine,
                                    const std::string &database,
                                    const std::string &schema,
                                    const std::string &table_name,
                                    const std::string &constraint_ddl) {
  saveDDLToFile(cluster, engine, database, schema, table_name, constraint_ddl,
                "constraints", "_constraints.sql", true, false);
}

void DDLExporter::saveFunctionDDL(const std::string &cluster,
                                  const std::string &engine,
                                  const std::string &database,
                                  const std::string &schema,
                                  const std::string &function_name,
                                  const std::string &function_ddl) {
  saveDDLToFile(cluster, engine, database, schema, function_name, function_ddl,
                "functions", ".sql", false, true);
}

std::string DDLExporter::getConnectionString(const SchemaInfo &schema) {
  return schema.connection_string;
}

std::string DDLExporter::sanitizeFileName(const std::string &name) {
  static const std::string invalidChars = " /\\:*?\"<>|";
  std::string sanitized = name;

  for (char &c : sanitized) {
    if (invalidChars.find(c) != std::string::npos) {
      c = '_';
    }
  }

  return sanitized;
}
