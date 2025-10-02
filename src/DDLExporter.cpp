#include "DDLExporter.h"
#include "MSSQLDDLExporter.h"
#include "MariaDBDDLExporter.h"
#include "PostgreSQLDDLExporter.h"
#include "logger.h"
#include <pqxx/pqxx>

DDLExporter::DDLExporter() : connectionManager(), fileManager() {}

void DDLExporter::exportAllDDL() {
  try {
    // Get all schemas from catalog
    std::vector<SchemaInfo> schemas;
    getSchemasFromCatalog(schemas);

    if (schemas.empty()) {
      Logger::warning(LogCategory::DDL_EXPORT, "DDLExporter",
                      "No schemas found in catalog for DDL export");
      return;
    }

    Logger::info(LogCategory::DDL_EXPORT, "DDLExporter",
                 "Starting DDL export for " + std::to_string(schemas.size()) +
                     " schemas");

    // Export DDL for each schema
    for (const auto &schema : schemas) {
      try {
        exportSchemaDDL(schema);
      } catch (const std::exception &e) {
        Logger::error(LogCategory::DDL_EXPORT, "DDLExporter",
                      "Failed to export DDL for schema " + schema.schema_name +
                          ": " + std::string(e.what()));
      }
    }

    Logger::info(LogCategory::DDL_EXPORT, "DDLExporter",
                 "DDL export completed successfully");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DDL_EXPORT, "DDLExporter",
                  "Error during DDL export: " + std::string(e.what()));
  }
}

void DDLExporter::exportSchemaDDL(const SchemaInfo &schema) {
  try {
    Logger::info(LogCategory::DDL_EXPORT, "DDLExporter",
                 "Exporting DDL for schema: " + schema.schema_name +
                     " (Engine: " + schema.db_engine + ")");

    // Create appropriate exporter based on database engine
    if (schema.db_engine == "mariadb") {
      MariaDBDDLExporter exporter(connectionManager, fileManager);
      exporter.exportDDL(schema);
    } else if (schema.db_engine == "postgresql") {
      PostgreSQLDDLExporter exporter(connectionManager, fileManager);
      exporter.exportDDL(schema);
    } else if (schema.db_engine == "mssql") {
      MSSQLDDLExporter exporter(connectionManager, fileManager);
      exporter.exportDDL(schema);
    } else {
      Logger::warning(LogCategory::DDL_EXPORT, "DDLExporter",
                      "Unsupported database engine: " + schema.db_engine +
                          " for schema: " + schema.schema_name);
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DDL_EXPORT, "DDLExporter",
                  "Error exporting DDL for schema " + schema.schema_name +
                      ": " + std::string(e.what()));
  }
}

void DDLExporter::getSchemasFromCatalog(std::vector<SchemaInfo> &schemas) {

  try {
    // Connect to PostgreSQL catalog
    pqxx::connection conn(DatabaseConfig::getPostgresConnectionString());
    if (!conn.is_open()) {
      Logger::error(LogCategory::DDL_EXPORT, "getSchemasFromCatalog",
                    "Failed to connect to PostgreSQL catalog");
      return;
    }

    pqxx::work txn(conn);

    // Query to get all schemas from catalog
    std::string query = "SELECT DISTINCT cluster_name, db_engine, schema_name, "
                        "connection_string "
                        "FROM metadata.catalog "
                        "WHERE schema_name IS NOT NULL AND schema_name != '' "
                        "ORDER BY cluster_name, db_engine, schema_name;";

    pqxx::result result = txn.exec(query);

    for (const auto &row : result) {
      SchemaInfo schema;
      schema.cluster_name = row["cluster_name"].as<std::string>();
      schema.db_engine = row["db_engine"].as<std::string>();
      schema.database_name = ""; // No hay database_name en la tabla catalog
      schema.schema_name = row["schema_name"].as<std::string>();
      schema.connection_string = row["connection_string"].as<std::string>();

      schemas.push_back(schema);
    }

    txn.commit();
    Logger::info(LogCategory::DDL_EXPORT, "getSchemasFromCatalog",
                 "Retrieved " + std::to_string(schemas.size()) +
                     " schemas from catalog");

  } catch (const std::exception &e) {
    Logger::error(LogCategory::DDL_EXPORT, "getSchemasFromCatalog",
                  "Error retrieving schemas from catalog: " +
                      std::string(e.what()));
  }
}
