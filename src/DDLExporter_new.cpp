#include "DDLExporter.h"
#include "MariaDBDDLExporter.h"
#include "PostgreSQLDDLExporter.h"
#include "MSSQLDDLExporter.h"
#include "MongoDBDDLExporter.h"
#include <chrono>

DDLExporter::DDLExporter() : fileManager("DDL_EXPORT") {
}

void DDLExporter::exportAllDDL() {
  auto startTime = std::chrono::high_resolution_clock::now();

  try {
    fileManager.createFolderStructure();
    getSchemasFromCatalog();

    Logger::info(LogCategory::DDL_EXPORT, "DDLExporter", 
                 "DDL export started - Found " + std::to_string(schemas.size()) + " schemas to export");

    size_t successCount = 0;
    size_t errorCount = 0;

    for (size_t i = 0; i < schemas.size(); ++i) {
      const auto &schema = schemas[i];
      try {
        Logger::info(LogCategory::DDL_EXPORT, "DDLExporter",
                     "Exporting schema " + std::to_string(i + 1) + "/" +
                         std::to_string(schemas.size()) + ": " +
                         schema.schema_name);

        exportSchemaDDL(schema);
        successCount++;

        Logger::info(LogCategory::DDL_EXPORT, "DDLExporter",
                     "Successfully exported schema: " + schema.schema_name);
      } catch (const std::exception &e) {
        errorCount++;
        Logger::error(LogCategory::DDL_EXPORT, "DDLExporter", 
                      "Error exporting schema " + schema.schema_name + ": " + std::string(e.what()));
      }
    }

    auto endTime = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(endTime - startTime);

    Logger::info(LogCategory::DDL_EXPORT, "DDLExporter",
                 "DDL export process completed in " + std::to_string(duration.count()) +
                 " seconds - Success: " + std::to_string(successCount) +
                 ", Errors: " + std::to_string(errorCount));
  } catch (const std::exception &e) {
    auto endTime = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(endTime - startTime);

    Logger::error(LogCategory::DDL_EXPORT, "DDLExporter",
                  "Error in DDL export process after " + std::to_string(duration.count()) +
                  " seconds: " + std::string(e.what()));
  }
}

void DDLExporter::getSchemasFromCatalog() {
  try {
    pqxx::connection conn(DatabaseConfig::getPostgresConnectionString());
    pqxx::work txn(conn);

    std::string query =
        "SELECT DISTINCT schema_name, db_engine, connection_string, cluster_name "
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

    Logger::info(LogCategory::DDL_EXPORT, "DDLExporter", 
                 "Retrieved " + std::to_string(schemas.size()) + " schemas from catalog");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DDL_EXPORT, "DDLExporter",
                  "Error getting schemas from catalog: " + std::string(e.what()));
  }
}

void DDLExporter::exportSchemaDDL(const SchemaInfo &schema) {
  try {
    fileManager.createClusterFolder(schema.cluster_name);
    fileManager.createEngineFolder(schema.cluster_name, schema.db_engine);
    fileManager.createDatabaseFolder(schema.cluster_name, schema.db_engine, schema.database_name);
    fileManager.createSchemaFolder(schema.cluster_name, schema.db_engine, schema.database_name, schema.schema_name);

    auto exporter = createExporter(schema.db_engine);
    if (exporter) {
      exporter->exportDDL(schema);
    } else {
      Logger::warning(LogCategory::DDL_EXPORT, "DDLExporter",
                      "Unknown database engine: " + schema.db_engine);
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DDL_EXPORT, "DDLExporter",
                  "Error exporting schema DDL: " + std::string(e.what()));
  }
}

std::unique_ptr<DDLExporterInterface> DDLExporter::createExporter(const std::string &dbEngine) {
  if (dbEngine == "MariaDB") {
    return std::make_unique<MariaDBDDLExporter>(connectionManager, fileManager);
  } else if (dbEngine == "PostgreSQL") {
    return std::make_unique<PostgreSQLDDLExporter>(connectionManager, fileManager);
  } else if (dbEngine == "MSSQL") {
    return std::make_unique<MSSQLDDLExporter>(connectionManager, fileManager);
  } else if (dbEngine == "MongoDB") {
    return std::make_unique<MongoDBDDLExporter>(connectionManager, fileManager);
  }
  
  return nullptr;
}
