#ifndef DDLEXPORTER_H
#define DDLEXPORTER_H

#include "Config.h"
#include "DatabaseConnectionManager.h"
#include "DDLFileManager.h"
#include "DDLExporterInterface.h"
#include "logger.h"
#include <memory>
#include <string>
#include <vector>

// Forward declarations
class MariaDBDDLExporter;
class PostgreSQLDDLExporter;
class MSSQLDDLExporter;
class MongoDBDDLExporter;

class DDLExporter {
public:
  DDLExporter();
  ~DDLExporter() = default;

  void exportAllDDL();

private:
  void getSchemasFromCatalog(std::vector<SchemaInfo> &schemas);
  void exportSchemaDDL(const SchemaInfo &schema);
  std::unique_ptr<DDLExporterInterface> createExporter(const std::string &dbEngine);

  // Core components
  DatabaseConnectionManager connectionManager;
  DDLFileManager fileManager;
  
  // Schema data
  std::vector<SchemaInfo> schemas;
};

#endif // DDLEXPORTER_H
