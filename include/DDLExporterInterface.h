#ifndef DDLEXPORTERINTERFACE_H
#define DDLEXPORTERINTERFACE_H

#include "DatabaseConnectionManager.h"
#include "DDLFileManager.h"
#include <string>

struct SchemaInfo {
  std::string schema_name;
  std::string db_engine;
  std::string database_name;
  std::string connection_string;
  std::string cluster_name;
};

class DDLExporterInterface {
public:
  DDLExporterInterface(DatabaseConnectionManager &connManager, DDLFileManager &fileManager);
  virtual ~DDLExporterInterface() = default;

  virtual void exportDDL(const SchemaInfo &schema) = 0;

protected:
  DatabaseConnectionManager &connectionManager;
  DDLFileManager &fileManager;
  
  std::string escapeSQL(const std::string &value);
  void logError(const std::string &operation, const std::string &error);
  void logInfo(const std::string &operation, const std::string &message);
};

#endif // DDLEXPORTERINTERFACE_H
