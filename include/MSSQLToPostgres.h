#ifndef MSSQLTOPOSTGRES_H
#define MSSQLTOPOSTGRES_H

#include "Config.h"
#include "MSSQLConnectionManager.h"
#include "MSSQLDataTransfer.h"
#include "MSSQLDataValidator.h"
#include "MSSQLQueryExecutor.h"
#include "MSSQLTableSetup.h"
#include "TableInfo.h"
#include "catalog_manager.h"
#include "logger.h"
#include <pqxx/pqxx>
#include <sql.h>
#include <string>
#include <unordered_map>
#include <vector>

class MSSQLToPostgres {
public:
  MSSQLToPostgres() = default;
  ~MSSQLToPostgres() = default;

  static std::unordered_map<std::string, std::string> dataTypeMap;
  static std::unordered_map<std::string, std::string> collationMap;

  // Public interface methods
  SQLHDBC getMSSQLConnection(const std::string &connectionString);
  void closeMSSQLConnection(SQLHDBC conn);
  void setupTableTargetMSSQLToPostgres();
  void transferDataMSSQLToPostgres();
  std::vector<TableInfo> getActiveTables(pqxx::connection &pgConn);
  void syncIndexesAndConstraints(const std::string &schema_name,
                                 const std::string &table_name,
                                 pqxx::connection &pgConn,
                                 const std::string &lowerSchemaName,
                                 const std::string &connection_string);

private:
  // Specialized components
  MSSQLConnectionManager connectionManager;
  MSSQLQueryExecutor queryExecutor;
  MSSQLDataValidator dataValidator;
  MSSQLDataTransfer dataTransfer;
  MSSQLTableSetup tableSetup;
};

// Static member definitions will be in .cpp file

#endif // MSSQLTOPOSTGRES_H
