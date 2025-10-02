#ifndef MSSQLTABLESETUP_H
#define MSSQLTABLESETUP_H

#include "MSSQLConnectionManager.h"
#include "TableInfo.h"
#include "logger.h"
#include <pqxx/pqxx>
#include <sql.h>
#include <string>
#include <unordered_map>
#include <vector>

class MSSQLTableSetup {
public:
  MSSQLTableSetup() = default;
  ~MSSQLTableSetup() = default;

  // Table setup operations
  void setupTableTargetMSSQLToPostgres();
  void syncIndexesAndConstraints(const std::string &schema_name,
                                 const std::string &table_name,
                                 pqxx::connection &pgConn,
                                 const std::string &lowerSchemaName,
                                 const std::string &connection_string);

  // Data type mapping
  std::string mapDataType(const std::string &mssqlType,
                          const std::string &maxLength,
                          const std::string &numericPrecision = "",
                          const std::string &numericScale = "");

  // Static data type maps
  static std::unordered_map<std::string, std::string> dataTypeMap;
  static std::unordered_map<std::string, std::string> collationMap;

private:
  MSSQLConnectionManager connectionManager;

  // Helper functions
  void createTableSchema(const TableInfo &table, pqxx::connection &pgConn,
                         SQLHDBC mssqlConn);
  void sortTablesByPriority(std::vector<TableInfo> &tables);
  std::string escapeSQL(const std::string &value);
};

#endif // MSSQLTABLESETUP_H
