#ifndef MARIADBTOPOSTGRES_H
#define MARIADBTOPOSTGRES_H

#include "Config.h"
#include "MariaDBConnectionManager.h"
#include "MariaDBDataTransfer.h"
#include "MariaDBDataValidator.h"
#include "MariaDBQueryExecutor.h"
#include "TableInfo.h"
#include "catalog_manager.h"
#include "logger.h"
#include <algorithm>
#include <cctype>
#include <mysql/mysql.h>
#include <pqxx/pqxx>
#include <string>
#include <unordered_map>
#include <vector>

class MariaDBToPostgres {
public:
  MariaDBToPostgres() = default;
  ~MariaDBToPostgres() = default;

  static std::unordered_map<std::string, std::string> dataTypeMap;

  // Public interface methods
  MYSQL *getMariaDBConnection(const std::string &connectionString);
  void setupTableTargetMariaDBToPostgres();
  void transferDataMariaDBToPostgres();
  std::vector<TableInfo> getActiveTables(pqxx::connection &pgConn) const;
  void syncIndexesAndConstraints(const std::string &schema_name,
                                 const std::string &table_name,
                                 pqxx::connection &pgConn,
                                 const std::string &lowerSchemaName,
                                 const std::string &connection_string);

private:
  // Specialized components
  MariaDBConnectionManager connectionManager;
  MariaDBQueryExecutor queryExecutor;
  MariaDBDataValidator dataValidator;
  MariaDBDataTransfer dataTransfer;

  // Helper methods
  void processTableSetup(const TableInfo &table, pqxx::connection &pgConn);
  void createTableSchema(const TableInfo &table, pqxx::connection &pgConn,
                         MYSQL *mariadbConn);
  std::string mapDataType(const std::string &mariaType,
                          const std::string &maxLength);
  void sortTablesByPriority(std::vector<TableInfo> &tables);
};

// Static member definitions will be in .cpp file

#endif // MARIADBTOPOSTGRES_H
