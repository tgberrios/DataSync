#ifndef MSSQLCONNECTIONMANAGER_H
#define MSSQLCONNECTIONMANAGER_H

#include "Config.h"
#include "logger.h"
#include <sql.h>
#include <sqlext.h>
#include <string>
#include <vector>

class MSSQLConnectionManager {
public:
  MSSQLConnectionManager() = default;
  ~MSSQLConnectionManager() = default;

  // Connection management
  SQLHDBC getMSSQLConnection(const std::string &connectionString);
  void closeMSSQLConnection(SQLHDBC conn);

  // Query execution
  std::vector<std::vector<std::string>>
  executeQueryMSSQL(SQLHDBC conn, const std::string &query);

  // Utility functions
  std::string extractDatabaseName(const std::string &connectionString);
  std::string escapeSQL(const std::string &value);

private:
  // Connection validation
  bool validateConnectionString(const std::string &connectionString);
  bool testConnection(SQLHDBC conn);
};

#endif // MSSQLCONNECTIONMANAGER_H
