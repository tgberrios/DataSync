#ifndef MSSQLQUERYEXECUTOR_H
#define MSSQLQUERYEXECUTOR_H

#include "MSSQLConnectionManager.h"
#include "TableInfo.h"
#include "logger.h"
#include <pqxx/pqxx>
#include <sql.h>
#include <string>
#include <vector>

class MSSQLQueryExecutor {
public:
  MSSQLQueryExecutor() = default;
  ~MSSQLQueryExecutor() = default;

  // Table queries
  std::vector<TableInfo> getActiveTables(pqxx::connection &pgConn);

  // Primary key operations
  std::vector<std::string> getPrimaryKeyColumns(SQLHDBC mssqlConn,
                                                const std::string &schema_name,
                                                const std::string &table_name);

  // Data comparison operations
  std::vector<std::vector<std::string>>
  findDeletedPrimaryKeys(SQLHDBC mssqlConn, const std::string &schema_name,
                         const std::string &table_name,
                         const std::vector<std::vector<std::string>> &pgPKs,
                         const std::vector<std::string> &pkColumns);

  // Catalog operations
  std::string getPKStrategyFromCatalog(pqxx::connection &pgConn,
                                       const std::string &schema_name,
                                       const std::string &table_name);
  std::vector<std::string>
  getPKColumnsFromCatalog(pqxx::connection &pgConn,
                          const std::string &schema_name,
                          const std::string &table_name);
  std::vector<std::string>
  getCandidateColumnsFromCatalog(pqxx::connection &pgConn,
                                 const std::string &schema_name,
                                 const std::string &table_name);
  std::string getLastProcessedPKFromCatalog(pqxx::connection &pgConn,
                                            const std::string &schema_name,
                                            const std::string &table_name);

  // Utility functions
  std::vector<std::string> parseJSONArray(const std::string &jsonArray);
  std::string
  getLastPKFromResults(const std::vector<std::vector<std::string>> &results,
                       const std::vector<std::string> &pkColumns,
                       const std::vector<std::string> &columnNames);
  std::vector<std::string> parseLastPK(const std::string &lastPK);

private:
  MSSQLConnectionManager connectionManager;
  std::string escapeSQL(const std::string &value);
};

#endif // MSSQLQUERYEXECUTOR_H
