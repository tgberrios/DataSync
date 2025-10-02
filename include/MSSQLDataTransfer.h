#ifndef MSSQLDATATRANSFER_H
#define MSSQLDATATRANSFER_H

#include "Config.h"
#include "MSSQLConnectionManager.h"
#include "MSSQLDataValidator.h"
#include "MSSQLQueryExecutor.h"
#include "TableInfo.h"
#include "logger.h"
#include <pqxx/pqxx>
#include <sql.h>
#include <string>
#include <vector>

class MSSQLDataTransfer {
public:
  MSSQLDataTransfer() = default;
  ~MSSQLDataTransfer() = default;

  // Main transfer operations
  void transferDataMSSQLToPostgres();

  // Data processing operations
  void processDeletesByPrimaryKey(const std::string &schema_name,
                                  const std::string &table_name,
                                  SQLHDBC mssqlConn, pqxx::connection &pgConn);
  void processUpdatesByPrimaryKey(const std::string &schema_name,
                                  const std::string &table_name,
                                  SQLHDBC mssqlConn, pqxx::connection &pgConn,
                                  const std::string &timeColumn,
                                  const std::string &lastSyncTime);

  // Bulk operations
  void performBulkUpsert(pqxx::connection &pgConn,
                         const std::vector<std::vector<std::string>> &results,
                         const std::vector<std::string> &columnNames,
                         const std::vector<std::string> &columnTypes,
                         const std::string &lowerSchemaName,
                         const std::string &tableName,
                         const std::string &sourceSchemaName);
  void performBulkInsert(pqxx::connection &pgConn,
                         const std::vector<std::vector<std::string>> &results,
                         const std::vector<std::string> &columnNames,
                         const std::vector<std::string> &columnTypes,
                         const std::string &lowerSchemaName,
                         const std::string &tableName);

  // Status management
  void updateStatus(pqxx::connection &pgConn, const std::string &schema_name,
                    const std::string &table_name, const std::string &status,
                    size_t offset = 0);
  void updateLastProcessedPK(pqxx::connection &pgConn,
                             const std::string &schema_name,
                             const std::string &table_name,
                             const std::string &lastPK);

  // Utility functions
  std::string getLastSyncTimeOptimized(pqxx::connection &pgConn,
                                       const std::string &schema_name,
                                       const std::string &table_name,
                                       const std::string &lastSyncColumn);

private:
  MSSQLConnectionManager connectionManager;
  MSSQLDataValidator dataValidator;
  MSSQLQueryExecutor queryExecutor;

  // Helper functions
  std::vector<std::string>
  getPrimaryKeyColumnsFromPostgres(pqxx::connection &pgConn,
                                   const std::string &schemaName,
                                   const std::string &tableName);
  std::string buildUpsertQuery(const std::vector<std::string> &columnNames,
                               const std::vector<std::string> &pkColumns,
                               const std::string &schemaName,
                               const std::string &tableName);
  std::string
  buildUpsertConflictClause(const std::vector<std::string> &columnNames,
                            const std::vector<std::string> &pkColumns);
  size_t deleteRecordsByPrimaryKey(
      pqxx::connection &pgConn, const std::string &lowerSchemaName,
      const std::string &table_name,
      const std::vector<std::vector<std::string>> &deletedPKs,
      const std::vector<std::string> &pkColumns);
  std::string escapeSQL(const std::string &value);
};

#endif // MSSQLDATATRANSFER_H
