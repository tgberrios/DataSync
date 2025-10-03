#ifndef MARIADBDATATRANSFER_H
#define MARIADBDATATRANSFER_H

#include "Config.h"
#include "TableInfo.h"
#include "logger.h"
#include <mysql/mysql.h>
#include <pqxx/pqxx>
#include <string>
#include <vector>

class MariaDBDataTransfer {
public:
  MariaDBDataTransfer() = default;
  ~MariaDBDataTransfer() = default;

  void transferData(MYSQL *mariadbConn, pqxx::connection &pgConn,
                    const TableInfo &table);
  void processFullLoad(MYSQL *mariadbConn, pqxx::connection &pgConn,
                       const TableInfo &table);
  void processIncrementalUpdates(MYSQL *mariadbConn, pqxx::connection &pgConn,
                                 const TableInfo &table);
  void processDeletes(MYSQL *mariadbConn, pqxx::connection &pgConn,
                      const TableInfo &table);
  void updateTableStatus(pqxx::connection &pgConn,
                         const std::string &schema_name,
                         const std::string &table_name,
                         const std::string &status, size_t offset = 0);

private:
  void performBulkUpsert(pqxx::connection &pgConn,
                         const std::vector<std::vector<std::string>> &results,
                         const std::vector<std::string> &columnNames,
                         const std::vector<std::string> &columnTypes,
                         const std::string &lowerSchemaName,
                         const std::string &tableName);
  void performBulkInsert(pqxx::connection &pgConn,
                         const std::vector<std::vector<std::string>> &results,
                         const std::vector<std::string> &columnNames,
                         const std::vector<std::string> &columnTypes,
                         const std::string &lowerSchemaName,
                         const std::string &tableName);
  std::string buildUpsertQuery(const std::vector<std::string> &columnNames,
                               const std::vector<std::string> &pkColumns,
                               const std::string &schemaName,
                               const std::string &tableName);
  std::string
  buildUpsertConflictClause(const std::vector<std::string> &columnNames,
                            const std::vector<std::string> &pkColumns);
  std::vector<std::string>
  getPrimaryKeyColumnsFromPostgres(pqxx::connection &pgConn,
                                   const std::string &schemaName,
                                   const std::string &tableName);
  std::string escapeSQL(const std::string &value);
};

#endif // MARIADBDATATRANSFER_H
