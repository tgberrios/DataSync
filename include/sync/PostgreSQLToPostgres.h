#ifndef POSTGRESQLTOPOSTGRES_H
#define POSTGRESQLTOPOSTGRES_H

#include "catalog/catalog_manager.h"
#include "engines/database_engine.h"
#include "sync/DatabaseToPostgresSync.h"
#include "sync/ICDCHandler.h"
#include "sync/SchemaSync.h"
#include "sync/TableProcessorThreadPool.h"
#include <memory>
#include <pqxx/pqxx>
#include <string>
#include <vector>

using json = nlohmann::json;
using namespace ParallelProcessing;

class PostgreSQLToPostgres : public DatabaseToPostgresSync, public ICDCHandler {
public:
  PostgreSQLToPostgres() = default;
  ~PostgreSQLToPostgres() { shutdownParallelProcessing(); }

  std::string cleanValueForPostgres(const std::string &value,
                                    const std::string &columnType) override;

  void processTableCDC(const DatabaseToPostgresSync::TableInfo &table,
                       pqxx::connection &pgConn) override;

  bool supportsCDC() const override { return true; }
  std::string getCDCMechanism() const override {
    return "Change Log Table (ds_change_log)";
  }

  std::vector<TableInfo> getActiveTables(pqxx::connection &pgConn);

  std::unique_ptr<pqxx::connection>
  getPostgreSQLConnection(const std::string &connectionString);

  void setupTableTargetPostgreSQLToPostgres();
  void transferDataPostgreSQLToPostgres();
  void transferDataPostgreSQLToPostgresParallel();
  void processTableParallelWithConnection(const TableInfo &table);
  void processTableParallel(const TableInfo &table, pqxx::connection &pgConn);
  void dataFetcherThread(const std::string &tableKey,
                         pqxx::connection *sourceConn, const TableInfo &table,
                         const std::vector<std::string> &columnNames,
                         const std::vector<std::string> &columnTypes);

private:
  void processTableCDC(const std::string &tableKey,
                       pqxx::connection *sourceConn, const TableInfo &table,
                       pqxx::connection &pgConn,
                       const std::vector<std::string> &columnNames,
                       const std::vector<std::string> &columnTypes);

  std::vector<std::string> getPrimaryKeyColumns(pqxx::connection *conn,
                                                const std::string &schema_name,
                                                const std::string &table_name);

  std::vector<std::vector<std::string>>
  executeQueryPostgreSQL(pqxx::connection *conn, const std::string &query);

  std::string escapeSQL(const std::string &value);
  void updateStatus(pqxx::connection &pgConn, const std::string &schema_name,
                    const std::string &table_name, const std::string &status,
                    size_t rowCount = 0);
};

#endif
