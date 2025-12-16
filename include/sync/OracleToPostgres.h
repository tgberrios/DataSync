#ifndef ORACLETOPOSTGRES_H
#define ORACLETOPOSTGRES_H

#include "catalog/catalog_manager.h"
#include "engines/database_engine.h"
#include "engines/oracle_engine.h"
#include "sync/DatabaseToPostgresSync.h"
#include "sync/ICDCHandler.h"
#include "sync/TableProcessorThreadPool.h"
#include <algorithm>
#include <cctype>
#include <chrono>
#include <oci.h>
#include <pqxx/pqxx>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

using namespace ParallelProcessing;

class OracleToPostgres : public DatabaseToPostgresSync, public ICDCHandler {
public:
  OracleToPostgres() = default;
  ~OracleToPostgres() { shutdownParallelProcessing(); }

  static std::unordered_map<std::string, std::string> dataTypeMap;

  std::string cleanValueForPostgres(const std::string &value,
                                    const std::string &columnType) override;

  std::unique_ptr<OCIConnection>
  getOracleConnection(const std::string &connectionString);

  std::vector<TableInfo> getActiveTables(pqxx::connection &pgConn);

  void setupTableTargetOracleToPostgres();
  void transferDataOracleToPostgres();
  void transferDataOracleToPostgresParallel();

  void processTableParallel(const TableInfo &table, pqxx::connection &pgConn);
  void processTableCDC(const DatabaseToPostgresSync::TableInfo &table,
                       pqxx::connection &pgConn) override;

  bool supportsCDC() const override { return true; }
  std::string getCDCMechanism() const override {
    return "Change Log Table (ds_change_log)";
  }

private:
  std::vector<std::vector<std::string>>
  executeQueryOracle(OCIConnection *conn, const std::string &query);

  void updateStatus(pqxx::connection &pgConn, const std::string &schema_name,
                    const std::string &table_name, const std::string &status,
                    size_t rowCount = 0);

  std::vector<std::string> getPrimaryKeyColumns(OCIConnection *conn,
                                                const std::string &schema_name,
                                                const std::string &table_name);

private:
  // Helper function to safely escape and validate Oracle identifiers/values
  static std::string escapeOracleValue(const std::string &value);
  static bool isValidOracleIdentifier(const std::string &identifier);
};

#endif
