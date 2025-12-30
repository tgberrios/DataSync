#ifndef DB2TOPOSTGRES_H
#define DB2TOPOSTGRES_H

#include "catalog/catalog_manager.h"
#include "engines/database_engine.h"
#include "sync/DatabaseToPostgresSync.h"
#include "sync/ICDCHandler.h"
#include "sync/SchemaSync.h"
#include "sync/TableProcessorThreadPool.h"
#include "third_party/json.hpp"
#include <algorithm>
#include <cctype>
#include <chrono>
#include <pqxx/pqxx>
#include <set>
#include <sql.h>
#include <sqlext.h>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

using json = nlohmann::json;
using namespace ParallelProcessing;

class DB2ToPostgres : public DatabaseToPostgresSync, public ICDCHandler {
public:
  DB2ToPostgres() = default;
  ~DB2ToPostgres() { shutdownParallelProcessing(); }

  static std::unordered_map<std::string, std::string> dataTypeMap;

  std::string cleanValueForPostgres(const std::string &value,
                                    const std::string &columnType) override;

  void processTableCDC(const DatabaseToPostgresSync::TableInfo &table,
                       pqxx::connection &pgConn) override;

  bool supportsCDC() const override { return true; }
  std::string getCDCMechanism() const override {
    return "Change Log Table (ds_change_log)";
  }

  SQLHDBC getDB2Connection(const std::string &connectionString);
  void closeDB2Connection(SQLHDBC conn);
  void setupTableTargetDB2ToPostgres();
  void transferDataDB2ToPostgres();
  void transferDataDB2ToPostgresParallel();

private:
  void processTableParallelWithConnection(const TableInfo &table);
  void processTableParallel(const TableInfo &table, pqxx::connection &pgConn);
  void processTableCDCInternal(const DatabaseToPostgresSync::TableInfo &table,
                               pqxx::connection &pgConn, SQLHDBC db2Conn);
  void processTableFullLoad(const DatabaseToPostgresSync::TableInfo &table,
                            pqxx::connection &pgConn, SQLHDBC db2Conn);
  std::string mapDB2DataTypeToPostgres(const std::string &db2Type);
  void createChangeLogTable(SQLHDBC db2Conn, const std::string &schema,
                            const std::string &table);
  void createChangeLogTriggers(SQLHDBC db2Conn, const std::string &schema,
                               const std::string &table);
  std::vector<std::vector<std::string>>
  executeQueryDB2(SQLHDBC dbc, const std::string &query);
  std::string extractDatabaseName(const std::string &connectionString);
  std::string escapeSQL(const std::string &value);
};

#endif
