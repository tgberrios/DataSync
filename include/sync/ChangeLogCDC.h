#ifndef CHANGELOGCDC_H
#define CHANGELOGCDC_H

#include "sync/DatabaseToPostgresSync.h"
#include "sync/ICDCHandler.h"
#include "third_party/json.hpp"
#include <mutex>
#include <string>
#include <vector>

using json = nlohmann::json;

class ChangeLogCDC : public DatabaseToPostgresSync, public ICDCHandler {
protected:
  struct ChangeLogEntry {
    long long change_id;
    char operation;
    json pk_values;
    json row_data;
  };

  virtual std::string getChangeLogTableName() const = 0;
  virtual std::string getDatabaseName() const = 0;
  virtual std::vector<std::vector<std::string>>
  executeChangeLogQuery(const std::string &query) = 0;
  virtual std::vector<std::vector<std::string>>
  executeSourceQuery(const std::string &query) = 0;
  virtual std::string escapeIdentifier(const std::string &name) = 0;
  virtual std::string escapeSQL(const std::string &value) = 0;

  long long getLastChangeId(pqxx::connection &pgConn, const TableInfo &table,
                            const std::string &dbEngine);

  void processChangeLogBatch(pqxx::connection &pgConn, const TableInfo &table,
                             const std::vector<ChangeLogEntry> &changes,
                             const std::vector<std::string> &columnNames,
                             const std::vector<std::string> &columnTypes,
                             const std::string &dbEngine);

  void updateLastChangeId(pqxx::connection &pgConn, const TableInfo &table,
                          long long changeId, const std::string &dbEngine);

public:
  bool supportsCDC() const override { return true; }
  std::string getCDCMechanism() const override {
    return "Change Log Table (ds_change_log)";
  }
};

#endif
