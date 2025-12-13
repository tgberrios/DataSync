#ifndef DATA_GOVERNANCE_ORACLE_H
#define DATA_GOVERNANCE_ORACLE_H

#include "engines/oracle_engine.h"
#include <memory>
#include <mutex>
#include <string>
#include <vector>

struct OracleGovernanceData {
  std::string server_name;
  std::string schema_name;
  std::string table_name;
  std::string index_name;
  std::string index_columns;
  bool index_unique = false;
  std::string index_type;
  long long row_count = 0;
  double table_size_mb = 0.0;
  double index_size_mb = 0.0;
  double total_size_mb = 0.0;
  double data_free_mb = 0.0;
  double fragmentation_pct = 0.0;
  std::string tablespace_name;
  std::string version;
  int block_size = 0;
  long long num_rows = 0;
  long long blocks = 0;
  long long empty_blocks = 0;
  long long avg_row_len = 0;
  long long chain_cnt = 0;
  long long avg_space = 0;
  std::string compression;
  std::string logging;
  std::string partitioned;
  std::string iot_type;
  std::string temporary;
  std::string access_frequency;
  std::string health_status;
  std::string recommendation_summary;
  double health_score = 0.0;
};

class DataGovernanceOracle {
private:
  std::string connectionString_;
  std::vector<OracleGovernanceData> governanceData_;
  mutable std::mutex governanceDataMutex_;

  std::string extractServerName(const std::string &connectionString);
  std::string extractSchemaName(const std::string &connectionString);
  std::string escapeSQL(const std::string &str);
  std::vector<std::vector<std::string>> executeQuery(OCIConnection *conn,
                                                     const std::string &query);

public:
  explicit DataGovernanceOracle(const std::string &connectionString);
  ~DataGovernanceOracle();

  void collectGovernanceData();
  void storeGovernanceData();
  void generateReport();

private:
  void queryTableStats();
  void queryIndexStats();
  void queryServerConfig();
  void calculateHealthScores();
};

#endif
