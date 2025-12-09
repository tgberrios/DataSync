#ifndef DATA_GOVERNANCE_MSSQL_H
#define DATA_GOVERNANCE_MSSQL_H

#include <string>
#include <vector>
#include <memory>
#include <sql.h>

struct MSSQLGovernanceData {
  std::string server_name;
  std::string database_name;
  std::string schema_name;
  std::string table_name;
  std::string object_name;
  std::string object_type;
  std::string index_name;
  int index_id = 0;
  long long row_count = 0;
  double table_size_mb = 0.0;
  double fragmentation_pct = 0.0;
  long long page_count = 0;
  int fill_factor = 0;
  long long user_seeks = 0;
  long long user_scans = 0;
  long long user_lookups = 0;
  long long user_updates = 0;
  long long page_splits = 0;
  long long leaf_inserts = 0;
  std::string index_key_columns;
  std::string index_include_columns;
  bool has_missing_index = false;
  bool is_unused = false;
  bool is_potential_duplicate = false;
  std::string last_full_backup;
  std::string last_diff_backup;
  std::string last_log_backup;
  int compatibility_level = 0;
  std::string recovery_model;
  std::string page_verify;
  bool auto_create_stats = false;
  bool auto_update_stats = false;
  bool auto_update_stats_async = false;
  std::string data_autogrowth_desc;
  std::string log_autogrowth_desc;
  int maxdop = 0;
  int cost_threshold = 0;
  double datafile_size_mb = 0.0;
  double datafile_free_mb = 0.0;
  double log_size_mb = 0.0;
  int log_vlf_count = 0;
  std::string access_frequency;
  std::string health_status;
  std::string recommendation_summary;
  double health_score = 0.0;
  std::string missing_index_equality_columns;
  std::string missing_index_inequality_columns;
  std::string missing_index_included_columns;
  std::string missing_index_create_statement;
  double missing_index_avg_user_impact = 0.0;
  long long missing_index_user_seeks = 0;
  long long missing_index_user_scans = 0;
  double missing_index_avg_total_user_cost = 0.0;
  long long missing_index_unique_compiles = 0;
  long long object_id = 0;
  std::string sp_name;
  double avg_execution_time_seconds = 0.0;
  long long avg_logical_reads = 0;
  long long avg_physical_reads = 0;
  long long execution_count = 0;
  long long total_elapsed_time_ms = 0;
};

class DataGovernanceMSSQL {
private:
  std::string connectionString_;
  std::vector<MSSQLGovernanceData> governanceData_;

  std::string extractServerName(const std::string &connectionString);
  std::string extractDatabaseName(const std::string &connectionString);
  std::string escapeSQL(const std::string &str);
  std::vector<std::vector<std::string>> executeQueryMSSQL(SQLHDBC conn, const std::string &query);

public:
  explicit DataGovernanceMSSQL(const std::string &connectionString);
  ~DataGovernanceMSSQL();

  void collectGovernanceData();
  void storeGovernanceData();
  void generateReport();

private:
  void queryIndexPhysicalStats();
  void queryIndexUsageStats();
  void queryMissingIndexes();
  void queryBackupInfo();
  void queryDatabaseConfig();
  void queryStoredProcedures();
  void calculateHealthScores();
};

#endif
