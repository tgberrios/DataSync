#ifndef DATA_GOVERNANCE_MARIADB_H
#define DATA_GOVERNANCE_MARIADB_H

#include <string>
#include <vector>
#include <memory>
#include <mysql/mysql.h>

struct MariaDBGovernanceData {
  std::string server_name;
  std::string database_name;
  std::string schema_name;
  std::string table_name;
  std::string index_name;
  std::string index_columns;
  bool index_non_unique = false;
  std::string index_type;
  long long row_count = 0;
  double data_size_mb = 0.0;
  double index_size_mb = 0.0;
  double total_size_mb = 0.0;
  double data_free_mb = 0.0;
  double fragmentation_pct = 0.0;
  std::string engine;
  std::string version;
  std::string innodb_version;
  int innodb_page_size = 0;
  bool innodb_file_per_table = false;
  int innodb_flush_log_at_trx_commit = 0;
  int sync_binlog = 0;
  long long table_reads = 0;
  long long table_writes = 0;
  long long index_reads = 0;
  int user_total = 0;
  int user_super_count = 0;
  int user_locked_count = 0;
  int user_expired_count = 0;
  std::string access_frequency;
  std::string health_status;
  std::string recommendation_summary;
  double health_score = 0.0;
};

class DataGovernanceMariaDB {
private:
  std::string connectionString_;
  std::vector<MariaDBGovernanceData> governanceData_;

  std::string extractServerName(const std::string &connectionString);
  std::string extractDatabaseName(const std::string &connectionString);
  std::string escapeSQL(MYSQL *conn, const std::string &str);
  std::vector<std::vector<std::string>> executeQuery(MYSQL *conn, const std::string &query);

public:
  explicit DataGovernanceMariaDB(const std::string &connectionString);
  ~DataGovernanceMariaDB();

  void collectGovernanceData();
  void storeGovernanceData();
  void generateReport();

private:
  void queryTableStats();
  void queryIndexStats();
  void queryUserInfo();
  void queryServerConfig();
  void calculateHealthScores();
};

#endif
