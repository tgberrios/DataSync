#ifndef QUERY_STORE_COLLECTOR_H
#define QUERY_STORE_COLLECTOR_H

#include <string>
#include <vector>
#include <memory>
#include <pqxx/pqxx>

struct QuerySnapshot {
  std::string dbname;
  std::string username;
  long long queryid = 0;
  std::string query_text;
  long long calls = 0;
  double total_time_ms = 0.0;
  double mean_time_ms = 0.0;
  long long rows_returned = 0;
  long long shared_blks_hit = 0;
  long long shared_blks_read = 0;
  long long shared_blks_dirtied = 0;
  long long shared_blks_written = 0;
  long long local_blks_hit = 0;
  long long local_blks_read = 0;
  long long local_blks_dirtied = 0;
  long long local_blks_written = 0;
  long long temp_blks_read = 0;
  long long temp_blks_written = 0;
  double blk_read_time_ms = 0.0;
  double blk_write_time_ms = 0.0;
  long long wal_records = 0;
  long long wal_fpi = 0;
  double wal_bytes = 0.0;
  double min_time_ms = 0.0;
  double max_time_ms = 0.0;
  std::string operation_type;
  std::string query_fingerprint;
  int tables_count = 0;
  bool has_joins = false;
  bool has_subqueries = false;
  bool has_cte = false;
  bool has_window_functions = false;
  bool has_functions = false;
  std::string query_category;
  long long estimated_rows = 0;
  bool is_prepared = false;
  bool plan_available = false;
  double estimated_cost = 0.0;
  std::string execution_plan_hash;
  double cache_hit_ratio = 0.0;
  double io_efficiency = 0.0;
  double query_efficiency_score = 0.0;
};

class QueryStoreCollector {
private:
  std::string connectionString_;
  std::vector<QuerySnapshot> snapshots_;

  void queryPgStatStatements(pqxx::connection &conn);
  void parseQueryText(QuerySnapshot &snapshot);
  void extractQueryMetadata(QuerySnapshot &snapshot);
  void calculateMetrics(QuerySnapshot &snapshot);
  std::string categorizeQuery(const std::string &queryText);
  std::string extractOperationType(const std::string &queryText);
  std::string generateFingerprint(const std::string &queryText);

public:
  explicit QueryStoreCollector(const std::string &connectionString);
  ~QueryStoreCollector();

  void collectQuerySnapshots();
  void storeSnapshots();
  void analyzeQueries();
};

#endif
