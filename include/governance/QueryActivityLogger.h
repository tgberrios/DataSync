#ifndef QUERY_ACTIVITY_LOGGER_H
#define QUERY_ACTIVITY_LOGGER_H

#include <string>
#include <vector>
#include <memory>
#include <pqxx/pqxx>

struct QueryActivity {
  int pid = 0;
  std::string dbname;
  std::string username;
  std::string application_name;
  std::string client_addr;
  std::string state;
  std::string wait_event_type;
  std::string wait_event;
  std::string query_text;
  double query_duration_ms = 0.0;
  std::string operation_type;
  bool plan_available = false;
  double estimated_cost = 0.0;
  long long estimated_rows = 0;
  std::string execution_plan_hash;
  std::string query_category;
  double query_efficiency_score = 0.0;
  bool is_long_running = false;
  bool is_blocking = false;
};

class QueryActivityLogger {
private:
  std::string connectionString_;
  std::vector<QueryActivity> activities_;

  void queryPgStatActivity(pqxx::connection &conn);
  void extractQueryInfo(QueryActivity &activity);
  void categorizeQueries();
  void calculateMetrics(QueryActivity &activity);
  std::string categorizeQuery(const std::string &queryText);
  std::string extractOperationType(const std::string &queryText);

public:
  explicit QueryActivityLogger(const std::string &connectionString);
  ~QueryActivityLogger();

  void logActiveQueries();
  void storeActivityLog();
  void analyzeActivity();
};

#endif
