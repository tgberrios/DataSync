#ifndef MAINTENANCE_MANAGER_H
#define MAINTENANCE_MANAGER_H

#include "third_party/json.hpp"
#include <chrono>
#include <memory>
#include <mysql/mysql.h>
#include <pqxx/pqxx>
#include <sql.h>
#include <sqlext.h>
#include <string>
#include <vector>

using json = nlohmann::json;

struct MaintenanceTask {
  int id = 0;
  std::string maintenance_type;
  std::string db_engine;
  std::string connection_string;
  std::string schema_name;
  std::string object_name;
  std::string object_type;
  bool auto_execute = true;
  bool enabled = true;
  int priority = 5;
  std::string status;
  std::chrono::system_clock::time_point next_maintenance_date;
  json thresholds;
  json metrics_before;
  json metrics_after;
  double space_reclaimed_mb = 0.0;
  double performance_improvement_pct = 0.0;
  double fragmentation_before = 0.0;
  double fragmentation_after = 0.0;
  long long dead_tuples_before = 0;
  long long dead_tuples_after = 0;
  double index_size_before_mb = 0.0;
  double index_size_after_mb = 0.0;
  double table_size_before_mb = 0.0;
  double table_size_after_mb = 0.0;
  std::string server_name;
  std::string database_name;
};

struct MaintenanceMetrics {
  double fragmentation_pct = 0.0;
  long long dead_tuples = 0;
  long long live_tuples = 0;
  double table_size_mb = 0.0;
  double index_size_mb = 0.0;
  double free_space_mb = 0.0;
  std::string last_vacuum;
  std::string last_analyze;
  std::string last_maintenance;
  long long page_count = 0;
  double avg_page_density = 0.0;
  double query_performance_score = 0.0;
};

class MaintenanceManager {
private:
  std::string metadataConnectionString_;
  json defaultThresholds_;

  void detectPostgreSQLMaintenance(const std::string &connStr);
  void detectMariaDBMaintenance(const std::string &connStr);
  void detectMSSQLMaintenance(const std::string &connStr);

  void detectVacuumNeeds(pqxx::connection &conn, const std::string &connStr);
  void detectAnalyzeNeeds(pqxx::connection &conn, const std::string &connStr);
  void detectReindexNeeds(pqxx::connection &conn, const std::string &connStr);

  void detectOptimizeNeeds(MYSQL *conn, const std::string &connStr);
  void detectAnalyzeTableNeeds(MYSQL *conn, const std::string &connStr);

  void detectUpdateStatisticsNeeds(SQLHDBC conn, const std::string &connStr);
  void detectRebuildIndexNeeds(SQLHDBC conn, const std::string &connStr);
  void detectReorganizeIndexNeeds(SQLHDBC conn, const std::string &connStr);

  void executePostgreSQLMaintenance(const MaintenanceTask &task);
  void executeMariaDBMaintenance(const MaintenanceTask &task);
  void executeMSSQLMaintenance(const MaintenanceTask &task);

  MaintenanceMetrics collectMetricsBefore(const MaintenanceTask &task);
  MaintenanceMetrics collectMetricsAfter(const MaintenanceTask &task);
  void calculateImpact(MaintenanceTask &task, const MaintenanceMetrics &before,
                       const MaintenanceMetrics &after);

  int calculatePriority(const MaintenanceMetrics &metrics,
                        const std::string &maintenanceType);
  void storeTask(const MaintenanceTask &task);
  void updateTaskStatus(int taskId, const std::string &status,
                        const std::string &resultMessage = "",
                        const std::string &errorDetails = "");
  void storeExecutionMetrics(const MaintenanceTask &task,
                             const MaintenanceMetrics &before,
                             const MaintenanceMetrics &after);

  std::vector<MaintenanceTask> getPendingTasks(int limit = 5);
  std::chrono::system_clock::time_point
  calculateNextMaintenanceDate(const std::string &maintenanceType);

  json getThresholds(const std::string &dbEngine,
                     const std::string &maintenanceType);
  void loadThresholdsFromDatabase();
  std::string escapeSQL(pqxx::connection &conn, const std::string &str);
  std::string escapeSQL(MYSQL *conn, const std::string &str);
  std::string escapeSQLMSSQL(const std::string &str);

  bool validatePostgreSQLObject(const MaintenanceTask &task);
  bool validateMariaDBObject(const MaintenanceTask &task);
  bool validateMSSQLObject(const MaintenanceTask &task);
  void cleanupNonExistentTask(int taskId, const std::string &reason);

public:
  explicit MaintenanceManager(const std::string &metadataConnectionString);
  ~MaintenanceManager();

  void detectMaintenanceNeeds();
  void executeMaintenance();
  void executeManual(int maintenanceId);
  void generateReport();
};

#endif
