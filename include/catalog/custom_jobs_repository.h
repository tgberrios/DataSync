#ifndef CUSTOM_JOBS_REPOSITORY_H
#define CUSTOM_JOBS_REPOSITORY_H

#include "third_party/json.hpp"
#include <pqxx/pqxx>
#include <string>
#include <vector>

using json = nlohmann::json;

struct CustomJob {
  int id;
  std::string job_name;
  std::string description;
  std::string source_db_engine;
  std::string source_connection_string;
  std::string query_sql;
  std::string target_db_engine;
  std::string target_connection_string;
  std::string target_schema;
  std::string target_table;
  std::string schedule_cron;
  bool active;
  bool enabled;
  json transform_config;
  json metadata;
  std::string created_at;
  std::string updated_at;
};

class CustomJobsRepository {
  std::string connectionString_;

public:
  explicit CustomJobsRepository(std::string connectionString);

  void createCustomJobsTable();
  void createJobResultsTable();
  std::vector<CustomJob> getActiveJobs();
  std::vector<CustomJob> getScheduledJobs();
  CustomJob getJob(const std::string &jobName);
  void insertOrUpdateJob(const CustomJob &job);
  void deleteJob(const std::string &jobName);
  void updateJobActive(const std::string &jobName, bool active);

private:
  pqxx::connection getConnection();
  CustomJob rowToJob(const pqxx::row &row);
};

#endif
