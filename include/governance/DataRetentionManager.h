#ifndef DATA_RETENTION_MANAGER_H
#define DATA_RETENTION_MANAGER_H

#include <pqxx/pqxx>
#include <string>
#include <vector>

struct RetentionJob {
  int id;
  std::string schema_name;
  std::string table_name;
  std::string job_type;
  std::string retention_policy;
  std::string scheduled_date;
  std::string status;
  long long rows_affected;
  std::string error_message;
};

class DataRetentionManager {
private:
  std::string connectionString_;

  bool isLegalHold(const std::string &schemaName, const std::string &tableName);
  std::string calculateExpirationDate(const std::string &retentionPolicy);
  bool archiveData(const std::string &schemaName, const std::string &tableName,
                   const std::string &archivalLocation);
  bool deleteExpiredData(const std::string &schemaName,
                         const std::string &tableName,
                         const std::string &expirationDate);

public:
  explicit DataRetentionManager(const std::string &connectionString);
  ~DataRetentionManager() = default;

  void scheduleRetentionJobs();
  bool executeRetentionJob(int jobId);
  std::vector<RetentionJob> getPendingJobs();
  std::vector<RetentionJob> getJobsByStatus(const std::string &status);

  bool enforceRetentionPolicy(const std::string &schemaName,
                              const std::string &tableName);
  bool setLegalHold(const std::string &schemaName, const std::string &tableName,
                    const std::string &reason, const std::string &holdUntil);
  bool releaseLegalHold(const std::string &schemaName,
                        const std::string &tableName);

  bool archiveTable(const std::string &schemaName, const std::string &tableName,
                    const std::string &archivalLocation);

  void processExpiredData();
};

#endif
