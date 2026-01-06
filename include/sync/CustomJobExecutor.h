#ifndef CUSTOM_JOB_EXECUTOR_H
#define CUSTOM_JOB_EXECUTOR_H

#include "catalog/custom_jobs_repository.h"
#include "core/logger.h"
#include "engines/mariadb_engine.h"
#include "engines/mongodb_engine.h"
#include "engines/mssql_engine.h"
#ifdef HAVE_ORACLE
#include "engines/oracle_engine.h"
#endif
#include "engines/postgres_engine.h"
#include "third_party/json.hpp"
#include <bson/bson.h>
#include <mongoc/mongoc.h>
#include <pqxx/pqxx>
#include <string>
#include <vector>

using json = nlohmann::json;

class CustomJobExecutor {
  std::string metadataConnectionString_;
  std::unique_ptr<CustomJobsRepository> jobsRepo_;

  std::vector<json> executeQueryPostgreSQL(const std::string &connectionString,
                                           const std::string &query);
  std::vector<json> executeQueryMariaDB(const std::string &connectionString,
                                        const std::string &query);
  std::vector<json> executeQueryMSSQL(const std::string &connectionString,
                                      const std::string &query);
  std::vector<json> executeQueryOracle(const std::string &connectionString,
                                       const std::string &query);
  std::vector<json> executeQueryMongoDB(const std::string &connectionString,
                                        const std::string &query);
  std::vector<json> executePythonScript(const std::string &script);

  std::vector<json> transformData(const std::vector<json> &data,
                                  const json &transformConfig);

  void insertDataToPostgreSQL(const CustomJob &job,
                              const std::vector<json> &data);
  void createPostgreSQLTable(const CustomJob &job,
                             const std::vector<std::string> &columns);
  void insertDataToMariaDB(const CustomJob &job, const std::vector<json> &data);
  void createMariaDBTable(const CustomJob &job,
                          const std::vector<std::string> &columns);
  void insertDataToMSSQL(const CustomJob &job, const std::vector<json> &data);
  void createMSSQLTable(const CustomJob &job,
                        const std::vector<std::string> &columns);
  void insertDataToOracle(const CustomJob &job, const std::vector<json> &data);
  void createOracleTable(const CustomJob &job,
                         const std::vector<std::string> &columns);
  void insertDataToMongoDB(const CustomJob &job, const std::vector<json> &data);
  void createMongoDBCollection(const CustomJob &job,
                               const std::vector<std::string> &columns);

  void saveJobResult(const std::string &jobName, int64_t processLogId,
                     int64_t rowCount, const std::vector<json> &sample);
  int64_t logToProcessLog(const std::string &jobName, const std::string &status,
                          int64_t totalRowsProcessed,
                          const std::string &errorMessage,
                          const json &metadata);

  std::vector<std::string> detectColumns(const std::vector<json> &data);

public:
  explicit CustomJobExecutor(std::string metadataConnectionString);
  ~CustomJobExecutor();

  CustomJobExecutor(const CustomJobExecutor &) = delete;
  CustomJobExecutor &operator=(const CustomJobExecutor &) = delete;

  CustomJobExecutor(CustomJobExecutor &&other) noexcept;
  CustomJobExecutor &operator=(CustomJobExecutor &&other) noexcept;

  void executeJob(const std::string &jobName);
  int64_t executeJobAndGetLogId(const std::string &jobName);
};

#endif
