#ifndef DBT_EXECUTOR_H
#define DBT_EXECUTOR_H

#include "catalog/dbt_repository.h"
#include "core/logger.h"
#include "engines/postgres_engine.h"
#include "third_party/json.hpp"
#include <string>
#include <vector>
#include <memory>

using json = nlohmann::json;

class DBTExecutor {
  std::string metadataConnectionString_;
  std::unique_ptr<DBTRepository> dbtRepo_;

  std::string compileSQL(const DBTModel &model);
  std::string expandMacros(const std::string &sql);
  std::string replaceMacroCalls(const std::string &sql, const std::vector<DBTMacro> &macros);
  
  void executeTableMaterialization(const DBTModel &model, const std::string &compiledSQL);
  void executeViewMaterialization(const DBTModel &model, const std::string &compiledSQL);
  void executeIncrementalMaterialization(const DBTModel &model, const std::string &compiledSQL);
  
  std::vector<DBTTestResult> runTests(const std::string &modelName, const std::string &runId);
  DBTTestResult runTest(const DBTTest &test, const std::string &modelName, const std::string &runId);
  std::string generateTestSQL(const DBTTest &test, const DBTModel &model);
  
  void extractLineage(const DBTModel &model, const std::string &compiledSQL);
  std::string getConnectionString(const std::string &schemaName, const std::string &databaseName = "");
  std::string getGitCommitHash();
  std::string getGitBranch();

public:
  explicit DBTExecutor(std::string metadataConnectionString);
  
  void executeModel(const std::string &modelName);
  void executeModel(const DBTModel &model);
  void runAllTests(const std::string &modelName);
  void runAllTests();
  
  std::string compileModel(const std::string &modelName);
  std::vector<DBTTestResult> getTestResults(const std::string &modelName, const std::string &runId = "");
};

#endif
