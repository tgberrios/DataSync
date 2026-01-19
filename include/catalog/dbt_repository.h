#ifndef DBT_REPOSITORY_H
#define DBT_REPOSITORY_H

#include "third_party/json.hpp"
#include <pqxx/pqxx>
#include <string>
#include <vector>

using json = nlohmann::json;

enum class MaterializationType {
  TABLE,
  VIEW,
  INCREMENTAL,
  EPHEMERAL
};

enum class TestType {
  NOT_NULL,
  UNIQUE,
  RELATIONSHIPS,
  ACCEPTED_VALUES,
  CUSTOM,
  EXPRESSION
};

enum class TestSeverity {
  ERROR,
  WARN
};

struct DBTColumn {
  std::string name;
  std::string data_type;
  std::string description;
  json tests;
  json metadata;
};

struct DBTTest {
  int id;
  std::string test_name;
  std::string model_name;
  TestType test_type;
  std::string column_name;
  json test_config;
  std::string test_sql;
  std::string description;
  TestSeverity severity;
  bool active;
  std::string created_at;
  std::string updated_at;
};

struct DBTTestResult {
  int64_t id;
  std::string test_name;
  std::string model_name;
  TestType test_type;
  std::string status;
  std::string error_message;
  int rows_affected;
  double execution_time_seconds;
  json test_result;
  std::string run_id;
  std::string created_at;
};

struct DBTDocumentation {
  int id;
  std::string model_name;
  std::string doc_type;
  std::string doc_key;
  std::string doc_content;
  std::string doc_format;
  std::string created_at;
  std::string updated_at;
};

struct DBTLineage {
  int id;
  std::string source_model;
  std::string target_model;
  std::string source_column;
  std::string target_column;
  std::string transformation_type;
  std::string transformation_sql;
  std::string created_at;
};

struct DBTMacro {
  int id;
  std::string macro_name;
  std::string macro_sql;
  json parameters;
  std::string description;
  std::string return_type;
  std::string examples;
  std::vector<std::string> tags;
  bool active;
  std::string created_at;
  std::string updated_at;
};

struct DBTSource {
  int id;
  std::string source_name;
  std::string source_type;
  std::string database_name;
  std::string schema_name;
  std::string table_name;
  std::string connection_string;
  std::string description;
  std::vector<DBTColumn> columns;
  json freshness_config;
  json metadata;
  bool active;
  std::string created_at;
  std::string updated_at;
};

struct DBTModel {
  int id;
  std::string model_name;
  std::string model_type;
  MaterializationType materialization;
  std::string schema_name;
  std::string database_name;
  std::string sql_content;
  json config;
  std::string description;
  std::vector<std::string> tags;
  std::vector<std::string> depends_on;
  std::vector<DBTColumn> columns;
  std::vector<DBTTest> tests;
  std::string documentation;
  json metadata;
  int version;
  std::string git_commit_hash;
  std::string git_branch;
  bool active;
  std::string created_at;
  std::string updated_at;
  std::string last_run_time;
  std::string last_run_status;
  int last_run_rows;
};

struct DBTModelRun {
  int64_t id;
  std::string model_name;
  std::string run_id;
  std::string status;
  MaterializationType materialization;
  std::string start_time;
  std::string end_time;
  double duration_seconds;
  int rows_affected;
  std::string error_message;
  std::string compiled_sql;
  std::string executed_sql;
  json metadata;
  std::string created_at;
};

class DBTRepository {
  std::string connectionString_;

public:
  explicit DBTRepository(std::string connectionString);

  void createTables();
  std::vector<DBTModel> getAllModels();
  std::vector<DBTModel> getActiveModels();
  DBTModel getModel(const std::string &modelName);
  void insertOrUpdateModel(const DBTModel &model);
  void deleteModel(const std::string &modelName);
  void updateModelActive(const std::string &modelName, bool active);
  void updateModelRunStatus(const std::string &modelName,
                           const std::string &runTime,
                           const std::string &status,
                           int rowsAffected);

  std::vector<DBTTest> getModelTests(const std::string &modelName);
  void insertOrUpdateTest(const DBTTest &test);
  void deleteTest(const std::string &testName, const std::string &modelName);

  int64_t createTestResult(const DBTTestResult &result);
  std::vector<DBTTestResult> getTestResults(const std::string &modelName,
                                            const std::string &runId = "");

  std::vector<DBTDocumentation> getModelDocumentation(const std::string &modelName);
  void insertOrUpdateDocumentation(const DBTDocumentation &doc);
  void deleteDocumentation(const std::string &modelName, const std::string &docType,
                          const std::string &docKey);

  std::vector<DBTLineage> getModelLineage(const std::string &modelName);
  void insertOrUpdateLineage(const DBTLineage &lineage);

  std::vector<DBTMacro> getAllMacros();
  DBTMacro getMacro(const std::string &macroName);
  void insertOrUpdateMacro(const DBTMacro &macro);
  void deleteMacro(const std::string &macroName);

  std::vector<DBTSource> getAllSources();
  DBTSource getSource(const std::string &sourceName, const std::string &schemaName,
                      const std::string &tableName);
  void insertOrUpdateSource(const DBTSource &source);
  void deleteSource(const std::string &sourceName, const std::string &schemaName,
                    const std::string &tableName);

  std::vector<DBTModelRun> getModelRuns(const std::string &modelName, int limit = 50);
  int64_t createModelRun(const DBTModelRun &run);
  void updateModelRun(const DBTModelRun &run);

  std::string materializationToString(MaterializationType type);
  MaterializationType stringToMaterialization(const std::string &str);
  std::string testTypeToString(TestType type);
  TestType stringToTestType(const std::string &str);
  std::string testSeverityToString(TestSeverity severity);
  TestSeverity stringToTestSeverity(const std::string &str);

private:
  pqxx::connection getConnection();
  DBTModel rowToModel(const pqxx::row &row);
  DBTTest rowToTest(const pqxx::row &row);
  DBTTestResult rowToTestResult(const pqxx::row &row);
  DBTDocumentation rowToDocumentation(const pqxx::row &row);
  DBTLineage rowToLineage(const pqxx::row &row);
  DBTMacro rowToMacro(const pqxx::row &row);
  DBTSource rowToSource(const pqxx::row &row);
  DBTModelRun rowToModelRun(const pqxx::row &row);
  std::vector<DBTColumn> parseColumns(const json &j);
  std::vector<DBTTest> parseTests(const json &j);
  json columnsToJson(const std::vector<DBTColumn> &columns);
  json testsToJson(const std::vector<DBTTest> &tests);
};

#endif
