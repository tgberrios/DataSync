#include "catalog/dbt_repository.h"
#include "core/logger.h"
#include <algorithm>
#include <stdexcept>
#include <sstream>
#include <cstdio>

DBTRepository::DBTRepository(std::string connectionString)
    : connectionString_(std::move(connectionString)) {}

pqxx::connection DBTRepository::getConnection() {
  return pqxx::connection(connectionString_);
}

void DBTRepository::createTables() {
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);
    
    txn.exec(
        "CREATE SCHEMA IF NOT EXISTS metadata;"
        "CREATE TABLE IF NOT EXISTS metadata.dbt_models ("
        "    id SERIAL PRIMARY KEY,"
        "    model_name VARCHAR(255) UNIQUE NOT NULL,"
        "    model_type VARCHAR(50) DEFAULT 'sql' CHECK (model_type IN ('sql', 'python', 'yaml')),"
        "    materialization VARCHAR(50) DEFAULT 'table' CHECK (materialization IN ('table', 'view', 'incremental', 'ephemeral')),"
        "    schema_name VARCHAR(100) NOT NULL,"
        "    database_name VARCHAR(100),"
        "    sql_content TEXT NOT NULL,"
        "    config JSONB DEFAULT '{}'::jsonb,"
        "    description TEXT,"
        "    tags TEXT[] DEFAULT ARRAY[]::TEXT[],"
        "    depends_on TEXT[] DEFAULT ARRAY[]::TEXT[],"
        "    columns JSONB DEFAULT '[]'::jsonb,"
        "    tests JSONB DEFAULT '[]'::jsonb,"
        "    documentation TEXT,"
        "    metadata JSONB DEFAULT '{}'::jsonb,"
        "    version INTEGER DEFAULT 1,"
        "    git_commit_hash VARCHAR(40),"
        "    git_branch VARCHAR(100),"
        "    active BOOLEAN DEFAULT true,"
        "    created_at TIMESTAMP DEFAULT NOW(),"
        "    updated_at TIMESTAMP DEFAULT NOW(),"
        "    last_run_time TIMESTAMP,"
        "    last_run_status VARCHAR(50),"
        "    last_run_rows INTEGER"
        ");"
        "CREATE TABLE IF NOT EXISTS metadata.dbt_tests ("
        "    id SERIAL PRIMARY KEY,"
        "    test_name VARCHAR(255) NOT NULL,"
        "    model_name VARCHAR(255) NOT NULL REFERENCES metadata.dbt_models(model_name) ON DELETE CASCADE,"
        "    test_type VARCHAR(50) NOT NULL CHECK (test_type IN ('not_null', 'unique', 'relationships', 'accepted_values', 'custom', 'expression')),"
        "    column_name VARCHAR(100),"
        "    test_config JSONB DEFAULT '{}'::jsonb,"
        "    test_sql TEXT,"
        "    description TEXT,"
        "    severity VARCHAR(20) DEFAULT 'error' CHECK (severity IN ('error', 'warn')),"
        "    active BOOLEAN DEFAULT true,"
        "    created_at TIMESTAMP DEFAULT NOW(),"
        "    updated_at TIMESTAMP DEFAULT NOW(),"
        "    CONSTRAINT uq_dbt_test UNIQUE (test_name, model_name)"
        ");"
        "CREATE TABLE IF NOT EXISTS metadata.dbt_test_results ("
        "    id BIGSERIAL PRIMARY KEY,"
        "    test_name VARCHAR(255) NOT NULL,"
        "    model_name VARCHAR(255) NOT NULL,"
        "    test_type VARCHAR(50) NOT NULL,"
        "    status VARCHAR(50) NOT NULL CHECK (status IN ('pass', 'fail', 'error', 'skipped')),"
        "    error_message TEXT,"
        "    rows_affected INTEGER,"
        "    execution_time_seconds NUMERIC(10, 3),"
        "    test_result JSONB DEFAULT '{}'::jsonb,"
        "    run_id VARCHAR(255),"
        "    created_at TIMESTAMP DEFAULT NOW()"
        ");"
        "CREATE TABLE IF NOT EXISTS metadata.dbt_documentation ("
        "    id SERIAL PRIMARY KEY,"
        "    model_name VARCHAR(255) NOT NULL REFERENCES metadata.dbt_models(model_name) ON DELETE CASCADE,"
        "    doc_type VARCHAR(50) DEFAULT 'model' CHECK (doc_type IN ('model', 'column', 'macro', 'source')),"
        "    doc_key VARCHAR(255) NOT NULL,"
        "    doc_content TEXT NOT NULL,"
        "    doc_format VARCHAR(20) DEFAULT 'markdown' CHECK (doc_format IN ('markdown', 'html', 'text')),"
        "    created_at TIMESTAMP DEFAULT NOW(),"
        "    updated_at TIMESTAMP DEFAULT NOW(),"
        "    CONSTRAINT uq_dbt_doc UNIQUE (model_name, doc_type, doc_key)"
        ");"
        "CREATE TABLE IF NOT EXISTS metadata.dbt_lineage ("
        "    id SERIAL PRIMARY KEY,"
        "    source_model VARCHAR(255) NOT NULL,"
        "    target_model VARCHAR(255) NOT NULL,"
        "    source_column VARCHAR(100),"
        "    target_column VARCHAR(100),"
        "    transformation_type VARCHAR(50),"
        "    transformation_sql TEXT,"
        "    created_at TIMESTAMP DEFAULT NOW(),"
        "    FOREIGN KEY (source_model) REFERENCES metadata.dbt_models(model_name) ON DELETE CASCADE,"
        "    FOREIGN KEY (target_model) REFERENCES metadata.dbt_models(model_name) ON DELETE CASCADE"
        ");"
        "CREATE TABLE IF NOT EXISTS metadata.dbt_macros ("
        "    id SERIAL PRIMARY KEY,"
        "    macro_name VARCHAR(255) UNIQUE NOT NULL,"
        "    macro_sql TEXT NOT NULL,"
        "    parameters JSONB DEFAULT '[]'::jsonb,"
        "    description TEXT,"
        "    return_type VARCHAR(50),"
        "    examples TEXT,"
        "    tags TEXT[] DEFAULT ARRAY[]::TEXT[],"
        "    active BOOLEAN DEFAULT true,"
        "    created_at TIMESTAMP DEFAULT NOW(),"
        "    updated_at TIMESTAMP DEFAULT NOW()"
        ");"
        "CREATE TABLE IF NOT EXISTS metadata.dbt_sources ("
        "    id SERIAL PRIMARY KEY,"
        "    source_name VARCHAR(255) NOT NULL,"
        "    source_type VARCHAR(50) NOT NULL CHECK (source_type IN ('table', 'view', 'query', 'api')),"
        "    database_name VARCHAR(100),"
        "    schema_name VARCHAR(100) NOT NULL,"
        "    table_name VARCHAR(100) NOT NULL,"
        "    connection_string TEXT,"
        "    description TEXT,"
        "    columns JSONB DEFAULT '[]'::jsonb,"
        "    freshness_config JSONB DEFAULT '{}'::jsonb,"
        "    metadata JSONB DEFAULT '{}'::jsonb,"
        "    active BOOLEAN DEFAULT true,"
        "    created_at TIMESTAMP DEFAULT NOW(),"
        "    updated_at TIMESTAMP DEFAULT NOW(),"
        "    CONSTRAINT uq_dbt_source UNIQUE (source_name, schema_name, table_name)"
        ");"
        "CREATE TABLE IF NOT EXISTS metadata.dbt_model_runs ("
        "    id BIGSERIAL PRIMARY KEY,"
        "    model_name VARCHAR(255) NOT NULL REFERENCES metadata.dbt_models(model_name) ON DELETE CASCADE,"
        "    run_id VARCHAR(255) NOT NULL,"
        "    status VARCHAR(50) NOT NULL CHECK (status IN ('pending', 'running', 'success', 'error', 'skipped')),"
        "    materialization VARCHAR(50),"
        "    start_time TIMESTAMP,"
        "    end_time TIMESTAMP,"
        "    duration_seconds NUMERIC(10, 3),"
        "    rows_affected INTEGER,"
        "    error_message TEXT,"
        "    compiled_sql TEXT,"
        "    executed_sql TEXT,"
        "    metadata JSONB DEFAULT '{}'::jsonb,"
        "    created_at TIMESTAMP DEFAULT NOW()"
        ");"
    );
    
    txn.exec(
        "CREATE INDEX IF NOT EXISTS idx_dbt_models_active ON metadata.dbt_models(active);"
        "CREATE INDEX IF NOT EXISTS idx_dbt_models_schema ON metadata.dbt_models(schema_name);"
        "CREATE INDEX IF NOT EXISTS idx_dbt_models_materialization ON metadata.dbt_models(materialization);"
        "CREATE INDEX IF NOT EXISTS idx_dbt_tests_model ON metadata.dbt_tests(model_name);"
        "CREATE INDEX IF NOT EXISTS idx_dbt_tests_active ON metadata.dbt_tests(active);"
        "CREATE INDEX IF NOT EXISTS idx_dbt_test_results_model ON metadata.dbt_test_results(model_name);"
        "CREATE INDEX IF NOT EXISTS idx_dbt_test_results_run ON metadata.dbt_test_results(run_id);"
        "CREATE INDEX IF NOT EXISTS idx_dbt_test_results_status ON metadata.dbt_test_results(status);"
        "CREATE INDEX IF NOT EXISTS idx_dbt_documentation_model ON metadata.dbt_documentation(model_name);"
        "CREATE INDEX IF NOT EXISTS idx_dbt_lineage_source ON metadata.dbt_lineage(source_model);"
        "CREATE INDEX IF NOT EXISTS idx_dbt_lineage_target ON metadata.dbt_lineage(target_model);"
        "CREATE INDEX IF NOT EXISTS idx_dbt_macros_active ON metadata.dbt_macros(active);"
        "CREATE INDEX IF NOT EXISTS idx_dbt_sources_active ON metadata.dbt_sources(active);"
        "CREATE INDEX IF NOT EXISTS idx_dbt_model_runs_model ON metadata.dbt_model_runs(model_name);"
        "CREATE INDEX IF NOT EXISTS idx_dbt_model_runs_run ON metadata.dbt_model_runs(run_id);"
        "CREATE INDEX IF NOT EXISTS idx_dbt_model_runs_status ON metadata.dbt_model_runs(status);"
    );
    
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "createTables",
                  "Error creating dbt tables: " + std::string(e.what()));
    throw;
  }
}

std::string DBTRepository::materializationToString(MaterializationType type) {
  switch (type) {
  case MaterializationType::TABLE:
    return "table";
  case MaterializationType::VIEW:
    return "view";
  case MaterializationType::INCREMENTAL:
    return "incremental";
  case MaterializationType::EPHEMERAL:
    return "ephemeral";
  default:
    return "table";
  }
}

MaterializationType DBTRepository::stringToMaterialization(const std::string &str) {
  if (str == "table")
    return MaterializationType::TABLE;
  if (str == "view")
    return MaterializationType::VIEW;
  if (str == "incremental")
    return MaterializationType::INCREMENTAL;
  if (str == "ephemeral")
    return MaterializationType::EPHEMERAL;
  return MaterializationType::TABLE;
}

std::string DBTRepository::testTypeToString(TestType type) {
  switch (type) {
  case TestType::NOT_NULL:
    return "not_null";
  case TestType::UNIQUE:
    return "unique";
  case TestType::RELATIONSHIPS:
    return "relationships";
  case TestType::ACCEPTED_VALUES:
    return "accepted_values";
  case TestType::CUSTOM:
    return "custom";
  case TestType::EXPRESSION:
    return "expression";
  default:
    return "custom";
  }
}

TestType DBTRepository::stringToTestType(const std::string &str) {
  if (str == "not_null")
    return TestType::NOT_NULL;
  if (str == "unique")
    return TestType::UNIQUE;
  if (str == "relationships")
    return TestType::RELATIONSHIPS;
  if (str == "accepted_values")
    return TestType::ACCEPTED_VALUES;
  if (str == "custom")
    return TestType::CUSTOM;
  if (str == "expression")
    return TestType::EXPRESSION;
  return TestType::CUSTOM;
}

std::string DBTRepository::testSeverityToString(TestSeverity severity) {
  switch (severity) {
  case TestSeverity::ERROR:
    return "error";
  case TestSeverity::WARN:
    return "warn";
  default:
    return "error";
  }
}

TestSeverity DBTRepository::stringToTestSeverity(const std::string &str) {
  if (str == "error")
    return TestSeverity::ERROR;
  if (str == "warn")
    return TestSeverity::WARN;
  return TestSeverity::ERROR;
}

std::vector<DBTColumn> DBTRepository::parseColumns(const json &j) {
  std::vector<DBTColumn> columns;
  if (j.is_array()) {
    for (const auto &col : j) {
      DBTColumn column;
      if (col.contains("name"))
        column.name = col["name"].get<std::string>();
      if (col.contains("data_type"))
        column.data_type = col["data_type"].get<std::string>();
      if (col.contains("description"))
        column.description = col["description"].get<std::string>();
      if (col.contains("tests"))
        column.tests = col["tests"];
      if (col.contains("metadata"))
        column.metadata = col["metadata"];
      columns.push_back(column);
    }
  }
  return columns;
}

std::vector<DBTTest> DBTRepository::parseTests(const json &j) {
  std::vector<DBTTest> tests;
  if (j.is_array()) {
    for (const auto &test : j) {
      DBTTest t;
      if (test.contains("test_name"))
        t.test_name = test["test_name"].get<std::string>();
      if (test.contains("test_type"))
        t.test_type = stringToTestType(test["test_type"].get<std::string>());
      if (test.contains("column_name"))
        t.column_name = test["column_name"].get<std::string>();
      if (test.contains("test_config"))
        t.test_config = test["test_config"];
      if (test.contains("test_sql"))
        t.test_sql = test["test_sql"].get<std::string>();
      if (test.contains("description"))
        t.description = test["description"].get<std::string>();
      if (test.contains("severity"))
        t.severity = stringToTestSeverity(test["severity"].get<std::string>());
      t.active = true;
      tests.push_back(t);
    }
  }
  return tests;
}

json DBTRepository::columnsToJson(const std::vector<DBTColumn> &columns) {
  json j = json::array();
  for (const auto &col : columns) {
    json colJson;
    colJson["name"] = col.name;
    colJson["data_type"] = col.data_type;
    colJson["description"] = col.description;
    colJson["tests"] = col.tests;
    colJson["metadata"] = col.metadata;
    j.push_back(colJson);
  }
  return j;
}

json DBTRepository::testsToJson(const std::vector<DBTTest> &tests) {
  json j = json::array();
  for (const auto &test : tests) {
    json testJson;
    testJson["test_name"] = test.test_name;
    testJson["test_type"] = testTypeToString(test.test_type);
    testJson["column_name"] = test.column_name;
    testJson["test_config"] = test.test_config;
    testJson["test_sql"] = test.test_sql;
    testJson["description"] = test.description;
    testJson["severity"] = testSeverityToString(test.severity);
    j.push_back(testJson);
  }
  return j;
}

std::vector<DBTModel> DBTRepository::getAllModels() {
  std::vector<DBTModel> models;
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);
    auto results = txn.exec(
        "SELECT id, model_name, model_type, materialization, schema_name, "
        "database_name, sql_content, config, description, tags, depends_on, "
        "columns, tests, documentation, metadata, version, git_commit_hash, "
        "git_branch, active, created_at, updated_at, last_run_time, "
        "last_run_status, last_run_rows "
        "FROM metadata.dbt_models ORDER BY model_name");

    for (const auto &row : results) {
      models.push_back(rowToModel(row));
    }
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "getAllModels",
                  "Error getting dbt models: " + std::string(e.what()));
  }
  return models;
}

std::vector<DBTModel> DBTRepository::getActiveModels() {
  std::vector<DBTModel> models;
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);
    auto results = txn.exec(
        "SELECT id, model_name, model_type, materialization, schema_name, "
        "database_name, sql_content, config, description, tags, depends_on, "
        "columns, tests, documentation, metadata, version, git_commit_hash, "
        "git_branch, active, created_at, updated_at, last_run_time, "
        "last_run_status, last_run_rows "
        "FROM metadata.dbt_models WHERE active = true ORDER BY model_name");

    for (const auto &row : results) {
      models.push_back(rowToModel(row));
    }
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "getActiveModels",
                  "Error getting active dbt models: " + std::string(e.what()));
  }
  return models;
}

DBTModel DBTRepository::getModel(const std::string &modelName) {
  DBTModel model;
  model.model_name = "";
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);
    auto results = txn.exec_params(
        "SELECT id, model_name, model_type, materialization, schema_name, "
        "database_name, sql_content, config, description, tags, depends_on, "
        "columns, tests, documentation, metadata, version, git_commit_hash, "
        "git_branch, active, created_at, updated_at, last_run_time, "
        "last_run_status, last_run_rows "
        "FROM metadata.dbt_models WHERE model_name = $1",
        modelName);

    if (!results.empty()) {
      model = rowToModel(results[0]);
    }
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "getModel",
                  "Error getting dbt model: " + std::string(e.what()));
  }
  return model;
}

void DBTRepository::insertOrUpdateModel(const DBTModel &model) {
  if (model.model_name.empty() || model.sql_content.empty()) {
    Logger::error(LogCategory::DATABASE, "insertOrUpdateModel",
                  "Invalid input: model_name and sql_content must not be empty");
    throw std::invalid_argument("model_name and sql_content must not be empty");
  }

  try {
    DBTModel modelWithGit = model;
    
    if (model.git_commit_hash.empty()) {
      FILE* pipe = popen("git rev-parse HEAD 2>/dev/null", "r");
      if (pipe) {
        char buffer[128];
        std::string result = "";
        while (fgets(buffer, sizeof(buffer), pipe) != nullptr) {
          result += buffer;
        }
        pclose(pipe);
        if (!result.empty() && result.back() == '\n') {
          result.pop_back();
        }
        if (!result.empty()) {
          modelWithGit.git_commit_hash = result;
        }
      }
    }
    
    if (model.git_branch.empty()) {
      FILE* pipe = popen("git rev-parse --abbrev-ref HEAD 2>/dev/null", "r");
      if (pipe) {
        char buffer[128];
        std::string result = "";
        while (fgets(buffer, sizeof(buffer), pipe) != nullptr) {
          result += buffer;
        }
        pclose(pipe);
        if (!result.empty() && result.back() == '\n') {
          result.pop_back();
        }
        if (!result.empty()) {
          modelWithGit.git_branch = result;
        }
      }
    }
    
    auto conn = getConnection();
    pqxx::work txn(conn);

    std::string configStr = model.config.dump();
    std::string tagsStr = "{" + (model.tags.empty() ? "" : "\"" + 
                                std::string(model.tags[0]) + "\"");
    for (size_t i = 1; i < model.tags.size(); ++i) {
      tagsStr += ",\"" + model.tags[i] + "\"";
    }
    tagsStr += "}";

    std::string dependsOnStr = "{" + (model.depends_on.empty() ? "" : "\"" + 
                                     std::string(model.depends_on[0]) + "\"");
    for (size_t i = 1; i < model.depends_on.size(); ++i) {
      dependsOnStr += ",\"" + model.depends_on[i] + "\"";
    }
    dependsOnStr += "}";

    std::string columnsStr = columnsToJson(model.columns).dump();
    std::string testsStr = testsToJson(model.tests).dump();
    std::string metadataStr = model.metadata.dump();
    std::string materializationStr = materializationToString(model.materialization);

    auto existing = txn.exec_params(
        "SELECT id FROM metadata.dbt_models WHERE model_name = $1",
        model.model_name);

    if (existing.empty()) {
      txn.exec_params(
          "INSERT INTO metadata.dbt_models (model_name, model_type, materialization, "
          "schema_name, database_name, sql_content, config, description, tags, "
          "depends_on, columns, tests, documentation, metadata, version, "
          "git_commit_hash, git_branch, active) "
          "VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9::text[], $10::text[], "
          "$11::jsonb, $12::jsonb, $13, $14::jsonb, $15, $16, $17, $18)",
          modelWithGit.model_name, modelWithGit.model_type, materializationStr, modelWithGit.schema_name,
          modelWithGit.database_name.empty() ? nullptr : modelWithGit.database_name,
          modelWithGit.sql_content, configStr, modelWithGit.description.empty() ? nullptr : modelWithGit.description,
          tagsStr, dependsOnStr, columnsStr, testsStr,
          modelWithGit.documentation.empty() ? nullptr : modelWithGit.documentation,
          metadataStr, modelWithGit.version,
          modelWithGit.git_commit_hash.empty() ? nullptr : modelWithGit.git_commit_hash,
          modelWithGit.git_branch.empty() ? nullptr : modelWithGit.git_branch, modelWithGit.active);
    } else {
      int id = existing[0][0].as<int>();
      txn.exec_params(
          "UPDATE metadata.dbt_models SET model_type = $2, materialization = $3, "
          "schema_name = $4, database_name = $5, sql_content = $6, config = $7::jsonb, "
          "description = $8, tags = $9::text[], depends_on = $10::text[], "
          "columns = $11::jsonb, tests = $12::jsonb, documentation = $13, "
          "metadata = $14::jsonb, version = $15, git_commit_hash = $16, "
          "git_branch = $17, active = $18, updated_at = NOW() WHERE id = $1",
          id, modelWithGit.model_type, materializationStr, modelWithGit.schema_name,
          modelWithGit.database_name.empty() ? nullptr : modelWithGit.database_name,
          modelWithGit.sql_content, configStr, modelWithGit.description.empty() ? nullptr : modelWithGit.description,
          tagsStr, dependsOnStr, columnsStr, testsStr,
          modelWithGit.documentation.empty() ? nullptr : modelWithGit.documentation,
          metadataStr, modelWithGit.version,
          modelWithGit.git_commit_hash.empty() ? nullptr : modelWithGit.git_commit_hash,
          modelWithGit.git_branch.empty() ? nullptr : modelWithGit.git_branch, modelWithGit.active);
    }

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "insertOrUpdateModel",
                  "Error inserting/updating dbt model: " + std::string(e.what()));
    throw;
  }
}

void DBTRepository::deleteModel(const std::string &modelName) {
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);
    txn.exec_params("DELETE FROM metadata.dbt_models WHERE model_name = $1",
                    modelName);
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "deleteModel",
                  "Error deleting dbt model: " + std::string(e.what()));
    throw;
  }
}

void DBTRepository::updateModelActive(const std::string &modelName, bool active) {
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);
    txn.exec_params(
        "UPDATE metadata.dbt_models SET active = $1, updated_at = NOW() "
        "WHERE model_name = $2",
        active, modelName);
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "updateModelActive",
                  "Error updating model active status: " + std::string(e.what()));
    throw;
  }
}

void DBTRepository::updateModelRunStatus(const std::string &modelName,
                                        const std::string &runTime,
                                        const std::string &status,
                                        int rowsAffected) {
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);
    txn.exec_params(
        "UPDATE metadata.dbt_models SET last_run_time = $1::timestamp, "
        "last_run_status = $2, last_run_rows = $3, updated_at = NOW() "
        "WHERE model_name = $4",
        runTime, status, rowsAffected, modelName);
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "updateModelRunStatus",
                  "Error updating model run status: " + std::string(e.what()));
    throw;
  }
}

std::vector<DBTTest> DBTRepository::getModelTests(const std::string &modelName) {
  std::vector<DBTTest> tests;
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);
    auto results = txn.exec_params(
        "SELECT id, test_name, model_name, test_type, column_name, test_config, "
        "test_sql, description, severity, active, created_at, updated_at "
        "FROM metadata.dbt_tests WHERE model_name = $1 ORDER BY test_name",
        modelName);

    for (const auto &row : results) {
      tests.push_back(rowToTest(row));
    }
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "getModelTests",
                  "Error getting model tests: " + std::string(e.what()));
  }
  return tests;
}

void DBTRepository::insertOrUpdateTest(const DBTTest &test) {
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);

    std::string testConfigStr = test.test_config.dump();

    auto existing = txn.exec_params(
        "SELECT id FROM metadata.dbt_tests WHERE test_name = $1 AND model_name = $2",
        test.test_name, test.model_name);

    if (existing.empty()) {
      txn.exec_params(
          "INSERT INTO metadata.dbt_tests (test_name, model_name, test_type, "
          "column_name, test_config, test_sql, description, severity, active) "
          "VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7, $8, $9)",
          test.test_name, test.model_name, testTypeToString(test.test_type),
          test.column_name.empty() ? nullptr : test.column_name, testConfigStr,
          test.test_sql.empty() ? nullptr : test.test_sql,
          test.description.empty() ? nullptr : test.description,
          testSeverityToString(test.severity), test.active);
    } else {
      int id = existing[0][0].as<int>();
      txn.exec_params(
          "UPDATE metadata.dbt_tests SET test_type = $3, column_name = $4, "
          "test_config = $5::jsonb, test_sql = $6, description = $7, "
          "severity = $8, active = $9, updated_at = NOW() "
          "WHERE id = $1 AND test_name = $2",
          id, test.test_name, testTypeToString(test.test_type),
          test.column_name.empty() ? nullptr : test.column_name, testConfigStr,
          test.test_sql.empty() ? nullptr : test.test_sql,
          test.description.empty() ? nullptr : test.description,
          testSeverityToString(test.severity), test.active);
    }

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "insertOrUpdateTest",
                  "Error inserting/updating test: " + std::string(e.what()));
    throw;
  }
}

void DBTRepository::deleteTest(const std::string &testName, const std::string &modelName) {
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);
    txn.exec_params(
        "DELETE FROM metadata.dbt_tests WHERE test_name = $1 AND model_name = $2",
        testName, modelName);
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "deleteTest",
                  "Error deleting test: " + std::string(e.what()));
    throw;
  }
}

int64_t DBTRepository::createTestResult(const DBTTestResult &result) {
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);

    std::string testResultStr = result.test_result.dump();

    auto res = txn.exec_params(
        "INSERT INTO metadata.dbt_test_results (test_name, model_name, test_type, "
        "status, error_message, rows_affected, execution_time_seconds, test_result, run_id) "
        "VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9) RETURNING id",
        result.test_name, result.model_name, testTypeToString(result.test_type),
        result.status, result.error_message.empty() ? nullptr : result.error_message,
        result.rows_affected, result.execution_time_seconds, testResultStr,
        result.run_id.empty() ? nullptr : result.run_id);

    if (!res.empty()) {
      int64_t id = res[0][0].as<int64_t>();
      txn.commit();
      return id;
    }
    txn.commit();
    return 0;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "createTestResult",
                  "Error creating test result: " + std::string(e.what()));
    throw;
  }
}

std::vector<DBTTestResult> DBTRepository::getTestResults(const std::string &modelName,
                                                          const std::string &runId) {
  std::vector<DBTTestResult> results;
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);

    pqxx::result queryResults;
    if (runId.empty()) {
      queryResults = txn.exec_params(
          "SELECT id, test_name, model_name, test_type, status, error_message, "
          "rows_affected, execution_time_seconds, test_result, run_id, created_at "
          "FROM metadata.dbt_test_results WHERE model_name = $1 "
          "ORDER BY created_at DESC LIMIT 100",
          modelName);
    } else {
      queryResults = txn.exec_params(
          "SELECT id, test_name, model_name, test_type, status, error_message, "
          "rows_affected, execution_time_seconds, test_result, run_id, created_at "
          "FROM metadata.dbt_test_results WHERE model_name = $1 AND run_id = $2 "
          "ORDER BY created_at DESC",
          modelName, runId);
    }

    for (const auto &row : queryResults) {
      results.push_back(rowToTestResult(row));
    }
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "getTestResults",
                  "Error getting test results: " + std::string(e.what()));
  }
  return results;
}

std::vector<DBTDocumentation> DBTRepository::getModelDocumentation(const std::string &modelName) {
  std::vector<DBTDocumentation> docs;
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);
    auto results = txn.exec_params(
        "SELECT id, model_name, doc_type, doc_key, doc_content, doc_format, "
        "created_at, updated_at "
        "FROM metadata.dbt_documentation WHERE model_name = $1",
        modelName);

    for (const auto &row : results) {
      docs.push_back(rowToDocumentation(row));
    }
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "getModelDocumentation",
                  "Error getting documentation: " + std::string(e.what()));
  }
  return docs;
}

void DBTRepository::insertOrUpdateDocumentation(const DBTDocumentation &doc) {
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);

    auto existing = txn.exec_params(
        "SELECT id FROM metadata.dbt_documentation "
        "WHERE model_name = $1 AND doc_type = $2 AND doc_key = $3",
        doc.model_name, doc.doc_type, doc.doc_key);

    if (existing.empty()) {
      txn.exec_params(
          "INSERT INTO metadata.dbt_documentation (model_name, doc_type, doc_key, "
          "doc_content, doc_format) VALUES ($1, $2, $3, $4, $5)",
          doc.model_name, doc.doc_type, doc.doc_key, doc.doc_content, doc.doc_format);
    } else {
      int id = existing[0][0].as<int>();
      txn.exec_params(
          "UPDATE metadata.dbt_documentation SET doc_content = $4, doc_format = $5, "
          "updated_at = NOW() WHERE id = $1 AND model_name = $2 AND doc_type = $3",
          id, doc.model_name, doc.doc_type, doc.doc_content, doc.doc_format);
    }

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "insertOrUpdateDocumentation",
                  "Error inserting/updating documentation: " + std::string(e.what()));
    throw;
  }
}

void DBTRepository::deleteDocumentation(const std::string &modelName,
                                       const std::string &docType,
                                       const std::string &docKey) {
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);
    txn.exec_params(
        "DELETE FROM metadata.dbt_documentation "
        "WHERE model_name = $1 AND doc_type = $2 AND doc_key = $3",
        modelName, docType, docKey);
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "deleteDocumentation",
                  "Error deleting documentation: " + std::string(e.what()));
    throw;
  }
}

std::vector<DBTLineage> DBTRepository::getModelLineage(const std::string &modelName) {
  std::vector<DBTLineage> lineage;
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);
    auto results = txn.exec_params(
        "SELECT id, source_model, target_model, source_column, target_column, "
        "transformation_type, transformation_sql, created_at "
        "FROM metadata.dbt_lineage WHERE target_model = $1 OR source_model = $1",
        modelName);

    for (const auto &row : results) {
      lineage.push_back(rowToLineage(row));
    }
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "getModelLineage",
                  "Error getting lineage: " + std::string(e.what()));
  }
  return lineage;
}

void DBTRepository::insertOrUpdateLineage(const DBTLineage &lineage) {
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);

    auto existing = txn.exec_params(
        "SELECT id FROM metadata.dbt_lineage "
        "WHERE source_model = $1 AND target_model = $2 AND "
        "source_column = $3 AND target_column = $4",
        lineage.source_model, lineage.target_model,
        lineage.source_column.empty() ? "" : lineage.source_column,
        lineage.target_column.empty() ? "" : lineage.target_column);

    if (existing.empty()) {
      txn.exec_params(
          "INSERT INTO metadata.dbt_lineage (source_model, target_model, "
          "source_column, target_column, transformation_type, transformation_sql) "
          "VALUES ($1, $2, $3, $4, $5, $6)",
          lineage.source_model, lineage.target_model,
          lineage.source_column.empty() ? nullptr : lineage.source_column,
          lineage.target_column.empty() ? nullptr : lineage.target_column,
          lineage.transformation_type.empty() ? nullptr : lineage.transformation_type,
          lineage.transformation_sql.empty() ? nullptr : lineage.transformation_sql);
    }

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "insertOrUpdateLineage",
                  "Error inserting/updating lineage: " + std::string(e.what()));
    throw;
  }
}

std::vector<DBTMacro> DBTRepository::getAllMacros() {
  std::vector<DBTMacro> macros;
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);
    auto results = txn.exec(
        "SELECT id, macro_name, macro_sql, parameters, description, return_type, "
        "examples, tags, active, created_at, updated_at "
        "FROM metadata.dbt_macros WHERE active = true ORDER BY macro_name");

    for (const auto &row : results) {
      macros.push_back(rowToMacro(row));
    }
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "getAllMacros",
                  "Error getting macros: " + std::string(e.what()));
  }
  return macros;
}

DBTMacro DBTRepository::getMacro(const std::string &macroName) {
  DBTMacro macro;
  macro.macro_name = "";
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);
    auto results = txn.exec_params(
        "SELECT id, macro_name, macro_sql, parameters, description, return_type, "
        "examples, tags, active, created_at, updated_at "
        "FROM metadata.dbt_macros WHERE macro_name = $1",
        macroName);

    if (!results.empty()) {
      macro = rowToMacro(results[0]);
    }
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "getMacro",
                  "Error getting macro: " + std::string(e.what()));
  }
  return macro;
}

void DBTRepository::insertOrUpdateMacro(const DBTMacro &macro) {
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);

    std::string paramsStr = macro.parameters.dump();
    std::string tagsStr = "{" + (macro.tags.empty() ? "" : "\"" + macro.tags[0] + "\"");
    for (size_t i = 1; i < macro.tags.size(); ++i) {
      tagsStr += ",\"" + macro.tags[i] + "\"";
    }
    tagsStr += "}";

    auto existing = txn.exec_params(
        "SELECT id FROM metadata.dbt_macros WHERE macro_name = $1",
        macro.macro_name);

    if (existing.empty()) {
      txn.exec_params(
          "INSERT INTO metadata.dbt_macros (macro_name, macro_sql, parameters, "
          "description, return_type, examples, tags, active) "
          "VALUES ($1, $2, $3::jsonb, $4, $5, $6, $7::text[], $8)",
          macro.macro_name, macro.macro_sql, paramsStr,
          macro.description.empty() ? nullptr : macro.description,
          macro.return_type.empty() ? nullptr : macro.return_type,
          macro.examples.empty() ? nullptr : macro.examples, tagsStr, macro.active);
    } else {
      int id = existing[0][0].as<int>();
      txn.exec_params(
          "UPDATE metadata.dbt_macros SET macro_sql = $2, parameters = $3::jsonb, "
          "description = $4, return_type = $5, examples = $6, tags = $7::text[], "
          "active = $8, updated_at = NOW() WHERE id = $1",
          id, macro.macro_sql, paramsStr,
          macro.description.empty() ? nullptr : macro.description,
          macro.return_type.empty() ? nullptr : macro.return_type,
          macro.examples.empty() ? nullptr : macro.examples, tagsStr, macro.active);
    }

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "insertOrUpdateMacro",
                  "Error inserting/updating macro: " + std::string(e.what()));
    throw;
  }
}

void DBTRepository::deleteMacro(const std::string &macroName) {
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);
    txn.exec_params("DELETE FROM metadata.dbt_macros WHERE macro_name = $1",
                    macroName);
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "deleteMacro",
                  "Error deleting macro: " + std::string(e.what()));
    throw;
  }
}

std::vector<DBTSource> DBTRepository::getAllSources() {
  std::vector<DBTSource> sources;
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);
    auto results = txn.exec(
        "SELECT id, source_name, source_type, database_name, schema_name, "
        "table_name, connection_string, description, columns, freshness_config, "
        "metadata, active, created_at, updated_at "
        "FROM metadata.dbt_sources WHERE active = true ORDER BY source_name");

    for (const auto &row : results) {
      sources.push_back(rowToSource(row));
    }
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "getAllSources",
                  "Error getting sources: " + std::string(e.what()));
  }
  return sources;
}

DBTSource DBTRepository::getSource(const std::string &sourceName,
                                   const std::string &schemaName,
                                   const std::string &tableName) {
  DBTSource source;
  source.source_name = "";
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);
    auto results = txn.exec_params(
        "SELECT id, source_name, source_type, database_name, schema_name, "
        "table_name, connection_string, description, columns, freshness_config, "
        "metadata, active, created_at, updated_at "
        "FROM metadata.dbt_sources "
        "WHERE source_name = $1 AND schema_name = $2 AND table_name = $3",
        sourceName, schemaName, tableName);

    if (!results.empty()) {
      source = rowToSource(results[0]);
    }
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "getSource",
                  "Error getting source: " + std::string(e.what()));
  }
  return source;
}

void DBTRepository::insertOrUpdateSource(const DBTSource &source) {
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);

    std::string columnsStr = columnsToJson(source.columns).dump();
    std::string freshnessStr = source.freshness_config.dump();
    std::string metadataStr = source.metadata.dump();

    auto existing = txn.exec_params(
        "SELECT id FROM metadata.dbt_sources "
        "WHERE source_name = $1 AND schema_name = $2 AND table_name = $3",
        source.source_name, source.schema_name, source.table_name);

    if (existing.empty()) {
      txn.exec_params(
          "INSERT INTO metadata.dbt_sources (source_name, source_type, database_name, "
          "schema_name, table_name, connection_string, description, columns, "
          "freshness_config, metadata, active) "
          "VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9::jsonb, $10::jsonb, $11)",
          source.source_name, source.source_type,
          source.database_name.empty() ? nullptr : source.database_name,
          source.schema_name, source.table_name,
          source.connection_string.empty() ? nullptr : source.connection_string,
          source.description.empty() ? nullptr : source.description,
          columnsStr, freshnessStr, metadataStr, source.active);
    } else {
      int id = existing[0][0].as<int>();
      txn.exec_params(
          "UPDATE metadata.dbt_sources SET source_type = $2, database_name = $3, "
          "connection_string = $4, description = $5, columns = $6::jsonb, "
          "freshness_config = $7::jsonb, metadata = $8::jsonb, active = $9, "
          "updated_at = NOW() WHERE id = $1",
          id, source.source_type,
          source.database_name.empty() ? nullptr : source.database_name,
          source.connection_string.empty() ? nullptr : source.connection_string,
          source.description.empty() ? nullptr : source.description,
          columnsStr, freshnessStr, metadataStr, source.active);
    }

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "insertOrUpdateSource",
                  "Error inserting/updating source: " + std::string(e.what()));
    throw;
  }
}

void DBTRepository::deleteSource(const std::string &sourceName,
                                 const std::string &schemaName,
                                 const std::string &tableName) {
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);
    txn.exec_params(
        "DELETE FROM metadata.dbt_sources "
        "WHERE source_name = $1 AND schema_name = $2 AND table_name = $3",
        sourceName, schemaName, tableName);
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "deleteSource",
                  "Error deleting source: " + std::string(e.what()));
    throw;
  }
}

std::vector<DBTModelRun> DBTRepository::getModelRuns(const std::string &modelName, int limit) {
  std::vector<DBTModelRun> runs;
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);
    auto results = txn.exec_params(
        "SELECT id, model_name, run_id, status, materialization, start_time, "
        "end_time, duration_seconds, rows_affected, error_message, compiled_sql, "
        "executed_sql, metadata, created_at "
        "FROM metadata.dbt_model_runs WHERE model_name = $1 "
        "ORDER BY created_at DESC LIMIT $2",
        modelName, limit);

    for (const auto &row : results) {
      runs.push_back(rowToModelRun(row));
    }
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "getModelRuns",
                  "Error getting model runs: " + std::string(e.what()));
  }
  return runs;
}

int64_t DBTRepository::createModelRun(const DBTModelRun &run) {
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);

    std::string metadataStr = run.metadata.dump();
    std::string materializationStr = materializationToString(run.materialization);

    auto res = txn.exec_params(
        "INSERT INTO metadata.dbt_model_runs (model_name, run_id, status, "
        "materialization, start_time, end_time, duration_seconds, rows_affected, "
        "error_message, compiled_sql, executed_sql, metadata) "
        "VALUES ($1, $2, $3, $4, $5::timestamp, $6::timestamp, $7, $8, $9, $10, $11, $12::jsonb) "
        "RETURNING id",
        run.model_name, run.run_id, run.status, materializationStr,
        run.start_time.empty() ? nullptr : run.start_time,
        run.end_time.empty() ? nullptr : run.end_time,
        run.duration_seconds, run.rows_affected,
        run.error_message.empty() ? nullptr : run.error_message,
        run.compiled_sql.empty() ? nullptr : run.compiled_sql,
        run.executed_sql.empty() ? nullptr : run.executed_sql, metadataStr);

    if (!res.empty()) {
      int64_t id = res[0][0].as<int64_t>();
      txn.commit();
      return id;
    }
    txn.commit();
    return 0;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "createModelRun",
                  "Error creating model run: " + std::string(e.what()));
    throw;
  }
}

void DBTRepository::updateModelRun(const DBTModelRun &run) {
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);

    std::string metadataStr = run.metadata.dump();
    std::string materializationStr = materializationToString(run.materialization);

    txn.exec_params(
        "UPDATE metadata.dbt_model_runs SET status = $2, materialization = $3, "
        "start_time = $4::timestamp, end_time = $5::timestamp, duration_seconds = $6, "
        "rows_affected = $7, error_message = $8, compiled_sql = $9, executed_sql = $10, "
        "metadata = $11::jsonb WHERE id = $1",
        run.id, run.status, materializationStr,
        run.start_time.empty() ? nullptr : run.start_time,
        run.end_time.empty() ? nullptr : run.end_time,
        run.duration_seconds, run.rows_affected,
        run.error_message.empty() ? nullptr : run.error_message,
        run.compiled_sql.empty() ? nullptr : run.compiled_sql,
        run.executed_sql.empty() ? nullptr : run.executed_sql, metadataStr);

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "updateModelRun",
                  "Error updating model run: " + std::string(e.what()));
    throw;
  }
}

DBTModel DBTRepository::rowToModel(const pqxx::row &row) {
  DBTModel model;
  model.id = row[0].as<int>();
  model.model_name = row[1].as<std::string>();
  model.model_type = row[2].is_null() ? "sql" : row[2].as<std::string>();
  model.materialization = stringToMaterialization(
      row[3].is_null() ? "table" : row[3].as<std::string>());
  model.schema_name = row[4].as<std::string>();
  model.database_name = row[5].is_null() ? "" : row[5].as<std::string>();
  model.sql_content = row[6].as<std::string>();
  
  if (!row[7].is_null()) {
    try {
      model.config = json::parse(row[7].as<std::string>());
    } catch (...) {
      model.config = json{};
    }
  }
  
  model.description = row[8].is_null() ? "" : row[8].as<std::string>();
  
  if (!row[9].is_null()) {
    std::string tagsStr = row[9].as<std::string>();
    if (!tagsStr.empty() && tagsStr != "{}") {
      try {
        json tagsJson = json::parse(tagsStr);
        if (tagsJson.is_array()) {
          for (const auto &tag : tagsJson) {
            if (tag.is_string()) {
              model.tags.push_back(tag.get<std::string>());
            }
          }
        }
      } catch (...) {
      }
    }
  }
  
  if (!row[10].is_null()) {
    std::string depsStr = row[10].as<std::string>();
    if (!depsStr.empty() && depsStr != "{}") {
      try {
        json depsJson = json::parse(depsStr);
        if (depsJson.is_array()) {
          for (const auto &dep : depsJson) {
            if (dep.is_string()) {
              model.depends_on.push_back(dep.get<std::string>());
            }
          }
        }
      } catch (...) {
      }
    }
  }
  
  if (!row[11].is_null()) {
    try {
      model.columns = parseColumns(json::parse(row[11].as<std::string>()));
    } catch (...) {
      model.columns = {};
    }
  }
  
  if (!row[12].is_null()) {
    try {
      model.tests = parseTests(json::parse(row[12].as<std::string>()));
    } catch (...) {
      model.tests = {};
    }
  }
  
  model.documentation = row[13].is_null() ? "" : row[13].as<std::string>();
  
  if (!row[14].is_null()) {
    try {
      model.metadata = json::parse(row[14].as<std::string>());
    } catch (...) {
      model.metadata = json{};
    }
  }
  
  model.version = row[15].is_null() ? 1 : row[15].as<int>();
  model.git_commit_hash = row[16].is_null() ? "" : row[16].as<std::string>();
  model.git_branch = row[17].is_null() ? "" : row[17].as<std::string>();
  model.active = row[18].as<bool>();
  model.created_at = row[19].is_null() ? "" : row[19].as<std::string>();
  model.updated_at = row[20].is_null() ? "" : row[20].as<std::string>();
  model.last_run_time = row[21].is_null() ? "" : row[21].as<std::string>();
  model.last_run_status = row[22].is_null() ? "" : row[22].as<std::string>();
  model.last_run_rows = row[23].is_null() ? 0 : row[23].as<int>();
  
  return model;
}

DBTTest DBTRepository::rowToTest(const pqxx::row &row) {
  DBTTest test;
  test.id = row[0].as<int>();
  test.test_name = row[1].as<std::string>();
  test.model_name = row[2].as<std::string>();
  test.test_type = stringToTestType(row[3].as<std::string>());
  test.column_name = row[4].is_null() ? "" : row[4].as<std::string>();
  
  if (!row[5].is_null()) {
    try {
      test.test_config = json::parse(row[5].as<std::string>());
    } catch (...) {
      test.test_config = json{};
    }
  }
  
  test.test_sql = row[6].is_null() ? "" : row[6].as<std::string>();
  test.description = row[7].is_null() ? "" : row[7].as<std::string>();
  test.severity = stringToTestSeverity(row[8].as<std::string>());
  test.active = row[9].as<bool>();
  test.created_at = row[10].is_null() ? "" : row[10].as<std::string>();
  test.updated_at = row[11].is_null() ? "" : row[11].as<std::string>();
  
  return test;
}

DBTTestResult DBTRepository::rowToTestResult(const pqxx::row &row) {
  DBTTestResult result;
  result.id = row[0].as<int64_t>();
  result.test_name = row[1].as<std::string>();
  result.model_name = row[2].as<std::string>();
  result.test_type = stringToTestType(row[3].as<std::string>());
  result.status = row[4].as<std::string>();
  result.error_message = row[5].is_null() ? "" : row[5].as<std::string>();
  result.rows_affected = row[6].is_null() ? 0 : row[6].as<int>();
  result.execution_time_seconds = row[7].is_null() ? 0.0 : row[7].as<double>();
  
  if (!row[8].is_null()) {
    try {
      result.test_result = json::parse(row[8].as<std::string>());
    } catch (...) {
      result.test_result = json{};
    }
  }
  
  result.run_id = row[9].is_null() ? "" : row[9].as<std::string>();
  result.created_at = row[10].is_null() ? "" : row[10].as<std::string>();
  
  return result;
}

DBTDocumentation DBTRepository::rowToDocumentation(const pqxx::row &row) {
  DBTDocumentation doc;
  doc.id = row[0].as<int>();
  doc.model_name = row[1].as<std::string>();
  doc.doc_type = row[2].as<std::string>();
  doc.doc_key = row[3].as<std::string>();
  doc.doc_content = row[4].as<std::string>();
  doc.doc_format = row[5].is_null() ? "markdown" : row[5].as<std::string>();
  doc.created_at = row[6].is_null() ? "" : row[6].as<std::string>();
  doc.updated_at = row[7].is_null() ? "" : row[7].as<std::string>();
  return doc;
}

DBTLineage DBTRepository::rowToLineage(const pqxx::row &row) {
  DBTLineage lineage;
  lineage.id = row[0].as<int>();
  lineage.source_model = row[1].as<std::string>();
  lineage.target_model = row[2].as<std::string>();
  lineage.source_column = row[3].is_null() ? "" : row[3].as<std::string>();
  lineage.target_column = row[4].is_null() ? "" : row[4].as<std::string>();
  lineage.transformation_type = row[5].is_null() ? "" : row[5].as<std::string>();
  lineage.transformation_sql = row[6].is_null() ? "" : row[6].as<std::string>();
  lineage.created_at = row[7].is_null() ? "" : row[7].as<std::string>();
  return lineage;
}

DBTMacro DBTRepository::rowToMacro(const pqxx::row &row) {
  DBTMacro macro;
  macro.id = row[0].as<int>();
  macro.macro_name = row[1].as<std::string>();
  macro.macro_sql = row[2].as<std::string>();
  
  if (!row[3].is_null()) {
    try {
      macro.parameters = json::parse(row[3].as<std::string>());
    } catch (...) {
      macro.parameters = json::array();
    }
  }
  
  macro.description = row[4].is_null() ? "" : row[4].as<std::string>();
  macro.return_type = row[5].is_null() ? "" : row[5].as<std::string>();
  macro.examples = row[6].is_null() ? "" : row[6].as<std::string>();
  
  if (!row[7].is_null()) {
    std::string tagsStr = row[7].as<std::string>();
    if (!tagsStr.empty() && tagsStr != "{}") {
      try {
        json tagsJson = json::parse(tagsStr);
        if (tagsJson.is_array()) {
          for (const auto &tag : tagsJson) {
            if (tag.is_string()) {
              macro.tags.push_back(tag.get<std::string>());
            }
          }
        }
      } catch (...) {
      }
    }
  }
  
  macro.active = row[8].as<bool>();
  macro.created_at = row[9].is_null() ? "" : row[9].as<std::string>();
  macro.updated_at = row[10].is_null() ? "" : row[10].as<std::string>();
  
  return macro;
}

DBTSource DBTRepository::rowToSource(const pqxx::row &row) {
  DBTSource source;
  source.id = row[0].as<int>();
  source.source_name = row[1].as<std::string>();
  source.source_type = row[2].as<std::string>();
  source.database_name = row[3].is_null() ? "" : row[3].as<std::string>();
  source.schema_name = row[4].as<std::string>();
  source.table_name = row[5].as<std::string>();
  source.connection_string = row[6].is_null() ? "" : row[6].as<std::string>();
  source.description = row[7].is_null() ? "" : row[7].as<std::string>();
  
  if (!row[8].is_null()) {
    try {
      source.columns = parseColumns(json::parse(row[8].as<std::string>()));
    } catch (...) {
      source.columns = {};
    }
  }
  
  if (!row[9].is_null()) {
    try {
      source.freshness_config = json::parse(row[9].as<std::string>());
    } catch (...) {
      source.freshness_config = json{};
    }
  }
  
  if (!row[10].is_null()) {
    try {
      source.metadata = json::parse(row[10].as<std::string>());
    } catch (...) {
      source.metadata = json{};
    }
  }
  
  source.active = row[11].as<bool>();
  source.created_at = row[12].is_null() ? "" : row[12].as<std::string>();
  source.updated_at = row[13].is_null() ? "" : row[13].as<std::string>();
  
  return source;
}

DBTModelRun DBTRepository::rowToModelRun(const pqxx::row &row) {
  DBTModelRun run;
  run.id = row[0].as<int64_t>();
  run.model_name = row[1].as<std::string>();
  run.run_id = row[2].as<std::string>();
  run.status = row[3].as<std::string>();
  run.materialization = stringToMaterialization(row[4].as<std::string>());
  run.start_time = row[5].is_null() ? "" : row[5].as<std::string>();
  run.end_time = row[6].is_null() ? "" : row[6].as<std::string>();
  run.duration_seconds = row[7].is_null() ? 0.0 : row[7].as<double>();
  run.rows_affected = row[8].is_null() ? 0 : row[8].as<int>();
  run.error_message = row[9].is_null() ? "" : row[9].as<std::string>();
  run.compiled_sql = row[10].is_null() ? "" : row[10].as<std::string>();
  run.executed_sql = row[11].is_null() ? "" : row[11].as<std::string>();
  
  if (!row[12].is_null()) {
    try {
      run.metadata = json::parse(row[12].as<std::string>());
    } catch (...) {
      run.metadata = json{};
    }
  }
  
  run.created_at = row[13].is_null() ? "" : row[13].as<std::string>();
  
  return run;
}

