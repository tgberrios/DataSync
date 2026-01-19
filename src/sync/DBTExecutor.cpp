#include "sync/DBTExecutor.h"
#include "core/database_config.h"
#include <chrono>
#include <iomanip>
#include <sstream>
#include <regex>
#include <cstdio>

DBTExecutor::DBTExecutor(std::string metadataConnectionString)
    : metadataConnectionString_(std::move(metadataConnectionString)),
      dbtRepo_(std::make_unique<DBTRepository>(metadataConnectionString_)) {
  dbtRepo_->createTables();
}

std::string DBTExecutor::getConnectionString(const std::string & /*schemaName*/,
                                            const std::string &databaseName) {
  std::string dbName = databaseName.empty() ? DatabaseConfig::getPostgresDB() : databaseName;
  std::string connStr = "host=" + DatabaseConfig::getPostgresHost() + 
                        " port=" + DatabaseConfig::getPostgresPort() +
                        " dbname=" + dbName +
                        " user=" + DatabaseConfig::getPostgresUser() + 
                        " password=" + DatabaseConfig::getPostgresPassword();
  return connStr;
}

std::string DBTExecutor::getGitCommitHash() {
  try {
    FILE* pipe = popen("git rev-parse HEAD 2>/dev/null", "r");
    if (!pipe) return "";
    
    char buffer[128];
    std::string result = "";
    while (fgets(buffer, sizeof(buffer), pipe) != nullptr) {
      result += buffer;
    }
    pclose(pipe);
    
    if (!result.empty() && result.back() == '\n') {
      result.pop_back();
    }
    return result;
  } catch (...) {
    return "";
  }
}

std::string DBTExecutor::getGitBranch() {
  try {
    FILE* pipe = popen("git rev-parse --abbrev-ref HEAD 2>/dev/null", "r");
    if (!pipe) return "";
    
    char buffer[128];
    std::string result = "";
    while (fgets(buffer, sizeof(buffer), pipe) != nullptr) {
      result += buffer;
    }
    pclose(pipe);
    
    if (!result.empty() && result.back() == '\n') {
      result.pop_back();
    }
    return result;
  } catch (...) {
    return "";
  }
}

std::string DBTExecutor::expandMacros(const std::string &sql) {
  std::string expandedSQL = sql;
  auto macros = dbtRepo_->getAllMacros();
  
  for (const auto &macro : macros) {
    std::regex macroPattern("\\{\\{\\s*" + macro.macro_name + "\\s*\\(([^)]*)\\)\\s*\\}\\}");
    std::smatch match;
    
    while (std::regex_search(expandedSQL, match, macroPattern)) {
      std::string args = match[1].str();
      std::string macroSQL = macro.macro_sql;
      
      if (!args.empty()) {
        std::vector<std::string> argList;
        std::istringstream iss(args);
        std::string arg;
        while (std::getline(iss, arg, ',')) {
          arg.erase(0, arg.find_first_not_of(" \t"));
          arg.erase(arg.find_last_not_of(" \t") + 1);
          argList.push_back(arg);
        }
        
        if (macro.parameters.is_array() && argList.size() == macro.parameters.size()) {
          for (size_t i = 0; i < argList.size(); ++i) {
            std::string paramName = macro.parameters[i].contains("name") 
                ? macro.parameters[i]["name"].get<std::string>() 
                : "param" + std::to_string(i);
            std::regex paramPattern("\\{\\{\\s*" + paramName + "\\s*\\}\\}");
            macroSQL = std::regex_replace(macroSQL, paramPattern, argList[i]);
          }
        }
      }
      
      expandedSQL = std::regex_replace(expandedSQL, macroPattern, macroSQL, std::regex_constants::format_first_only);
    }
  }
  
  return expandedSQL;
}

std::string DBTExecutor::compileSQL(const DBTModel &model) {
  std::string sql = model.sql_content;
  
  sql = expandMacros(sql);
  
  for (const auto &dep : model.depends_on) {
    DBTModel depModel = dbtRepo_->getModel(dep);
    if (!depModel.model_name.empty()) {
      std::string depTable = depModel.schema_name + "." + depModel.model_name;
      std::regex depPattern("\\{\\{\\s*ref\\s*\\(['\"]?" + dep + "['\"]?\\)\\s*\\}\\}");
      sql = std::regex_replace(sql, depPattern, depTable);
    }
  }
  
  std::regex refPattern("\\{\\{\\s*ref\\s*\\(['\"]?([^'\"]+)['\"]?\\)\\s*\\}\\}");
  std::smatch match;
  while (std::regex_search(sql, match, refPattern)) {
    std::string modelRef = match[1].str();
    DBTModel refModel = dbtRepo_->getModel(modelRef);
    if (!refModel.model_name.empty()) {
      std::string refTable = refModel.schema_name + "." + refModel.model_name;
      sql = std::regex_replace(sql, refPattern, refTable, std::regex_constants::format_first_only);
    } else {
      Logger::info(LogCategory::TRANSFER, "compileSQL",
                  "Model reference not found: " + modelRef);
      sql = std::regex_replace(sql, refPattern, modelRef, std::regex_constants::format_first_only);
    }
  }
  
  std::regex sourcePattern("\\{\\{\\s*source\\s*\\(['\"]?([^'\"]+)['\"]?\\s*,\\s*['\"]?([^'\"]+)['\"]?\\)\\s*\\}\\}");
  while (std::regex_search(sql, match, sourcePattern)) {
    std::string sourceName = match[1].str();
    std::string tableName = match[2].str();
    DBTSource source = dbtRepo_->getSource(sourceName, model.schema_name, tableName);
    if (!source.source_name.empty()) {
      std::string sourceTable = source.schema_name + "." + source.table_name;
      sql = std::regex_replace(sql, sourcePattern, sourceTable, std::regex_constants::format_first_only);
    } else {
      Logger::info(LogCategory::TRANSFER, "compileSQL",
                  "Source not found: " + sourceName + "." + tableName);
      sql = std::regex_replace(sql, sourcePattern, tableName, std::regex_constants::format_first_only);
    }
  }
  
  return sql;
}

void DBTExecutor::executeTableMaterialization(const DBTModel &model,
                                              const std::string &compiledSQL) {
  try {
    std::string connStr = getConnectionString(model.schema_name, model.database_name);
    pqxx::connection conn(connStr);
    pqxx::work txn(conn);
    
    std::string fullTableName = model.schema_name + "." + model.model_name;
    
    txn.exec("CREATE SCHEMA IF NOT EXISTS " + txn.quote_name(model.schema_name));
    
    std::string dropSQL = "DROP TABLE IF EXISTS " + txn.quote_name(model.schema_name) + 
                          "." + txn.quote_name(model.model_name) + " CASCADE";
    txn.exec(dropSQL);
    
    std::string createSQL = "CREATE TABLE " + txn.quote_name(model.schema_name) + 
                            "." + txn.quote_name(model.model_name) + " AS " + compiledSQL;
    txn.exec(createSQL);
    
    txn.commit();
    
    Logger::info(LogCategory::TRANSFER, "executeTableMaterialization",
                "Created table: " + fullTableName);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "executeTableMaterialization",
                  "Error creating table: " + std::string(e.what()));
    throw;
  }
}

void DBTExecutor::executeViewMaterialization(const DBTModel &model,
                                             const std::string &compiledSQL) {
  try {
    std::string connStr = getConnectionString(model.schema_name, model.database_name);
    pqxx::connection conn(connStr);
    pqxx::work txn(conn);
    
    txn.exec("CREATE SCHEMA IF NOT EXISTS " + txn.quote_name(model.schema_name));
    
    std::string dropSQL = "DROP VIEW IF EXISTS " + txn.quote_name(model.schema_name) + 
                          "." + txn.quote_name(model.model_name) + " CASCADE";
    txn.exec(dropSQL);
    
    std::string createSQL = "CREATE VIEW " + txn.quote_name(model.schema_name) + 
                            "." + txn.quote_name(model.model_name) + " AS " + compiledSQL;
    txn.exec(createSQL);
    
    txn.commit();
    
    Logger::info(LogCategory::TRANSFER, "executeViewMaterialization",
                "Created view: " + model.schema_name + "." + model.model_name);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "executeViewMaterialization",
                  "Error creating view: " + std::string(e.what()));
    throw;
  }
}

void DBTExecutor::executeIncrementalMaterialization(const DBTModel &model,
                                                   const std::string &compiledSQL) {
  try {
    std::string connStr = getConnectionString(model.schema_name, model.database_name);
    pqxx::connection conn(connStr);
    pqxx::work txn(conn);
    
    txn.exec("CREATE SCHEMA IF NOT EXISTS " + txn.quote_name(model.schema_name));
    
    std::string fullTableName = txn.quote_name(model.schema_name) + "." + 
                                txn.quote_name(model.model_name);
    
    auto tableExists = txn.exec(
        "SELECT EXISTS (SELECT FROM information_schema.tables "
        "WHERE table_schema = " + txn.quote(model.schema_name) + 
        " AND table_name = " + txn.quote(model.model_name) + ")");
    
    bool exists = tableExists[0][0].as<bool>();
    
    if (!exists) {
      std::string createSQL = "CREATE TABLE " + fullTableName + " AS " + compiledSQL;
      txn.exec(createSQL);
    } else {
      json config = model.config;
      std::string uniqueKey = config.contains("unique_key") 
          ? config["unique_key"].get<std::string>() 
          : "";
      
      if (!uniqueKey.empty()) {
        std::string mergeSQL = "INSERT INTO " + fullTableName + " " + compiledSQL +
                              " ON CONFLICT (" + uniqueKey + ") DO UPDATE SET " +
                              uniqueKey + " = EXCLUDED." + uniqueKey;
        txn.exec(mergeSQL);
      } else {
        std::string insertSQL = "INSERT INTO " + fullTableName + " " + compiledSQL;
        txn.exec(insertSQL);
      }
    }
    
    txn.commit();
    
    Logger::info(LogCategory::TRANSFER, "executeIncrementalMaterialization",
                "Updated incremental table: " + model.schema_name + "." + model.model_name);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "executeIncrementalMaterialization",
                  "Error updating incremental table: " + std::string(e.what()));
    throw;
  }
}

void DBTExecutor::executeModel(const std::string &modelName) {
  DBTModel model = dbtRepo_->getModel(modelName);
  if (model.model_name.empty()) {
    throw std::runtime_error("Model not found: " + modelName);
  }
  executeModel(model);
}

void DBTExecutor::executeModel(const DBTModel &model) {
  if (!model.active) {
    Logger::info(LogCategory::TRANSFER, "executeModel",
                "Model is inactive, skipping: " + model.model_name);
    return;
  }
  
  DBTModel modelWithGit = model;
  if (model.git_commit_hash.empty()) {
    modelWithGit.git_commit_hash = getGitCommitHash();
  }
  if (model.git_branch.empty()) {
    modelWithGit.git_branch = getGitBranch();
  }
  
  auto startTime = std::chrono::system_clock::now();
  std::string runId = std::to_string(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          startTime.time_since_epoch()).count());
  
  DBTModelRun run;
  run.model_name = model.model_name;
  run.run_id = runId;
  run.status = "running";
  run.materialization = model.materialization;
  run.start_time = std::to_string(
      std::chrono::duration_cast<std::chrono::seconds>(
          startTime.time_since_epoch()).count());
  run.rows_affected = 0;
  run.duration_seconds = 0.0;
  
  int64_t runIdDb = dbtRepo_->createModelRun(run);
  
  try {
    std::string compiledSQL = compileSQL(model);
    run.compiled_sql = compiledSQL;
    
    int rowsAffected = 0;
    
    switch (model.materialization) {
      case MaterializationType::TABLE:
        executeTableMaterialization(model, compiledSQL);
        break;
      case MaterializationType::VIEW:
        executeViewMaterialization(model, compiledSQL);
        break;
      case MaterializationType::INCREMENTAL:
        executeIncrementalMaterialization(model, compiledSQL);
        break;
      case MaterializationType::EPHEMERAL:
        Logger::info(LogCategory::TRANSFER, "executeModel",
                    "Ephemeral model, not materializing: " + model.model_name);
        break;
    }
    
    if (model.materialization != MaterializationType::EPHEMERAL) {
      std::string connStr = getConnectionString(model.schema_name, model.database_name);
      pqxx::connection conn(connStr);
      pqxx::work txn(conn);
      auto countResult = txn.exec(
          "SELECT COUNT(*) FROM " + txn.quote_name(model.schema_name) + 
          "." + txn.quote_name(model.model_name));
      rowsAffected = countResult[0][0].as<int>();
      txn.commit();
    }
    
    auto endTime = std::chrono::system_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        endTime - startTime).count() / 1000.0;
    
    run.id = runIdDb;
    run.status = "success";
    run.end_time = std::to_string(
        std::chrono::duration_cast<std::chrono::seconds>(
            endTime.time_since_epoch()).count());
    run.rows_affected = rowsAffected;
    run.duration_seconds = duration;
    run.executed_sql = compiledSQL;
    
    dbtRepo_->updateModelRun(run);
    dbtRepo_->updateModelRunStatus(model.model_name, run.end_time, "success", rowsAffected);
    
    extractLineage(model, compiledSQL);
    
    if (!model.documentation.empty()) {
      DBTDocumentation doc;
      doc.model_name = model.model_name;
      doc.doc_type = "model";
      doc.doc_key = "description";
      doc.doc_content = model.documentation;
      doc.doc_format = "markdown";
      dbtRepo_->insertOrUpdateDocumentation(doc);
    }
    
    for (const auto &col : model.columns) {
      if (!col.description.empty()) {
        DBTDocumentation colDoc;
        colDoc.model_name = model.model_name;
        colDoc.doc_type = "column";
        colDoc.doc_key = col.name;
        colDoc.doc_content = col.description;
        colDoc.doc_format = "markdown";
        dbtRepo_->insertOrUpdateDocumentation(colDoc);
      }
    }
    
    Logger::info(LogCategory::TRANSFER, "executeModel",
                "Model executed successfully: " + model.model_name + 
                " (" + std::to_string(rowsAffected) + " rows)");
    
  } catch (const std::exception &e) {
    auto endTime = std::chrono::system_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        endTime - startTime).count() / 1000.0;
    
    run.id = runIdDb;
    run.status = "error";
    run.end_time = std::to_string(
        std::chrono::duration_cast<std::chrono::seconds>(
            endTime.time_since_epoch()).count());
    run.duration_seconds = duration;
    run.error_message = std::string(e.what());
    
    dbtRepo_->updateModelRun(run);
    dbtRepo_->updateModelRunStatus(model.model_name, run.end_time, "error", 0);
    
    Logger::error(LogCategory::TRANSFER, "executeModel",
                  "Error executing model: " + model.model_name + " - " + std::string(e.what()));
    throw;
  }
}

std::string DBTExecutor::compileModel(const std::string &modelName) {
  DBTModel model = dbtRepo_->getModel(modelName);
  if (model.model_name.empty()) {
    throw std::runtime_error("Model not found: " + modelName);
  }
  return compileSQL(model);
}

std::string DBTExecutor::generateTestSQL(const DBTTest &test, const DBTModel &model) {
  std::string testSQL;
  std::string tableName = model.schema_name + "." + model.model_name;
  
  switch (test.test_type) {
    case TestType::NOT_NULL:
      if (!test.column_name.empty()) {
        testSQL = "SELECT COUNT(*) FROM " + tableName + 
                  " WHERE " + test.column_name + " IS NULL";
      }
      break;
      
    case TestType::UNIQUE:
      if (!test.column_name.empty()) {
        testSQL = "SELECT COUNT(*) FROM ("
                  "SELECT " + test.column_name + ", COUNT(*) as cnt "
                  "FROM " + tableName + 
                  " GROUP BY " + test.column_name + 
                  " HAVING COUNT(*) > 1) as duplicates";
      }
      break;
      
    case TestType::RELATIONSHIPS:
      if (test.test_config.contains("to") && !test.column_name.empty()) {
        std::string refTable = test.test_config["to"].get<std::string>();
        std::string refColumn = test.test_config.contains("field") 
            ? test.test_config["field"].get<std::string>() 
            : test.column_name;
        testSQL = "SELECT COUNT(*) FROM " + tableName + " t1 "
                  "LEFT JOIN " + refTable + " t2 ON t1." + test.column_name + 
                  " = t2." + refColumn + 
                  " WHERE t2." + refColumn + " IS NULL";
      }
      break;
      
    case TestType::ACCEPTED_VALUES:
      if (test.test_config.contains("values") && !test.column_name.empty()) {
        json values = test.test_config["values"];
        std::string valuesList;
        for (size_t i = 0; i < values.size(); ++i) {
          if (i > 0) valuesList += ", ";
          valuesList += "'" + values[i].get<std::string>() + "'";
        }
        testSQL = "SELECT COUNT(*) FROM " + tableName + 
                  " WHERE " + test.column_name + " NOT IN (" + valuesList + ")";
      }
      break;
      
    case TestType::EXPRESSION:
      if (!test.test_sql.empty()) {
        testSQL = test.test_sql;
        std::regex tablePattern("\\{\\{\\s*ref\\s*\\(['\"]?" + model.model_name + 
                                "['\"]?\\)\\s*\\}\\}");
        testSQL = std::regex_replace(testSQL, tablePattern, tableName);
      }
      break;
      
    case TestType::CUSTOM:
      if (!test.test_sql.empty()) {
        testSQL = test.test_sql;
        std::regex tablePattern("\\{\\{\\s*ref\\s*\\(['\"]?" + model.model_name + 
                                "['\"]?\\)\\s*\\}\\}");
        testSQL = std::regex_replace(testSQL, tablePattern, tableName);
      }
      break;
  }
  
  return testSQL;
}

DBTTestResult DBTExecutor::runTest(const DBTTest &test, const std::string &modelName,
                                    const std::string &runId) {
  DBTTestResult result;
  result.test_name = test.test_name;
  result.model_name = modelName;
  result.test_type = test.test_type;
  result.run_id = runId;
  result.status = "error";
  result.rows_affected = 0;
  result.execution_time_seconds = 0.0;
  
  if (!test.active) {
    result.status = "skipped";
    return result;
  }
  
  DBTModel model = dbtRepo_->getModel(modelName);
  if (model.model_name.empty()) {
    result.error_message = "Model not found: " + modelName;
    return result;
  }
  
  auto startTime = std::chrono::system_clock::now();
  
  try {
    std::string testSQL = generateTestSQL(test, model);
    
    if (testSQL.empty()) {
      result.error_message = "Could not generate test SQL";
      return result;
    }
    
    std::string connStr = getConnectionString(model.schema_name, model.database_name);
    pqxx::connection conn(connStr);
    pqxx::work txn(conn);
    
    auto testResult = txn.exec(testSQL);
    int failureCount = testResult[0][0].as<int>();
    
    auto endTime = std::chrono::system_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        endTime - startTime).count() / 1000.0;
    
    result.execution_time_seconds = duration;
    result.rows_affected = failureCount;
    
    if (failureCount == 0) {
      result.status = "pass";
    } else {
      result.status = "fail";
      result.error_message = "Test failed: " + std::to_string(failureCount) + " rows failed";
    }
    
    json testResultJson;
    testResultJson["failure_count"] = failureCount;
    testResultJson["test_sql"] = testSQL;
    result.test_result = testResultJson;
    
    txn.commit();
    
  } catch (const std::exception &e) {
    auto endTime = std::chrono::system_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        endTime - startTime).count() / 1000.0;
    
    result.execution_time_seconds = duration;
    result.status = "error";
    result.error_message = std::string(e.what());
  }
  
  return result;
}

std::vector<DBTTestResult> DBTExecutor::runTests(const std::string &modelName,
                                                  const std::string &runId) {
  std::vector<DBTTestResult> results;
  auto tests = dbtRepo_->getModelTests(modelName);
  
  for (const auto &test : tests) {
    DBTTestResult result = runTest(test, modelName, runId);
    dbtRepo_->createTestResult(result);
    results.push_back(result);
  }
  
  return results;
}

void DBTExecutor::runAllTests(const std::string &modelName) {
  std::string runId = std::to_string(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch()).count());
  runTests(modelName, runId);
}

void DBTExecutor::runAllTests() {
  auto models = dbtRepo_->getActiveModels();
  std::string runId = std::to_string(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch()).count());
  
  for (const auto &model : models) {
    runTests(model.model_name, runId);
  }
}

std::vector<DBTTestResult> DBTExecutor::getTestResults(const std::string &modelName,
                                                        const std::string &runId) {
  return dbtRepo_->getTestResults(modelName, runId);
}

void DBTExecutor::extractLineage(const DBTModel &model, const std::string &compiledSQL) {
  try {
    std::regex refPattern("\\{\\{\\s*ref\\s*\\(['\"]?([^'\"]+)['\"]?\\)\\s*\\}\\}");
    std::smatch match;
    std::string sql = compiledSQL;
    
    std::vector<std::string> sourceModels;
    while (std::regex_search(sql, match, refPattern)) {
      std::string sourceModel = match[1].str();
      sourceModels.push_back(sourceModel);
      sql = match.suffix().str();
    }
    
    std::regex sourcePattern("\\{\\{\\s*source\\s*\\(['\"]?([^'\"]+)['\"]?\\s*,\\s*['\"]?([^'\"]+)['\"]?\\)\\s*\\}\\}");
    while (std::regex_search(sql, match, sourcePattern)) {
      std::string sourceName = match[1].str();
      std::string tableName = match[2].str();
      DBTSource source = dbtRepo_->getSource(sourceName, model.schema_name, tableName);
      if (!source.source_name.empty()) {
        DBTLineage lineage;
        lineage.source_model = source.schema_name + "." + source.table_name;
        lineage.target_model = model.model_name;
        lineage.transformation_type = "source";
        dbtRepo_->insertOrUpdateLineage(lineage);
      }
      sql = match.suffix().str();
    }
    
    for (const auto &sourceModel : sourceModels) {
      DBTModel sourceModelObj = dbtRepo_->getModel(sourceModel);
      if (!sourceModelObj.model_name.empty()) {
        DBTLineage lineage;
        lineage.source_model = sourceModel;
        lineage.target_model = model.model_name;
        lineage.transformation_type = "ref";
        dbtRepo_->insertOrUpdateLineage(lineage);
      }
    }
    
    std::regex columnPattern("SELECT\\s+([^\\s,]+)\\s+FROM|,\\s*([^\\s,]+)\\s+FROM");
    sql = compiledSQL;
    while (std::regex_search(sql, match, columnPattern)) {
      std::string column = match[1].str().empty() ? match[2].str() : match[1].str();
      if (!column.empty() && column != "*") {
        for (const auto &sourceModel : sourceModels) {
          DBTLineage lineage;
          lineage.source_model = sourceModel;
          lineage.target_model = model.model_name;
          lineage.target_column = column;
          lineage.transformation_type = "select";
          dbtRepo_->insertOrUpdateLineage(lineage);
        }
      }
      sql = match.suffix().str();
    }
    
    Logger::info(LogCategory::TRANSFER, "extractLineage",
                "Extracted lineage for model: " + model.model_name);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "extractLineage",
                  "Error extracting lineage: " + std::string(e.what()));
  }
}
