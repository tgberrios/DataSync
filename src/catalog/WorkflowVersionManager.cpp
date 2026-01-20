#include "catalog/WorkflowVersionManager.h"
#include "catalog/workflow_repository.h"
#include "core/database_config.h"
#include "core/logger.h"
#include <pqxx/pqxx>
#include <algorithm>

WorkflowVersionManager& WorkflowVersionManager::getInstance() {
  static WorkflowVersionManager instance;
  return instance;
}

int WorkflowVersionManager::createVersion(const std::string& workflowName, const std::string& createdBy, const std::string& description) {
  try {
    std::string connStr = DatabaseConfig::getPostgresConnectionString();
    pqxx::connection conn(connStr);
    pqxx::work txn(conn);
    
    auto currentVersionResult = txn.exec_params(
        "SELECT COALESCE(MAX(version), 0) FROM metadata.workflow_versions WHERE workflow_name = $1",
        workflowName);
    
    int newVersion = 1;
    if (!currentVersionResult.empty()) {
      newVersion = currentVersionResult[0][0].as<int>() + 1;
    }
    
    txn.exec_params(
        "UPDATE metadata.workflow_versions SET is_current = false WHERE workflow_name = $1",
        workflowName);
    
    WorkflowRepository repo(connStr);
    WorkflowModel workflow = repo.getWorkflow(workflowName);
    
    json workflowDef;
    workflowDef["tasks"] = json::array();
    workflowDef["dependencies"] = json::array();
    
    for (const auto& task : workflow.tasks) {
      json taskJson;
      taskJson["task_name"] = task.task_name;
      taskJson["task_type"] = repo.taskTypeToString(task.task_type);
      taskJson["task_reference"] = task.task_reference;
      taskJson["description"] = task.description;
      taskJson["task_config"] = task.task_config;
      taskJson["priority"] = task.priority;
      taskJson["condition_type"] = repo.conditionTypeToString(task.condition_type);
      taskJson["condition_expression"] = task.condition_expression;
      workflowDef["tasks"].push_back(taskJson);
    }
    
    for (const auto& dep : workflow.dependencies) {
      json depJson;
      depJson["upstream_task_name"] = dep.upstream_task_name;
      depJson["downstream_task_name"] = dep.downstream_task_name;
      depJson["dependency_type"] = repo.dependencyTypeToString(dep.dependency_type);
      workflowDef["dependencies"].push_back(depJson);
    }
    
    std::string workflowDefStr = workflowDef.dump();
    
    txn.exec_params(
        "INSERT INTO metadata.workflow_versions (workflow_name, version, description, "
        "created_by, is_current, workflow_definition) "
        "VALUES ($1, $2, $3, $4, true, $5::jsonb)",
        workflowName, newVersion, description, createdBy, workflowDefStr);
    
    txn.commit();
    
    std::lock_guard<std::mutex> lock(versionsMutex_);
    currentVersions_[workflowName] = newVersion;
    
    Logger::info(LogCategory::MONITORING, "createVersion",
                 "Created version " + std::to_string(newVersion) + " for workflow: " + workflowName);
    
    return newVersion;
  } catch (const std::exception& e) {
    Logger::error(LogCategory::MONITORING, "createVersion",
                  "Error creating version: " + std::string(e.what()));
    throw;
  }
}

std::vector<WorkflowVersionManager::WorkflowVersion> WorkflowVersionManager::getVersions(const std::string& workflowName) const {
  std::vector<WorkflowVersion> versions;
  
  try {
    std::string connStr = DatabaseConfig::getPostgresConnectionString();
    pqxx::connection conn(connStr);
    pqxx::work txn(conn);
    
    auto results = txn.exec_params(
        "SELECT version, workflow_name, description, created_at, created_by, "
        "is_current, workflow_definition "
        "FROM metadata.workflow_versions WHERE workflow_name = $1 ORDER BY version DESC",
        workflowName);
    
    for (const auto& row : results) {
      WorkflowVersion version;
      version.version = row[0].as<int>();
      version.workflow_name = row[1].as<std::string>();
      version.description = row[2].is_null() ? "" : row[2].as<std::string>();
      version.created_at = row[3].is_null() ? "" : row[3].as<std::string>();
      version.created_by = row[4].is_null() ? "" : row[4].as<std::string>();
      version.is_current = row[5].as<bool>();
      
      if (!row[6].is_null()) {
        try {
          version.workflow_definition = json::parse(row[6].as<std::string>());
        } catch (...) {
          version.workflow_definition = json{};
        }
      }
      
      versions.push_back(version);
    }
  } catch (const std::exception& e) {
    Logger::error(LogCategory::MONITORING, "getVersions",
                  "Error getting versions: " + std::string(e.what()));
  }
  
  return versions;
}

WorkflowVersionManager::WorkflowVersion WorkflowVersionManager::getVersion(const std::string& workflowName, int version) const {
  WorkflowVersion workflowVersion;
  
  try {
    std::string connStr = DatabaseConfig::getPostgresConnectionString();
    pqxx::connection conn(connStr);
    pqxx::work txn(conn);
    
    auto result = txn.exec_params(
        "SELECT version, workflow_name, description, created_at, created_by, "
        "is_current, workflow_definition "
        "FROM metadata.workflow_versions WHERE workflow_name = $1 AND version = $2",
        workflowName, version);
    
    if (!result.empty()) {
      const auto& row = result[0];
      workflowVersion.version = row[0].as<int>();
      workflowVersion.workflow_name = row[1].as<std::string>();
      workflowVersion.description = row[2].is_null() ? "" : row[2].as<std::string>();
      workflowVersion.created_at = row[3].is_null() ? "" : row[3].as<std::string>();
      workflowVersion.created_by = row[4].is_null() ? "" : row[4].as<std::string>();
      workflowVersion.is_current = row[5].as<bool>();
      
      if (!row[6].is_null()) {
        try {
          workflowVersion.workflow_definition = json::parse(row[6].as<std::string>());
        } catch (...) {
          workflowVersion.workflow_definition = json{};
        }
      }
    }
  } catch (const std::exception& e) {
    Logger::error(LogCategory::MONITORING, "getVersion",
                  "Error getting version: " + std::string(e.what()));
  }
  
  return workflowVersion;
}

bool WorkflowVersionManager::restoreVersion(const std::string& workflowName, int version) {
  try {
    WorkflowVersion workflowVersion = getVersion(workflowName, version);
    if (workflowVersion.workflow_name.empty()) {
      return false;
    }
    
    std::string connStr = DatabaseConfig::getPostgresConnectionString();
    WorkflowRepository repo(connStr);
    WorkflowModel workflow = repo.getWorkflow(workflowName);
    
    if (workflow.workflow_name.empty()) {
      return false;
    }
    
    if (workflowVersion.workflow_definition.contains("tasks")) {
      workflow.tasks.clear();
      for (const auto& taskJson : workflowVersion.workflow_definition["tasks"]) {
        WorkflowTask task;
        task.workflow_name = workflowName;
        task.task_name = taskJson["task_name"].get<std::string>();
        task.task_type = repo.stringToTaskType(taskJson["task_type"].get<std::string>());
        task.task_reference = taskJson["task_reference"].get<std::string>();
        task.description = taskJson.contains("description") ? taskJson["description"].get<std::string>() : "";
        task.task_config = taskJson.contains("task_config") ? taskJson["task_config"] : json{};
        task.priority = taskJson.contains("priority") ? taskJson["priority"].get<int>() : 0;
        task.condition_type = repo.stringToConditionType(
            taskJson.contains("condition_type") ? taskJson["condition_type"].get<std::string>() : "ALWAYS");
        task.condition_expression = taskJson.contains("condition_expression") ? 
            taskJson["condition_expression"].get<std::string>() : "";
        workflow.tasks.push_back(task);
      }
    }
    
    if (workflowVersion.workflow_definition.contains("dependencies")) {
      workflow.dependencies.clear();
      for (const auto& depJson : workflowVersion.workflow_definition["dependencies"]) {
        WorkflowDependency dep;
        dep.workflow_name = workflowName;
        dep.upstream_task_name = depJson["upstream_task_name"].get<std::string>();
        dep.downstream_task_name = depJson["downstream_task_name"].get<std::string>();
        dep.dependency_type = repo.stringToDependencyType(
            depJson["dependency_type"].get<std::string>());
        workflow.dependencies.push_back(dep);
      }
    }
    
    repo.insertOrUpdateWorkflow(workflow);
    
    std::string connStr2 = DatabaseConfig::getPostgresConnectionString();
    pqxx::connection conn(connStr2);
    pqxx::work txn(conn);
    txn.exec_params(
        "UPDATE metadata.workflow_versions SET is_current = false WHERE workflow_name = $1",
        workflowName);
    txn.exec_params(
        "UPDATE metadata.workflow_versions SET is_current = true WHERE workflow_name = $1 AND version = $2",
        workflowName, version);
    txn.commit();
    
    std::lock_guard<std::mutex> lock(versionsMutex_);
    currentVersions_[workflowName] = version;
    
    Logger::info(LogCategory::MONITORING, "restoreVersion",
                 "Restored version " + std::to_string(version) + " for workflow: " + workflowName);
    
    return true;
  } catch (const std::exception& e) {
    Logger::error(LogCategory::MONITORING, "restoreVersion",
                  "Error restoring version: " + std::string(e.what()));
    return false;
  }
}

bool WorkflowVersionManager::deleteVersion(const std::string& workflowName, int version) {
  try {
    std::string connStr = DatabaseConfig::getPostgresConnectionString();
    pqxx::connection conn(connStr);
    pqxx::work txn(conn);
    
    txn.exec_params(
        "DELETE FROM metadata.workflow_versions WHERE workflow_name = $1 AND version = $2",
        workflowName, version);
    txn.commit();
    
    Logger::info(LogCategory::MONITORING, "deleteVersion",
                 "Deleted version " + std::to_string(version) + " for workflow: " + workflowName);
    
    return true;
  } catch (const std::exception& e) {
    Logger::error(LogCategory::MONITORING, "deleteVersion",
                  "Error deleting version: " + std::string(e.what()));
    return false;
  }
}

int WorkflowVersionManager::getCurrentVersion(const std::string& workflowName) const {
  std::lock_guard<std::mutex> lock(versionsMutex_);
  auto it = currentVersions_.find(workflowName);
  if (it != currentVersions_.end()) {
    return it->second;
  }
  return 1;
}
