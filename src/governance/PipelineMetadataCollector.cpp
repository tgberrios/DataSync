#include "governance/PipelineMetadataCollector.h"
#include <pqxx/pqxx>
#include <sstream>
#include <iomanip>

PipelineMetadataCollector::PipelineMetadataCollector(
    const std::string& connectionString)
    : connectionString_(connectionString) {
}

PipelineMetadataCollector::PipelineMetadata 
PipelineMetadataCollector::collectWorkflowMetadata(const std::string& workflowName) {
  
  PipelineMetadata metadata;
  metadata.workflowName = workflowName;

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    // Obtener información del workflow
    auto workflowResult = txn.exec_params(
        "SELECT description, schedule, sla_config, created_at "
        "FROM metadata.workflows "
        "WHERE workflow_name = $1",
        workflowName
    );

    if (!workflowResult.empty()) {
      const auto& row = workflowResult[0];
      if (!row["description"].is_null()) {
        metadata.description = row["description"].as<std::string>();
      }
      if (!row["schedule"].is_null()) {
        metadata.schedule = row["schedule"].as<std::string>();
      }
      if (!row["sla_config"].is_null()) {
        metadata.slaConfig = json::parse(row["sla_config"].as<std::string>());
      }
      auto createdAtTimeT = row["created_at"].as<std::time_t>();
      metadata.createdAt = std::chrono::system_clock::from_time_t(createdAtTimeT);
    }

    // Obtener tareas
    auto tasksResult = txn.exec_params(
        "SELECT task_name, task_config, dependencies "
        "FROM metadata.workflow_tasks "
        "WHERE workflow_name = $1",
        workflowName
    );

    for (const auto& row : tasksResult) {
      std::string taskName = row["task_name"].as<std::string>();
      metadata.tasks.push_back(taskName);

      if (!row["task_config"].is_null()) {
        json taskConfig = json::parse(row["task_config"].as<std::string>());
        if (taskConfig.contains("description")) {
          metadata.taskDescriptions[taskName] = taskConfig["description"].get<std::string>();
        }
      }

      if (!row["dependencies"].is_null()) {
        // Parsear array manualmente
        std::string arrayStr = row["dependencies"].as<std::string>();
        std::vector<std::string> deps;
        if (!arrayStr.empty() && arrayStr != "{}") {
          std::string content = arrayStr;
          if (content.front() == '{' && content.back() == '}') {
            content = content.substr(1, content.length() - 2);
          }
          std::istringstream iss(content);
          std::string item;
          while (std::getline(iss, item, ',')) {
            if (!item.empty()) {
              if (item.front() == '"' && item.back() == '"') {
                item = item.substr(1, item.length() - 2);
              }
              deps.push_back(item);
            }
          }
        }
        metadata.dependencies[taskName] = deps;
      }
    }

    // Obtener estadísticas de ejecución
    auto statsResult = txn.exec_params(
        "SELECT COUNT(*) as total, "
        "COUNT(*) FILTER (WHERE status = 'SUCCESS') as successful, "
        "COUNT(*) FILTER (WHERE status = 'FAILED') as failed, "
        "AVG(EXTRACT(EPOCH FROM (completed_at - started_at)) * 1000) as avg_time, "
        "MAX(completed_at) as last_execution "
        "FROM metadata.workflow_executions "
        "WHERE workflow_name = $1",
        workflowName
    );

    if (!statsResult.empty()) {
      const auto& row = statsResult[0];
      metadata.totalExecutions = row["total"].as<int64_t>();
      metadata.successfulExecutions = row["successful"].as<int64_t>();
      metadata.failedExecutions = row["failed"].as<int64_t>();
      if (!row["avg_time"].is_null()) {
        metadata.averageExecutionTimeMs = row["avg_time"].as<double>();
      }
      if (!row["last_execution"].is_null()) {
        auto lastExecTimeT = row["last_execution"].as<std::time_t>();
        metadata.lastExecution = std::chrono::system_clock::from_time_t(lastExecTimeT);
      }
    }

    txn.commit();
  } catch (const std::exception& e) {
    Logger::error(LogCategory::GOVERNANCE, "PipelineMetadataCollector",
                  "Error collecting workflow metadata: " + std::string(e.what()));
  }

  return metadata;
}

PipelineMetadataCollector::ExecutionMetadata 
PipelineMetadataCollector::collectExecutionMetadata(int64_t executionId) {
  
  ExecutionMetadata metadata;
  metadata.executionId = executionId;

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    auto result = txn.exec_params1(
        "SELECT workflow_name, started_at, completed_at, status, "
        "EXTRACT(EPOCH FROM (completed_at - started_at)) * 1000 as execution_time "
        "FROM metadata.workflow_executions "
        "WHERE id = $1",
        executionId
    );

    metadata.workflowName = result["workflow_name"].as<std::string>();
    auto startedTimeT = result["started_at"].as<std::time_t>();
    metadata.startedAt = std::chrono::system_clock::from_time_t(startedTimeT);
    
    if (!result["completed_at"].is_null()) {
      auto completedTimeT = result["completed_at"].as<std::time_t>();
      metadata.completedAt = std::chrono::system_clock::from_time_t(completedTimeT);
    }

    metadata.status = result["status"].as<std::string>();
    if (!result["execution_time"].is_null()) {
      metadata.executionTimeMs = result["execution_time"].as<double>();
    }

    // Obtener tareas ejecutadas
    auto tasksResult = txn.exec_params(
        "SELECT task_name, status, started_at, completed_at, "
        "EXTRACT(EPOCH FROM (completed_at - started_at)) * 1000 as task_time, "
        "output_data, error_message "
        "FROM metadata.workflow_task_executions "
        "WHERE workflow_execution_id = $1",
        executionId
    );

    for (const auto& row : tasksResult) {
      std::string taskName = row["task_name"].as<std::string>();
      metadata.executedTasks.push_back(taskName);

      if (!row["task_time"].is_null()) {
        metadata.taskExecutionTimes[taskName] = row["task_time"].as<double>();
      }

      if (!row["output_data"].is_null()) {
        metadata.taskOutputs[taskName] = json::parse(row["output_data"].as<std::string>());
      }

      if (!row["error_message"].is_null() && metadata.status == "FAILED") {
        metadata.errorMessage = row["error_message"].as<std::string>();
      }
    }

    txn.commit();
  } catch (const std::exception& e) {
    Logger::error(LogCategory::GOVERNANCE, "PipelineMetadataCollector",
                  "Error collecting execution metadata: " + std::string(e.what()));
  }

  return metadata;
}

json PipelineMetadataCollector::collectExecutionStatistics(
    const std::string& workflowName,
    int days) {
  
  json stats;

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    auto result = txn.exec_params(
        "SELECT DATE(started_at) as execution_date, "
        "COUNT(*) as total, "
        "COUNT(*) FILTER (WHERE status = 'SUCCESS') as successful, "
        "COUNT(*) FILTER (WHERE status = 'FAILED') as failed, "
        "AVG(EXTRACT(EPOCH FROM (completed_at - started_at)) * 1000) as avg_time "
        "FROM metadata.workflow_executions "
        "WHERE workflow_name = $1 AND started_at >= NOW() - INTERVAL '" + 
        std::to_string(days) + " days' "
        "GROUP BY DATE(started_at) "
        "ORDER BY execution_date DESC",
        workflowName
    );

    json dailyStats = json::array();
    for (const auto& row : result) {
      json dayStat;
      dayStat["date"] = row["execution_date"].as<std::string>();
      dayStat["total"] = row["total"].as<int64_t>();
      dayStat["successful"] = row["successful"].as<int64_t>();
      dayStat["failed"] = row["failed"].as<int64_t>();
      if (!row["avg_time"].is_null()) {
        dayStat["avg_time_ms"] = row["avg_time"].as<double>();
      }
      dailyStats.push_back(dayStat);
    }

    stats["daily"] = dailyStats;

    txn.commit();
  } catch (const std::exception& e) {
    Logger::error(LogCategory::GOVERNANCE, "PipelineMetadataCollector",
                  "Error collecting execution statistics: " + std::string(e.what()));
  }

  return stats;
}

std::vector<json> PipelineMetadataCollector::collectTransformations(
    const std::string& workflowName,
    int64_t executionId) {
  
  std::vector<json> transformations;

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = 
        "SELECT transformation_type, transformation_config, "
        "input_tables, output_tables, rows_processed, execution_time_ms "
        "FROM metadata.transformation_lineage "
        "WHERE workflow_name = $1";

    std::vector<std::string> params = {workflowName};

    if (executionId > 0) {
      query += " AND workflow_execution_id = $2";
      params.push_back(std::to_string(executionId));
    }

    query += " ORDER BY executed_at ASC";

    auto result = txn.exec_params(query, params);
    for (const auto& row : result) {
      json trans;
      trans["type"] = row["transformation_type"].as<std::string>();
      trans["config"] = json::parse(row["transformation_config"].as<std::string>());
      trans["rows_processed"] = row["rows_processed"].as<int64_t>();
      trans["execution_time_ms"] = row["execution_time_ms"].as<double>();
      transformations.push_back(trans);
    }

    txn.commit();
  } catch (const std::exception& e) {
    Logger::error(LogCategory::GOVERNANCE, "PipelineMetadataCollector",
                  "Error collecting transformations: " + std::string(e.what()));
  }

  return transformations;
}
