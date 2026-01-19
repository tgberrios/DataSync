#include "sync/WorkflowExecutor.h"
#include <sstream>
#include <iomanip>
#include <random>
#include <algorithm>
#include <thread>

WorkflowExecutor::WorkflowExecutor(std::string metadataConnectionString)
    : metadataConnectionString_(std::move(metadataConnectionString)),
      workflowRepo_(std::make_unique<WorkflowRepository>(metadataConnectionString_)),
      customJobExecutor_(std::make_unique<CustomJobExecutor>(metadataConnectionString_)) {
  workflowRepo_->createTables();
}

WorkflowExecutor::~WorkflowExecutor() = default;

std::string WorkflowExecutor::generateExecutionId() {
  auto now = std::chrono::system_clock::now();
  auto timeT = std::chrono::system_clock::to_time_t(now);
  std::ostringstream oss;
  oss << std::put_time(std::gmtime(&timeT), "%Y%m%d_%H%M%S");
  
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis(1000, 9999);
  oss << "_" << dis(gen);
  
  return oss.str();
}

std::string WorkflowExecutor::getCurrentTimestamp() {
  auto now = std::chrono::system_clock::now();
  auto timeT = std::chrono::system_clock::to_time_t(now);
  std::ostringstream oss;
  oss << std::put_time(std::gmtime(&timeT), "%Y-%m-%d %H:%M:%S");
  return oss.str();
}

std::map<std::string, std::set<std::string>>
WorkflowExecutor::buildDependencyGraph(const WorkflowModel &workflow) {
  std::map<std::string, std::set<std::string>> graph;
  
  for (const auto &task : workflow.tasks) {
    graph[task.task_name] = std::set<std::string>();
  }
  
  for (const auto &dep : workflow.dependencies) {
    graph[dep.downstream_task_name].insert(dep.upstream_task_name);
  }
  
  return graph;
}

std::vector<std::string> WorkflowExecutor::getReadyTasks(
    const WorkflowModel &workflow,
    const std::map<std::string, std::set<std::string>> &dependencyGraph,
    const std::map<std::string, ExecutionStatus> &taskStatuses) {
  std::vector<std::string> readyTasks;
  
  for (const auto &task : workflow.tasks) {
    if (taskStatuses.find(task.task_name) != taskStatuses.end()) {
      continue;
    }
    
    bool allDependenciesMet = true;
    auto it = dependencyGraph.find(task.task_name);
    if (it != dependencyGraph.end()) {
      for (const auto &upstreamTask : it->second) {
        auto statusIt = taskStatuses.find(upstreamTask);
        if (statusIt == taskStatuses.end() ||
            (statusIt->second != ExecutionStatus::SUCCESS &&
             statusIt->second != ExecutionStatus::SKIPPED)) {
          allDependenciesMet = false;
          break;
        }
      }
    }
    
    if (allDependenciesMet) {
      readyTasks.push_back(task.task_name);
    }
  }
  
  return readyTasks;
}

bool WorkflowExecutor::shouldRetry(const WorkflowTask &task, int retryCount) {
  return retryCount < task.retry_policy.max_retries;
}

int WorkflowExecutor::calculateRetryDelay(const WorkflowTask &task, int retryCount) {
  int baseDelay = task.retry_policy.retry_delay_seconds;
  double multiplier = task.retry_policy.retry_backoff_multiplier;
  return static_cast<int>(baseDelay * std::pow(multiplier, retryCount));
}

bool WorkflowExecutor::checkSLA(const WorkflowModel &workflow,
                                const WorkflowExecution &execution) {
  if (execution.duration_seconds > workflow.sla_config.max_execution_time_seconds) {
    if (workflow.sla_config.alert_on_sla_breach) {
      Logger::warning(LogCategory::MONITORING, "checkSLA",
                      "SLA breach detected for workflow: " + workflow.workflow_name +
                      " (Duration: " + std::to_string(execution.duration_seconds) +
                      "s, Max: " + std::to_string(workflow.sla_config.max_execution_time_seconds) + "s)");
    }
    return false;
  }
  return true;
}

bool WorkflowExecutor::executeCustomJob(const std::string &jobName) {
  try {
    customJobExecutor_->executeJob(jobName);
    return true;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "executeCustomJob",
                  "Error executing custom job " + jobName + ": " + std::string(e.what()));
    return false;
  }
}

bool WorkflowExecutor::executeDataWarehouse(const std::string &warehouseName) {
  try {
    if (!warehouseBuilder_) {
      warehouseBuilder_ = std::make_unique<DataWarehouseBuilder>(metadataConnectionString_);
    }
    warehouseBuilder_->buildWarehouse(warehouseName);
    return true;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "executeDataWarehouse",
                  "Error executing data warehouse " + warehouseName + ": " + std::string(e.what()));
    return false;
  }
}

bool WorkflowExecutor::executeDataVault(const std::string &vaultName) {
  try {
    if (!vaultBuilder_) {
      vaultBuilder_ = std::make_unique<DataVaultBuilder>(metadataConnectionString_);
    }
    vaultBuilder_->buildVault(vaultName);
    return true;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "executeDataVault",
                  "Error executing data vault " + vaultName + ": " + std::string(e.what()));
    return false;
  }
}

bool WorkflowExecutor::executeSync(const json &syncConfig) {
  Logger::warning(LogCategory::TRANSFER, "executeSync",
                  "Sync task execution not yet implemented");
  return false;
}

bool WorkflowExecutor::executeAPICall(const json &apiConfig) {
  Logger::warning(LogCategory::TRANSFER, "executeAPICall",
                  "API call task execution not yet implemented");
  return false;
}

bool WorkflowExecutor::executeScript(const json &scriptConfig) {
  Logger::warning(LogCategory::TRANSFER, "executeScript",
                  "Script task execution not yet implemented");
  return false;
}

bool WorkflowExecutor::executeTask(const WorkflowModel &workflow,
                                   const WorkflowTask &task,
                                   int64_t workflowExecutionId,
                                   int64_t &taskExecutionId) {
  auto startTime = std::chrono::system_clock::now();
  std::string startTimeStr = getCurrentTimestamp();
  
  TaskExecution taskExecution;
  taskExecution.workflow_execution_id = workflowExecutionId;
  taskExecution.workflow_name = workflow.workflow_name;
  taskExecution.task_name = task.task_name;
  taskExecution.status = ExecutionStatus::RUNNING;
  taskExecution.start_time = startTimeStr;
  taskExecution.retry_count = 0;
  
  taskExecutionId = workflowRepo_->createTaskExecution(taskExecution);
  
  bool success = false;
  std::string errorMessage;
  
  try {
    switch (task.task_type) {
    case TaskType::CUSTOM_JOB:
      success = executeCustomJob(task.task_reference);
      break;
    case TaskType::DATA_WAREHOUSE:
      success = executeDataWarehouse(task.task_reference);
      break;
    case TaskType::DATA_VAULT:
      success = executeDataVault(task.task_reference);
      break;
    case TaskType::SYNC:
      success = executeSync(task.task_config);
      break;
    case TaskType::API_CALL:
      success = executeAPICall(task.task_config);
      break;
    case TaskType::SCRIPT:
      success = executeScript(task.task_config);
      break;
    default:
      errorMessage = "Unknown task type";
      success = false;
    }
  } catch (const std::exception &e) {
    errorMessage = e.what();
    success = false;
  }
  
  auto endTime = std::chrono::system_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::seconds>(endTime - startTime);
  std::string endTimeStr = getCurrentTimestamp();
  
  taskExecution.id = taskExecutionId;
  taskExecution.end_time = endTimeStr;
  taskExecution.duration_seconds = duration.count();
  taskExecution.status = success ? ExecutionStatus::SUCCESS : ExecutionStatus::FAILED;
  taskExecution.error_message = errorMessage;
  
  workflowRepo_->updateTaskExecution(taskExecution);
  
  return success;
}

void WorkflowExecutor::executeWorkflow(const std::string &workflowName,
                                       TriggerType triggerType) {
  WorkflowModel workflow = workflowRepo_->getWorkflow(workflowName);
  if (workflow.workflow_name.empty()) {
    throw std::runtime_error("Workflow not found: " + workflowName);
  }
  
  if (!workflow.active || !workflow.enabled) {
    throw std::runtime_error("Workflow is not active or enabled: " + workflowName);
  }
  
  std::string executionId = generateExecutionId();
  auto startTime = std::chrono::system_clock::now();
  std::string startTimeStr = getCurrentTimestamp();
  
  WorkflowExecution execution;
  execution.workflow_name = workflowName;
  execution.execution_id = executionId;
  execution.status = ExecutionStatus::RUNNING;
  execution.trigger_type = triggerType;
  execution.start_time = startTimeStr;
  execution.total_tasks = workflow.tasks.size();
  execution.completed_tasks = 0;
  execution.failed_tasks = 0;
  execution.skipped_tasks = 0;
  
  int64_t workflowExecutionId = workflowRepo_->createWorkflowExecution(execution);
  execution.id = workflowExecutionId;
  
  auto dependencyGraph = buildDependencyGraph(workflow);
  std::map<std::string, ExecutionStatus> taskStatuses;
  std::map<std::string, int> taskRetryCounts;
  
  bool workflowFailed = false;
  std::string workflowError;
  
  while (taskStatuses.size() < workflow.tasks.size() && !workflowFailed) {
    auto readyTasks = getReadyTasks(workflow, dependencyGraph, taskStatuses);
    
    if (readyTasks.empty()) {
      bool allCompleted = true;
      for (const auto &task : workflow.tasks) {
        if (taskStatuses.find(task.task_name) == taskStatuses.end()) {
          allCompleted = false;
          break;
        }
      }
      if (allCompleted) {
        break;
      }
      
      workflowFailed = true;
      workflowError = "Deadlock detected: no tasks can be executed";
      break;
    }
    
    std::vector<std::thread> threads;
    std::mutex statusMutex;
    
    for (const auto &taskName : readyTasks) {
      auto taskIt = std::find_if(workflow.tasks.begin(), workflow.tasks.end(),
                                 [&taskName](const WorkflowTask &t) {
                                   return t.task_name == taskName;
                                 });
      
      if (taskIt == workflow.tasks.end()) {
        continue;
      }
      
      threads.emplace_back([&, taskName, taskIt]() {
        int64_t taskExecutionId = 0;
        bool success = executeTask(workflow, *taskIt, workflowExecutionId, taskExecutionId);
        
        std::lock_guard<std::mutex> lock(statusMutex);
        if (success) {
          taskStatuses[taskName] = ExecutionStatus::SUCCESS;
          execution.completed_tasks++;
        } else {
          int retryCount = taskRetryCounts[taskName];
          if (shouldRetry(*taskIt, retryCount)) {
            taskRetryCounts[taskName]++;
            taskStatuses[taskName] = ExecutionStatus::RETRYING;
            std::this_thread::sleep_for(
                std::chrono::seconds(calculateRetryDelay(*taskIt, retryCount)));
            taskStatuses.erase(taskName);
          } else {
            taskStatuses[taskName] = ExecutionStatus::FAILED;
            execution.failed_tasks++;
            workflowFailed = true;
          }
        }
      });
    }
    
    for (auto &thread : threads) {
      thread.join();
    }
  }
  
  auto endTime = std::chrono::system_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::seconds>(endTime - startTime);
  std::string endTimeStr = getCurrentTimestamp();
  
  execution.end_time = endTimeStr;
  execution.duration_seconds = duration.count();
  execution.status = workflowFailed ? ExecutionStatus::FAILED : ExecutionStatus::SUCCESS;
  execution.error_message = workflowError;
  
  workflowRepo_->updateWorkflowExecution(execution);
  std::string statusStr = workflowRepo_->executionStatusToString(execution.status);
  workflowRepo_->updateLastExecution(workflowName, endTimeStr, statusStr);
  
  checkSLA(workflow, execution);
}

void WorkflowExecutor::executeWorkflowAsync(const std::string &workflowName,
                                             TriggerType triggerType) {
  std::thread([this, workflowName, triggerType]() {
    try {
      executeWorkflow(workflowName, triggerType);
    } catch (const std::exception &e) {
      Logger::error(LogCategory::MONITORING, "executeWorkflowAsync",
                    "Error executing workflow " + workflowName + ": " + std::string(e.what()));
    }
  }).detach();
}

std::vector<WorkflowExecution>
WorkflowExecutor::getWorkflowExecutions(const std::string &workflowName, int limit) {
  return workflowRepo_->getWorkflowExecutions(workflowName, limit);
}

WorkflowExecution WorkflowExecutor::getWorkflowExecution(const std::string &executionId) {
  return workflowRepo_->getWorkflowExecution(executionId);
}

std::vector<TaskExecution>
WorkflowExecutor::getTaskExecutions(int64_t workflowExecutionId) {
  return workflowRepo_->getTaskExecutions(workflowExecutionId);
}
