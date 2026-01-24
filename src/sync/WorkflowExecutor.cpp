#include "sync/WorkflowExecutor.h"
#include "sync/DistributedProcessingManager.h"
#include <regex>
#include <algorithm>
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
  
  // Inicializar DistributedProcessingManager
  DistributedProcessingManager::ProcessingConfig distConfig;
  distConfig.sparkConfig.appName = "DataSync-Workflow";
  distConfig.sparkConfig.masterUrl = "local[*]"; // Default, puede ser configurado
  distributedManager_ = std::make_unique<DistributedProcessingManager>(distConfig);
  distributedManager_->initialize();
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
  std::map<std::string, json> taskOutputs;
  return getReadyTasks(workflow, dependencyGraph, taskStatuses, taskOutputs);
}

std::vector<std::string> WorkflowExecutor::getReadyTasks(
    const WorkflowModel &workflow,
    const std::map<std::string, std::set<std::string>> &dependencyGraph,
    const std::map<std::string, ExecutionStatus> &taskStatuses,
    const std::map<std::string, json> &taskOutputs) {
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
    
    if (allDependenciesMet && shouldExecuteTask(task, workflow, taskStatuses, taskOutputs)) {
      readyTasks.push_back(task.task_name);
    }
  }
  
  return sortTasksByPriority(readyTasks, workflow);
}

bool WorkflowExecutor::shouldRetry(const WorkflowTask &task, int retryCount) {
  return retryCount < task.retry_policy.max_retries;
}

int WorkflowExecutor::calculateRetryDelay(const WorkflowTask &task, int retryCount) {
  int baseDelay = task.retry_policy.retry_delay_seconds;
  double multiplier = task.retry_policy.retry_backoff_multiplier;
  return static_cast<int>(baseDelay * std::pow(multiplier, retryCount));
}

bool WorkflowExecutor::evaluateCondition(
    const std::string &conditionExpression,
    const WorkflowModel &workflow,
    const std::map<std::string, ExecutionStatus> &taskStatuses,
    const std::map<std::string, json> &taskOutputs) {
  if (conditionExpression.empty()) {
    return true;
  }
  
  std::string expr = conditionExpression;
  
  // Replace task status variables: ${task_name.status}
  std::regex statusRegex(R"(\$\{([^}]+)\.status\})");
  std::smatch matches;
  
  while (std::regex_search(expr, matches, statusRegex)) {
    std::string taskName = matches[1].str();
    auto statusIt = taskStatuses.find(taskName);
    std::string statusValue = "UNKNOWN";
    
    if (statusIt != taskStatuses.end()) {
      statusValue = workflowRepo_->executionStatusToString(statusIt->second);
    }
    
    expr.replace(matches.position(), matches.length(), "\"" + statusValue + "\"");
  }
  
  // Replace task output variables: ${task_name.output.field}
  std::regex outputRegex(R"(\$\{([^}]+)\.output\.([^}]+)\})");
  while (std::regex_search(expr, matches, outputRegex)) {
    std::string taskName = matches[1].str();
    std::string field = matches[2].str();
    std::string outputValue = "null";
    
    auto outputIt = taskOutputs.find(taskName);
    if (outputIt != taskOutputs.end() && outputIt->second.contains(field)) {
      outputValue = outputIt->second[field].dump();
    }
    
    expr.replace(matches.position(), matches.length(), outputValue);
  }
  
  // Simple expression evaluator (supports basic comparisons)
  // For production, use a proper expression parser library
  try {
    // Check for ==, !=, >, <, >=, <=
    if (expr.find("==") != std::string::npos) {
      size_t pos = expr.find("==");
      std::string left = expr.substr(0, pos);
      std::string right = expr.substr(pos + 2);
      // Trim and compare
      left.erase(0, left.find_first_not_of(" \t"));
      left.erase(left.find_last_not_of(" \t") + 1);
      right.erase(0, right.find_first_not_of(" \t"));
      right.erase(right.find_last_not_of(" \t") + 1);
      return left == right;
    } else if (expr.find("!=") != std::string::npos) {
      size_t pos = expr.find("!=");
      std::string left = expr.substr(0, pos);
      std::string right = expr.substr(pos + 2);
      left.erase(0, left.find_first_not_of(" \t"));
      left.erase(left.find_last_not_of(" \t") + 1);
      right.erase(0, right.find_first_not_of(" \t"));
      right.erase(right.find_last_not_of(" \t") + 1);
      return left != right;
    }
    
    // Default: evaluate as boolean
    return expr == "true" || expr == "1" || expr == "\"SUCCESS\"";
  } catch (...) {
    Logger::warning(LogCategory::MONITORING, "evaluateCondition",
                    "Error evaluating condition: " + conditionExpression);
    return false;
  }
}

bool WorkflowExecutor::shouldExecuteTask(
    const WorkflowTask &task,
    const WorkflowModel &workflow,
    const std::map<std::string, ExecutionStatus> &taskStatuses,
    const std::map<std::string, json> &taskOutputs) {
  if (task.condition_type == ConditionType::ALWAYS) {
    return true;
  }
  
  if (task.condition_type == ConditionType::IF) {
    return evaluateCondition(task.condition_expression, workflow, taskStatuses, taskOutputs);
  }
  
  if (task.condition_type == ConditionType::ELSE || task.condition_type == ConditionType::ELSE_IF) {
    if (task.parent_condition_task_name.empty()) {
      return false;
    }
    
    auto parentStatusIt = taskStatuses.find(task.parent_condition_task_name);
    if (parentStatusIt == taskStatuses.end()) {
      return false;
    }
    
    bool parentExecuted = (parentStatusIt->second == ExecutionStatus::SUCCESS ||
                          parentStatusIt->second == ExecutionStatus::FAILED ||
                          parentStatusIt->second == ExecutionStatus::SKIPPED);
    
    if (!parentExecuted) {
      return false;
    }
    
    // Check if any IF/ELSE_IF in the same block was executed
    bool anySiblingExecuted = false;
    for (const auto &t : workflow.tasks) {
      if (t.parent_condition_task_name == task.parent_condition_task_name &&
          (t.condition_type == ConditionType::IF || t.condition_type == ConditionType::ELSE_IF) &&
          taskStatuses.find(t.task_name) != taskStatuses.end() &&
          taskStatuses.at(t.task_name) == ExecutionStatus::SUCCESS) {
        anySiblingExecuted = true;
        break;
      }
    }
    
    if (task.condition_type == ConditionType::ELSE) {
      return !anySiblingExecuted;
    } else {
      return !anySiblingExecuted && evaluateCondition(task.condition_expression, workflow, taskStatuses, taskOutputs);
    }
  }
  
  return true;
}

std::vector<std::string> WorkflowExecutor::sortTasksByPriority(
    const std::vector<std::string> &taskNames,
    const WorkflowModel &workflow) {
  std::vector<std::pair<std::string, int>> tasksWithPriority;
  
  for (const auto &taskName : taskNames) {
    auto taskIt = std::find_if(workflow.tasks.begin(), workflow.tasks.end(),
                               [&taskName](const WorkflowTask &t) {
                                 return t.task_name == taskName;
                               });
    if (taskIt != workflow.tasks.end()) {
      tasksWithPriority.push_back({taskName, taskIt->priority});
    } else {
      tasksWithPriority.push_back({taskName, 0});
    }
  }
  
  std::sort(tasksWithPriority.begin(), tasksWithPriority.end(),
            [](const std::pair<std::string, int> &a, const std::pair<std::string, int> &b) {
              return a.second > b.second;
            });
  
  std::vector<std::string> sortedTasks;
  for (const auto &[taskName, priority] : tasksWithPriority) {
    sortedTasks.push_back(taskName);
  }
  
  return sortedTasks;
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

bool WorkflowExecutor::executeSubWorkflow(const std::string &subWorkflowName) {
  try {
    executeWorkflow(subWorkflowName, TriggerType::MANUAL);
    return true;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "executeSubWorkflow",
                  "Error executing sub-workflow " + subWorkflowName + ": " + std::string(e.what()));
    return false;
  }
}

bool WorkflowExecutor::executeTaskWithLoop(const WorkflowModel &workflow, const WorkflowTask &task,
                                          int64_t workflowExecutionId, int64_t &taskExecutionId,
                                          const std::map<std::string, ExecutionStatus> &taskStatuses,
                                          const std::map<std::string, json> &taskOutputs) {
  if (task.loop_type == LoopType::FOR) {
    int iterations = 1;
    if (task.loop_config.contains("iterations")) {
      iterations = task.loop_config["iterations"].get<int>();
    }
    
    bool allSuccess = true;
    for (int i = 0; i < iterations; i++) {
      int64_t loopTaskExecutionId = 0;
      bool success = executeTask(workflow, task, workflowExecutionId, loopTaskExecutionId);
      if (!success) {
        allSuccess = false;
        if (task.loop_config.contains("stop_on_error") && task.loop_config["stop_on_error"].get<bool>()) {
          break;
        }
      }
      
      if (task.loop_config.contains("delay_seconds")) {
        int delay = task.loop_config["delay_seconds"].get<int>();
        if (delay > 0 && i < iterations - 1) {
          std::this_thread::sleep_for(std::chrono::seconds(delay));
        }
      }
    }
    
    return allSuccess;
  } else if (task.loop_type == LoopType::WHILE) {
    std::string conditionExpr = "";
    if (task.loop_config.contains("condition")) {
      conditionExpr = task.loop_config["condition"].get<std::string>();
    }
    
    int maxIterations = 1000;
    if (task.loop_config.contains("max_iterations")) {
      maxIterations = task.loop_config["max_iterations"].get<int>();
    }
    
    int iteration = 0;
    bool allSuccess = true;
    
    while (iteration < maxIterations) {
      if (!conditionExpr.empty()) {
        bool conditionMet = evaluateCondition(conditionExpr, workflow, taskStatuses, taskOutputs);
        if (!conditionMet) {
          break;
        }
      }
      
      int64_t loopTaskExecutionId = 0;
      bool success = executeTask(workflow, task, workflowExecutionId, loopTaskExecutionId);
      if (!success) {
        allSuccess = false;
        if (task.loop_config.contains("stop_on_error") && task.loop_config["stop_on_error"].get<bool>()) {
          break;
        }
      }
      
      iteration++;
      
      if (task.loop_config.contains("delay_seconds")) {
        int delay = task.loop_config["delay_seconds"].get<int>();
        if (delay > 0) {
          std::this_thread::sleep_for(std::chrono::seconds(delay));
        }
      }
    }
    
    return allSuccess;
  } else if (task.loop_type == LoopType::FOREACH) {
    std::vector<json> items;
    if (task.loop_config.contains("items")) {
      items = task.loop_config["items"].get<std::vector<json>>();
    } else if (task.loop_config.contains("item_source")) {
      std::string sourceTask = task.loop_config["item_source"].get<std::string>();
      auto outputIt = taskOutputs.find(sourceTask);
      if (outputIt != taskOutputs.end() && outputIt->second.contains("items")) {
        items = outputIt->second["items"].get<std::vector<json>>();
      }
    }
    
    bool allSuccess = true;
    for (size_t i = 0; i < items.size(); i++) {
      WorkflowTask loopTask = task;
      loopTask.task_config["loop_item"] = items[i];
      loopTask.task_config["loop_index"] = i;
      
      int64_t loopTaskExecutionId = 0;
      bool success = executeTask(workflow, loopTask, workflowExecutionId, loopTaskExecutionId);
      if (!success) {
        allSuccess = false;
        if (task.loop_config.contains("stop_on_error") && task.loop_config["stop_on_error"].get<bool>()) {
          break;
        }
      }
      
      if (task.loop_config.contains("delay_seconds")) {
        int delay = task.loop_config["delay_seconds"].get<int>();
        if (delay > 0 && i < items.size() - 1) {
          std::this_thread::sleep_for(std::chrono::seconds(delay));
        }
      }
    }
    
    return allSuccess;
  }
  
  return executeTask(workflow, task, workflowExecutionId, taskExecutionId);
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
    // Verificar si la tarea tiene configuración de procesamiento distribuido
    bool useDistributed = false;
    if (task.task_config.contains("use_distributed")) {
      useDistributed = task.task_config["use_distributed"].get<bool>();
    } else if (task.task_config.contains("processing_mode")) {
      std::string mode = task.task_config["processing_mode"].get<std::string>();
      useDistributed = (mode == "distributed" || mode == "auto");
    }
    
    // Si se requiere procesamiento distribuido y está disponible
    if (useDistributed && distributedManager_ && distributedManager_->isSparkAvailable()) {
      DistributedProcessingManager::ProcessingTask distTask;
      distTask.taskId = task.task_name + "_" + std::to_string(taskExecutionId);
      distTask.taskType = task.task_type == TaskType::DATA_WAREHOUSE ? "warehouse_build" :
                         task.task_type == TaskType::SYNC ? "sync" : "custom";
      distTask.config = task.task_config;
      
      // Estimar tamaño si está disponible en config
      if (task.task_config.contains("estimated_rows")) {
        distTask.estimatedRows = task.task_config["estimated_rows"].get<int64_t>();
      }
      if (task.task_config.contains("estimated_size_mb")) {
        distTask.estimatedSizeMB = task.task_config["estimated_size_mb"].get<int64_t>();
      }
      
      auto distResult = distributedManager_->executeTask(distTask);
      success = distResult.success;
      errorMessage = distResult.errorMessage;
      
      // Guardar metadata del resultado
      if (taskExecutionId > 0) {
        json taskOutput = json::object();
        taskOutput["execution_mode"] = distResult.executionMode;
        taskOutput["rows_processed"] = distResult.rowsProcessed;
        taskOutput["metadata"] = distResult.metadata;
        
        TaskExecution te;
        te.id = taskExecutionId;
        te.task_output = taskOutput;
        workflowRepo_->updateTaskExecution(te);
      }
    } else {
      // Ejecución normal (local o sin procesamiento distribuido)
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
  std::map<std::string, json> taskOutputs;
  std::map<std::string, int> taskRetryCounts;
  
  bool workflowFailed = false;
  std::string workflowError;
  
  while (taskStatuses.size() < workflow.tasks.size() && !workflowFailed) {
    auto readyTasks = getReadyTasks(workflow, dependencyGraph, taskStatuses, taskOutputs);
    
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
        bool success = false;
        
        if (taskIt->loop_type == LoopType::FOR || taskIt->loop_type == LoopType::WHILE || taskIt->loop_type == LoopType::FOREACH) {
          success = executeTaskWithLoop(workflow, *taskIt, workflowExecutionId, taskExecutionId, taskStatuses, taskOutputs);
        } else {
          success = executeTask(workflow, *taskIt, workflowExecutionId, taskExecutionId);
        }
        
        json taskOutput = json::object();
        if (taskExecutionId > 0) {
          auto taskExecutions = workflowRepo_->getTaskExecutions(workflowExecutionId);
          for (const auto &te : taskExecutions) {
            if (te.task_name == taskName) {
              taskOutput = te.task_output;
              break;
            }
          }
        }
        
        std::lock_guard<std::mutex> lock(statusMutex);
        if (success) {
          taskStatuses[taskName] = ExecutionStatus::SUCCESS;
          taskOutputs[taskName] = taskOutput;
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
            taskOutputs[taskName] = taskOutput;
            execution.failed_tasks++;
            
            if (taskIt->condition_type == ConditionType::IF ||
                taskIt->condition_type == ConditionType::ELSE_IF) {
              // Conditional task failed, but don't fail entire workflow
              taskStatuses[taskName] = ExecutionStatus::SKIPPED;
              execution.skipped_tasks++;
            } else {
              workflowFailed = true;
            }
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
  
  if (shouldRollback(workflow, execution)) {
    Logger::info(LogCategory::MONITORING, "executeWorkflow",
                 "Initiating rollback for workflow: " + workflowName + ", execution: " + executionId);
    rollbackWorkflow(workflowName, executionId);
  }
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

bool WorkflowExecutor::shouldRollback(const WorkflowModel &workflow, const WorkflowExecution &execution) {
  if (!workflow.rollback_config.enabled) {
    return false;
  }
  
  if (workflow.rollback_config.on_failure && execution.status == ExecutionStatus::FAILED) {
    return true;
  }
  
  if (workflow.rollback_config.on_timeout) {
    if (execution.duration_seconds > workflow.sla_config.max_execution_time_seconds) {
      return true;
    }
  }
  
  return false;
}

void WorkflowExecutor::rollbackWorkflow(const std::string &workflowName, const std::string &executionId) {
  try {
    WorkflowExecution execution = workflowRepo_->getWorkflowExecution(executionId);
    if (execution.execution_id.empty()) {
      Logger::error(LogCategory::MONITORING, "rollbackWorkflow",
                    "Execution not found: " + executionId);
      return;
    }
    
    WorkflowModel workflow = workflowRepo_->getWorkflow(workflowName);
    if (workflow.workflow_name.empty()) {
      Logger::error(LogCategory::MONITORING, "rollbackWorkflow",
                    "Workflow not found: " + workflowName);
      return;
    }
    
    execution.rollback_status = RollbackStatus::IN_PROGRESS;
    execution.rollback_started_at = getCurrentTimestamp();
    workflowRepo_->updateWorkflowExecution(execution);
    
    std::vector<TaskExecution> taskExecutions = workflowRepo_->getTaskExecutions(execution.id);
    
    std::sort(taskExecutions.begin(), taskExecutions.end(),
              [](const TaskExecution &a, const TaskExecution &b) {
                return a.id > b.id;
              });
    
    int rollbackDepth = 0;
    int maxDepth = workflow.rollback_config.max_rollback_depth;
    
    for (const auto &taskExec : taskExecutions) {
      if (rollbackDepth >= maxDepth) {
        break;
      }
      
      if (taskExec.status == ExecutionStatus::SUCCESS) {
        auto taskIt = std::find_if(workflow.tasks.begin(), workflow.tasks.end(),
                                   [&taskExec](const WorkflowTask &t) {
                                     return t.task_name == taskExec.task_name;
                                   });
        
        if (taskIt != workflow.tasks.end()) {
          json rollbackConfig = taskIt->task_config.contains("rollback") 
                                ? taskIt->task_config["rollback"] 
                                : json{};
          
          if (!rollbackTask(*taskIt, taskExec, rollbackConfig)) {
            Logger::warning(LogCategory::MONITORING, "rollbackWorkflow",
                           "Failed to rollback task: " + taskExec.task_name);
          }
          rollbackDepth++;
        }
      }
    }
    
    execution.rollback_status = RollbackStatus::COMPLETED;
    execution.rollback_completed_at = getCurrentTimestamp();
    workflowRepo_->updateWorkflowExecution(execution);
    
    Logger::info(LogCategory::MONITORING, "rollbackWorkflow",
                 "Rollback completed for workflow: " + workflowName + ", execution: " + executionId);
  } catch (const std::exception &e) {
    try {
      WorkflowExecution execution = workflowRepo_->getWorkflowExecution(executionId);
      execution.rollback_status = RollbackStatus::FAILED;
      execution.rollback_error_message = e.what();
      execution.rollback_completed_at = getCurrentTimestamp();
      workflowRepo_->updateWorkflowExecution(execution);
    } catch (...) {
    }
    
    Logger::error(LogCategory::MONITORING, "rollbackWorkflow",
                  "Error during rollback for workflow " + workflowName + ": " + std::string(e.what()));
  }
}

bool WorkflowExecutor::rollbackTask(const WorkflowTask &task, const TaskExecution &, const json &rollbackConfig) {
  try {
    if (rollbackConfig.empty()) {
      Logger::info(LogCategory::MONITORING, "rollbackTask",
                   "No rollback configuration for task: " + task.task_name);
      return true;
    }
    
    if (task.task_type == TaskType::DATA_WAREHOUSE || task.task_type == TaskType::DATA_VAULT) {
      Logger::info(LogCategory::MONITORING, "rollbackTask",
                   "Rollback for data warehouse/vault tasks not yet implemented: " + task.task_name);
      return true;
    }
    
    if (task.task_type == TaskType::CUSTOM_JOB) {
      Logger::info(LogCategory::MONITORING, "rollbackTask",
                   "Rollback for custom job tasks not yet implemented: " + task.task_name);
      return true;
    }
    
    Logger::info(LogCategory::MONITORING, "rollbackTask",
                 "Rollback completed for task: " + task.task_name);
    return true;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::MONITORING, "rollbackTask",
                  "Error rolling back task " + task.task_name + ": " + std::string(e.what()));
    return false;
  }
}
