#ifndef WORKFLOW_EXECUTOR_H
#define WORKFLOW_EXECUTOR_H

#include "catalog/workflow_repository.h"
#include "catalog/custom_jobs_repository.h"
#include "sync/CustomJobExecutor.h"
#include "sync/DataWarehouseBuilder.h"
#include "sync/DataVaultBuilder.h"
#include "core/logger.h"
#include <map>
#include <set>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <chrono>
#include <memory>

class WorkflowExecutor {
  std::string metadataConnectionString_;
  std::unique_ptr<WorkflowRepository> workflowRepo_;
  std::unique_ptr<CustomJobExecutor> customJobExecutor_;
  std::unique_ptr<DataWarehouseBuilder> warehouseBuilder_;
  std::unique_ptr<DataVaultBuilder> vaultBuilder_;

  std::map<std::string, std::set<std::string>> buildDependencyGraph(
      const WorkflowModel &workflow);
  std::vector<std::string> getReadyTasks(
      const WorkflowModel &workflow,
      const std::map<std::string, std::set<std::string>> &dependencyGraph,
      const std::map<std::string, ExecutionStatus> &taskStatuses);
  
  bool executeTask(const WorkflowModel &workflow, const WorkflowTask &task,
                   int64_t workflowExecutionId, int64_t &taskExecutionId);
  bool executeCustomJob(const std::string &jobName);
  bool executeDataWarehouse(const std::string &warehouseName);
  bool executeDataVault(const std::string &vaultName);
  bool executeSync(const json &syncConfig);
  bool executeAPICall(const json &apiConfig);
  bool executeScript(const json &scriptConfig);

  bool shouldRetry(const WorkflowTask &task, int retryCount);
  int calculateRetryDelay(const WorkflowTask &task, int retryCount);
  bool checkSLA(const WorkflowModel &workflow, const WorkflowExecution &execution);
  
  std::string generateExecutionId();
  std::string getCurrentTimestamp();

public:
  explicit WorkflowExecutor(std::string metadataConnectionString);
  ~WorkflowExecutor();

  WorkflowExecutor(const WorkflowExecutor &) = delete;
  WorkflowExecutor &operator=(const WorkflowExecutor &) = delete;

  void executeWorkflow(const std::string &workflowName, TriggerType triggerType = TriggerType::MANUAL);
  void executeWorkflowAsync(const std::string &workflowName, TriggerType triggerType = TriggerType::MANUAL);
  
  std::vector<WorkflowExecution> getWorkflowExecutions(const std::string &workflowName, int limit = 50);
  WorkflowExecution getWorkflowExecution(const std::string &executionId);
  std::vector<TaskExecution> getTaskExecutions(int64_t workflowExecutionId);
};

#endif
