#ifndef WORKFLOW_REPOSITORY_H
#define WORKFLOW_REPOSITORY_H

#include "third_party/json.hpp"
#include <pqxx/pqxx>
#include <string>
#include <vector>
#include <map>

using json = nlohmann::json;

enum class TaskType {
  CUSTOM_JOB,
  DATA_WAREHOUSE,
  DATA_VAULT,
  SYNC,
  API_CALL,
  SCRIPT,
  SUB_WORKFLOW
};

enum class DependencyType {
  SUCCESS,
  COMPLETION,
  SKIP_ON_FAILURE
};

enum class ConditionType {
  ALWAYS,
  IF,
  ELSE,
  ELSE_IF
};

enum class LoopType {
  FOR,
  WHILE,
  FOREACH
};

enum class ExecutionStatus {
  PENDING,
  RUNNING,
  SUCCESS,
  FAILED,
  CANCELLED,
  SKIPPED,
  RETRYING
};

enum class TriggerType {
  SCHEDULED,
  MANUAL,
  API,
  EVENT
};

struct RetryPolicy {
  int max_retries = 3;
  int retry_delay_seconds = 60;
  double retry_backoff_multiplier = 2.0;
};

struct SLAConfig {
  int max_execution_time_seconds = 3600;
  bool alert_on_sla_breach = true;
};

struct RollbackConfig {
  bool enabled = false;
  bool on_failure = true;
  bool on_timeout = false;
  int max_rollback_depth = 10;
};

struct WorkflowTask {
  int id;
  std::string workflow_name;
  std::string task_name;
  TaskType task_type;
  std::string task_reference;
  std::string description;
  json task_config;
  RetryPolicy retry_policy;
  int position_x = 0;
  int position_y = 0;
  json metadata;
  int priority = 0;
  ConditionType condition_type = ConditionType::ALWAYS;
  std::string condition_expression;
  std::string parent_condition_task_name;
  LoopType loop_type = LoopType::FOR;
  json loop_config;
  std::string created_at;
  std::string updated_at;
};

struct WorkflowDependency {
  int id;
  std::string workflow_name;
  std::string upstream_task_name;
  std::string downstream_task_name;
  DependencyType dependency_type = DependencyType::SUCCESS;
  std::string condition_expression;
  std::string created_at;
};

struct WorkflowModel {
  int id;
  std::string workflow_name;
  std::string description;
  std::string schedule_cron;
  bool active = true;
  bool enabled = true;
  RetryPolicy retry_policy;
  SLAConfig sla_config;
  RollbackConfig rollback_config;
  json metadata;
  std::string created_at;
  std::string updated_at;
  std::string last_execution_time;
  std::string last_execution_status;
  std::vector<WorkflowTask> tasks;
  std::vector<WorkflowDependency> dependencies;
};

enum class RollbackStatus {
  PENDING,
  IN_PROGRESS,
  COMPLETED,
  FAILED
};

struct WorkflowExecution {
  int64_t id;
  std::string workflow_name;
  std::string execution_id;
  ExecutionStatus status = ExecutionStatus::PENDING;
  TriggerType trigger_type = TriggerType::SCHEDULED;
  std::string start_time;
  std::string end_time;
  int duration_seconds = 0;
  int total_tasks = 0;
  int completed_tasks = 0;
  int failed_tasks = 0;
  int skipped_tasks = 0;
  std::string error_message;
  RollbackStatus rollback_status = RollbackStatus::PENDING;
  std::string rollback_started_at;
  std::string rollback_completed_at;
  std::string rollback_error_message;
  json metadata;
  std::string created_at;
};

struct TaskExecution {
  int64_t id;
  int64_t workflow_execution_id;
  std::string workflow_name;
  std::string task_name;
  ExecutionStatus status = ExecutionStatus::PENDING;
  std::string start_time;
  std::string end_time;
  int duration_seconds = 0;
  int retry_count = 0;
  std::string error_message;
  json task_output;
  json metadata;
  std::string created_at;
};

class WorkflowRepository {
  std::string connectionString_;

public:
  explicit WorkflowRepository(std::string connectionString);

  void createTables();
  std::vector<WorkflowModel> getAllWorkflows();
  std::vector<WorkflowModel> getActiveWorkflows();
  WorkflowModel getWorkflow(const std::string &workflowName);
  void insertOrUpdateWorkflow(const WorkflowModel &workflow);
  void deleteWorkflow(const std::string &workflowName);
  void updateWorkflowActive(const std::string &workflowName, bool active);
  void updateWorkflowEnabled(const std::string &workflowName, bool enabled);
  void updateLastExecution(const std::string &workflowName,
                          const std::string &executionTime,
                          const std::string &status);

  std::vector<WorkflowExecution> getWorkflowExecutions(
      const std::string &workflowName, int limit = 50);
  WorkflowExecution getWorkflowExecution(const std::string &executionId);
  int64_t createWorkflowExecution(const WorkflowExecution &execution);
  void updateWorkflowExecution(const WorkflowExecution &execution);

  std::vector<TaskExecution> getTaskExecutions(int64_t workflowExecutionId);
  int64_t createTaskExecution(const TaskExecution &execution);
  void updateTaskExecution(const TaskExecution &execution);

  std::string taskTypeToString(TaskType type);
  TaskType stringToTaskType(const std::string &str);
  std::string dependencyTypeToString(DependencyType type);
  DependencyType stringToDependencyType(const std::string &str);
  std::string executionStatusToString(ExecutionStatus status);
  ExecutionStatus stringToExecutionStatus(const std::string &str);
  std::string triggerTypeToString(TriggerType type);
  TriggerType stringToTriggerType(const std::string &str);
  std::string conditionTypeToString(ConditionType type);
  ConditionType stringToConditionType(const std::string &str);
  std::string loopTypeToString(LoopType type);
  LoopType stringToLoopType(const std::string &str);

private:
  pqxx::connection getConnection();
  WorkflowModel rowToWorkflow(const pqxx::row &row);
  WorkflowTask rowToTask(const pqxx::row &row);
  WorkflowDependency rowToDependency(const pqxx::row &row);
  WorkflowExecution rowToExecution(const pqxx::row &row);
  TaskExecution rowToTaskExecution(const pqxx::row &row);
  RetryPolicy parseRetryPolicy(const json &j);
  SLAConfig parseSLAConfig(const json &j);
  RollbackConfig parseRollbackConfig(const json &j);
  json retryPolicyToJson(const RetryPolicy &policy);
  json slaConfigToJson(const SLAConfig &config);
  json rollbackConfigToJson(const RollbackConfig &config);
  std::string rollbackStatusToString(RollbackStatus status);
};

#endif
