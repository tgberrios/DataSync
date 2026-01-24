#ifndef PIPELINE_METADATA_COLLECTOR_H
#define PIPELINE_METADATA_COLLECTOR_H

#include "third_party/json.hpp"
#include "core/logger.h"
#include <string>
#include <vector>
#include <map>
#include <chrono>

using json = nlohmann::json;

// PipelineMetadataCollector: Recopila metadata de pipelines y ejecuciones
class PipelineMetadataCollector {
public:
  struct PipelineMetadata {
    std::string workflowName;
    std::string description;
    std::vector<std::string> tasks;
    std::map<std::string, std::string> taskDescriptions;
    std::map<std::string, std::vector<std::string>> dependencies;
    std::string schedule;
    json slaConfig;
    int64_t totalExecutions{0};
    int64_t successfulExecutions{0};
    int64_t failedExecutions{0};
    double averageExecutionTimeMs{0.0};
    std::chrono::system_clock::time_point lastExecution;
    std::chrono::system_clock::time_point createdAt;
  };

  struct ExecutionMetadata {
    int64_t executionId;
    std::string workflowName;
    std::chrono::system_clock::time_point startedAt;
    std::chrono::system_clock::time_point completedAt;
    std::string status;  // "success", "failed", "running", "cancelled"
    double executionTimeMs{0.0};
    std::vector<std::string> executedTasks;
    std::map<std::string, double> taskExecutionTimes;
    json taskOutputs;
    std::string errorMessage;
  };

  explicit PipelineMetadataCollector(const std::string& connectionString);
  ~PipelineMetadataCollector() = default;

  // Recopilar metadata de un workflow
  PipelineMetadata collectWorkflowMetadata(const std::string& workflowName);

  // Recopilar metadata de una ejecución
  ExecutionMetadata collectExecutionMetadata(int64_t executionId);

  // Recopilar estadísticas de ejecución
  json collectExecutionStatistics(
      const std::string& workflowName,
      int days = 30
  );

  // Recopilar todas las transformaciones aplicadas en un workflow
  std::vector<json> collectTransformations(
      const std::string& workflowName,
      int64_t executionId = 0
  );

private:
  std::string connectionString_;
};

#endif // PIPELINE_METADATA_COLLECTOR_H
