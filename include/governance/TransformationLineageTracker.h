#ifndef TRANSFORMATION_LINEAGE_TRACKER_H
#define TRANSFORMATION_LINEAGE_TRACKER_H

#include "third_party/json.hpp"
#include "core/logger.h"
#include <string>
#include <vector>
#include <map>
#include <chrono>
#include <memory>

using json = nlohmann::json;

// TransformationLineageTracker: Tracking de lineage de transformaciones aplicadas
class TransformationLineageTracker {
public:
  struct TransformationRecord {
    std::string transformationId;
    std::string transformationType;
    json transformationConfig;
    std::string workflowName;
    std::string taskName;
    int64_t workflowExecutionId{0};
    int64_t taskExecutionId{0};
    
    // Input/Output
    std::vector<std::string> inputSchemas;
    std::vector<std::string> inputTables;
    std::vector<std::string> inputColumns;
    std::vector<std::string> outputSchemas;
    std::vector<std::string> outputTables;
    std::vector<std::string> outputColumns;
    
    // Metadata
    std::chrono::system_clock::time_point executedAt;
    size_t rowsProcessed{0};
    double executionTimeMs{0.0};
    bool success{true};
    std::string errorMessage;
  };

  explicit TransformationLineageTracker(const std::string& connectionString);
  ~TransformationLineageTracker() = default;

  // Registrar transformación aplicada
  void recordTransformation(const TransformationRecord& record);

  // Obtener lineage de transformaciones para un recurso
  std::vector<TransformationRecord> getTransformationLineage(
      const std::string& schemaName,
      const std::string& tableName,
      const std::string& columnName = ""
  );

  // Obtener historial de transformaciones para un workflow
  std::vector<TransformationRecord> getWorkflowTransformationHistory(
      const std::string& workflowName,
      int64_t executionId = 0
  );

  // Obtener pipeline completo de transformaciones
  std::vector<TransformationRecord> getTransformationPipeline(
      const std::string& workflowName,
      int64_t executionId = 0
  );

  // Obtener transformaciones que produjeron un recurso
  std::vector<TransformationRecord> getProducingTransformations(
      const std::string& schemaName,
      const std::string& tableName,
      const std::string& columnName = ""
  );

  // Obtener transformaciones que consumen un recurso
  std::vector<TransformationRecord> getConsumingTransformations(
      const std::string& schemaName,
      const std::string& tableName,
      const std::string& columnName = ""
  );

private:
  std::string connectionString_;

  // Helper methods
  void saveToDatabase(const TransformationRecord& record);
  TransformationRecord loadFromDatabaseRow(const pqxx::row& row);
};

#endif // TRANSFORMATION_LINEAGE_TRACKER_H
