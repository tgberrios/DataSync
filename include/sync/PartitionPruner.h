#ifndef PARTITION_PRUNER_H
#define PARTITION_PRUNER_H

#include "sync/PartitioningManager.h"
#include "third_party/json.hpp"
#include "core/logger.h"
#include <string>
#include <vector>
#include <chrono>
#include <optional>

using json = nlohmann::json;

// PartitionPruner: Optimiza queries usando solo particiones necesarias
class PartitionPruner {
public:
  struct PruningResult {
    bool canPrune{false};
    std::vector<std::string> requiredPartitions;
    std::string optimizedQuery;
    size_t partitionsPruned{0};
    size_t totalPartitions{0};
  };

  struct QueryAnalysis {
    std::vector<std::string> filterColumns;
    std::vector<std::string> filterValues;
    std::string filterOperator;  // "=", "IN", "BETWEEN", ">", "<", etc.
    bool hasDateFilter{false};
    std::chrono::system_clock::time_point dateFilterValue;
    std::string dateFilterColumn;
  };

  // Analizar query y determinar qué particiones son necesarias
  static PruningResult prunePartitions(
      const std::string& query,
      const PartitioningManager::PartitionInfo& partitionInfo,
      const std::vector<std::string>& allPartitions
  );

  // Analizar query para extraer filtros relevantes
  static QueryAnalysis analyzeQuery(const std::string& query);

  // Generar query optimizada con filtros de partición
  static std::string generatePrunedQuery(
      const std::string& originalQuery,
      const PartitioningManager::PartitionInfo& partitionInfo,
      const std::vector<std::string>& requiredPartitions
  );

  // Obtener particiones modificadas desde última ejecución
  static std::vector<std::string> getModifiedPartitions(
      const PartitioningManager::PartitionInfo& partitionInfo,
      const std::chrono::system_clock::time_point& lastExecutionTime,
      const std::vector<std::string>& allPartitions
  );

  // Verificar si una partición es necesaria basado en filtros
  static bool isPartitionNeeded(
      const std::string& partitionValue,
      const PartitioningManager::PartitionInfo& partitionInfo,
      const QueryAnalysis& analysis
  );

private:
  // Helper methods
  static bool matchesDateFilter(
      const std::string& partitionValue,
      const PartitioningManager::PartitionInfo& partitionInfo,
      const QueryAnalysis& analysis
  );

  static bool matchesRangeFilter(
      const std::string& partitionValue,
      const PartitioningManager::PartitionInfo& partitionInfo,
      const QueryAnalysis& analysis
  );

  static bool matchesListFilter(
      const std::string& partitionValue,
      const PartitioningManager::PartitionInfo& partitionInfo,
      const QueryAnalysis& analysis
  );

  static std::string extractColumnFromFilter(const std::string& filter);
  static std::vector<std::string> extractValuesFromFilter(const std::string& filter);
};

#endif // PARTITION_PRUNER_H
