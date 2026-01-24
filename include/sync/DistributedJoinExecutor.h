#ifndef DISTRIBUTED_JOIN_EXECUTOR_H
#define DISTRIBUTED_JOIN_EXECUTOR_H

#include "engines/spark_engine.h"
#include "sync/DistributedProcessingManager.h"
#include "transformations/spark_translator.h"
#include "third_party/json.hpp"
#include "core/logger.h"
#include <string>
#include <memory>

using json = nlohmann::json;

// DistributedJoinExecutor: Ejecuta joins distribuidos con diferentes algoritmos
class DistributedJoinExecutor {
public:
  enum class JoinAlgorithm {
    AUTO,              // Detección automática
    BROADCAST,         // Broadcast join (tabla pequeña)
    SHUFFLE_HASH,      // Shuffle hash join
    SORT_MERGE         // Sort-merge join
  };

  struct JoinConfig {
    std::string leftTable;
    std::string rightTable;
    std::vector<std::string> leftColumns;
    std::vector<std::string> rightColumns;
    std::string joinType;              // "inner", "left", "right", "full_outer"
    std::string joinCondition;         // Condición SQL opcional
    int64_t leftTableSizeMB{0};        // Tamaño estimado de tabla izquierda
    int64_t rightTableSizeMB{0};       // Tamaño estimado de tabla derecha
    int64_t leftTableRows{0};          // Número de filas estimado
    int64_t rightTableRows{0};
    JoinAlgorithm algorithm{JoinAlgorithm::AUTO};
  };

  struct JoinResult {
    bool success{false};
    std::string resultTable;
    std::string algorithmUsed;
    int64_t resultRows{0};
    std::string errorMessage;
    json metadata;
  };

  explicit DistributedJoinExecutor(std::shared_ptr<SparkEngine> sparkEngine);
  ~DistributedJoinExecutor() = default;

  // Ejecutar join distribuido
  JoinResult executeJoin(const JoinConfig& config);

  // Detectar mejor algoritmo para el join
  JoinAlgorithm detectBestAlgorithm(const JoinConfig& config);

  // Ejecutar broadcast join
  JoinResult executeBroadcastJoin(const JoinConfig& config);

  // Ejecutar shuffle hash join
  JoinResult executeShuffleHashJoin(const JoinConfig& config);

  // Ejecutar sort-merge join
  JoinResult executeSortMergeJoin(const JoinConfig& config);

private:
  std::shared_ptr<SparkEngine> sparkEngine_;
  int64_t broadcastThresholdMB_{10};  // Threshold para broadcast join

  // Generar SQL para join con algoritmo específico
  std::string generateJoinSQL(const JoinConfig& config, JoinAlgorithm algorithm);

  // Obtener estadísticas de tabla (si disponible)
  json getTableStats(const std::string& tableName);
};

#endif // DISTRIBUTED_JOIN_EXECUTOR_H
