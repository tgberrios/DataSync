#ifndef MERGE_STRATEGY_EXECUTOR_H
#define MERGE_STRATEGY_EXECUTOR_H

#include "engines/spark_engine.h"
#include "sync/DistributedProcessingManager.h"
#include "third_party/json.hpp"
#include "core/logger.h"
#include <string>
#include <vector>
#include <memory>

using json = nlohmann::json;

// MergeStrategyExecutor: Ejecuta diferentes estrategias de merge (UPSERT, SCD Type 4/6)
class MergeStrategyExecutor {
public:
  enum class MergeStrategy {
    UPSERT,           // Insert or update basado en primary key
    SCD_TYPE_4,       // History table (mantener historial completo)
    SCD_TYPE_6,       // Hybrid (current + history)
    INCREMENTAL_MERGE // Solo procesar cambios desde última ejecución
  };

  struct MergeConfig {
    std::string targetTable;
    std::string sourceTable;
    std::vector<std::string> primaryKeyColumns;
    std::vector<std::string> mergeColumns;
    MergeStrategy strategy{MergeStrategy::UPSERT};
    std::string timestampColumn;      // Para SCD Type 4/6
    std::string historyTable;         // Para SCD Type 4
    bool useDistributed{false};       // Usar Spark para merge
  };

  struct MergeResult {
    bool success{false};
    int64_t rowsInserted{0};
    int64_t rowsUpdated{0};
    int64_t rowsDeleted{0};
    std::string errorMessage;
    json metadata;
  };

  explicit MergeStrategyExecutor(std::shared_ptr<SparkEngine> sparkEngine = nullptr);
  ~MergeStrategyExecutor() = default;

  // Ejecutar merge con estrategia especificada
  MergeResult executeMerge(const MergeConfig& config);

  // Ejecutar UPSERT
  MergeResult executeUPSERT(const MergeConfig& config);

  // Ejecutar SCD Type 4 (History table)
  MergeResult executeSCDType4(const MergeConfig& config);

  // Ejecutar SCD Type 6 (Hybrid)
  MergeResult executeSCDType6(const MergeConfig& config);

  // Ejecutar incremental merge
  MergeResult executeIncrementalMerge(const MergeConfig& config);

private:
  std::shared_ptr<SparkEngine> sparkEngine_;

  // Generar SQL para merge
  std::string generateMergeSQL(const MergeConfig& config);
};

#endif // MERGE_STRATEGY_EXECUTOR_H
