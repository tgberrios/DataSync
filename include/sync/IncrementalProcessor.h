#ifndef INCREMENTAL_PROCESSOR_H
#define INCREMENTAL_PROCESSOR_H

#include "sync/PartitioningManager.h"
#include "sync/ChangeLogCDC.h"
#include "third_party/json.hpp"
#include "core/logger.h"
#include <string>
#include <vector>
#include <chrono>

using json = nlohmann::json;

// IncrementalProcessor: Procesamiento incremental inteligente
class IncrementalProcessor {
public:
  enum class ChangeDetectionMethod {
    TIMESTAMP,         // Detectar cambios por columna timestamp
    CDC,               // Usar CDC logs
    CHECKSUM,          // Comparar checksums/hashes
    PARTITION          // Procesar solo particiones modificadas
  };

  struct IncrementalConfig {
    std::string tableName;
    std::string timestampColumn;       // Para detección por timestamp
    ChangeDetectionMethod method{ChangeDetectionMethod::TIMESTAMP};
    std::chrono::system_clock::time_point lastExecutionTime;
    bool usePartitions{false};
    PartitioningManager::PartitionInfo partitionInfo;
  };

  struct IncrementalResult {
    bool hasChanges{false};
    int64_t rowsToProcess{0};
    std::vector<std::string> modifiedPartitions;
    std::string filterSQL;             // SQL filter para procesar solo cambios
    json metadata;
  };

  // Detectar cambios desde última ejecución
  static IncrementalResult detectChanges(const IncrementalConfig& config);

  // Generar query incremental
  static std::string generateIncrementalQuery(
    const std::string& baseQuery,
    const IncrementalConfig& config
  );

  // Procesar solo cambios detectados
  static bool processIncremental(
    const IncrementalConfig& config,
    std::function<void(const std::string&)> processor
  );
};

#endif // INCREMENTAL_PROCESSOR_H
