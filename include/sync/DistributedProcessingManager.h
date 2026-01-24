#ifndef DISTRIBUTED_PROCESSING_MANAGER_H
#define DISTRIBUTED_PROCESSING_MANAGER_H

#include "engines/spark_engine.h"
#include "sync/ParallelProcessing.h"

// Forward declaration to break circular dependency
class TableProcessorThreadPool;
#include "third_party/json.hpp"
#include "core/logger.h"
#include <string>
#include <memory>
#include <atomic>
#include <map>

using json = nlohmann::json;

// DistributedProcessingManager: Decide automáticamente si usar
// procesamiento local (thread pools) o distribuido (Spark)
class DistributedProcessingManager {
public:
  struct ProcessingConfig {
    // Thresholds para decisión automática
    int64_t distributedThresholdRows{1000000};      // 1M rows
    int64_t distributedThresholdSizeMB{100};         // 100 MB
    int64_t broadcastJoinThresholdMB{10};            // 10 MB para broadcast join
    
    // Configuración de Spark
    SparkEngine::SparkConfig sparkConfig;
    
    // Forzar modo (opcional)
    enum class ForceMode {
      AUTO,           // Decisión automática
      LOCAL_ONLY,     // Forzar procesamiento local
      DISTRIBUTED_ONLY // Forzar procesamiento distribuido
    };
    ForceMode forceMode{ForceMode::AUTO};
    
    // Configuración de complejidad
    int complexityScore{0};  // Score de complejidad (0-100)
  };

  struct ProcessingDecision {
    bool useDistributed{false};
    std::string reason;
    int64_t estimatedRows{0};
    int64_t estimatedSizeMB{0};
    int complexityScore{0};
  };

  struct ProcessingTask {
    std::string taskId;
    std::string taskType;              // "sync", "transform", "join", etc.
    json config;                       // Configuración de la tarea
    std::string inputPath;             // Path a datos de entrada
    std::string outputPath;            // Path a datos de salida
    int64_t estimatedRows{0};
    int64_t estimatedSizeMB{0};
    int complexityScore{0};
  };

  struct ProcessingResult {
    bool success{false};
    std::string taskId;
    std::string executionMode;         // "local" o "distributed"
    int64_t rowsProcessed{0};
    std::string outputPath;
    std::string errorMessage;
    json metadata;
  };

  explicit DistributedProcessingManager(const ProcessingConfig& config);
  ~DistributedProcessingManager();

  // Inicializar el manager
  bool initialize();

  // Cerrar el manager
  void shutdown();

  // Decidir si usar procesamiento distribuido
  ProcessingDecision shouldUseDistributed(const ProcessingTask& task);

  // Ejecutar tarea (decide automáticamente local vs distribuido)
  ProcessingResult executeTask(const ProcessingTask& task);

  // Ejecutar tarea forzando modo local
  ProcessingResult executeTaskLocal(const ProcessingTask& task,
                                    std::function<void()> localProcessor);

  // Ejecutar tarea forzando modo distribuido
  ProcessingResult executeTaskDistributed(const ProcessingTask& task);

  // Obtener estadísticas de procesamiento
  json getStatistics() const;

  // Verificar si Spark está disponible
  bool isSparkAvailable() const;

private:
  ProcessingConfig config_;
  std::unique_ptr<SparkEngine> sparkEngine_;
  std::unique_ptr<TableProcessorThreadPool> threadPool_;
  bool initialized_{false};
  
  // Estadísticas
  std::atomic<int64_t> localTasksExecuted_{0};
  std::atomic<int64_t> distributedTasksExecuted_{0};
  std::atomic<int64_t> localRowsProcessed_{0};
  std::atomic<int64_t> distributedRowsProcessed_{0};
  std::atomic<int64_t> localExecutionTimeMs_{0};
  std::atomic<int64_t> distributedExecutionTimeMs_{0};

  // Calcular complejidad de una tarea
  int calculateComplexity(const ProcessingTask& task) const;

  // Estimar tamaño de datos
  int64_t estimateDataSize(const ProcessingTask& task) const;

  // Estimar número de filas
  int64_t estimateRowCount(const ProcessingTask& task) const;

  // Validar configuración
  bool validateConfig() const;
};

#endif // DISTRIBUTED_PROCESSING_MANAGER_H
