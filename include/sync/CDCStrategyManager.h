#ifndef CDC_STRATEGY_MANAGER_H
#define CDC_STRATEGY_MANAGER_H

#include "sync/ChangeLogCDC.h"
#include "engines/spark_engine.h"
#include "third_party/json.hpp"
#include "core/logger.h"
#include <string>
#include <memory>

using json = nlohmann::json;

// CDCStrategyManager: Decide automáticamente qué estrategia CDC usar
class CDCStrategyManager {
public:
  enum class CDCStrategy {
    INTERNAL_CHANGELOG,      // ds_change_log tables
    NATIVE_BINLOG,           // MySQL/MariaDB binlog
    NATIVE_WAL,              // PostgreSQL WAL
    NATIVE_TXN_LOG,          // MSSQL transaction log
    NATIVE_REDO_LOG,         // Oracle redo log
    NATIVE_CHANGE_STREAMS,   // MongoDB change streams
    EXTERNAL_DEBEZIUM,       // Debezium + Kafka
    SPARK_STRUCTURED_STREAM  // Spark Structured Streaming
  };

  struct DatabaseCapabilities {
    bool hasBinlog{false};
    bool hasWAL{false};
    bool hasTxnLog{false};
    bool hasRedoLog{false};
    bool hasChangeStreams{false};
    bool hasDebezium{false};
    bool hasSpark{false};
  };

  struct ChangeVolume {
    int64_t changesPerHour{0};
    bool isHighVolume{false};
  };

  struct LatencyRequirement {
    enum class Level {
      BATCH,           // Minutos/horas
      NEAR_REAL_TIME,  // Segundos
      REAL_TIME        // Sub-segundo
    };
    Level level{Level::BATCH};
  };

  // Seleccionar estrategia CDC automáticamente
  static CDCStrategy selectCDCStrategy(
    const std::string& dbEngine,
    const DatabaseCapabilities& capabilities,
    const ChangeVolume& volume,
    const LatencyRequirement& latency
  );

  // Detectar capacidades de base de datos
  static DatabaseCapabilities detectCapabilities(const std::string& dbEngine, 
                                                  const std::string& connectionString);

  // Verificar disponibilidad de estrategia
  static bool isStrategyAvailable(CDCStrategy strategy, 
                                  const DatabaseCapabilities& capabilities);
};

#endif // CDC_STRATEGY_MANAGER_H
