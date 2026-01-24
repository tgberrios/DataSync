#include "sync/CDCStrategyManager.h"
#include "core/logger.h"
#include <algorithm>

CDCStrategyManager::CDCStrategy
CDCStrategyManager::selectCDCStrategy(
    const std::string& dbEngine,
    const DatabaseCapabilities& capabilities,
    const ChangeVolume& volume,
    const LatencyRequirement& latency) {
  
  std::string lowerEngine = dbEngine;
  std::transform(lowerEngine.begin(), lowerEngine.end(), lowerEngine.begin(), ::tolower);
  
  // Criterio 1: Latencia requerida
  if (latency.level == LatencyRequirement::Level::REAL_TIME) {
    if (capabilities.hasSpark) {
      return CDCStrategy::SPARK_STRUCTURED_STREAM;
    } else if (capabilities.hasDebezium) {
      return CDCStrategy::EXTERNAL_DEBEZIUM;
    }
  }
  
  // Criterio 2: Volumen de cambios
  if (volume.isHighVolume || volume.changesPerHour > 1000000) {
    if (capabilities.hasSpark) {
      return CDCStrategy::SPARK_STRUCTURED_STREAM;
    } else if (capabilities.hasDebezium) {
      return CDCStrategy::EXTERNAL_DEBEZIUM;
    }
  }
  
  // Criterio 3: Disponibilidad de CDC nativo
  if (lowerEngine.find("mariadb") != std::string::npos || 
      lowerEngine.find("mysql") != std::string::npos) {
    if (capabilities.hasBinlog) {
      return CDCStrategy::NATIVE_BINLOG;
    }
  } else if (lowerEngine.find("postgres") != std::string::npos) {
    if (capabilities.hasWAL) {
      return CDCStrategy::NATIVE_WAL;
    }
  } else if (lowerEngine.find("mssql") != std::string::npos || 
             lowerEngine.find("sqlserver") != std::string::npos) {
    if (capabilities.hasTxnLog) {
      return CDCStrategy::NATIVE_TXN_LOG;
    }
  } else if (lowerEngine.find("oracle") != std::string::npos) {
    if (capabilities.hasRedoLog) {
      return CDCStrategy::NATIVE_REDO_LOG;
    }
  } else if (lowerEngine.find("mongodb") != std::string::npos) {
    if (capabilities.hasChangeStreams) {
      return CDCStrategy::NATIVE_CHANGE_STREAMS;
    }
  }
  
  // Fallback: CDC interno
  return CDCStrategy::INTERNAL_CHANGELOG;
}

CDCStrategyManager::DatabaseCapabilities
CDCStrategyManager::detectCapabilities(const std::string& dbEngine,
                                        const std::string& connectionString) {
  DatabaseCapabilities capabilities;
  std::string lowerEngine = dbEngine;
  std::transform(lowerEngine.begin(), lowerEngine.end(), lowerEngine.begin(), ::tolower);
  
  // Detectar capacidades basado en tipo de DB
  if (lowerEngine.find("mariadb") != std::string::npos || 
      lowerEngine.find("mysql") != std::string::npos) {
    // Verificar si binlog está habilitado (placeholder)
    capabilities.hasBinlog = true; // En implementación real, verificaría configuración
  } else if (lowerEngine.find("postgres") != std::string::npos) {
    // Verificar WAL level (placeholder)
    capabilities.hasWAL = true;
  } else if (lowerEngine.find("mssql") != std::string::npos) {
    capabilities.hasTxnLog = true;
  } else if (lowerEngine.find("oracle") != std::string::npos) {
    capabilities.hasRedoLog = true;
  } else if (lowerEngine.find("mongodb") != std::string::npos) {
    capabilities.hasChangeStreams = true;
  }
  
  // Verificar infraestructura externa
  // En implementación real, verificaría:
  // - Kafka disponible para Debezium
  // - Spark cluster disponible
  capabilities.hasDebezium = false; // Placeholder - se detectaría verificando Kafka
  capabilities.hasSpark = false;    // Placeholder - se detectaría desde SparkEngine
  
  // TODO: Integrar con StreamProcessingManager para verificar disponibilidad de Kafka/RabbitMQ/Redis
  // Esto permitiría usar streams de mensajería como estrategia CDC
  
  Logger::info(LogCategory::SYSTEM, "CDCStrategyManager",
               "Detected capabilities for " + dbEngine + 
               " - Binlog: " + std::to_string(capabilities.hasBinlog) +
               ", WAL: " + std::to_string(capabilities.hasWAL) +
               ", TxnLog: " + std::to_string(capabilities.hasTxnLog) +
               ", RedoLog: " + std::to_string(capabilities.hasRedoLog) +
               ", ChangeStreams: " + std::to_string(capabilities.hasChangeStreams));
  
  return capabilities;
}

bool CDCStrategyManager::isStrategyAvailable(CDCStrategy strategy,
                                              const DatabaseCapabilities& capabilities) {
  switch (strategy) {
    case CDCStrategy::NATIVE_BINLOG:
      return capabilities.hasBinlog;
    case CDCStrategy::NATIVE_WAL:
      return capabilities.hasWAL;
    case CDCStrategy::NATIVE_TXN_LOG:
      return capabilities.hasTxnLog;
    case CDCStrategy::NATIVE_REDO_LOG:
      return capabilities.hasRedoLog;
    case CDCStrategy::NATIVE_CHANGE_STREAMS:
      return capabilities.hasChangeStreams;
    case CDCStrategy::EXTERNAL_DEBEZIUM:
      return capabilities.hasDebezium;
    case CDCStrategy::SPARK_STRUCTURED_STREAM:
      return capabilities.hasSpark;
    case CDCStrategy::INTERNAL_CHANGELOG:
      return true; // Siempre disponible
  }
  return false;
}
