#include "sync/DistributedJoinExecutor.h"
#include "core/logger.h"
#include <algorithm>
#include <sstream>

DistributedJoinExecutor::DistributedJoinExecutor(std::shared_ptr<SparkEngine> sparkEngine)
    : sparkEngine_(sparkEngine) {
  Logger::info(LogCategory::SYSTEM, "DistributedJoinExecutor",
               "Initializing DistributedJoinExecutor");
}

DistributedJoinExecutor::JoinAlgorithm
DistributedJoinExecutor::detectBestAlgorithm(const JoinConfig& config) {
  // Si el algoritmo está forzado, usarlo
  if (config.algorithm != JoinAlgorithm::AUTO) {
    return config.algorithm;
  }

  // Criterio 1: Broadcast join para tablas pequeñas
  // Si una tabla es menor que el threshold, usar broadcast
  int64_t smallerTableSize = std::min(config.leftTableSizeMB, config.rightTableSizeMB);
  if (smallerTableSize > 0 && smallerTableSize < broadcastThresholdMB_) {
    Logger::info(LogCategory::SYSTEM, "DistributedJoinExecutor",
                 "Detected broadcast join: smaller table size " + 
                 std::to_string(smallerTableSize) + " MB < threshold " +
                 std::to_string(broadcastThresholdMB_) + " MB");
    return JoinAlgorithm::BROADCAST;
  }

  // Criterio 2: Sort-merge para tablas grandes y ordenadas
  // Si ambas tablas son grandes (> 100MB) y están ordenadas por join key
  if (config.leftTableSizeMB > 100 && config.rightTableSizeMB > 100) {
    Logger::info(LogCategory::SYSTEM, "DistributedJoinExecutor",
                 "Detected sort-merge join: both tables large");
    return JoinAlgorithm::SORT_MERGE;
  }

  // Criterio 3: Shuffle hash para casos intermedios
  // Default para la mayoría de casos
  Logger::info(LogCategory::SYSTEM, "DistributedJoinExecutor",
               "Detected shuffle hash join: default for intermediate cases");
  return JoinAlgorithm::SHUFFLE_HASH;
}

DistributedJoinExecutor::JoinResult
DistributedJoinExecutor::executeJoin(const JoinConfig& config) {
  JoinResult result;

  if (!sparkEngine_ || !sparkEngine_->isAvailable()) {
    result.errorMessage = "Spark engine not available";
    Logger::error(LogCategory::SYSTEM, "DistributedJoinExecutor",
                 result.errorMessage);
    return result;
  }

  // Detectar mejor algoritmo si es AUTO
  JoinAlgorithm algorithm = detectBestAlgorithm(config);
  result.algorithmUsed = algorithm == JoinAlgorithm::BROADCAST ? "broadcast" :
                        algorithm == JoinAlgorithm::SHUFFLE_HASH ? "shuffle_hash" :
                        algorithm == JoinAlgorithm::SORT_MERGE ? "sort_merge" : "auto";

  Logger::info(LogCategory::SYSTEM, "DistributedJoinExecutor",
               "Executing " + result.algorithmUsed + " join");

  try {
    switch (algorithm) {
      case JoinAlgorithm::BROADCAST:
        result = executeBroadcastJoin(config);
        break;
      case JoinAlgorithm::SHUFFLE_HASH:
        result = executeShuffleHashJoin(config);
        break;
      case JoinAlgorithm::SORT_MERGE:
        result = executeSortMergeJoin(config);
        break;
      default:
        result = executeShuffleHashJoin(config); // Default
        break;
    }
  } catch (const std::exception& e) {
    result.success = false;
    result.errorMessage = std::string(e.what());
    Logger::error(LogCategory::SYSTEM, "DistributedJoinExecutor",
                 "Error executing join: " + result.errorMessage);
  }

  result.metadata["algorithm"] = result.algorithmUsed;
  result.metadata["left_table_size_mb"] = config.leftTableSizeMB;
  result.metadata["right_table_size_mb"] = config.rightTableSizeMB;
  result.metadata["join_type"] = config.joinType;

  return result;
}

DistributedJoinExecutor::JoinResult
DistributedJoinExecutor::executeBroadcastJoin(const JoinConfig& config) {
  JoinResult result;
  result.algorithmUsed = "broadcast";

  // Generar SQL con hint de broadcast
  std::string sql = generateJoinSQL(config, JoinAlgorithm::BROADCAST);

  // Agregar hint de broadcast en Spark SQL
  // En Spark, se puede usar /*+ BROADCAST(table) */ hint
  std::string smallerTable = config.rightTableSizeMB < config.leftTableSizeMB ? 
                            config.rightTable : config.leftTable;
  
  // Reemplazar SELECT con hint
  size_t selectPos = sql.find("SELECT");
  if (selectPos != std::string::npos) {
    sql.insert(selectPos + 6, " /*+ BROADCAST(" + smallerTable + ") */");
  }

  // Ejecutar en Spark
  std::string outputPath = "/tmp/datasync_join_" + 
                          std::to_string(std::chrono::system_clock::now().time_since_epoch().count());
  
  SparkEngine::SparkJobResult sparkResult = sparkEngine_->executeSQL(sql, outputPath);
  
  result.success = sparkResult.success;
  result.resultTable = outputPath;
  result.resultRows = sparkResult.rowsProcessed;
  result.errorMessage = sparkResult.errorMessage;
  result.metadata = sparkResult.metadata;

  return result;
}

DistributedJoinExecutor::JoinResult
DistributedJoinExecutor::executeShuffleHashJoin(const JoinConfig& config) {
  JoinResult result;
  result.algorithmUsed = "shuffle_hash";

  // Shuffle hash join es el default de Spark para la mayoría de casos
  std::string sql = generateJoinSQL(config, JoinAlgorithm::SHUFFLE_HASH);

  std::string outputPath = "/tmp/datasync_join_" + 
                          std::to_string(std::chrono::system_clock::now().time_since_epoch().count());
  
  SparkEngine::SparkJobResult sparkResult = sparkEngine_->executeSQL(sql, outputPath);
  
  result.success = sparkResult.success;
  result.resultTable = outputPath;
  result.resultRows = sparkResult.rowsProcessed;
  result.errorMessage = sparkResult.errorMessage;
  result.metadata = sparkResult.metadata;

  return result;
}

DistributedJoinExecutor::JoinResult
DistributedJoinExecutor::executeSortMergeJoin(const JoinConfig& config) {
  JoinResult result;
  result.algorithmUsed = "sort_merge";

  // Sort-merge join requiere que ambas tablas estén ordenadas por join key
  // Generar SQL que ordena primero y luego hace join
  std::ostringstream sql;
  
  // Ordenar tablas por join key primero
  sql << "WITH sorted_left AS (\n";
  sql << "  SELECT * FROM " << config.leftTable << "\n";
  if (!config.leftColumns.empty()) {
    sql << "  ORDER BY ";
    for (size_t i = 0; i < config.leftColumns.size(); ++i) {
      if (i > 0) sql << ", ";
      sql << config.leftColumns[i];
    }
  }
  sql << "\n),\n";
  
  sql << "sorted_right AS (\n";
  sql << "  SELECT * FROM " << config.rightTable << "\n";
  if (!config.rightColumns.empty()) {
    sql << "  ORDER BY ";
    for (size_t i = 0; i < config.rightColumns.size(); ++i) {
      if (i > 0) sql << ", ";
      sql << config.rightColumns[i];
    }
  }
  sql << "\n)\n";
  
  // Join ordenado
  sql << "SELECT *\n";
  sql << "FROM sorted_left\n";
  
  if (config.joinType == "left" || config.joinType == "left_outer") {
    sql << "LEFT JOIN ";
  } else if (config.joinType == "right" || config.joinType == "right_outer") {
    sql << "RIGHT JOIN ";
  } else if (config.joinType == "full" || config.joinType == "full_outer") {
    sql << "FULL OUTER JOIN ";
  } else {
    sql << "INNER JOIN ";
  }
  
  sql << "sorted_right\n";
  sql << "ON ";
  
  if (!config.joinCondition.empty()) {
    sql << config.joinCondition;
  } else if (!config.leftColumns.empty() && !config.rightColumns.empty() &&
             config.leftColumns.size() == config.rightColumns.size()) {
    for (size_t i = 0; i < config.leftColumns.size(); ++i) {
      if (i > 0) sql << " AND ";
      sql << "sorted_left." << config.leftColumns[i] << " = sorted_right." 
          << config.rightColumns[i];
    }
  }

  std::string outputPath = "/tmp/datasync_join_" + 
                          std::to_string(std::chrono::system_clock::now().time_since_epoch().count());
  
  SparkEngine::SparkJobResult sparkResult = sparkEngine_->executeSQL(sql.str(), outputPath);
  
  result.success = sparkResult.success;
  result.resultTable = outputPath;
  result.resultRows = sparkResult.rowsProcessed;
  result.errorMessage = sparkResult.errorMessage;
  result.metadata = sparkResult.metadata;

  return result;
}

std::string DistributedJoinExecutor::generateJoinSQL(const JoinConfig& config, 
                                                      JoinAlgorithm algorithm) {
  std::ostringstream sql;
  
  sql << "SELECT ";
  
  // Columnas a seleccionar
  if (config.leftColumns.empty() && config.rightColumns.empty()) {
    sql << "*";
  } else {
    // Agregar columnas del left table
    for (size_t i = 0; i < config.leftColumns.size(); ++i) {
      if (i > 0) sql << ", ";
      sql << config.leftTable << "." << config.leftColumns[i];
    }
    // Agregar columnas del right table
    for (size_t i = 0; i < config.rightColumns.size(); ++i) {
      sql << ", " << config.rightTable << "." << config.rightColumns[i];
    }
  }
  
  sql << "\nFROM " << config.leftTable << "\n";
  
  // Tipo de join
  if (config.joinType == "left" || config.joinType == "left_outer") {
    sql << "LEFT JOIN ";
  } else if (config.joinType == "right" || config.joinType == "right_outer") {
    sql << "RIGHT JOIN ";
  } else if (config.joinType == "full" || config.joinType == "full_outer") {
    sql << "FULL OUTER JOIN ";
  } else {
    sql << "INNER JOIN ";
  }
  
  sql << config.rightTable << "\n";
  sql << "ON ";
  
  // Condición de join
  if (!config.joinCondition.empty()) {
    sql << config.joinCondition;
  } else if (!config.leftColumns.empty() && !config.rightColumns.empty() &&
             config.leftColumns.size() == config.rightColumns.size()) {
    for (size_t i = 0; i < config.leftColumns.size(); ++i) {
      if (i > 0) sql << " AND ";
      sql << config.leftTable << "." << config.leftColumns[i] << " = "
          << config.rightTable << "." << config.rightColumns[i];
    }
  } else {
    sql << "1=1"; // Fallback
  }
  
  return sql.str();
}

json DistributedJoinExecutor::getTableStats(const std::string& tableName) {
  // Placeholder - en implementación real, consultaría estadísticas de Spark
  json stats;
  stats["table_name"] = tableName;
  stats["row_count"] = 0;
  stats["size_mb"] = 0;
  return stats;
}
