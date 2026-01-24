#include "sync/IncrementalProcessor.h"
#include "core/logger.h"
#include <sstream>
#include <iomanip>

IncrementalProcessor::IncrementalResult
IncrementalProcessor::detectChanges(const IncrementalConfig& config) {
  IncrementalResult result;

  switch (config.method) {
    case ChangeDetectionMethod::TIMESTAMP:
      if (!config.timestampColumn.empty()) {
        result.hasChanges = true;
        result.filterSQL = config.timestampColumn + " > '" + 
                          std::to_string(std::chrono::duration_cast<std::chrono::seconds>(
                            config.lastExecutionTime.time_since_epoch()).count()) + "'";
      }
      break;
      
    case ChangeDetectionMethod::CDC:
      // Usar CDC logs para detectar cambios
      result.hasChanges = true; // Placeholder
      result.filterSQL = "1=1"; // Placeholder
      break;
      
    case ChangeDetectionMethod::CHECKSUM:
      // Comparar checksums (placeholder)
      result.hasChanges = true;
      result.filterSQL = "1=1";
      break;
      
    case ChangeDetectionMethod::PARTITION:
      if (config.usePartitions) {
        auto modified = PartitioningManager::getModifiedPartitions(
          config.tableName,
          config.partitionInfo,
          config.lastExecutionTime
        );
        result.modifiedPartitions = modified;
        result.hasChanges = !modified.empty();
        result.filterSQL = PartitioningManager::generatePartitionFilter(
          config.partitionInfo,
          modified
        );
      }
      break;
  }

  Logger::info(LogCategory::SYSTEM, "IncrementalProcessor",
               "Detected changes for " + config.tableName + ": " + 
               (result.hasChanges ? "YES" : "NO"));

  return result;
}

std::string IncrementalProcessor::generateIncrementalQuery(
    const std::string& baseQuery,
    const IncrementalConfig& config) {
  
  IncrementalResult changes = detectChanges(config);
  
  if (!changes.hasChanges) {
    return ""; // No hay cambios
  }
  
  std::ostringstream query;
  query << baseQuery;
  
  // Agregar WHERE clause si no existe
  if (baseQuery.find("WHERE") == std::string::npos) {
    query << " WHERE ";
  } else {
    query << " AND ";
  }
  
  query << changes.filterSQL;
  
  return query.str();
}

bool IncrementalProcessor::processIncremental(
    const IncrementalConfig& config,
    std::function<void(const std::string&)> processor) {
  
  IncrementalResult changes = detectChanges(config);
  
  if (!changes.hasChanges) {
    Logger::info(LogCategory::SYSTEM, "IncrementalProcessor",
                 "No changes detected for " + config.tableName);
    return true;
  }
  
  if (config.usePartitions && !changes.modifiedPartitions.empty()) {
    // Procesar solo particiones modificadas
    for (const auto& partition : changes.modifiedPartitions) {
      std::string partitionFilter = PartitioningManager::generatePartitionFilter(
        config.partitionInfo,
        {partition}
      );
      processor(partitionFilter);
    }
  } else {
    // Procesar con filter SQL
    processor(changes.filterSQL);
  }
  
  return true;
}
