#include "sync/PartitioningManager.h"
#include "core/logger.h"
#include <algorithm>
#include <regex>
#include <sstream>
#include <iomanip>

PartitioningManager::PartitionDetectionResult
PartitioningManager::detectPartitions(
    const std::string& schemaName,
    const std::string& tableName,
    const std::vector<std::string>& columnNames,
    const std::vector<std::string>& columnTypes) {
  
  PartitionDetectionResult result;

  // Buscar columnas de fecha
  for (size_t i = 0; i < columnNames.size(); ++i) {
    std::string colName = columnNames[i];
    std::string colType = i < columnTypes.size() ? columnTypes[i] : "";
    
    if (isDateColumn(colName, colType)) {
      PartitionInfo partition;
      partition.columnName = colName;
      partition.type = PartitionType::DATE;
      partition.format = "year-month-day"; // Default
      
      // Detectar formato basado en nombre de columna
      std::string lowerName = colName;
      std::transform(lowerName.begin(), lowerName.end(), lowerName.begin(), ::tolower);
      
      if (lowerName.find("year") != std::string::npos) {
        partition.format = "year";
      } else if (lowerName.find("month") != std::string::npos) {
        partition.format = "year-month";
      }
      
      result.partitions.push_back(partition);
      result.hasPartitions = true;
      
      if (result.recommendedPartitionColumn.empty()) {
        result.recommendedPartitionColumn = colName;
        result.recommendedType = PartitionType::DATE;
      }
    } else if (isRegionColumn(colName, colType)) {
      PartitionInfo partition;
      partition.columnName = colName;
      partition.type = PartitionType::REGION;
      result.partitions.push_back(partition);
      result.hasPartitions = true;
      
      if (result.recommendedPartitionColumn.empty()) {
        result.recommendedPartitionColumn = colName;
        result.recommendedType = PartitionType::REGION;
      }
    }
  }

  // Si no se encontraron particiones automáticas, buscar columnas numéricas para RANGE
  if (!result.hasPartitions) {
    for (size_t i = 0; i < columnNames.size(); ++i) {
      std::string colType = i < columnTypes.size() ? columnTypes[i] : "";
      std::transform(colType.begin(), colType.end(), colType.begin(), ::tolower);
      
      if (colType.find("int") != std::string::npos ||
          colType.find("numeric") != std::string::npos ||
          colType.find("decimal") != std::string::npos) {
        PartitionInfo partition;
        partition.columnName = columnNames[i];
        partition.type = PartitionType::RANGE;
        result.partitions.push_back(partition);
        result.hasPartitions = true;
        
        if (result.recommendedPartitionColumn.empty()) {
          result.recommendedPartitionColumn = columnNames[i];
          result.recommendedType = PartitionType::RANGE;
        }
        break;
      }
    }
  }

  Logger::info(LogCategory::SYSTEM, "PartitioningManager",
               "Detected " + std::to_string(result.partitions.size()) + 
               " potential partition columns for " + schemaName + "." + tableName);

  return result;
}

bool PartitioningManager::isDateColumn(const std::string& columnName, const std::string& columnType) {
  std::string lowerName = columnName;
  std::transform(lowerName.begin(), lowerName.end(), lowerName.begin(), ::tolower);
  
  std::string lowerType = columnType;
  std::transform(lowerType.begin(), lowerType.end(), lowerType.begin(), ::tolower);
  
  // Verificar tipo de columna
  if (lowerType.find("date") != std::string::npos ||
      lowerType.find("timestamp") != std::string::npos ||
      lowerType.find("time") != std::string::npos) {
    return true;
  }
  
  // Verificar nombre de columna
  if (lowerName.find("date") != std::string::npos ||
      lowerName.find("time") != std::string::npos ||
      lowerName.find("created") != std::string::npos ||
      lowerName.find("updated") != std::string::npos ||
      lowerName.find("modified") != std::string::npos ||
      lowerName.find("timestamp") != std::string::npos) {
    return true;
  }
  
  return false;
}

bool PartitioningManager::isRegionColumn(const std::string& columnName, const std::string& columnType) {
  std::string lowerName = columnName;
  std::transform(lowerName.begin(), lowerName.end(), lowerName.begin(), ::tolower);
  
  if (lowerName.find("region") != std::string::npos ||
      lowerName.find("country") != std::string::npos ||
      lowerName.find("state") != std::string::npos ||
      lowerName.find("province") != std::string::npos ||
      lowerName.find("location") != std::string::npos ||
      lowerName.find("geo") != std::string::npos) {
    return true;
  }
  
  return false;
}

std::string PartitioningManager::generatePartitionSQL(
    const std::string& tableName,
    const PartitionInfo& partitionInfo) {
  
  std::ostringstream sql;
  
  sql << "PARTITION BY ";
  
  switch (partitionInfo.type) {
    case PartitionType::DATE:
      sql << "DATE(" << partitionInfo.columnName << ")";
      if (partitionInfo.format == "year") {
        sql << " YEAR";
      } else if (partitionInfo.format == "year-month") {
        sql << " YEAR, MONTH";
      } else {
        sql << " YEAR, MONTH, DAY";
      }
      break;
      
    case PartitionType::REGION:
      sql << partitionInfo.columnName;
      break;
      
    case PartitionType::RANGE:
      sql << "RANGE(" << partitionInfo.columnName << ")";
      break;
      
    case PartitionType::HASH:
      sql << "HASH(" << partitionInfo.columnName << ")";
      break;
      
    case PartitionType::LIST:
      sql << "LIST(" << partitionInfo.columnName << ")";
      break;
  }
  
  return sql.str();
}

std::vector<std::string> PartitioningManager::getModifiedPartitions(
    const std::string& tableName,
    const PartitionInfo& partitionInfo,
    const std::chrono::system_clock::time_point& lastExecutionTime) {
  
  // Placeholder - en implementación real, consultaría metadata para obtener
  // particiones modificadas desde lastExecutionTime
  std::vector<std::string> modifiedPartitions;
  
  // Por ahora, retornar todas las particiones (procesamiento completo)
  // En implementación real, se consultaría:
  // - Metadata repository para tracking de particiones
  // - Timestamps de última modificación por partición
  // - CDC logs para cambios por partición
  
  Logger::info(LogCategory::SYSTEM, "PartitioningManager",
               "Getting modified partitions for " + tableName + 
               " (implementation placeholder)");
  
  return modifiedPartitions;
}

std::string PartitioningManager::generatePartitionFilter(
    const PartitionInfo& partitionInfo,
    const std::vector<std::string>& partitionValues) {
  
  if (partitionValues.empty()) {
    return "1=1"; // No filter
  }
  
  std::ostringstream filter;
  
  switch (partitionInfo.type) {
    case PartitionType::DATE:
      filter << partitionInfo.columnName << " >= '" << partitionValues[0] << "'";
      if (partitionValues.size() > 1) {
        filter << " AND " << partitionInfo.columnName << " < '" << partitionValues[1] << "'";
      }
      break;
      
    case PartitionType::REGION:
    case PartitionType::LIST:
      filter << partitionInfo.columnName << " IN (";
      for (size_t i = 0; i < partitionValues.size(); ++i) {
        if (i > 0) filter << ", ";
        filter << "'" << partitionValues[i] << "'";
      }
      filter << ")";
      break;
      
    case PartitionType::RANGE:
      if (partitionValues.size() >= 2) {
        filter << partitionInfo.columnName << " >= " << partitionValues[0] << 
                  " AND " << partitionInfo.columnName << " < " << partitionValues[1];
      } else if (partitionValues.size() == 1) {
        filter << partitionInfo.columnName << " = " << partitionValues[0];
      }
      break;
      
    case PartitionType::HASH:
      // Hash partitioning típicamente no se filtra directamente
      filter << "1=1";
      break;
  }
  
  return filter.str();
}
