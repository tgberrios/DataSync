#include "sync/PartitioningManager.h"
#include "core/logger.h"
#include <pqxx/pqxx>
#include <algorithm>
#include <regex>
#include <sstream>
#include <iomanip>
#include <chrono>

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

bool PartitioningManager::createDynamicPartition(
    const std::string& schemaName,
    const std::string& tableName,
    const PartitionInfo& partitionInfo,
    const std::string& partitionValue,
    const std::string& connectionString) {
  
  try {
    pqxx::connection conn(connectionString);
    pqxx::work txn(conn);

    std::string partitionName = generatePartitionName(partitionInfo, partitionValue);
    
    // Verificar si la partición ya existe
    auto checkResult = txn.exec_params(
        "SELECT COUNT(*) FROM pg_inherits WHERE inhrelid = "
        "(SELECT oid FROM pg_class WHERE relname = $1)",
        partitionName
    );
    
    if (checkResult[0][0].as<int>() > 0) {
      Logger::debug(LogCategory::SYSTEM, "PartitioningManager",
                    "Partition already exists: " + partitionName);
      txn.commit();
      return true;
    }

    // Generar SQL para crear partición según tipo
    std::stringstream sql;
    sql << "CREATE TABLE IF NOT EXISTS " << schemaName << "." << partitionName 
        << " PARTITION OF " << schemaName << "." << tableName;

    switch (partitionInfo.type) {
      case PartitionType::DATE: {
        // Para particiones de fecha, usar FOR VALUES FROM ... TO
        sql << " FOR VALUES FROM ('" << partitionValue << "') TO ('";
        // Calcular siguiente valor (simplificado)
        if (partitionInfo.format == "year") {
          int year = std::stoi(partitionValue);
          sql << (year + 1);
        } else if (partitionInfo.format == "year-month") {
          // Incrementar mes
          sql << partitionValue << "-01";  // Simplificado
        } else {
          sql << partitionValue << " 00:00:00'::timestamp + INTERVAL '1 day'";
        }
        sql << "')";
        break;
      }
      
      case PartitionType::RANGE: {
        sql << " FOR VALUES FROM (" << partitionValue << ") TO (";
        // Calcular siguiente valor en rango (simplificado)
        sql << partitionValue << " + 1000)";  // Placeholder
        sql << ")";
        break;
      }
      
      case PartitionType::LIST: {
        sql << " FOR VALUES IN ('" << partitionValue << "')";
        break;
      }
      
      default:
        Logger::error(LogCategory::SYSTEM, "PartitioningManager",
                      "Unsupported partition type for dynamic creation");
        return false;
    }

    txn.exec(sql.str());
    txn.commit();

    Logger::info(LogCategory::SYSTEM, "PartitioningManager",
                 "Created dynamic partition: " + partitionName);
    return true;
  } catch (const std::exception& e) {
    Logger::error(LogCategory::SYSTEM, "PartitioningManager",
                  "Error creating dynamic partition: " + std::string(e.what()));
    return false;
  }
}

std::vector<std::string> PartitioningManager::getExistingPartitions(
    const std::string& schemaName,
    const std::string& tableName,
    const PartitionInfo& partitionInfo,
    const std::string& connectionString) {
  
  std::vector<std::string> partitions;

  try {
    pqxx::connection conn(connectionString);
    pqxx::work txn(conn);

    // Obtener particiones de la tabla
    auto result = txn.exec_params(
        "SELECT relname FROM pg_inherits i "
        "JOIN pg_class c ON i.inhrelid = c.oid "
        "JOIN pg_class p ON i.inhparent = p.oid "
        "WHERE p.relname = $1 AND c.relnamespace = "
        "(SELECT oid FROM pg_namespace WHERE nspname = $2)",
        tableName, schemaName
    );

    for (const auto& row : result) {
      std::string partitionName = row["relname"].as<std::string>();
      // Extraer valor de partición del nombre (simplificado)
      partitions.push_back(partitionName);
    }

    txn.commit();
  } catch (const std::exception& e) {
    Logger::error(LogCategory::SYSTEM, "PartitioningManager",
                  "Error getting existing partitions: " + std::string(e.what()));
  }

  return partitions;
}

bool PartitioningManager::needsNewPartition(
    const std::string& partitionValue,
    const std::vector<std::string>& existingPartitions,
    const PartitionInfo& partitionInfo) {
  
  // Verificar si ya existe una partición para este valor
  std::string expectedName = generatePartitionName(partitionInfo, partitionValue);
  
  for (const auto& existing : existingPartitions) {
    if (existing.find(expectedName) != std::string::npos ||
        existing.find(partitionValue) != std::string::npos) {
      return false;  // Ya existe
    }
  }

  return true;  // Necesita nueva partición
}

std::string PartitioningManager::generatePartitionName(
    const PartitionInfo& partitionInfo,
    const std::string& partitionValue) {
  
  std::stringstream name;
  name << partitionInfo.columnName << "_";
  
  switch (partitionInfo.type) {
    case PartitionType::DATE: {
      name << "p" << partitionValue;
      // Reemplazar caracteres no válidos
      std::string dateStr = name.str();
      std::replace(dateStr.begin(), dateStr.end(), '-', '_');
      std::replace(dateStr.begin(), dateStr.end(), ' ', '_');
      std::replace(dateStr.begin(), dateStr.end(), ':', '_');
      return dateStr;
    }
    
    case PartitionType::RANGE:
      name << "p" << partitionValue;
      return name.str();
    
    case PartitionType::LIST: {
      name << "p" << partitionValue;
      // Sanitizar para nombre de tabla
      std::string listStr = name.str();
      std::replace(listStr.begin(), listStr.end(), ' ', '_');
      std::replace(listStr.begin(), listStr.end(), '-', '_');
      return listStr;
    }
    
    default:
      name << "p" << std::hash<std::string>{}(partitionValue);
      return name.str();
  }
}

size_t PartitioningManager::dropOldPartitions(
    const std::string& schemaName,
    const std::string& tableName,
    const PartitionInfo& partitionInfo,
    int retentionDays,
    const std::string& connectionString) {
  
  size_t dropped = 0;

  try {
    pqxx::connection conn(connectionString);
    pqxx::work txn(conn);

    // Calcular fecha de corte (retentionDays en días)
    auto cutoffDate = std::chrono::system_clock::now() - 
                      std::chrono::hours(24 * retentionDays);
    auto cutoffTimeT = std::chrono::system_clock::to_time_t(cutoffDate);

    // Obtener particiones antiguas (para DATE partitions)
    if (partitionInfo.type == PartitionType::DATE) {
      auto result = txn.exec_params(
          "SELECT relname FROM pg_inherits i "
          "JOIN pg_class c ON i.inhrelid = c.oid "
          "JOIN pg_class p ON i.inhparent = p.oid "
          "WHERE p.relname = $1 AND c.relnamespace = "
          "(SELECT oid FROM pg_namespace WHERE nspname = $2) "
          "AND pg_stat_file(pg_relation_filepath(c.oid))::text::timestamp < to_timestamp($3)",
          tableName, schemaName, cutoffTimeT
      );

      for (const auto& row : result) {
        std::string partitionName = row["relname"].as<std::string>();
        txn.exec("DROP TABLE IF EXISTS " + schemaName + "." + partitionName);
        dropped++;
      }
    }

    txn.commit();

    if (dropped > 0) {
      Logger::info(LogCategory::SYSTEM, "PartitioningManager",
                   "Dropped " + std::to_string(dropped) + " old partitions");
    }
  } catch (const std::exception& e) {
    Logger::error(LogCategory::SYSTEM, "PartitioningManager",
                  "Error dropping old partitions: " + std::string(e.what()));
  }

  return dropped;
}
