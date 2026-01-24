#include "sync/PartitionPruner.h"
#include <algorithm>
#include <regex>
#include <sstream>
#include <iomanip>

PartitionPruner::PruningResult PartitionPruner::prunePartitions(
    const std::string& query,
    const PartitioningManager::PartitionInfo& partitionInfo,
    const std::vector<std::string>& allPartitions) {
  
  PruningResult result;
  result.totalPartitions = allPartitions.size();

  // Analizar query para extraer filtros
  QueryAnalysis analysis = analyzeQuery(query);

  // Si no hay filtros relevantes, no se puede hacer pruning
  if (analysis.filterColumns.empty() || 
      std::find(analysis.filterColumns.begin(), analysis.filterColumns.end(),
                partitionInfo.columnName) == analysis.filterColumns.end()) {
    result.canPrune = false;
    result.optimizedQuery = query;
    return result;
  }

  // Determinar qué particiones son necesarias
  std::vector<std::string> requiredPartitions;
  for (const auto& partition : allPartitions) {
    if (isPartitionNeeded(partition, partitionInfo, analysis)) {
      requiredPartitions.push_back(partition);
    }
  }

  result.canPrune = requiredPartitions.size() < allPartitions.size();
  result.requiredPartitions = requiredPartitions;
  result.partitionsPruned = allPartitions.size() - requiredPartitions.size();

  // Generar query optimizada
  if (result.canPrune) {
    result.optimizedQuery = generatePrunedQuery(query, partitionInfo, requiredPartitions);
  } else {
    result.optimizedQuery = query;
  }

  Logger::info(LogCategory::SYSTEM, "PartitionPruner",
               "Pruned " + std::to_string(result.partitionsPruned) + 
               " out of " + std::to_string(result.totalPartitions) + " partitions");

  return result;
}

PartitionPruner::QueryAnalysis PartitionPruner::analyzeQuery(const std::string& query) {
  QueryAnalysis analysis;

  // Convertir a minúsculas para análisis
  std::string lowerQuery = query;
  std::transform(lowerQuery.begin(), lowerQuery.end(), lowerQuery.begin(), ::tolower);

  // Buscar patrones de WHERE clause
  std::regex whereRegex(R"(\bwhere\s+(.+?)(?:\s+group\s+by|\s+order\s+by|\s+limit|$))", 
                        std::regex_constants::icase);
  std::smatch whereMatch;
  
  if (std::regex_search(query, whereMatch, whereRegex)) {
    std::string whereClause = whereMatch[1].str();

    // Buscar filtros de igualdad (=)
    std::regex eqRegex(R"((\w+)\s*=\s*['"]?([^'"]+)['"]?)");
    std::sregex_iterator iter(whereClause.begin(), whereClause.end(), eqRegex);
    std::sregex_iterator end;

    for (; iter != end; ++iter) {
      analysis.filterColumns.push_back(iter->str(1));
      analysis.filterValues.push_back(iter->str(2));
      analysis.filterOperator = "=";
    }

    // Buscar filtros IN
    std::regex inRegex(R"((\w+)\s+in\s*\(([^)]+)\))", std::regex_constants::icase);
    std::smatch inMatch;
    if (std::regex_search(whereClause, inMatch, inRegex)) {
      analysis.filterColumns.push_back(inMatch.str(1));
      std::string values = inMatch.str(2);
      // Extraer valores individuales
      std::regex valueRegex(R"(['"]?([^,'"]+)['"]?)");
      std::sregex_iterator valueIter(values.begin(), values.end(), valueRegex);
      for (; valueIter != end; ++valueIter) {
        analysis.filterValues.push_back(valueIter->str(1));
      }
      analysis.filterOperator = "IN";
    }

    // Buscar filtros de fecha
    std::regex dateRegex(R"((\w*date\w*|\w*timestamp\w*)\s*(>=|<=|>|<|=)\s*['"]?([^'"]+)['"]?)", 
                         std::regex_constants::icase);
    std::smatch dateMatch;
    if (std::regex_search(whereClause, dateMatch, dateRegex)) {
      analysis.hasDateFilter = true;
      analysis.dateFilterColumn = dateMatch.str(1);
      analysis.filterOperator = dateMatch.str(2);
      
      // Parsear fecha (simplificado)
      std::string dateStr = dateMatch.str(3);
      // En implementación real, parsear fecha correctamente
    }
  }

  return analysis;
}

std::string PartitionPruner::generatePrunedQuery(
    const std::string& originalQuery,
    const PartitioningManager::PartitionInfo& partitionInfo,
    const std::vector<std::string>& requiredPartitions) {
  
  if (requiredPartitions.empty()) {
    return originalQuery;
  }

  std::string optimizedQuery = originalQuery;

  // Generar condición de partición
  std::stringstream partitionCondition;
  partitionCondition << partitionInfo.columnName << " IN (";

  for (size_t i = 0; i < requiredPartitions.size(); ++i) {
    if (i > 0) partitionCondition << ", ";
    
    if (partitionInfo.type == PartitioningManager::PartitionType::DATE) {
      partitionCondition << "'" << requiredPartitions[i] << "'";
    } else {
      partitionCondition << "'" << requiredPartitions[i] << "'";
    }
  }
  partitionCondition << ")";

  // Agregar condición de partición a WHERE clause
  std::regex whereRegex(R"(\bwhere\b)", std::regex_constants::icase);
  if (std::regex_search(optimizedQuery, whereRegex)) {
    // Ya existe WHERE, agregar AND
    optimizedQuery = std::regex_replace(optimizedQuery, whereRegex,
                                        "WHERE " + partitionCondition.str() + " AND ",
                                        std::regex_constants::format_first_only);
  } else {
    // No existe WHERE, agregar uno nuevo
    std::regex fromRegex(R"(\bfrom\s+(\w+))", std::regex_constants::icase);
    optimizedQuery = std::regex_replace(optimizedQuery, fromRegex,
                                        "FROM $1 WHERE " + partitionCondition.str(),
                                        std::regex_constants::format_first_only);
  }

  return optimizedQuery;
}

std::vector<std::string> PartitionPruner::getModifiedPartitions(
    const PartitioningManager::PartitionInfo& partitionInfo,
    const std::chrono::system_clock::time_point& lastExecutionTime,
    const std::vector<std::string>& allPartitions) {
  
  // En implementación real, consultar metadata de particiones modificadas
  // Por ahora, retornar todas las particiones (placeholder)
  return allPartitions;
}

bool PartitionPruner::isPartitionNeeded(
    const std::string& partitionValue,
    const PartitioningManager::PartitionInfo& partitionInfo,
    const QueryAnalysis& analysis) {
  
  // Si no hay filtros relevantes, todas las particiones son necesarias
  if (analysis.filterColumns.empty()) {
    return true;
  }

  // Verificar si la columna de partición está en los filtros
  bool columnInFilters = std::find(analysis.filterColumns.begin(),
                                    analysis.filterColumns.end(),
                                    partitionInfo.columnName) != analysis.filterColumns.end();

  if (!columnInFilters) {
    return true;  // Sin filtro en columna de partición, necesita todas
  }

  // Verificar según tipo de partición
  switch (partitionInfo.type) {
    case PartitioningManager::PartitionType::DATE:
      return matchesDateFilter(partitionValue, partitionInfo, analysis);
    
    case PartitioningManager::PartitionType::RANGE:
      return matchesRangeFilter(partitionValue, partitionInfo, analysis);
    
    case PartitioningManager::PartitionType::LIST:
      return matchesListFilter(partitionValue, partitionInfo, analysis);
    
    default:
      return true;  // Por seguridad, incluir todas
  }
}

bool PartitionPruner::matchesDateFilter(
    const std::string& partitionValue,
    const PartitioningManager::PartitionInfo& partitionInfo,
    const QueryAnalysis& analysis) {
  
  if (!analysis.hasDateFilter) {
    return true;
  }

  // Comparación simplificada de fechas
  // En implementación real, parsear y comparar fechas correctamente
  for (const auto& filterValue : analysis.filterValues) {
    if (partitionValue == filterValue || partitionValue.find(filterValue) != std::string::npos) {
      return true;
    }
  }

  return false;
}

bool PartitionPruner::matchesRangeFilter(
    const std::string& partitionValue,
    const PartitioningManager::PartitionInfo& partitionInfo,
    const QueryAnalysis& analysis) {
  
  // Para RANGE, verificar si el valor está en el rango
  // Implementación simplificada
  for (const auto& filterValue : analysis.filterValues) {
    if (partitionValue == filterValue) {
      return true;
    }
  }

  return false;
}

bool PartitionPruner::matchesListFilter(
    const std::string& partitionValue,
    const PartitioningManager::PartitionInfo& partitionInfo,
    const QueryAnalysis& analysis) {
  
  // Para LIST, verificar si el valor está en la lista de filtros
  for (const auto& filterValue : analysis.filterValues) {
    if (partitionValue == filterValue) {
      return true;
    }
  }

  return false;
}

std::string PartitionPruner::extractColumnFromFilter(const std::string& filter) {
  std::regex colRegex(R"((\w+)\s*[=<>])");
  std::smatch match;
  if (std::regex_search(filter, match, colRegex)) {
    return match.str(1);
  }
  return "";
}

std::vector<std::string> PartitionPruner::extractValuesFromFilter(const std::string& filter) {
  std::vector<std::string> values;
  std::regex valueRegex(R"(['"]?([^'"]+)['"]?)");
  std::sregex_iterator iter(filter.begin(), filter.end(), valueRegex);
  std::sregex_iterator end;

  for (; iter != end; ++iter) {
    values.push_back(iter->str(1));
  }

  return values;
}
