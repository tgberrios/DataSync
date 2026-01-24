#include "sync/PushdownOptimizer.h"
#include <algorithm>
#include <regex>
#include <sstream>

PushdownOptimizer::OptimizedQuery PushdownOptimizer::optimize(
    const std::string& originalQuery,
    const std::string& dbEngine,
    const EngineCapabilities& capabilities) {
  
  OptimizedQuery result;
  result.fullyOptimized = false;

  std::string normalizedQuery = normalizeQuery(originalQuery);

  // Extraer componentes de la query
  std::vector<std::string> selectColumns = parseSelectColumns(normalizedQuery);
  std::string whereClause = parseWhereClause(normalizedQuery);
  std::vector<std::string> groupBy = parseGroupBy(normalizedQuery);
  std::vector<std::string> having = parseHaving(normalizedQuery);
  std::string orderBy = parseOrderBy(normalizedQuery);
  std::string limit = parseLimit(normalizedQuery);

  // Extraer nombre de tabla (simplificado)
  std::regex fromRegex(R"(\bfrom\s+(\w+\.\w+|\w+))", std::regex_constants::icase);
  std::smatch fromMatch;
  std::string baseTable = "table";
  if (std::regex_search(normalizedQuery, fromMatch, fromRegex)) {
    baseTable = fromMatch.str(1);
  }

  // Determinar qué puede hacer pushdown
  std::string pushdownFilters;
  std::vector<std::string> pushdownColumns;
  std::vector<std::string> pushdownAggregations;

  // Pushdown de filtros
  if (!whereClause.empty() && 
      std::find(capabilities.supportedCapabilities.begin(),
                capabilities.supportedCapabilities.end(),
                PushdownCapability::FILTERS) != capabilities.supportedCapabilities.end()) {
    pushdownFilters = extractPushdownFilters(whereClause);
    if (!pushdownFilters.empty() && canPushdownFilter(pushdownFilters, capabilities)) {
      result.capabilitiesUsed.push_back(PushdownCapability::FILTERS);
    }
  }

  // Pushdown de proyecciones
  if (!selectColumns.empty() &&
      std::find(capabilities.supportedCapabilities.begin(),
                capabilities.supportedCapabilities.end(),
                PushdownCapability::PROJECTIONS) != capabilities.supportedCapabilities.end()) {
    pushdownColumns = extractPushdownProjections(normalizedQuery);
    if (!pushdownColumns.empty()) {
      result.capabilitiesUsed.push_back(PushdownCapability::PROJECTIONS);
    }
  }

  // Pushdown de agregaciones
  if (!groupBy.empty() &&
      std::find(capabilities.supportedCapabilities.begin(),
                capabilities.supportedCapabilities.end(),
                PushdownCapability::AGGREGATIONS) != capabilities.supportedCapabilities.end()) {
    pushdownAggregations = extractPushdownAggregations(normalizedQuery);
    if (!pushdownAggregations.empty()) {
      result.capabilitiesUsed.push_back(PushdownCapability::AGGREGATIONS);
    }
  }

  // Pushdown de LIMIT
  if (!limit.empty() &&
      std::find(capabilities.supportedCapabilities.begin(),
                capabilities.supportedCapabilities.end(),
                PushdownCapability::LIMIT) != capabilities.supportedCapabilities.end()) {
    result.capabilitiesUsed.push_back(PushdownCapability::LIMIT);
  }

  // Generar query optimizada
  if (!result.capabilitiesUsed.empty()) {
    result.sourceQuery = generatePushdownQuery(
        baseTable,
        pushdownColumns.empty() ? selectColumns : pushdownColumns,
        pushdownFilters,
        pushdownAggregations,
        dbEngine
    );

    // Si se aplicó LIMIT, agregarlo
    if (!limit.empty() && 
        std::find(result.capabilitiesUsed.begin(), result.capabilitiesUsed.end(),
                  PushdownCapability::LIMIT) != result.capabilitiesUsed.end()) {
      result.sourceQuery += " " + limit;
    }

    // Si hay post-procesamiento necesario (ORDER BY, HAVING complejo, etc.)
    if (!orderBy.empty() || !having.empty()) {
      result.postProcessingQuery = "SELECT * FROM (" + result.sourceQuery + ") AS subquery";
      if (!orderBy.empty()) {
        result.postProcessingQuery += " " + orderBy;
      }
      if (!having.empty()) {
        result.postProcessingQuery += " HAVING " + having[0];  // Simplificado
      }
    } else {
      result.fullyOptimized = true;
    }

    Logger::info(LogCategory::SYSTEM, "PushdownOptimizer",
                 "Optimized query with " + std::to_string(result.capabilitiesUsed.size()) + 
                 " pushdown capabilities");
  } else {
    result.sourceQuery = originalQuery;
    result.fullyOptimized = false;
  }

  return result;
}

PushdownOptimizer::EngineCapabilities PushdownOptimizer::detectCapabilities(
    const std::string& dbEngine) {
  
  EngineCapabilities capabilities;
  capabilities.dbEngine = dbEngine;
  std::string lowerEngine = dbEngine;
  std::transform(lowerEngine.begin(), lowerEngine.end(), lowerEngine.begin(), ::tolower);

  // Todos los engines soportan filtros básicos
  capabilities.supportedCapabilities.push_back(PushdownCapability::FILTERS);
  capabilities.supportedCapabilities.push_back(PushdownCapability::PROJECTIONS);
  capabilities.supportedCapabilities.push_back(PushdownCapability::LIMIT);

  // Agregaciones dependen del engine
  if (lowerEngine.find("postgres") != std::string::npos ||
      lowerEngine.find("mysql") != std::string::npos ||
      lowerEngine.find("mariadb") != std::string::npos ||
      lowerEngine.find("mssql") != std::string::npos ||
      lowerEngine.find("oracle") != std::string::npos) {
    capabilities.supportedCapabilities.push_back(PushdownCapability::AGGREGATIONS);
    capabilities.supportsComplexFilters = true;
    capabilities.supportsSubqueries = true;
  }

  // MongoDB tiene capacidades limitadas
  if (lowerEngine.find("mongo") != std::string::npos) {
    capabilities.supportsComplexFilters = false;
    capabilities.supportsSubqueries = false;
    capabilities.maxFilterComplexity = 5;
  }

  return capabilities;
}

std::string PushdownOptimizer::extractPushdownFilters(const std::string& query) {
  std::string whereClause = parseWhereClause(query);
  
  // Remover funciones complejas que no pueden hacer pushdown
  // (simplificado - en implementación real, análisis más profundo)
  std::regex complexFuncRegex(R"(\b(extract|date_trunc|to_char|cast)\s*\()", 
                              std::regex_constants::icase);
  if (std::regex_search(whereClause, complexFuncRegex)) {
    // Si hay funciones complejas, no hacer pushdown completo
    return "";
  }

  return whereClause;
}

std::vector<std::string> PushdownOptimizer::extractPushdownProjections(
    const std::string& query) {
  
  return parseSelectColumns(query);
}

std::vector<std::string> PushdownOptimizer::extractPushdownAggregations(
    const std::string& query) {
  
  std::vector<std::string> aggregations;
  std::regex aggRegex(R"(\b(count|sum|avg|min|max)\s*\([^)]+\))", 
                      std::regex_constants::icase);
  std::sregex_iterator iter(query.begin(), query.end(), aggRegex);
  std::sregex_iterator end;

  for (; iter != end; ++iter) {
    aggregations.push_back(iter->str());
  }

  return aggregations;
}

std::string PushdownOptimizer::generatePushdownQuery(
    const std::string& baseTable,
    const std::vector<std::string>& columns,
    const std::string& filters,
    const std::vector<std::string>& aggregations,
    const std::string& dbEngine) {
  
  std::stringstream query;

  query << "SELECT ";
  
  if (!aggregations.empty()) {
    // Si hay agregaciones, usar GROUP BY
    for (size_t i = 0; i < aggregations.size(); ++i) {
      if (i > 0) query << ", ";
      query << aggregations[i];
    }
  } else if (!columns.empty()) {
    // Columnas específicas
    for (size_t i = 0; i < columns.size(); ++i) {
      if (i > 0) query << ", ";
      query << columns[i];
    }
  } else {
    query << "*";
  }

  query << " FROM " << baseTable;

  if (!filters.empty()) {
    query << " WHERE " << filters;
  }

  if (!aggregations.empty() && !columns.empty()) {
    query << " GROUP BY ";
    for (size_t i = 0; i < columns.size(); ++i) {
      if (i > 0) query << ", ";
      query << columns[i];
    }
  }

  return query.str();
}

bool PushdownOptimizer::canPushdownFilter(const std::string& filter,
                                           const EngineCapabilities& capabilities) {
  if (!capabilities.supportsComplexFilters) {
    // Verificar complejidad del filtro
    std::regex conditionRegex(R"(\b(and|or)\b)", std::regex_constants::icase);
    std::sregex_iterator iter(filter.begin(), filter.end(), conditionRegex);
    size_t complexity = std::distance(iter, std::sregex_iterator()) + 1;
    
    if (complexity > capabilities.maxFilterComplexity) {
      return false;
    }
  }

  return true;
}

bool PushdownOptimizer::canPushdownAggregation(const std::string& aggregation,
                                                 const EngineCapabilities& capabilities) {
  return std::find(capabilities.supportedCapabilities.begin(),
                   capabilities.supportedCapabilities.end(),
                   PushdownCapability::AGGREGATIONS) != capabilities.supportedCapabilities.end();
}

std::string PushdownOptimizer::normalizeQuery(const std::string& query) {
  // Normalizar espacios y convertir a minúsculas para análisis
  std::string normalized = query;
  std::regex multiSpace(R"(\s+)");
  normalized = std::regex_replace(normalized, multiSpace, " ");
  return normalized;
}

std::vector<std::string> PushdownOptimizer::parseSelectColumns(const std::string& query) {
  std::vector<std::string> columns;
  
  std::regex selectRegex(R"(\bselect\s+(.+?)\s+from)", std::regex_constants::icase);
  std::smatch match;
  
  if (std::regex_search(query, match, selectRegex)) {
    std::string selectClause = match.str(1);
    
    // Si es *, retornar vacío (todas las columnas)
    if (selectClause.find("*") != std::string::npos) {
      return columns;
    }

    // Parsear columnas separadas por coma
    std::regex colRegex(R"(([^,]+))");
    std::sregex_iterator iter(selectClause.begin(), selectClause.end(), colRegex);
    std::sregex_iterator end;

    for (; iter != end; ++iter) {
      std::string col = iter->str();
      // Trim whitespace
      col.erase(0, col.find_first_not_of(" \t"));
      col.erase(col.find_last_not_of(" \t") + 1);
      if (!col.empty()) {
        columns.push_back(col);
      }
    }
  }

  return columns;
}

std::string PushdownOptimizer::parseWhereClause(const std::string& query) {
  std::regex whereRegex(R"(\bwhere\s+(.+?)(?:\s+group\s+by|\s+order\s+by|\s+limit|$))", 
                        std::regex_constants::icase);
  std::smatch match;
  
  if (std::regex_search(query, match, whereRegex)) {
    return match.str(1);
  }

  return "";
}

std::vector<std::string> PushdownOptimizer::parseGroupBy(const std::string& query) {
  std::vector<std::string> columns;
  
  std::regex groupByRegex(R"(\bgroup\s+by\s+(.+?)(?:\s+having|\s+order\s+by|\s+limit|$))", 
                          std::regex_constants::icase);
  std::smatch match;
  
  if (std::regex_search(query, match, groupByRegex)) {
    std::string groupByClause = match.str(1);
    std::regex colRegex(R"(([^,]+))");
    std::sregex_iterator iter(groupByClause.begin(), groupByClause.end(), colRegex);
    std::sregex_iterator end;

    for (; iter != end; ++iter) {
      std::string col = iter->str();
      col.erase(0, col.find_first_not_of(" \t"));
      col.erase(col.find_last_not_of(" \t") + 1);
      if (!col.empty()) {
        columns.push_back(col);
      }
    }
  }

  return columns;
}

std::vector<std::string> PushdownOptimizer::parseHaving(const std::string& query) {
  std::vector<std::string> conditions;
  
  std::regex havingRegex(R"(\bhaving\s+(.+?)(?:\s+order\s+by|\s+limit|$))", 
                         std::regex_constants::icase);
  std::smatch match;
  
  if (std::regex_search(query, match, havingRegex)) {
    conditions.push_back(match.str(1));
  }

  return conditions;
}

std::string PushdownOptimizer::parseOrderBy(const std::string& query) {
  std::regex orderByRegex(R"(\border\s+by\s+(.+?)(?:\s+limit|$))", 
                          std::regex_constants::icase);
  std::smatch match;
  
  if (std::regex_search(query, match, orderByRegex)) {
    return "ORDER BY " + match.str(1);
  }

  return "";
}

std::string PushdownOptimizer::parseLimit(const std::string& query) {
  std::regex limitRegex(R"(\blimit\s+(\d+)(?:\s+offset\s+(\d+))?)", 
                        std::regex_constants::icase);
  std::smatch match;
  
  if (std::regex_search(query, match, limitRegex)) {
    std::string limit = "LIMIT " + match.str(1);
    if (match.size() > 2 && !match.str(2).empty()) {
      limit += " OFFSET " + match.str(2);
    }
    return limit;
  }

  return "";
}
