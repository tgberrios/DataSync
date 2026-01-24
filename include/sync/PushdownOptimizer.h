#ifndef PUSHDOWN_OPTIMIZER_H
#define PUSHDOWN_OPTIMIZER_H

#include "third_party/json.hpp"
#include "core/logger.h"
#include <string>
#include <vector>
#include <map>
#include <optional>

using json = nlohmann::json;

// PushdownOptimizer: Optimiza queries ejecutando filtros/proyecciones/agregaciones en source
class PushdownOptimizer {
public:
  enum class PushdownCapability {
    FILTERS,        // WHERE clauses
    PROJECTIONS,    // SELECT columns
    AGGREGATIONS,   // COUNT, SUM, AVG, etc.
    JOINS,          // JOIN operations
    LIMIT           // LIMIT/OFFSET
  };

  struct OptimizedQuery {
    std::string sourceQuery;      // Query optimizada para source
    std::string postProcessingQuery;  // Query para post-procesamiento si es necesario
    std::vector<PushdownCapability> capabilitiesUsed;
    bool fullyOptimized{false};
    size_t estimatedRowsReduced{0};
  };

  struct EngineCapabilities {
    std::string dbEngine;
    std::vector<PushdownCapability> supportedCapabilities;
    bool supportsComplexFilters{false};
    bool supportsSubqueries{false};
    size_t maxFilterComplexity{10};  // Número máximo de condiciones en WHERE
  };

  // Optimizar query con pushdown
  static OptimizedQuery optimize(
      const std::string& originalQuery,
      const std::string& dbEngine,
      const EngineCapabilities& capabilities
  );

  // Detectar capacidades de un engine
  static EngineCapabilities detectCapabilities(const std::string& dbEngine);

  // Extraer filtros que pueden hacer pushdown
  static std::string extractPushdownFilters(const std::string& query);

  // Extraer proyecciones que pueden hacer pushdown
  static std::vector<std::string> extractPushdownProjections(const std::string& query);

  // Extraer agregaciones que pueden hacer pushdown
  static std::vector<std::string> extractPushdownAggregations(const std::string& query);

  // Generar query con pushdown aplicado
  static std::string generatePushdownQuery(
      const std::string& baseTable,
      const std::vector<std::string>& columns,
      const std::string& filters,
      const std::vector<std::string>& aggregations,
      const std::string& dbEngine
  );

  // Verificar si un filtro puede hacer pushdown
  static bool canPushdownFilter(const std::string& filter, const EngineCapabilities& capabilities);

  // Verificar si una agregación puede hacer pushdown
  static bool canPushdownAggregation(const std::string& aggregation, 
                                      const EngineCapabilities& capabilities);

private:
  // Helper methods
  static std::string normalizeQuery(const std::string& query);
  static std::vector<std::string> parseSelectColumns(const std::string& query);
  static std::string parseWhereClause(const std::string& query);
  static std::vector<std::string> parseGroupBy(const std::string& query);
  static std::vector<std::string> parseHaving(const std::string& query);
  static std::string parseOrderBy(const std::string& query);
  static std::string parseLimit(const std::string& query);
};

#endif // PUSHDOWN_OPTIMIZER_H
