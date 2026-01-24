#ifndef IMPACT_ANALYZER_H
#define IMPACT_ANALYZER_H

#include "third_party/json.hpp"
#include "core/logger.h"
#include <string>
#include <vector>
#include <map>
#include <set>
#include <memory>

using json = nlohmann::json;

// ImpactAnalyzer: Analiza impacto de cambios en tablas/columnas/workflows
class ImpactAnalyzer {
public:
  struct ImpactResult {
    std::string resourceType;  // "table", "column", "workflow", "transformation"
    std::string resourceName;
    std::string schemaName;
    std::string tableName;
    std::string columnName;
    
    // Impacto downstream (qué se afecta)
    std::vector<std::string> affectedTables;
    std::vector<std::string> affectedWorkflows;
    std::vector<std::string> affectedTransformations;
    std::vector<std::string> affectedColumns;
    
    // Impacto upstream (qué afecta a este recurso)
    std::vector<std::string> dependentTables;
    std::vector<std::string> dependentWorkflows;
    std::vector<std::string> dependentTransformations;
    
    // Estadísticas
    size_t totalDownstreamImpact{0};
    size_t totalUpstreamImpact{0};
    double confidenceScore{1.0};
    
    // Detalles de dependencias
    std::vector<json> dependencyDetails;
  };

  struct AnalysisConfig {
    bool includeWorkflows;
    bool includeTransformations;
    bool includeColumns;
    bool includeLineage;
    int maxDepth;
    bool useCache;

    AnalysisConfig() 
      : includeWorkflows(true),
        includeTransformations(true),
        includeColumns(true),
        includeLineage(true),
        maxDepth(10),
        useCache(true) {}
  };

  explicit ImpactAnalyzer(const std::string& connectionString);
  ~ImpactAnalyzer() = default;

  // Analizar impacto downstream (qué se afecta si cambio X)
  ImpactResult analyzeDownstreamImpact(
      const std::string& schemaName,
      const std::string& tableName,
      const std::string& columnName = "",
      const AnalysisConfig& config = AnalysisConfig()
  );

  // Analizar impacto upstream (qué afecta a X)
  ImpactResult analyzeUpstreamImpact(
      const std::string& schemaName,
      const std::string& tableName,
      const std::string& columnName = "",
      const AnalysisConfig& config = AnalysisConfig()
  );

  // Analizar impacto completo (upstream + downstream)
  ImpactResult analyzeFullImpact(
      const std::string& schemaName,
      const std::string& tableName,
      const std::string& columnName = "",
      const AnalysisConfig& config = AnalysisConfig()
  );

  // Analizar impacto de cambio de schema
  ImpactResult analyzeSchemaChangeImpact(
      const std::string& schemaName,
      const std::string& changeType,  // "drop_table", "rename_table", "alter_column", etc.
      const std::string& tableName = "",
      const std::string& columnName = ""
  );

  // Generar reporte de impacto
  json generateImpactReport(const ImpactResult& result);

private:
  std::string connectionString_;

  // Helper methods
  std::vector<std::string> findAffectedWorkflows(
      const std::string& schemaName,
      const std::string& tableName,
      const std::string& columnName
  );

  std::vector<std::string> findAffectedTransformations(
      const std::string& schemaName,
      const std::string& tableName,
      const std::string& columnName
  );

  std::vector<std::string> findAffectedTablesFromLineage(
      const std::string& schemaName,
      const std::string& tableName,
      const std::string& columnName,
      bool downstream
  );

  std::vector<std::string> findAffectedColumns(
      const std::string& schemaName,
      const std::string& tableName,
      const std::string& columnName
  );

  void traverseLineage(
      const std::string& schemaName,
      const std::string& tableName,
      const std::string& columnName,
      std::set<std::string>& visited,
      std::vector<std::string>& results,
      bool downstream,
      int depth,
      int maxDepth
  );
};

#endif // IMPACT_ANALYZER_H
