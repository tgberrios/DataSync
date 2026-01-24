#ifndef IMPACT_ANALYSIS_REPOSITORY_H
#define IMPACT_ANALYSIS_REPOSITORY_H

#include "governance/ImpactAnalyzer.h"
#include "third_party/json.hpp"
#include <pqxx/pqxx>
#include <string>
#include <vector>
#include <chrono>

using json = nlohmann::json;

// ImpactAnalysisRepository: Persistencia de análisis de impacto
class ImpactAnalysisRepository {
public:
  explicit ImpactAnalysisRepository(pqxx::connection& conn);
  ~ImpactAnalysisRepository() = default;

  // Guardar resultado de análisis
  int saveAnalysisResult(
      const ImpactAnalyzer::ImpactResult& result,
      const std::string& changeType = "",
      const std::string& userId = ""
  );

  // Obtener análisis guardado
  ImpactAnalyzer::ImpactResult getAnalysisResult(int analysisId);

  // Obtener historial de análisis para un recurso
  std::vector<json> getAnalysisHistory(
      const std::string& schemaName,
      const std::string& tableName,
      const std::string& columnName = "",
      int limit = 50
  );

  // Eliminar análisis antiguos
  void cleanupOldAnalyses(int daysToKeep = 90);

  // Inicializar tablas si no existen
  static void initializeTables(pqxx::connection& conn);

private:
  pqxx::connection& conn_;
};

#endif // IMPACT_ANALYSIS_REPOSITORY_H
