#ifndef COLUMN_LINEAGE_EXTRACTOR_H
#define COLUMN_LINEAGE_EXTRACTOR_H

#include "third_party/json.hpp"
#include "core/logger.h"
#include <string>
#include <vector>
#include <map>

using json = nlohmann::json;

// ColumnLineageExtractor: Extrae lineage a nivel de columna desde queries SQL
class ColumnLineageExtractor {
public:
  struct ColumnLineage {
    std::string sourceSchema;
    std::string sourceTable;
    std::string sourceColumn;
    std::string targetSchema;
    std::string targetTable;
    std::string targetColumn;
    std::string transformationType;  // "direct", "expression", "aggregation", "join"
    std::string transformationExpression;  // SQL expression o descripción
    double confidenceScore{1.0};
  };

  // Extraer lineage de columnas desde una query SQL
  static std::vector<ColumnLineage> extractFromQuery(
      const std::string& query,
      const std::string& targetSchema = "",
      const std::string& targetTable = ""
  );

  // Extraer lineage desde múltiples queries (pipeline)
  static std::vector<ColumnLineage> extractFromPipeline(
      const std::vector<std::string>& queries
  );

  // Analizar expresión SQL para encontrar columnas fuente
  static std::vector<std::pair<std::string, std::string>> extractSourceColumns(
      const std::string& expression,
      const std::string& defaultSchema = ""
  );

  // Detectar tipo de transformación
  static std::string detectTransformationType(const std::string& expression);

private:
  // Helper methods
  static std::vector<std::string> parseSelectClause(const std::string& query);
  static std::vector<std::string> parseFromClause(const std::string& query);
  static std::vector<std::string> parseJoinClause(const std::string& query);
  static std::string normalizeColumnReference(const std::string& columnRef);
  static std::pair<std::string, std::string> parseTableColumn(
      const std::string& reference
  );
};

#endif // COLUMN_LINEAGE_EXTRACTOR_H
