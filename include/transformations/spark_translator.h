#ifndef SPARK_TRANSLATOR_H
#define SPARK_TRANSLATOR_H

#include "third_party/json.hpp"
#include "core/logger.h"
#include <string>
#include <vector>
#include <map>

using json = nlohmann::json;

// SparkTranslator: Traduce transformaciones DataSync a Spark SQL/DataFrames
class SparkTranslator {
public:
  struct TranslationResult {
    std::string sparkSQL;              // Spark SQL query generada
    std::string dataframeCode;         // Código Python para DataFrame (alternativa)
    bool useSQL{true};                 // Si true, usar SQL; si false, usar DataFrame API
    std::vector<std::string> tempViews; // Nombres de temp views creadas
    json metadata;                     // Metadata adicional
  };

  // Traducir una transformación individual
  static TranslationResult translateTransformation(const json& transformationConfig);

  // Traducir un pipeline completo de transformaciones
  static TranslationResult translatePipeline(const json& pipelineConfig);

  // Traducir transformación de tipo específico
  static std::string translateJoin(const json& config);
  static std::string translateAggregate(const json& config);
  static std::string translateFilter(const json& config);
  static std::string translateSort(const json& config);
  static std::string translateExpression(const json& config);
  static std::string translateLookup(const json& config);
  static std::string translateUnion(const json& config);
  static std::string translateWindowFunction(const json& config);
  static std::string translateDeduplication(const json& config);
  static std::string translateRouter(const json& config);
  static std::string translateNormalizer(const json& config);
  static std::string translateDataCleansing(const json& config);
  static std::string translateDataValidation(const json& config);

  // Generar código Python para DataFrame API
  static std::string generateDataFrameCode(const json& transformationConfig);

  // Optimizar query Spark SQL
  static std::string optimizeSQL(const std::string& sql);

private:
  // Helpers para construcción de SQL
  static std::string escapeSQLIdentifier(const std::string& identifier);
  static std::string escapeSQLValue(const std::string& value);
  static std::string buildColumnList(const std::vector<std::string>& columns);
  static std::string buildJoinCondition(const json& joinConfig);
  static std::string buildAggregateExpression(const json& aggConfig);
  static std::string buildFilterExpression(const json& filterConfig);
  static std::string buildSortExpression(const json& sortConfig);
  static std::string buildWindowExpression(const json& windowConfig);
};

#endif // SPARK_TRANSLATOR_H
