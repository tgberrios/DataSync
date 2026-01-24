#include "transformations/spark_transformation.h"
#include "core/logger.h"

#ifdef HAVE_SPARK

SparkTransformation::SparkTransformation(std::shared_ptr<SparkEngine> sparkEngine)
    : sparkEngine_(sparkEngine) {
  Logger::info(LogCategory::TRANSFORM, "SparkTransformation",
               "Initializing SparkTransformation");
}

std::vector<json> SparkTransformation::execute(
    const std::vector<json>& inputData,
    const json& config) {
  
  if (!sparkEngine_ || !sparkEngine_->isAvailable()) {
    Logger::warning(LogCategory::TRANSFORM, "SparkTransformation",
                    "Spark not available, cannot execute transformation");
    return inputData;
  }
  
  // Traducir transformación a Spark SQL
  SparkTranslator::TranslationResult translation = SparkTranslator::translateTransformation(config);
  
  if (translation.sparkSQL.empty()) {
    Logger::error(LogCategory::TRANSFORM, "SparkTransformation",
                  "Failed to translate transformation to Spark SQL");
    return inputData;
  }
  
  // Ejecutar en Spark
  return executeInSpark(config, inputData);
}

bool SparkTransformation::validateConfig(const json& config) const {
  if (!config.contains("type") || !config["type"].is_string()) {
    return false;
  }
  
  // Validar que el tipo de transformación sea soportado
  std::string type = config["type"].get<std::string>();
  std::vector<std::string> supportedTypes = {
    "join", "aggregate", "filter", "sort", "expression",
    "lookup", "union", "window", "deduplication"
  };
  
  return std::find(supportedTypes.begin(), supportedTypes.end(), type) != supportedTypes.end();
}

std::vector<json> SparkTransformation::executeInSpark(
    const json& transformationConfig,
    const std::vector<json>& inputData) {
  
  // Traducir a Spark SQL
  SparkTranslator::TranslationResult translation = SparkTranslator::translateTransformation(transformationConfig);
  
  // Crear tabla temporal con datos de entrada
  std::string tempTablePath = "/tmp/datasync_input_" + 
                             std::to_string(std::chrono::system_clock::now().time_since_epoch().count());
  
  // Ejecutar query Spark
  std::string outputPath = "/tmp/datasync_output_" + 
                          std::to_string(std::chrono::system_clock::now().time_since_epoch().count());
  
  SparkEngine::SparkJobResult result = sparkEngine_->executeSQL(translation.sparkSQL, outputPath);
  
  if (!result.success) {
    Logger::error(LogCategory::TRANSFORM, "SparkTransformation",
                  "Spark execution failed: " + result.errorMessage);
    return {};
  }
  
  // En una implementación real, leeríamos los resultados desde outputPath
  // Por ahora, retornamos datos vacíos como placeholder
  std::vector<json> outputData;
  
  Logger::info(LogCategory::TRANSFORM, "SparkTransformation",
               "Transformation executed successfully in Spark");
  
  return outputData;
}

#endif // HAVE_SPARK
