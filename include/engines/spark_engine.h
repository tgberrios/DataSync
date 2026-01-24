#ifndef SPARK_ENGINE_H
#define SPARK_ENGINE_H

#include "third_party/json.hpp"
#include "core/logger.h"
#include <string>
#include <vector>
#include <memory>
#include <map>

using json = nlohmann::json;

#ifdef HAVE_SPARK

// SparkEngine: Wrapper de Apache Spark para procesamiento distribuido
// Soporta Spark Connect (REST API) para integración sin dependencias JNI
class SparkEngine {
public:
  struct SparkConfig {
    std::string masterUrl;           // spark://host:port o local[*]
    std::string appName;
    std::string sparkHome;           // Path a instalación de Spark
    std::string connectUrl;          // URL para Spark Connect (opcional)
    int executorMemoryMB{2048};
    int executorCores{2};
    int maxRetries{3};
    int retryDelaySeconds{5};
    std::map<std::string, std::string> sparkConf;  // Configuraciones adicionales
  };

  struct SparkJob {
    std::string jobId;
    std::string sqlQuery;            // Spark SQL query
    json transformationConfig;       // Configuración de transformación DataSync
    std::string inputPath;            // Path a datos de entrada (S3, HDFS, etc.)
    std::string outputPath;            // Path a datos de salida
    std::vector<std::string> inputFormats;  // Parquet, CSV, JSON, etc.
    std::string outputFormat;
  };

  struct SparkJobResult {
    bool success{false};
    std::string jobId;
    int64_t rowsProcessed{0};
    std::string outputPath;
    std::string errorMessage;
    json metadata;                    // Metadata adicional del job
  };

  explicit SparkEngine(const SparkConfig& config);
  ~SparkEngine();

  // Inicializar conexión con Spark
  bool initialize();

  // Cerrar conexión con Spark
  void shutdown();

  // Verificar si Spark está disponible
  bool isAvailable() const { return available_; }

  // Ejecutar un job Spark
  SparkJobResult executeJob(const SparkJob& job);

  // Ejecutar Spark SQL query directamente
  SparkJobResult executeSQL(const std::string& sqlQuery, const std::string& outputPath = "");

  // Crear DataFrame desde path (S3, HDFS, local)
  std::string createDataFrame(const std::string& path, const std::string& format);

  // Obtener estadísticas de un DataFrame
  json getDataFrameStats(const std::string& dataframeId);

  // Traducir transformación DataSync a Spark SQL
  std::string translateTransformation(const json& transformationConfig);

private:
  SparkConfig config_;
  bool initialized_{false};
  bool available_{false};
  std::string sessionId_;            // Spark session ID para Spark Connect

  // Ejecutar comando REST a Spark Connect
  json executeSparkConnectRequest(const std::string& endpoint, const json& payload);

  // Ejecutar comando via spark-submit
  bool executeSparkSubmit(const std::string& scriptPath, const std::vector<std::string>& args);

  // Generar script Python para Spark job
  std::string generateSparkScript(const SparkJob& job);

  // Retry logic para jobs
  SparkJobResult executeJobWithRetry(const SparkJob& job, int maxRetries);

  // Validar configuración
  bool validateConfig() const;

  // Detectar si Spark está disponible
  bool detectSparkAvailability();
};

#else

// Stub implementation cuando Spark no está disponible
class SparkEngine {
public:
  struct SparkConfig {
    std::string masterUrl;
    std::string appName;
    std::string sparkHome;
    std::string connectUrl;
    int executorMemoryMB{2048};
    int executorCores{2};
    int maxRetries{3};
    int retryDelaySeconds{5};
    std::map<std::string, std::string> sparkConf;
  };

  struct SparkJob {
    std::string jobId;
    std::string sqlQuery;
    json transformationConfig;
    std::string inputPath;
    std::string outputPath;
    std::vector<std::string> inputFormats;
    std::string outputFormat;
  };

  struct SparkJobResult {
    bool success{false};
    std::string jobId;
    int64_t rowsProcessed{0};
    std::string outputPath;
    std::string errorMessage;
    json metadata;
  };

  explicit SparkEngine(const SparkConfig& config [[maybe_unused]]) {
    Logger::warning(LogCategory::SYSTEM, "SparkEngine",
                    "Spark support not compiled. Install Spark and rebuild with HAVE_SPARK.");
  }

  ~SparkEngine() = default;

  bool initialize() { return false; }
  void shutdown() {}
  bool isAvailable() const { return false; }
  SparkJobResult executeJob(const SparkJob&) {
    SparkJobResult result;
    result.success = false;
    result.errorMessage = "Spark not available";
    return result;
  }
  SparkJobResult executeSQL(const std::string&, const std::string& = "") {
    SparkJobResult result;
    result.success = false;
    result.errorMessage = "Spark not available";
    return result;
  }
};

#endif // HAVE_SPARK

#endif // SPARK_ENGINE_H
