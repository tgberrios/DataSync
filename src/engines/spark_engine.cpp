#include "engines/spark_engine.h"
#include "core/logger.h"
#include <curl/curl.h>
#include <fstream>
#include <sstream>
#include <thread>
#include <chrono>
#include <cstdlib>
#include <filesystem>

#ifdef HAVE_SPARK

namespace fs = std::filesystem;

// Callback para escribir respuesta HTTP
static size_t WriteCallback(void* contents, size_t size, size_t nmemb, std::string* data) {
  size_t totalSize = size * nmemb;
  data->append((char*)contents, totalSize);
  return totalSize;
}

SparkEngine::SparkEngine(const SparkConfig& config) : config_(config) {
  Logger::info(LogCategory::SYSTEM, "SparkEngine", 
               "Initializing SparkEngine with app: " + config_.appName);
}

SparkEngine::~SparkEngine() {
  shutdown();
}

bool SparkEngine::initialize() {
  if (initialized_) {
    Logger::warning(LogCategory::SYSTEM, "SparkEngine", 
                    "Already initialized");
    return true;
  }

  if (!validateConfig()) {
    Logger::error(LogCategory::SYSTEM, "SparkEngine", 
                 "Invalid Spark configuration");
    return false;
  }

  // Detectar disponibilidad de Spark
  available_ = detectSparkAvailability();
  if (!available_) {
    Logger::warning(LogCategory::SYSTEM, "SparkEngine", 
                    "Spark not available, will use fallback processing");
    return false;
  }

  // Si se usa Spark Connect, inicializar sesión
  if (!config_.connectUrl.empty()) {
    try {
      json initPayload;
      initPayload["client_type"] = "datasync";
      initPayload["client_version"] = "1.0";
      
      json response = executeSparkConnectRequest("/session/create", initPayload);
      if (response.contains("session_id")) {
        sessionId_ = response["session_id"];
        Logger::info(LogCategory::SYSTEM, "SparkEngine", 
                     "Spark Connect session created: " + sessionId_);
      } else {
        Logger::warning(LogCategory::SYSTEM, "SparkEngine", 
                        "Could not create Spark Connect session, will use spark-submit");
      }
    } catch (const std::exception& e) {
      Logger::warning(LogCategory::SYSTEM, "SparkEngine", 
                      "Spark Connect initialization failed: " + std::string(e.what()) + 
                      ", will use spark-submit");
    }
  }

  initialized_ = true;
  Logger::info(LogCategory::SYSTEM, "SparkEngine", 
               "SparkEngine initialized successfully");
  return true;
}

void SparkEngine::shutdown() {
  if (!initialized_) {
    return;
  }

  // Cerrar sesión de Spark Connect si existe
  if (!sessionId_.empty() && !config_.connectUrl.empty()) {
    try {
      json closePayload;
      closePayload["session_id"] = sessionId_;
      executeSparkConnectRequest("/session/close", closePayload);
    } catch (const std::exception& e) {
      Logger::warning(LogCategory::SYSTEM, "SparkEngine", 
                      "Error closing Spark Connect session: " + std::string(e.what()));
    }
    sessionId_.clear();
  }

  initialized_ = false;
  Logger::info(LogCategory::SYSTEM, "SparkEngine", "SparkEngine shutdown");
}

bool SparkEngine::validateConfig() const {
  if (config_.appName.empty()) {
    Logger::error(LogCategory::SYSTEM, "SparkEngine", 
                 "App name is required");
    return false;
  }

  if (config_.masterUrl.empty() && config_.connectUrl.empty() && config_.sparkHome.empty()) {
    Logger::error(LogCategory::SYSTEM, "SparkEngine", 
                 "Either master URL, connect URL, or spark home must be specified");
    return false;
  }

  return true;
}

bool SparkEngine::detectSparkAvailability() {
  // Verificar si spark-submit está disponible
  std::string sparkSubmit = "spark-submit";
  if (!config_.sparkHome.empty()) {
    sparkSubmit = config_.sparkHome + "/bin/spark-submit";
  }

  // Intentar ejecutar spark-submit --version
  std::string command = sparkSubmit + " --version 2>&1";
  FILE* pipe = popen(command.c_str(), "r");
  if (!pipe) {
    Logger::warning(LogCategory::SYSTEM, "SparkEngine", 
                    "Could not execute spark-submit, Spark may not be installed");
    return false;
  }

  char buffer[128];
  std::string result;
  while (fgets(buffer, sizeof(buffer), pipe) != nullptr) {
    result += buffer;
  }
  pclose(pipe);

  if (result.find("version") != std::string::npos || 
      result.find("Spark") != std::string::npos) {
    Logger::info(LogCategory::SYSTEM, "SparkEngine", 
                 "Spark detected: " + result.substr(0, 100));
    return true;
  }

  // Si no se encuentra spark-submit, verificar Spark Connect
  if (!config_.connectUrl.empty()) {
    CURL* curl = curl_easy_init();
    if (curl) {
      curl_easy_setopt(curl, CURLOPT_URL, (config_.connectUrl + "/health").c_str());
      curl_easy_setopt(curl, CURLOPT_TIMEOUT, 5L);
      curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 5L);
      
      CURLcode res = curl_easy_perform(curl);
      long responseCode;
      curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &responseCode);
      curl_easy_cleanup(curl);

      if (res == CURLE_OK && responseCode == 200) {
        Logger::info(LogCategory::SYSTEM, "SparkEngine", 
                     "Spark Connect available at: " + config_.connectUrl);
        return true;
      }
    }
  }

  Logger::warning(LogCategory::SYSTEM, "SparkEngine", 
                  "Spark not available");
  return false;
}

json SparkEngine::executeSparkConnectRequest(const std::string& endpoint, const json& payload) {
  if (config_.connectUrl.empty()) {
    throw std::runtime_error("Spark Connect URL not configured");
  }

  std::string url = config_.connectUrl + endpoint;
  std::string jsonPayload = payload.dump();

  CURL* curl = curl_easy_init();
  if (!curl) {
    throw std::runtime_error("Failed to initialize CURL");
  }

  std::string responseData;
  struct curl_slist* headers = nullptr;
  std::string contentType = "Content-Type: application/json";
  headers = curl_slist_append(headers, contentType.c_str());

  curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
  curl_easy_setopt(curl, CURLOPT_POSTFIELDS, jsonPayload.c_str());
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, &responseData);
  curl_easy_setopt(curl, CURLOPT_TIMEOUT, 300L);
  curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 30L);

  CURLcode res = curl_easy_perform(curl);
  long responseCode;
  curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &responseCode);

  curl_slist_free_all(headers);
  curl_easy_cleanup(curl);

  if (res != CURLE_OK) {
    throw std::runtime_error("CURL error: " + std::string(curl_easy_strerror(res)));
  }

  if (responseCode != 200 && responseCode != 201) {
    throw std::runtime_error("HTTP error: " + std::to_string(responseCode) + 
                            ", response: " + responseData);
  }

  try {
    return json::parse(responseData);
  } catch (const json::exception& e) {
    throw std::runtime_error("Failed to parse JSON response: " + std::string(e.what()));
  }
}

bool SparkEngine::executeSparkSubmit(const std::string& scriptPath, 
                                     const std::vector<std::string>& args) {
  std::string sparkSubmit = "spark-submit";
  if (!config_.sparkHome.empty()) {
    sparkSubmit = config_.sparkHome + "/bin/spark-submit";
  }

  std::string command = sparkSubmit;
  
  // Agregar configuración de Spark
  for (const auto& conf : config_.sparkConf) {
    command += " --conf " + conf.first + "=" + conf.second;
  }
  
  command += " --master " + (config_.masterUrl.empty() ? "local[*]" : config_.masterUrl);
  command += " --executor-memory " + std::to_string(config_.executorMemoryMB) + "m";
  command += " --executor-cores " + std::to_string(config_.executorCores);
  command += " --name " + config_.appName;

  // Agregar argumentos
  for (const auto& arg : args) {
    command += " " + arg;
  }

  command += " " + scriptPath;
  command += " 2>&1";

  Logger::info(LogCategory::SYSTEM, "SparkEngine", 
               "Executing: " + command);

  FILE* pipe = popen(command.c_str(), "r");
  if (!pipe) {
    Logger::error(LogCategory::SYSTEM, "SparkEngine", 
                  "Failed to execute spark-submit");
    return false;
  }

  char buffer[4096];
  std::string output;
  while (fgets(buffer, sizeof(buffer), pipe) != nullptr) {
    output += buffer;
    Logger::debug(LogCategory::SYSTEM, "SparkEngine", 
                  "Spark output: " + std::string(buffer));
  }

  int exitCode = pclose(pipe);
  
  if (exitCode != 0) {
    Logger::error(LogCategory::SYSTEM, "SparkEngine", 
                  "Spark job failed with exit code: " + std::to_string(exitCode) + 
                  ", output: " + output);
    return false;
  }

  Logger::info(LogCategory::SYSTEM, "SparkEngine", 
               "Spark job completed successfully");
  return true;
}

std::string SparkEngine::generateSparkScript(const SparkJob& job) {
  std::ostringstream script;
  
  script << "from pyspark.sql import SparkSession\n";
  script << "from pyspark.sql.functions import *\n";
  script << "import sys\n";
  script << "import json\n\n";
  
  script << "spark = SparkSession.builder \\\n";
  script << "    .appName('" << config_.appName << "') \\\n";
  script << "    .getOrCreate()\n\n";
  
  // Leer datos de entrada
  if (!job.inputPath.empty()) {
    script << "# Read input data\n";
    script << "df = spark.read.format('" << job.inputFormats[0] << "') \\\n";
    script << "    .load('" << job.inputPath << "')\n\n";
  }
  
  // Ejecutar SQL si está disponible
  if (!job.sqlQuery.empty()) {
    script << "# Execute SQL query\n";
    script << "df.createOrReplaceTempView('input_table')\n";
    script << "result = spark.sql(\"\"\"" << job.sqlQuery << "\"\"\")\n\n";
  } else {
    script << "result = df\n\n";
  }
  
  // Escribir resultados
  if (!job.outputPath.empty()) {
    script << "# Write output\n";
    script << "result.write.format('" << job.outputFormat << "') \\\n";
    script << "    .mode('overwrite') \\\n";
    script << "    .save('" << job.outputPath << "')\n\n";
    
    script << "# Get row count\n";
    script << "row_count = result.count()\n";
    script << "print(json.dumps({'rows_processed': row_count, 'output_path': '";
    script << job.outputPath << "'}))\n";
  } else {
    script << "# Collect results\n";
    script << "row_count = result.count()\n";
    script << "print(json.dumps({'rows_processed': row_count}))\n";
  }
  
  script << "\nspark.stop()\n";
  
  return script.str();
}

SparkEngine::SparkJobResult SparkEngine::executeJobWithRetry(const SparkJob& job, int maxRetries) {
  SparkJobResult result;
  result.jobId = job.jobId;
  
  for (int attempt = 0; attempt < maxRetries; ++attempt) {
    if (attempt > 0) {
      Logger::info(LogCategory::SYSTEM, "SparkEngine", 
                   "Retrying job " + job.jobId + " (attempt " + 
                   std::to_string(attempt + 1) + "/" + std::to_string(maxRetries) + ")");
      std::this_thread::sleep_for(std::chrono::seconds(config_.retryDelaySeconds * attempt));
    }

    try {
      // Si Spark Connect está disponible, usarlo
      if (!sessionId_.empty() && !config_.connectUrl.empty()) {
        json jobPayload;
        jobPayload["session_id"] = sessionId_;
        jobPayload["sql"] = job.sqlQuery;
        if (!job.inputPath.empty()) {
          jobPayload["input_path"] = job.inputPath;
        }
        if (!job.outputPath.empty()) {
          jobPayload["output_path"] = job.outputPath;
        }
        
        json response = executeSparkConnectRequest("/sql/execute", jobPayload);
        
        if (response.contains("success") && response["success"].get<bool>()) {
          result.success = true;
          if (response.contains("rows_processed")) {
            result.rowsProcessed = response["rows_processed"].get<int64_t>();
          }
          if (response.contains("output_path")) {
            result.outputPath = response["output_path"].get<std::string>();
          }
          result.metadata = response;
          return result;
        } else {
          result.errorMessage = response.value("error", "Unknown error");
        }
      } else {
        // Usar spark-submit
        std::string scriptContent = generateSparkScript(job);
        
        // Crear archivo temporal para el script
        std::string tempDir = "/tmp/datasync_spark_" + job.jobId;
        fs::create_directories(tempDir);
        std::string scriptPath = tempDir + "/job.py";
        
        std::ofstream scriptFile(scriptPath);
        scriptFile << scriptContent;
        scriptFile.close();
        
        std::vector<std::string> args;
        bool submitSuccess = executeSparkSubmit(scriptPath, args);
        
        // Limpiar archivo temporal
        fs::remove_all(tempDir);
        
        if (submitSuccess) {
          result.success = true;
          // En una implementación real, parsearíamos la salida para obtener row_count
          result.rowsProcessed = 0;  // Placeholder
          result.outputPath = job.outputPath;
          return result;
        } else {
          result.errorMessage = "spark-submit execution failed";
        }
      }
    } catch (const std::exception& e) {
      result.errorMessage = std::string(e.what());
      Logger::error(LogCategory::SYSTEM, "SparkEngine", 
                    "Error executing Spark job: " + result.errorMessage);
    }
  }
  
  result.success = false;
  Logger::error(LogCategory::SYSTEM, "SparkEngine", 
                "Spark job failed after " + std::to_string(maxRetries) + " attempts: " + 
                result.errorMessage);
  return result;
}

SparkEngine::SparkJobResult SparkEngine::executeJob(const SparkJob& job) {
  if (!initialized_) {
    Logger::error(LogCategory::SYSTEM, "SparkEngine", 
                  "SparkEngine not initialized");
    return SparkJobResult{false, job.jobId, 0, "", "Not initialized", json::object()};
  }

  if (!available_) {
    Logger::warning(LogCategory::SYSTEM, "SparkEngine", 
                    "Spark not available, cannot execute job");
    return SparkJobResult{false, job.jobId, 0, "", "Spark not available", json::object()};
  }

  Logger::info(LogCategory::SYSTEM, "SparkEngine", 
               "Executing Spark job: " + job.jobId);
  
  return executeJobWithRetry(job, config_.maxRetries);
}

SparkEngine::SparkJobResult SparkEngine::executeSQL(const std::string& sqlQuery, 
                                                      const std::string& outputPath) {
  SparkJob job;
  job.jobId = "sql_" + std::to_string(std::chrono::system_clock::now().time_since_epoch().count());
  job.sqlQuery = sqlQuery;
  job.outputPath = outputPath;
  job.outputFormat = "parquet";
  
  return executeJob(job);
}

std::string SparkEngine::createDataFrame(const std::string& path, const std::string& format) {
  // En una implementación real, esto crearía un DataFrame y retornaría su ID
  // Por ahora, retornamos un identificador simple
  return "df_" + std::to_string(std::chrono::system_clock::now().time_since_epoch().count());
}

json SparkEngine::getDataFrameStats(const std::string& dataframeId) {
  // Placeholder - en implementación real, consultaría estadísticas del DataFrame
  json stats;
  stats["dataframe_id"] = dataframeId;
  stats["row_count"] = 0;
  stats["column_count"] = 0;
  return stats;
}

std::string SparkEngine::translateTransformation(const json& transformationConfig) {
  // Placeholder - la traducción real se implementará en SparkTranslator
  std::string transformationType = transformationConfig.value("type", "");
  
  if (transformationType == "join") {
    // Traducir join a Spark SQL
    return "SELECT * FROM table1 JOIN table2 ON ...";
  } else if (transformationType == "aggregate") {
    return "SELECT ... GROUP BY ...";
  } else if (transformationType == "filter") {
    return "SELECT * FROM table WHERE ...";
  }
  
  return "SELECT * FROM table";
}

#endif // HAVE_SPARK
