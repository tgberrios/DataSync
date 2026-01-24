#ifndef LOG_AGGREGATOR_H
#define LOG_AGGREGATOR_H

#include "core/logger.h"
#include "third_party/json.hpp"
#include <string>
#include <vector>

using json = nlohmann::json;

// LogAggregator: Export de logs a Elasticsearch y Splunk
class LogAggregator {
public:
  struct AggregationConfig {
    std::string id;
    std::string type; // "elasticsearch", "splunk"
    std::string endpoint;
    std::string index;      // Para Elasticsearch
    std::string token;       // Para Splunk HEC
    std::string username;
    std::string password;
    bool enabled;
    int batchSize;
    int batchIntervalSeconds;
  };

  struct AggregationStatus {
    std::string configId;
    std::chrono::system_clock::time_point lastExport;
    int64_t logsExported;
    int64_t logsFailed;
    std::string lastError;
    bool isActive;
  };

  explicit LogAggregator(const std::string& connectionString);
  ~LogAggregator() = default;

  // Configurar destino de agregación
  bool configure(const AggregationConfig& config);

  // Obtener configuración
  std::unique_ptr<AggregationConfig> getConfig(const std::string& configId);

  // Listar configuraciones
  std::vector<AggregationConfig> listConfigs();

  // Export logs a destino configurado
  int exportLogs(const std::string& configId, int limit = 1000);

  // Obtener estado de exportación
  std::unique_ptr<AggregationStatus> getStatus(const std::string& configId);

  // Export a Elasticsearch
  bool exportToElasticsearch(const AggregationConfig& config,
                             const std::vector<json>& logs);

  // Export a Splunk
  bool exportToSplunk(const AggregationConfig& config, const std::vector<json>& logs);

private:
  std::string connectionString_;

  std::vector<json> fetchLogsFromDatabase(int limit);
  json formatLogForExport(const json& log);
  bool saveConfigToDatabase(const AggregationConfig& config);
  bool saveStatusToDatabase(const AggregationStatus& status);
};

#endif // LOG_AGGREGATOR_H
