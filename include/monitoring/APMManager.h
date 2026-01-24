#ifndef APM_MANAGER_H
#define APM_MANAGER_H

#include "core/logger.h"
#include "third_party/json.hpp"
#include <chrono>
#include <map>
#include <string>
#include <vector>

using json = nlohmann::json;

// APMManager: Application Performance Monitoring con métricas, health checks y baselines
class APMManager {
public:
  struct APMMetric {
    std::string operationName;
    std::string serviceName;
    int64_t requestCount;
    int64_t errorCount;
    double latencyP50;  // milliseconds
    double latencyP95;  // milliseconds
    double latencyP99;  // milliseconds
    double throughput;  // requests per second
    double errorRate;   // percentage
    std::chrono::system_clock::time_point timestamp;
    std::string timeWindow; // "1min", "5min", "1h"
  };

  struct Baseline {
    std::string operationName;
    std::string serviceName;
    double latencyP50;
    double latencyP95;
    double latencyP99;
    double throughput;
    double errorRate;
    std::chrono::system_clock::time_point calculatedAt;
    int sampleCount;
  };

  struct HealthCheck {
    std::string checkName;
    std::string component; // "database", "external_service", "disk_space"
    std::string status;    // "healthy", "degraded", "unhealthy"
    std::string message;
    std::chrono::system_clock::time_point timestamp;
    json metadata;
  };

  explicit APMManager(const std::string& connectionString);
  ~APMManager() = default;

  // Registrar métrica de request
  void recordRequest(const std::string& operationName, const std::string& serviceName,
                     int64_t latencyMs, bool isError = false);

  // Obtener métricas actuales
  std::vector<APMMetric> getMetrics(const std::string& operationName = "",
                                    const std::string& timeWindow = "1min");

  // Calcular baselines automáticamente
  std::unique_ptr<Baseline> calculateBaseline(const std::string& operationName,
                                              const std::string& serviceName, int days = 7);

  // Obtener baseline
  std::unique_ptr<Baseline> getBaseline(const std::string& operationName,
                                        const std::string& serviceName);

  // Listar todos los baselines
  std::vector<Baseline> listBaselines();

  // Ejecutar health check
  HealthCheck performHealthCheck(const std::string& checkName, const std::string& component);

  // Obtener todos los health checks
  std::vector<HealthCheck> getHealthChecks();

  // Verificar si métrica excede baseline
  bool exceedsBaseline(const APMMetric& metric, const Baseline& baseline, double threshold = 1.5);

private:
  std::string connectionString_;
  std::map<std::string, std::vector<int64_t>> latencySamples_; // operation -> latencies

  void aggregateMetrics();
  bool saveMetricToDatabase(const APMMetric& metric);
  bool saveBaselineToDatabase(const Baseline& baseline);
  bool saveHealthCheckToDatabase(const HealthCheck& healthCheck);
  HealthCheck checkDatabaseHealth();
  HealthCheck checkDiskSpaceHealth();
  HealthCheck checkExternalServiceHealth(const std::string& serviceName);
};

#endif // APM_MANAGER_H
