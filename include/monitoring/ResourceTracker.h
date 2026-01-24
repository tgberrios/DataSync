#ifndef RESOURCE_TRACKER_H
#define RESOURCE_TRACKER_H

#include "core/logger.h"
#include "third_party/json.hpp"
#include <chrono>
#include <map>
#include <string>
#include <vector>

using json = nlohmann::json;

// ResourceTracker: Tracking detallado de recursos del sistema
class ResourceTracker {
public:
  struct ResourceMetrics {
    double cpuPercent;
    std::vector<double> cpuPerCore;
    double memoryUsedMB;
    double memoryTotalMB;
    double memoryPercent;
    int64_t ioReadOps;
    int64_t ioWriteOps;
    double ioReadMBps;
    double ioWriteMBps;
    int64_t networkBytesIn;
    int64_t networkBytesOut;
    double networkMBpsIn;
    double networkMBpsOut;
    int dbConnections;
    int dbLocks;
    double dbCacheHitRatio;
    std::string workflowId;
    std::string jobId;
    std::chrono::system_clock::time_point timestamp;
  };

  struct ResourcePrediction {
    std::string resourceType; // "cpu", "memory", "disk"
    double currentUsage;
    double predictedUsage;
    std::chrono::system_clock::time_point predictedAt;
    int daysUntilExhaustion;
    double confidence;
  };

  explicit ResourceTracker(const std::string& connectionString);
  ~ResourceTracker() = default;

  // Recolectar métricas actuales
  ResourceMetrics collectCurrentMetrics();

  // Guardar métricas en base de datos
  bool saveMetrics(const ResourceMetrics& metrics);

  // Obtener métricas históricas
  std::vector<ResourceMetrics> getHistory(const std::string& workflowId = "",
                                          const std::chrono::hours& duration = std::chrono::hours(24));

  // Obtener métricas por workflow
  std::vector<ResourceMetrics> getMetricsByWorkflow(const std::string& workflowId);

  // Predecir capacidad
  std::vector<ResourcePrediction> predictCapacity(int daysAhead = 30);

private:
  std::string connectionString_;

  double getCpuPercent();
  std::vector<double> getCpuPerCore();
  double getMemoryUsedMB();
  double getMemoryTotalMB();
  int64_t getIoReadOps();
  int64_t getIoWriteOps();
  double getIoReadMBps();
  double getIoWriteMBps();
  int64_t getNetworkBytesIn();
  int64_t getNetworkBytesOut();
  int getDbConnections();
  int getDbLocks();
  double getDbCacheHitRatio();
  bool savePredictionToDatabase(const ResourcePrediction& prediction);
};

#endif // RESOURCE_TRACKER_H
