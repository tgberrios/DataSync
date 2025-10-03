#ifndef MONITORINGSERVICE_H
#define MONITORINGSERVICE_H

#include "Config.h"
#include "logger.h"
#include <atomic>
#include <memory>

// Forward declarations
class DataQuality;
class MetricsCollector;

class MonitoringService {
public:
  MonitoringService();
  ~MonitoringService();

  // Main monitoring
  void startMonitoring();
  void stopMonitoring();
  bool isMonitoring() const;

  // Quality monitoring
  void runQualityChecks();
  void validateTablesByEngine(const std::string &engine);

  // Metrics monitoring
  void collectSystemMetrics();

private:
  std::atomic<bool> monitoring{false};

  // Components
  DataQuality *dataQuality;
  MetricsCollector *metricsCollector;

  // Helper methods
  void *createConnection();
  void validateConnection(void *conn);
  void logQualityResults(const std::string &engine, size_t tableCount);
  void logMetricsResults();
};

#endif // MONITORINGSERVICE_H
