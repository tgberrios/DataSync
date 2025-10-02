#ifndef MAINTENANCESERVICE_H
#define MAINTENANCESERVICE_H

#include "Config.h"
#include "logger.h"
#include <atomic>
#include <memory>

// Forward declarations
class MetricsCollector;
class CatalogManager;

class MaintenanceService {
public:
  MaintenanceService();
  ~MaintenanceService();

  // Main maintenance
  void startMaintenance();
  void stopMaintenance();
  bool isMaintaining() const;

  // Maintenance tasks
  void runMaintenanceCycle();
  void performCatalogMaintenance();
  void performTableMaintenance();
  void performMetricsMaintenance();

private:
  std::atomic<bool> maintaining{false};

  // Components
  CatalogManager *catalogManager;
  MetricsCollector *metricsCollector;

  // Helper methods
  void logMaintenanceStart();
  void logMaintenanceComplete(int durationSeconds);
  void logMaintenanceError(const std::string &task, const std::string &error);
};

#endif // MAINTENANCESERVICE_H
