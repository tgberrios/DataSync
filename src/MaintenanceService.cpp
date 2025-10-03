#include "MaintenanceService.h"
#include "MetricsCollector.h"
#include "catalog_manager.h"
#include <chrono>

MaintenanceService::MaintenanceService()
    : catalogManager(std::make_unique<CatalogManager>()),
      metricsCollector(std::make_unique<MetricsCollector>()) {
  Logger::getInstance().info(LogCategory::MONITORING,
                             "MaintenanceService initialized");
}

MaintenanceService::~MaintenanceService() = default;

void MaintenanceService::startMaintenance() {
  maintaining = true;
  Logger::getInstance().info(LogCategory::MONITORING,
                             "Maintenance service started");
}

void MaintenanceService::stopMaintenance() {
  maintaining = false;
  Logger::getInstance().info(LogCategory::MONITORING,
                             "Maintenance service stopped");
}

bool MaintenanceService::isMaintaining() const { return maintaining.load(); }

void MaintenanceService::runMaintenanceCycle() {
  if (!maintaining)
    return;

  try {
    logMaintenanceStart();
    auto cycleStartTime = std::chrono::high_resolution_clock::now();

    performCatalogMaintenance();
    performTableMaintenance();
    performMetricsMaintenance();

    auto cycleEndTime = std::chrono::high_resolution_clock::now();
    auto cycleDuration = std::chrono::duration_cast<std::chrono::seconds>(
        cycleEndTime - cycleStartTime);

    logMaintenanceComplete(static_cast<int>(cycleDuration.count()));
  } catch (const std::exception &e) {
    logMaintenanceError("maintenance cycle", e.what());
  }
}

void MaintenanceService::performCatalogMaintenance() {
  try {
    Logger::getInstance().info(LogCategory::MONITORING,
                               "Performing catalog maintenance");

    // Sync catalogs
    catalogManager->syncCatalogMSSQLToPostgres();
    catalogManager->syncCatalogPostgresToPostgres();

    // Cleanup
    catalogManager->cleanCatalog();
    catalogManager->deactivateNoDataTables();

    Logger::getInstance().info(LogCategory::MONITORING,
                               "Catalog maintenance completed");
  } catch (const std::exception &e) {
    logMaintenanceError("catalog maintenance", e.what());
  }
}

void MaintenanceService::performTableMaintenance() {
  try {
    Logger::getInstance().info(LogCategory::MONITORING,
                               "Performing table maintenance");

    // This would include table optimization, index maintenance, etc.
    // For now, we'll just log that it's happening

    Logger::getInstance().info(LogCategory::MONITORING,
                               "Table maintenance completed");
  } catch (const std::exception &e) {
    logMaintenanceError("table maintenance", e.what());
  }
}

void MaintenanceService::performMetricsMaintenance() {
  try {
    Logger::getInstance().info(LogCategory::MONITORING,
                               "Performing metrics maintenance");

    metricsCollector->collectAllMetrics();

    Logger::getInstance().info(LogCategory::MONITORING,
                               "Metrics maintenance completed");
  } catch (const std::exception &e) {
    logMaintenanceError("metrics maintenance", e.what());
  }
}

void MaintenanceService::logMaintenanceStart() {
  Logger::getInstance().info(LogCategory::MONITORING,
                             "Starting maintenance cycle");
}

void MaintenanceService::logMaintenanceComplete(int durationSeconds) {
  Logger::getInstance().info(LogCategory::MONITORING,
                             "Maintenance cycle completed successfully in " +
                                 std::to_string(durationSeconds) + " seconds");
}

void MaintenanceService::logMaintenanceError(const std::string &task,
                                             const std::string &error) {
  Logger::getInstance().error(LogCategory::MONITORING,
                              "Error in " + task + ": " + error);
}
