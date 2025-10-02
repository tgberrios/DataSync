#ifndef STREAMINGDATA_H
#define STREAMINGDATA_H

#include "ConfigManager.h"
#include "MaintenanceService.h"
#include "MonitoringService.h"
#include "ThreadManager.h"
#include "TransferOrchestrator.h"
#include "logger.h"
#include <memory>

class StreamingData {
public:
  StreamingData();
  ~StreamingData();

  // Main control
  void initialize();
  void run();
  void shutdown();

private:
  // Core components
  std::unique_ptr<ThreadManager> threadManager;
  std::unique_ptr<ConfigManager> configManager;
  std::unique_ptr<TransferOrchestrator> transferOrchestrator;
  std::unique_ptr<MonitoringService> monitoringService;
  std::unique_ptr<MaintenanceService> maintenanceService;

  // Thread functions
  void initializationThread();
  void catalogSyncThread();
  void transferThread();
  void monitoringThread();
  void maintenanceThread();

  // Helper methods
  void initializeComponents();
  void startAllThreads();
  void logSystemStatus();
};

#endif // STREAMINGDATA_H