#include "StreamingData.h"
#include "DDLExporter.h"
#include "DataGovernance.h"
#include "catalog_manager.h"
#include <chrono>
#include <pqxx/pqxx>
#include <thread>

StreamingData::StreamingData() {
  // Initialize components
  threadManager = std::make_unique<ThreadManager>();
  configManager = std::make_unique<ConfigManager>();
  transferOrchestrator = std::make_unique<TransferOrchestrator>();
  monitoringService = std::make_unique<MonitoringService>();
  maintenanceService = std::make_unique<MaintenanceService>();

  Logger::getInstance().info(LogCategory::MONITORING,
                             "StreamingData initialized");
}

StreamingData::~StreamingData() { shutdown(); }

void StreamingData::initialize() {
  try {
    Logger::getInstance().info(LogCategory::MONITORING,
                               "Initializing DataSync system components");

    initializeComponents();
    startAllThreads();

    Logger::getInstance().info(LogCategory::MONITORING,
                               "System initialization completed successfully");
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::MONITORING,
                                "Error during system initialization: " +
                                    std::string(e.what()));
  }
}

void StreamingData::run() {
  try {
    Logger::getInstance().info(LogCategory::MONITORING,
                               "Starting DataSync system");

    threadManager->start();
    threadManager->waitForAll();

    Logger::getInstance().info(LogCategory::MONITORING,
                               "DataSync system completed");
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::MONITORING,
                                "Error running DataSync system: " +
                                    std::string(e.what()));
  }
}

void StreamingData::shutdown() {
  try {
    Logger::getInstance().info(LogCategory::MONITORING,
                               "Shutting down DataSync system");

    if (threadManager) {
      threadManager->stop();
    }

    if (transferOrchestrator) {
      transferOrchestrator->stop();
    }

    if (monitoringService) {
      monitoringService->stopMonitoring();
    }

    if (maintenanceService) {
      maintenanceService->stopMaintenance();
    }

    Logger::getInstance().info(LogCategory::MONITORING,
                               "Shutdown completed successfully");
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::MONITORING,
                                "Error during shutdown: " +
                                    std::string(e.what()));
  }
}

void StreamingData::initializeComponents() {
  try {
    // DataGovernance
    Logger::getInstance().info(LogCategory::MONITORING,
                               "Initializing DataGovernance component");
    DataGovernance dg;
    dg.initialize();
    dg.runDiscovery();
    dg.generateReport();
    Logger::getInstance().info(LogCategory::MONITORING,
                               "DataGovernance initialized successfully");

    // DDLExporter
    Logger::getInstance().info(LogCategory::MONITORING,
                               "Initializing DDLExporter component");
    DDLExporter ddlExporter;
    ddlExporter.exportAllDDL();
    Logger::getInstance().info(LogCategory::MONITORING,
                               "DDLExporter completed successfully");

    // TransferOrchestrator
    Logger::getInstance().info(LogCategory::MONITORING,
                               "Initializing TransferOrchestrator component");
    transferOrchestrator->initialize();
    Logger::getInstance().info(LogCategory::MONITORING,
                               "TransferOrchestrator initialized successfully");

    Logger::getInstance().info(LogCategory::MONITORING,
                               "All components initialized successfully");
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::MONITORING,
                                "Error initializing components: " +
                                    std::string(e.what()));
  }
}

void StreamingData::startAllThreads() {
  try {
    Logger::getInstance().info(LogCategory::MONITORING,
                               "Starting all system threads");

    // Add all thread functions
    threadManager->addThread("initialization",
                             [this]() { initializationThread(); });
    threadManager->addThread("catalog_sync", [this]() { catalogSyncThread(); });
    threadManager->addThread("transfer", [this]() { transferThread(); });
    threadManager->addThread("monitoring", [this]() { monitoringThread(); });
    threadManager->addThread("maintenance", [this]() { maintenanceThread(); });

    Logger::getInstance().info(LogCategory::MONITORING,
                               "All threads started successfully");
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::MONITORING,
                                "Error starting threads: " +
                                    std::string(e.what()));
  }
}

void StreamingData::initializationThread() {
  try {
    Logger::getInstance().info(LogCategory::MONITORING,
                               "Initialization thread started");

    // This thread runs once during startup
    Logger::getInstance().info(LogCategory::MONITORING,
                               "Initialization thread completed");
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::MONITORING,
                                "Error in initialization thread: " +
                                    std::string(e.what()));
  }
}

void StreamingData::catalogSyncThread() {
  try {
    Logger::getInstance().info(LogCategory::MONITORING,
                               "Catalog sync thread started");

    while (threadManager->isRunning()) {
      try {
        if (!threadManager->isRunning())
          break;

        // Mark catalog sync as running
        catalogSyncRunning.store(true);

        Logger::getInstance().info(LogCategory::MONITORING,
                                   "Starting catalog synchronization cycle");

        // Create catalog manager for this thread
        CatalogManager catalogManager;

        // Sync all catalogs
        catalogManager.syncCatalogMariaDBToPostgres();
        catalogManager.syncCatalogMSSQLToPostgres();
        catalogManager.syncCatalogPostgresToPostgres();

        // Cleanup
        catalogManager.cleanCatalog();
        catalogManager.deactivateNoDataTables();

        Logger::getInstance().info(LogCategory::MONITORING,
                                   "Catalog synchronization cycle completed");

        // Mark catalog sync as completed
        catalogSyncRunning.store(false);

      } catch (const std::exception &e) {
        catalogSyncRunning.store(false);
        Logger::getInstance().error(LogCategory::MONITORING,
                                    "Error in catalog sync cycle: " +
                                        std::string(e.what()));
      }

      threadManager->sleepFor(
          "catalog_sync", static_cast<int>(configManager->getSyncInterval()));
    }

    Logger::getInstance().info(LogCategory::MONITORING,
                               "Catalog sync thread stopped");
  } catch (const std::exception &e) {
    catalogSyncRunning.store(false);
    Logger::getInstance().error(LogCategory::MONITORING,
                                "Critical error in catalog sync thread: " +
                                    std::string(e.what()));
  }
}

void StreamingData::transferThread() {
  try {
    Logger::getInstance().info(LogCategory::MONITORING,
                               "Transfer thread started");

    while (threadManager->isRunning()) {
      try {
        if (!threadManager->isRunning())
          break;

        // Mark transfer as running
        transferRunning.store(true);

        Logger::getInstance().info(LogCategory::MONITORING,
                                   "Starting transfer process...");

        transferOrchestrator->runTransfers();

        Logger::getInstance().info(LogCategory::MONITORING,
                                   "Transfer process completed");

        // Mark transfer as completed
        transferRunning.store(false);

      } catch (const std::exception &e) {
        transferRunning.store(false);
        Logger::getInstance().error(LogCategory::MONITORING,
                                    "Error in transfer thread: " +
                                        std::string(e.what()));
      }

      threadManager->sleepFor("transfer", 10); // Sleep between transfer cycles
    }

    Logger::getInstance().info(LogCategory::MONITORING,
                               "Transfer thread stopped");
  } catch (const std::exception &e) {
    transferRunning.store(false);
    Logger::getInstance().error(LogCategory::MONITORING,
                                "Critical error in transfer thread: " +
                                    std::string(e.what()));
  }
}

void StreamingData::monitoringThread() {
  try {
    Logger::getInstance().info(LogCategory::MONITORING,
                               "Monitoring thread started");

    monitoringService->startMonitoring();

    while (threadManager->isRunning()) {
      try {
        // Load configuration
        configManager->refreshConfig();

        // Run quality checks
        monitoringService->runQualityChecks();

        // Collect metrics
        monitoringService->collectSystemMetrics();

      } catch (const std::exception &e) {
        Logger::getInstance().error(LogCategory::MONITORING,
                                    "Error in monitoring thread: " +
                                        std::string(e.what()));
      }

      threadManager->sleepFor(
          "monitoring", static_cast<int>(configManager->getSyncInterval()));
    }

    monitoringService->stopMonitoring();
    Logger::getInstance().info(LogCategory::MONITORING,
                               "Monitoring thread stopped");
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::MONITORING,
                                "Critical error in monitoring thread: " +
                                    std::string(e.what()));
  }
}

void StreamingData::maintenanceThread() {
  try {
    Logger::getInstance().info(LogCategory::MONITORING,
                               "Maintenance thread started");

    maintenanceService->startMaintenance();

    while (threadManager->isRunning()) {
      try {
        maintenanceService->runMaintenanceCycle();
      } catch (const std::exception &e) {
        Logger::getInstance().error(LogCategory::MONITORING,
                                    "Error in maintenance thread: " +
                                        std::string(e.what()));
      }

      // Maintenance runs less frequently
      threadManager->sleepFor(
          "maintenance",
          static_cast<int>(configManager->getSyncInterval() * 4));
    }

    maintenanceService->stopMaintenance();
    Logger::getInstance().info(LogCategory::MONITORING,
                               "Maintenance thread stopped");
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::MONITORING,
                                "Critical error in maintenance thread: " +
                                    std::string(e.what()));
  }
}

void StreamingData::logSystemStatus() {
  Logger::getInstance().info(LogCategory::MONITORING, "=== SYSTEM STATUS ===");
  Logger::getInstance().info(
      LogCategory::MONITORING,
      "ThreadManager running: " +
          std::string(threadManager->isRunning() ? "YES" : "NO"));
  Logger::getInstance().info(
      LogCategory::MONITORING,
      "TransferOrchestrator running: " +
          std::string(transferOrchestrator->isRunning() ? "YES" : "NO"));
  Logger::getInstance().info(
      LogCategory::MONITORING,
      "Monitoring active: " +
          std::string(monitoringService->isMonitoring() ? "YES" : "NO"));
  Logger::getInstance().info(
      LogCategory::MONITORING,
      "Maintenance active: " +
          std::string(maintenanceService->isMaintaining() ? "YES" : "NO"));
  Logger::getInstance().info(LogCategory::MONITORING, "=== END STATUS ===");
}
