#include "TransferOrchestrator.h"
#include "MSSQLToPostgres.h"
#include "MariaDBToPostgres.h"
#include "PostgresToPostgres.h"
#include "catalog_manager.h"
#include <chrono>
#include <thread>

TransferOrchestrator::TransferOrchestrator()
    : mariaToPg(std::make_unique<MariaDBToPostgres>()),
      mssqlToPg(std::make_unique<MSSQLToPostgres>()),
      pgToPg(std::make_unique<PostgresToPostgres>()),
      catalogManager(std::make_unique<CatalogManager>()) {
  Logger::getInstance().info(LogCategory::MONITORING,
                             "TransferOrchestrator initialized");
}

TransferOrchestrator::~TransferOrchestrator() = default;

void TransferOrchestrator::initialize() {
  try {
    Logger::getInstance().info(LogCategory::MONITORING,
                               "Initializing transfer orchestrator");
    setupAllTables();
    Logger::getInstance().info(
        LogCategory::MONITORING,
        "Transfer orchestrator initialized successfully");
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::MONITORING,
                                "Error initializing transfer orchestrator: " +
                                    std::string(e.what()));
  }
}

void TransferOrchestrator::runTransfers() {
  Logger::getInstance().info(LogCategory::MONITORING,
                             "Starting transfer orchestration");

  while (running) {
    try {
      runMariaDBTransfer();
      sleepBetweenTransfers();

      runMSSQLTransfer();
      sleepBetweenTransfers();

      runPostgreSQLTransfer();
      sleepBetweenTransfers();

    } catch (const std::exception &e) {
      Logger::getInstance().error(LogCategory::MONITORING,
                                  "Error in transfer orchestration cycle: " +
                                      std::string(e.what()));
      sleepBetweenTransfers();
    }
  }

  Logger::getInstance().info(LogCategory::MONITORING,
                             "Transfer orchestration stopped");
}

void TransferOrchestrator::runMariaDBTransfer() {
  try {
    logTransferStart("MariaDB");
    auto startTime = std::chrono::high_resolution_clock::now();

    mariaToPg->transferDataMariaDBToPostgres();

    auto endTime = std::chrono::high_resolution_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::seconds>(endTime - startTime);

    logTransferComplete("MariaDB", static_cast<int>(duration.count()));
  } catch (const std::exception &e) {
    logTransferError("MariaDB", e.what());
  }
}

void TransferOrchestrator::runMSSQLTransfer() {
  try {
    logTransferStart("MSSQL");
    auto startTime = std::chrono::high_resolution_clock::now();

    mssqlToPg->transferDataMSSQLToPostgres();

    auto endTime = std::chrono::high_resolution_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::seconds>(endTime - startTime);

    logTransferComplete("MSSQL", static_cast<int>(duration.count()));
  } catch (const std::exception &e) {
    logTransferError("MSSQL", e.what());
  }
}

void TransferOrchestrator::runPostgreSQLTransfer() {
  try {
    logTransferStart("PostgreSQL");
    auto startTime = std::chrono::high_resolution_clock::now();

    pgToPg->transferDataPostgresToPostgres();

    auto endTime = std::chrono::high_resolution_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::seconds>(endTime - startTime);

    logTransferComplete("PostgreSQL", static_cast<int>(duration.count()));
  } catch (const std::exception &e) {
    logTransferError("PostgreSQL", e.what());
  }
}

void TransferOrchestrator::setupAllTables() {
  setupMariaDBTables();
  setupMSSQLTables();
  setupPostgreSQLTables();
}

void TransferOrchestrator::setupMariaDBTables() {
  try {
    Logger::getInstance().info(LogCategory::MONITORING,
                               "Setting up MariaDB target tables");
    mariaToPg->setupTableTargetMariaDBToPostgres();
    Logger::getInstance().info(LogCategory::MONITORING,
                               "MariaDB target tables setup completed");
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::MONITORING,
                                "Error setting up MariaDB tables: " +
                                    std::string(e.what()));
  }
}

void TransferOrchestrator::setupMSSQLTables() {
  try {
    Logger::getInstance().info(LogCategory::MONITORING,
                               "Setting up MSSQL target tables");
    mssqlToPg->setupTableTargetMSSQLToPostgres();
    Logger::getInstance().info(LogCategory::MONITORING,
                               "MSSQL target tables setup completed");
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::MONITORING,
                                "Error setting up MSSQL tables: " +
                                    std::string(e.what()));
  }
}

void TransferOrchestrator::setupPostgreSQLTables() {
  try {
    Logger::getInstance().info(LogCategory::MONITORING,
                               "Setting up PostgreSQL target tables");
    pgToPg->setupTableTargetPostgresToPostgres();
    Logger::getInstance().info(LogCategory::MONITORING,
                               "PostgreSQL target tables setup completed");
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::MONITORING,
                                "Error setting up PostgreSQL tables: " +
                                    std::string(e.what()));
  }
}

void TransferOrchestrator::stop() {
  running = false;
  Logger::getInstance().info(LogCategory::MONITORING,
                             "Transfer orchestrator stop requested");
}

bool TransferOrchestrator::isRunning() const { return running.load(); }

void TransferOrchestrator::logTransferStart(const std::string &engine) {
  Logger::getInstance().info(LogCategory::MONITORING,
                             "Starting " + engine + " transfer cycle");
}

void TransferOrchestrator::logTransferComplete(const std::string &engine,
                                               int durationSeconds) {
  Logger::getInstance().info(LogCategory::MONITORING,
                             engine +
                                 " transfer cycle completed successfully in " +
                                 std::to_string(durationSeconds) + " seconds");
}

void TransferOrchestrator::logTransferError(const std::string &engine,
                                            const std::string &error) {
  Logger::getInstance().error(LogCategory::MONITORING,
                              "Error in " + engine +
                                  " transfer cycle: " + error);
}

void TransferOrchestrator::sleepBetweenTransfers() {
  int sleepTime =
      std::max(5, static_cast<int>(SyncConfig::getSyncInterval() / 4));
  std::this_thread::sleep_for(std::chrono::seconds(sleepTime));
}
