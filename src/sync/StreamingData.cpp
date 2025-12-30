#include "sync/StreamingData.h"
#include "catalog/custom_jobs_repository.h"
#include "core/database_config.h"
#include "governance/QueryActivityLogger.h"
#include "governance/QueryStoreCollector.h"
#include "third_party/json.hpp"
#include <chrono>
#include <ctime>
#include <mutex>
#include <pqxx/pqxx>
#include <sstream>
#include <thread>

using json = nlohmann::json;

// Constructor initializes API sync component with PostgreSQL connection string
StreamingData::StreamingData()
    : apiToDb(DatabaseConfig::getPostgresConnectionString()),
      csvToDb(DatabaseConfig::getPostgresConnectionString()),
      sheetsToDb(DatabaseConfig::getPostgresConnectionString()) {}

// Destructor automatically shuts down the system, ensuring all threads are
// properly joined and resources are cleaned up.
StreamingData::~StreamingData() {
  if (!shutdownCalled.load()) {
    shutdown();
  }
}

// Initializes the DataSync system components. Currently performs minimal
// initialization, logging that database connections will be created as needed.
// This is a placeholder for future initialization logic. Should be called
// before run() to prepare the system.
void StreamingData::initialize() {
  Logger::info(LogCategory::MONITORING,
               "Initializing DataSync system components");

  Logger::info(LogCategory::MONITORING,
               "Database connections will be created as needed");

  try {
    std::string connStr = DatabaseConfig::getPostgresConnectionString();
    customJobExecutor = std::make_unique<CustomJobExecutor>(connStr);
    Logger::info(LogCategory::MONITORING,
                 "Custom job executor initialized successfully");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::MONITORING, "initialize",
                  "Error initializing custom job executor: " +
                      std::string(e.what()));
  }

  try {
    std::string connStr = DatabaseConfig::getPostgresConnectionString();
    warehouseBuilder = std::make_unique<DataWarehouseBuilder>(connStr);
    Logger::info(LogCategory::MONITORING,
                 "Data warehouse builder initialized successfully");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::MONITORING, "initialize",
                  "Error initializing data warehouse builder: " +
                      std::string(e.what()));
  }

  Logger::info(LogCategory::MONITORING,
               "System initialization completed successfully");
}

// Main entry point that starts the multi-threaded DataSync system. Launches
// all core threads (initialization, catalog sync, monitoring, quality,
// maintenance) and transfer threads (MariaDB, MSSQL). Waits for all threads to
// complete before returning. Handles exceptions when joining threads. This
// method blocks until all threads finish execution. Should be called after
// initialize(). The shutdownCheck function is called periodically to check if
// shutdown was requested externally (e.g., from a signal handler).
void StreamingData::run(std::function<bool()> shutdownCheck) {
  Logger::info(LogCategory::MONITORING,
               "Starting multi-threaded DataSync system");

  Logger::info(
      "Launching core threads (init, sync, monitor, quality, maintenance)");
  threads.emplace_back(&StreamingData::initializationThread, this);
  threads.emplace_back(&StreamingData::catalogSyncThread, this);
  threads.emplace_back(&StreamingData::monitoringThread, this);
  threads.emplace_back(&StreamingData::qualityThread, this);
  threads.emplace_back(&StreamingData::maintenanceThread, this);
  Logger::info(LogCategory::MONITORING, "Core threads launched successfully");

  Logger::info(LogCategory::MONITORING,
               "Launching transfer threads (MariaDB, MSSQL, MongoDB, Oracle, "
               "PostgreSQL, API, CSV, Google Sheets, Custom Jobs, Data "
               "Warehouse, Datalake "
               "Scheduler)");
  threads.emplace_back(&StreamingData::mariaTransferThread, this);
  threads.emplace_back(&StreamingData::mssqlTransferThread, this);
  threads.emplace_back(&StreamingData::mongoTransferThread, this);
  threads.emplace_back(&StreamingData::oracleTransferThread, this);
  threads.emplace_back(&StreamingData::postgresTransferThread, this);
  threads.emplace_back(&StreamingData::apiTransferThread, this);
  threads.emplace_back(&StreamingData::csvTransferThread, this);
  threads.emplace_back(&StreamingData::googleSheetsTransferThread, this);
  threads.emplace_back(&StreamingData::customJobsSchedulerThread, this);
  threads.emplace_back(&StreamingData::warehouseBuilderThread, this);
  threads.emplace_back(&StreamingData::datalakeSchedulerThread, this);

  Logger::info(LogCategory::MONITORING,
               "Transfer threads launched successfully");

  Logger::info(LogCategory::MONITORING,
               "All threads launched successfully - System running");

  Logger::info(LogCategory::MONITORING, "Waiting for all threads to complete");
  std::vector<std::exception_ptr> threadExceptions;
  std::mutex exceptionMutex;

  while (running.load()) {
    if (shutdownCheck && shutdownCheck()) {
      Logger::info(LogCategory::MONITORING, "run",
                   "Shutdown requested externally, initiating shutdown");
      shutdown();
      break;
    }

    bool allFinished = true;
    for (auto &thread : threads) {
      if (thread.joinable()) {
        allFinished = false;
        break;
      }
    }
    if (allFinished) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  // Now join all threads (they should finish quickly since running = false)
  for (auto &thread : threads) {
    if (thread.joinable()) {
      try {
        auto start = std::chrono::steady_clock::now();
        const auto threadTimeout = std::chrono::seconds(5);
        while (thread.joinable() &&
               (std::chrono::steady_clock::now() - start) < threadTimeout) {
          std::this_thread::sleep_for(std::chrono::milliseconds(100));
          if (!thread.joinable()) {
            break;
          }
        }
        if (thread.joinable()) {
          Logger::warning(LogCategory::MONITORING, "run",
                          "Thread did not finish in time, forcing join");
          try {
            thread.join();
          } catch (...) {
            Logger::error(LogCategory::MONITORING, "run",
                          "Error joining thread, detaching");
            thread.detach();
          }
        }
      } catch (const std::exception &e) {
        Logger::error(LogCategory::MONITORING, "run",
                      "Error joining thread in run(): " +
                          std::string(e.what()));
        std::lock_guard<std::mutex> lock(exceptionMutex);
        threadExceptions.push_back(std::current_exception());
      } catch (...) {
        Logger::error(LogCategory::MONITORING, "run",
                      "Unknown error joining thread in run()");
        std::lock_guard<std::mutex> lock(exceptionMutex);
        threadExceptions.push_back(std::current_exception());
      }
    }
  }

  if (!threadExceptions.empty()) {
    Logger::warning(LogCategory::MONITORING, "run",
                    std::to_string(threadExceptions.size()) +
                        " threads had exceptions during join");
  }

  Logger::info(LogCategory::MONITORING, "All threads completed");
}

// Shuts down the DataSync system gracefully. Sets the running flag to false
// to signal all threads to stop, then waits for all threads to finish by
// joining them. Clears the threads vector after all threads are joined.
// Handles exceptions when joining threads. Idempotent - can be called
// multiple times safely. Should be called before destroying the object.
void StreamingData::shutdown() {
  bool expected = false;
  if (!shutdownCalled.compare_exchange_strong(expected, true)) {
    return;
  }
  Logger::info(LogCategory::MONITORING, "Shutting down DataSync system");
  running = false;

  Logger::info(LogCategory::MONITORING,
               "Waiting for all threads to finish (max 30 seconds)");
  std::vector<std::exception_ptr> threadExceptions;
  std::mutex exceptionMutex;

  auto startTime = std::chrono::steady_clock::now();
  const auto maxWaitTime = std::chrono::seconds(30);

  for (auto &thread : threads) {
    if (thread.joinable()) {
      try {
        // Wait with timeout
        auto elapsed = std::chrono::steady_clock::now() - startTime;
        if (elapsed >= maxWaitTime) {
          Logger::warning(
              LogCategory::MONITORING, "shutdown",
              "Shutdown timeout reached, detaching remaining threads");
          thread.detach();
          continue;
        }

        auto threadStart = std::chrono::steady_clock::now();
        const auto threadTimeout = std::chrono::seconds(5);
        while (thread.joinable() && (std::chrono::steady_clock::now() -
                                     threadStart) < threadTimeout) {
          std::this_thread::sleep_for(std::chrono::milliseconds(100));
          if (!thread.joinable()) {
            break;
          }
        }

        if (thread.joinable()) {
          Logger::warning(LogCategory::MONITORING, "shutdown",
                          "Thread did not finish in time, forcing join");
          try {
            thread.join();
          } catch (...) {
            Logger::error(LogCategory::MONITORING, "shutdown",
                          "Error joining thread, detaching");
            thread.detach();
          }
        }
      } catch (const std::exception &e) {
        Logger::error(LogCategory::MONITORING, "shutdown",
                      "Error joining thread: " + std::string(e.what()));
        std::lock_guard<std::mutex> lock(exceptionMutex);
        threadExceptions.push_back(std::current_exception());
      } catch (...) {
        Logger::error(LogCategory::MONITORING, "shutdown",
                      "Unknown error joining thread");
        std::lock_guard<std::mutex> lock(exceptionMutex);
        threadExceptions.push_back(std::current_exception());
      }
    }
  }

  if (!threadExceptions.empty()) {
    Logger::warning(LogCategory::MONITORING, "shutdown",
                    std::to_string(threadExceptions.size()) +
                        " threads had exceptions during join");
  }

  threads.clear();
  Logger::info(LogCategory::MONITORING, "All threads finished successfully");

  Logger::info(LogCategory::MONITORING, "Shutdown completed successfully");
}

// Loads configuration parameters from metadata.config table in PostgreSQL.
// Queries for chunk_size, sync_interval, max_workers, and
// max_tables_per_cycle. Validates connection is open before and after
// transaction. Validates numeric values and ranges before updating
// SyncConfig. Only updates if the new value differs from current value.
// Handles SQL errors, connection errors, and general exceptions, logging them
// appropriately. Does not throw exceptions, allowing the system to continue
// with default values if configuration load fails.
void StreamingData::loadConfigFromDatabase(pqxx::connection &pgConn) {
  try {
    Logger::info(LogCategory::MONITORING,
                 "Starting configuration load from database");

    if (!pgConn.is_open()) {
      Logger::error(
          LogCategory::MONITORING,
          "Database connection is not open in loadConfigFromDatabase");
      return;
    }

    pqxx::work txn(pgConn);
    auto results =
        txn.exec("SELECT key, value FROM metadata.config WHERE key IN "
                 "('chunk_size', 'sync_interval', 'max_workers', "
                 "'max_tables_per_cycle');");

    Logger::info(LogCategory::MONITORING,
                 "Configuration query executed, found " +
                     std::to_string(results.size()) + " config entries");

    txn.commit();

    if (!pgConn.is_open()) {
      Logger::error(LogCategory::MONITORING,
                    "CRITICAL ERROR: Connection lost after transaction commit");
      return;
    }

    for (const auto &row : results) {
      if (row.size() < 2) {
        Logger::error(LogCategory::MONITORING, "loadConfigFromDatabase",
                      "Invalid configuration row - insufficient columns: " +
                          std::to_string(row.size()));
        continue;
      }

      if (row[0].is_null() || row[1].is_null()) {
        continue;
      }

      std::string key = row[0].as<std::string>();
      std::string value = row[1].as<std::string>();

      if (key.empty() || value.empty()) {
        continue;
      }

      Logger::info(LogCategory::MONITORING,
                   "Processing config key: " + key + " = " + value);

      if (key == "chunk_size") {
        try {
          if (value.empty() || value.length() > 20) {
            throw std::invalid_argument("Invalid chunk_size value length");
          }
          size_t newSize = std::stoul(value);
          constexpr size_t MAX_CHUNK_SIZE = 1024ULL * 1024 * 1024;
          if (newSize >= 1 && newSize <= MAX_CHUNK_SIZE &&
              newSize != SyncConfig::getChunkSize()) {
            Logger::info(LogCategory::MONITORING,
                         "Updating chunk_size from " +
                             std::to_string(SyncConfig::getChunkSize()) +
                             " to " + std::to_string(newSize));
            SyncConfig::setChunkSize(newSize);
          }
        } catch (const std::exception &e) {
          Logger::error(LogCategory::MONITORING, "loadConfigFromDatabase",
                        "Failed to parse chunk_size value '" + value +
                            "': " + std::string(e.what()));
        }
      } else if (key == "sync_interval") {
        try {
          if (value.empty() || value.length() > 10) {
            throw std::invalid_argument("Invalid sync_interval value length");
          }
          size_t newInterval = std::stoul(value);
          constexpr size_t MAX_SYNC_INTERVAL = 3600;
          if (newInterval >= 5 && newInterval <= MAX_SYNC_INTERVAL &&
              newInterval != SyncConfig::getSyncInterval()) {
            Logger::info(LogCategory::MONITORING,
                         "Updating sync_interval from " +
                             std::to_string(SyncConfig::getSyncInterval()) +
                             " to " + std::to_string(newInterval));
            SyncConfig::setSyncInterval(newInterval);
          }
        } catch (const std::exception &e) {
          Logger::error(LogCategory::MONITORING, "loadConfigFromDatabase",
                        "Failed to parse sync_interval value '" + value +
                            "': " + std::string(e.what()));
        }
      } else if (key == "max_workers") {
        try {
          if (value.empty() || value.length() > 5) {
            throw std::invalid_argument("Invalid max_workers value length");
          }
          size_t v = std::stoul(value);
          constexpr size_t MAX_WORKERS = 128;
          if (v >= 1 && v <= MAX_WORKERS && v != SyncConfig::getMaxWorkers()) {
            Logger::info(LogCategory::MONITORING,
                         "Updating max_workers from " +
                             std::to_string(SyncConfig::getMaxWorkers()) +
                             " to " + std::to_string(v));
            SyncConfig::setMaxWorkers(v);
          }
        } catch (const std::exception &e) {
          Logger::error(LogCategory::MONITORING, "loadConfigFromDatabase",
                        "Failed to parse max_workers value '" + value +
                            "': " + std::string(e.what()));
        }
      } else if (key == "max_tables_per_cycle") {
        try {
          if (value.empty() || value.length() > 10) {
            throw std::invalid_argument(
                "Invalid max_tables_per_cycle value length");
          }
          size_t v = std::stoul(value);
          constexpr size_t MAX_TABLES_PER_CYCLE = 1000000;
          if (v >= 1 && v <= MAX_TABLES_PER_CYCLE &&
              v != SyncConfig::getMaxTablesPerCycle()) {
            Logger::info(
                LogCategory::MONITORING,
                "Updating max_tables_per_cycle from " +
                    std::to_string(SyncConfig::getMaxTablesPerCycle()) +
                    " to " + std::to_string(v));
            SyncConfig::setMaxTablesPerCycle(v);
          }
        } catch (const std::exception &e) {
          Logger::error(LogCategory::MONITORING,
                        "Failed to parse max_tables_per_cycle value '" + value +
                            "': " + std::string(e.what()));
        }
      }
    }

    Logger::info(LogCategory::MONITORING,
                 "Configuration load completed successfully");
  } catch (const pqxx::sql_error &e) {
    Logger::error(LogCategory::MONITORING, "loadConfigFromDatabase",
                  "SQL ERROR in loadConfigFromDatabase: " +
                      std::string(e.what()) + " [SQL State: " + e.sqlstate() +
                      "] - Configuration may not be applied");
  } catch (const pqxx::broken_connection &e) {
    Logger::error(
        LogCategory::MONITORING, "loadConfigFromDatabase",
        "CONNECTION ERROR in loadConfigFromDatabase: " + std::string(e.what()) +
            " - Database connection lost during configuration load");
  } catch (const std::exception &e) {
    Logger::error(
        LogCategory::MONITORING, "loadConfigFromDatabase",
        "CRITICAL ERROR in loadConfigFromDatabase: " + std::string(e.what()) +
            " - Configuration may not be applied");
  }
}

void StreamingData::initializationThread() {
  try {
    Logger::info(LogCategory::MONITORING,
                 "Starting system initialization thread");

    initializeDataGovernance();
    initializeMetricsCollector();
    initializeQueryStoreCollector();
    initializeQueryActivityLogger();
    initializeDatabaseTables();

    Logger::info(LogCategory::MONITORING,
                 "System initialization thread completed successfully");
  } catch (const std::exception &e) {
    Logger::error(
        LogCategory::MONITORING, "initializationThread",
        "CRITICAL ERROR in initializationThread: " + std::string(e.what()) +
            " - System initialization failed completely");
  }
}

void StreamingData::initializeDataGovernance() {
  try {
    Logger::info(LogCategory::MONITORING,
                 "Initializing DataGovernance component");
    DataGovernance dg;
    dg.initialize();
    Logger::info(LogCategory::MONITORING,
                 "DataGovernance initialized successfully");

    dg.runDiscovery();
    Logger::info(LogCategory::MONITORING, "DataGovernance discovery completed");

    dg.generateReport();
    Logger::info(LogCategory::MONITORING, "DataGovernance report generated");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::MONITORING, "initializeDataGovernance",
                  "CRITICAL ERROR in DataGovernance initialization: " +
                      std::string(e.what()) +
                      " - System may not function properly");
  }
}

void StreamingData::initializeMetricsCollector() {
  try {
    Logger::info(LogCategory::MONITORING,
                 "Initializing MetricsCollector component");
    MetricsCollector metricsCollector;
    metricsCollector.collectAllMetrics();
    Logger::info(LogCategory::MONITORING,
                 "MetricsCollector completed successfully");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::MONITORING, "initializeMetricsCollector",
                  "CRITICAL ERROR in MetricsCollector: " +
                      std::string(e.what()) + " - Metrics collection failed");
  }
}

void StreamingData::initializeQueryStoreCollector() {
  try {
    Logger::info(LogCategory::MONITORING,
                 "Initializing QueryStoreCollector component");
    std::string pgConnStr = DatabaseConfig::getPostgresConnectionString();
    QueryStoreCollector queryStoreCollector(pgConnStr);
    queryStoreCollector.collectQuerySnapshots();
    queryStoreCollector.storeSnapshots();
    queryStoreCollector.analyzeQueries();
    Logger::info(LogCategory::MONITORING,
                 "QueryStoreCollector completed successfully");
  } catch (const std::exception &e) {
    Logger::error(
        LogCategory::MONITORING, "initializeQueryStoreCollector",
        "CRITICAL ERROR in QueryStoreCollector: " + std::string(e.what()) +
            " - Query snapshot collection failed");
  }
}

void StreamingData::initializeQueryActivityLogger() {
  try {
    Logger::info(LogCategory::MONITORING,
                 "Initializing QueryActivityLogger component");
    std::string pgConnStr = DatabaseConfig::getPostgresConnectionString();
    QueryActivityLogger queryActivityLogger(pgConnStr);
    queryActivityLogger.logActiveQueries();
    queryActivityLogger.storeActivityLog();
    queryActivityLogger.analyzeActivity();
    Logger::info(LogCategory::MONITORING,
                 "QueryActivityLogger completed successfully");
  } catch (const std::exception &e) {
    Logger::error(
        LogCategory::MONITORING, "initializeQueryActivityLogger",
        "CRITICAL ERROR in QueryActivityLogger: " + std::string(e.what()) +
            " - Query activity logging failed");
  }
}

void StreamingData::initializeDatabaseTables() {
  try {
    Logger::info(LogCategory::MONITORING, "Setting up MariaDB target tables");
    mariaToPg.setupTableTargetMariaDBToPostgres();
    Logger::info(LogCategory::MONITORING,
                 "MariaDB target tables setup completed");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::MONITORING, "initializeDatabaseTables",
                  "CRITICAL ERROR in MariaDB table setup: " +
                      std::string(e.what()) + " - MariaDB sync may fail");
  }

  try {
    Logger::info(LogCategory::MONITORING, "Setting up MSSQL target tables");
    mssqlToPg.setupTableTargetMSSQLToPostgres();
    Logger::info(LogCategory::MONITORING,
                 "MSSQL target tables setup completed");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::MONITORING, "initializeDatabaseTables",
                  "CRITICAL ERROR in MSSQL table setup: " +
                      std::string(e.what()) + " - MSSQL sync may fail");
  }

  try {
    Logger::info(LogCategory::MONITORING, "Setting up Oracle target tables");
    oracleToPg.setupTableTargetOracleToPostgres();
    Logger::info(LogCategory::MONITORING,
                 "Oracle target tables setup completed");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::MONITORING, "initializeDatabaseTables",
                  "CRITICAL ERROR in Oracle table setup: " +
                      std::string(e.what()) + " - Oracle sync may fail");
  }
}

void StreamingData::catalogSyncThread() {
  Logger::info(LogCategory::MONITORING, "Catalog sync thread started");
  while (running) {
    try {
      Logger::info(LogCategory::MONITORING,
                   "Starting catalog synchronization cycle");

      std::vector<std::exception_ptr> exceptions;
      std::mutex exceptionMutex;

      performCatalogSyncs(exceptions, exceptionMutex);
      processCatalogSyncExceptions(exceptions);
      performCatalogMaintenance();

      Logger::info(LogCategory::MONITORING,
                   "Catalog synchronization cycle completed");
    } catch (const std::exception &e) {
      Logger::error(LogCategory::MONITORING, "catalogSyncThread",
                    "CRITICAL ERROR in catalog synchronization cycle: " +
                        std::string(e.what()) +
                        " - Catalog sync completely failed");
    }

    std::this_thread::sleep_for(
        std::chrono::seconds(SyncConfig::getSyncInterval()));
  }
  Logger::info(LogCategory::MONITORING, "Catalog sync thread stopped");
}

void StreamingData::performCatalogSyncs(
    std::vector<std::exception_ptr> &exceptions, std::mutex &exceptionMutex) {
  std::vector<std::thread> syncThreads;

  syncThreads.emplace_back([this, &exceptions, &exceptionMutex]() {
    try {
      Logger::info(LogCategory::MONITORING, "Starting MariaDB catalog sync");
      catalogManager.syncCatalogMariaDBToPostgres();
      Logger::info(LogCategory::MONITORING,
                   "MariaDB catalog sync completed successfully");
    } catch (const std::exception &e) {
      Logger::error(LogCategory::MONITORING, "performCatalogSyncs",
                    "ERROR in MariaDB catalog sync: " + std::string(e.what()) +
                        " - MariaDB catalog may be out of sync");
      std::lock_guard<std::mutex> lock(exceptionMutex);
      exceptions.push_back(std::current_exception());
    }
  });

  syncThreads.emplace_back([this, &exceptions, &exceptionMutex]() {
    try {
      Logger::info(LogCategory::MONITORING, "Starting MSSQL catalog sync");
      catalogManager.syncCatalogMSSQLToPostgres();
      Logger::info(LogCategory::MONITORING,
                   "MSSQL catalog sync completed successfully");
    } catch (const std::exception &e) {
      Logger::error(LogCategory::MONITORING, "performCatalogSyncs",
                    "ERROR in MSSQL catalog sync: " + std::string(e.what()) +
                        " - MSSQL catalog may be out of sync");
      std::lock_guard<std::mutex> lock(exceptionMutex);
      exceptions.push_back(std::current_exception());
    }
  });

  syncThreads.emplace_back([this, &exceptions, &exceptionMutex]() {
    try {
      Logger::info(LogCategory::MONITORING, "Starting Oracle catalog sync");
      catalogManager.syncCatalogOracleToPostgres();
      Logger::info(LogCategory::MONITORING,
                   "Oracle catalog sync completed successfully");
    } catch (const std::exception &e) {
      Logger::error(LogCategory::MONITORING, "performCatalogSyncs",
                    "ERROR in Oracle catalog sync: " + std::string(e.what()) +
                        " - Oracle catalog may be out of sync");
      std::lock_guard<std::mutex> lock(exceptionMutex);
      exceptions.push_back(std::current_exception());
    }
  });

  syncThreads.emplace_back([this, &exceptions, &exceptionMutex]() {
    try {
      Logger::info(LogCategory::MONITORING, "Starting MongoDB catalog sync");
      catalogManager.syncCatalogMongoDBToPostgres();
      Logger::info(LogCategory::MONITORING,
                   "MongoDB catalog sync completed successfully");
    } catch (const std::exception &e) {
      Logger::error(LogCategory::MONITORING, "performCatalogSyncs",
                    "ERROR in MongoDB catalog sync: " + std::string(e.what()) +
                        " - MongoDB catalog may be out of sync");
      std::lock_guard<std::mutex> lock(exceptionMutex);
      exceptions.push_back(std::current_exception());
    }
  });

  for (auto &thread : syncThreads) {
    thread.join();
  }
}

void StreamingData::processCatalogSyncExceptions(
    const std::vector<std::exception_ptr> &exceptions) {
  if (!exceptions.empty()) {
    Logger::error(LogCategory::MONITORING, "processCatalogSyncExceptions",
                  "CRITICAL: " + std::to_string(exceptions.size()) +
                      " catalog sync operations failed - system may be in "
                      "inconsistent state");
    for (const auto &exPtr : exceptions) {
      if (exPtr) {
        try {
          std::rethrow_exception(exPtr);
        } catch (const std::exception &e) {
          Logger::error(LogCategory::MONITORING, "processCatalogSyncExceptions",
                        "Exception details: " + std::string(e.what()));
        } catch (...) {
          Logger::error(LogCategory::MONITORING, "processCatalogSyncExceptions",
                        "Unknown exception in catalog sync");
        }
      }
    }
  }
}

void StreamingData::performCatalogMaintenance() {
  try {
    Logger::info(LogCategory::MONITORING, "Starting catalog cleanup");
    catalogManager.cleanCatalog();
    Logger::info(LogCategory::MONITORING,
                 "Catalog cleanup completed successfully");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::MONITORING, "performCatalogMaintenance",
                  "ERROR in catalog cleanup: " + std::string(e.what()) +
                      " - Catalog may contain stale data");
  }

  try {
    Logger::info(LogCategory::MONITORING,
                 "Starting no-data table deactivation");
    catalogManager.deactivateNoDataTables();
    Logger::info(LogCategory::MONITORING,
                 "No-data table deactivation completed successfully");
  } catch (const std::exception &e) {
    Logger::error(
        LogCategory::MONITORING, "performCatalogMaintenance",
        "ERROR in no-data table deactivation: " + std::string(e.what()) +
            " - Inactive tables may not be properly marked");
  }
}

// MariaDB transfer thread that runs continuously while the system is running.
// Performs periodic data transfer from MariaDB to PostgreSQL using parallel
// processing. Measures and logs transfer duration. Sleeps for
// max(5, sync_interval/4) seconds between cycles to avoid overwhelming the
// system. Handles exceptions by logging errors and continuing to the next
// cycle. This thread is responsible for keeping MariaDB data synchronized
// with PostgreSQL.
void StreamingData::mariaTransferThread() {
  Logger::info(LogCategory::MONITORING, "MariaDB transfer thread started");
  while (running) {
    try {
      Logger::info(LogCategory::MONITORING,
                   "Starting MariaDB transfer cycle - sync interval: " +
                       std::to_string(SyncConfig::getSyncInterval()) +
                       " seconds");

      auto startTime = std::chrono::high_resolution_clock::now();
      mariaToPg.transferDataMariaDBToPostgresParallel();
      auto endTime = std::chrono::high_resolution_clock::now();

      auto duration =
          std::chrono::duration_cast<std::chrono::seconds>(endTime - startTime);
      Logger::info(LogCategory::MONITORING,
                   "MariaDB transfer cycle completed successfully in " +
                       std::to_string(duration.count()) + " seconds");
    } catch (const std::exception &e) {
      Logger::error(
          LogCategory::MONITORING, "mariaTransferThread",
          "CRITICAL ERROR in MariaDB transfer cycle: " + std::string(e.what()) +
              " - MariaDB data sync failed, retrying in " +
              std::to_string(SyncConfig::getSyncInterval()) + " seconds");
    }

    size_t interval = SyncConfig::getSyncInterval();
    size_t sleepSeconds = (interval > 0 && interval >= 4) ? (interval / 4) : 5;
    if (sleepSeconds < 5)
      sleepSeconds = 5;
    std::this_thread::sleep_for(std::chrono::seconds(sleepSeconds));
  }
  Logger::info(LogCategory::MONITORING, "MariaDB transfer thread stopped");
}

// MSSQL transfer thread that runs continuously while the system is running.
// Performs periodic data transfer from MSSQL to PostgreSQL using parallel
// processing. Measures and logs transfer duration. Sleeps for
// max(5, sync_interval/4) seconds between cycles to avoid overwhelming the
// system. Handles exceptions by logging errors and continuing to the next
// cycle. This thread is responsible for keeping MSSQL data synchronized with
// PostgreSQL.
void StreamingData::mssqlTransferThread() {
  Logger::info(LogCategory::MONITORING, "MSSQL transfer thread started");
  while (running) {
    try {
      Logger::info(LogCategory::MONITORING,
                   "Starting MSSQL transfer cycle - sync interval: " +
                       std::to_string(SyncConfig::getSyncInterval()) +
                       " seconds");

      auto startTime = std::chrono::high_resolution_clock::now();
      mssqlToPg.transferDataMSSQLToPostgresParallel();
      auto endTime = std::chrono::high_resolution_clock::now();

      auto duration =
          std::chrono::duration_cast<std::chrono::seconds>(endTime - startTime);
      Logger::info(LogCategory::MONITORING,
                   "MSSQL transfer cycle completed successfully in " +
                       std::to_string(duration.count()) + " seconds");
    } catch (const std::exception &e) {
      Logger::error(
          LogCategory::MONITORING, "mssqlTransferThread",
          "CRITICAL ERROR in MSSQL transfer cycle: " + std::string(e.what()) +
              " - MSSQL data sync failed, retrying in " +
              std::to_string(SyncConfig::getSyncInterval()) + " seconds");
    }

    size_t interval = SyncConfig::getSyncInterval();
    size_t sleepSeconds = (interval > 0 && interval >= 4) ? (interval / 4) : 5;
    if (sleepSeconds < 5)
      sleepSeconds = 5;
    std::this_thread::sleep_for(std::chrono::seconds(sleepSeconds));
  }
  Logger::info(LogCategory::MONITORING, "MSSQL transfer thread stopped");
}

// MongoDB transfer thread that runs continuously while the system is running.
// Performs periodic data transfer from MongoDB to PostgreSQL using TRUNCATE +
// FULL_LOAD strategy. Verifies if collections need sync (24+ hours since last
// sync). Measures and logs transfer duration. Sleeps for 1 hour between cycles
// to check for collections that need sync. Handles exceptions by logging errors
// and continuing to the next cycle. This thread is responsible for keeping
// MongoDB data synchronized with PostgreSQL.
void StreamingData::mongoTransferThread() {
  Logger::info(LogCategory::MONITORING, "MongoDB transfer thread started");
  while (running) {
    try {
      Logger::info(LogCategory::MONITORING, "Starting MongoDB transfer cycle");

      auto startTime = std::chrono::high_resolution_clock::now();
      mongoToPg.transferDataMongoDBToPostgresParallel();
      auto endTime = std::chrono::high_resolution_clock::now();

      auto duration =
          std::chrono::duration_cast<std::chrono::seconds>(endTime - startTime);
      Logger::info(LogCategory::MONITORING,
                   "MongoDB transfer cycle completed successfully in " +
                       std::to_string(duration.count()) + " seconds");
    } catch (const std::exception &e) {
      Logger::error(
          LogCategory::MONITORING, "mongoTransferThread",
          "CRITICAL ERROR in MongoDB transfer cycle: " + std::string(e.what()) +
              " - MongoDB data sync failed, retrying in 1 hour");
    }

    std::this_thread::sleep_for(std::chrono::seconds(3600));
  }
  Logger::info(LogCategory::MONITORING, "MongoDB transfer thread stopped");
}

// Oracle transfer thread that runs continuously while the system is running.
// Performs periodic data transfer from Oracle to PostgreSQL using parallel
// processing. Measures and logs transfer duration. Sleeps for
// max(5, sync_interval/4) seconds between cycles to avoid overwhelming the
// system. Handles exceptions by logging errors and continuing to the next
// cycle. This thread is responsible for keeping Oracle data synchronized with
// PostgreSQL.
void StreamingData::oracleTransferThread() {
  Logger::info(LogCategory::MONITORING, "Oracle transfer thread started");
  while (running) {
    try {
      Logger::info(LogCategory::MONITORING,
                   "Starting Oracle transfer cycle - sync interval: " +
                       std::to_string(SyncConfig::getSyncInterval()) +
                       " seconds");

      auto startTime = std::chrono::high_resolution_clock::now();
      oracleToPg.transferDataOracleToPostgresParallel();
      auto endTime = std::chrono::high_resolution_clock::now();

      auto duration =
          std::chrono::duration_cast<std::chrono::seconds>(endTime - startTime);
      Logger::info(LogCategory::MONITORING,
                   "Oracle transfer cycle completed in " +
                       std::to_string(duration.count()) + " seconds");

    } catch (const std::exception &e) {
      Logger::error(LogCategory::MONITORING, "oracleTransferThread",
                    "Error in Oracle transfer cycle: " + std::string(e.what()));
    }

    std::this_thread::sleep_for(
        std::chrono::seconds(SyncConfig::getSyncInterval()));
  }
  Logger::info(LogCategory::MONITORING, "Oracle transfer thread stopped");
}

void StreamingData::postgresTransferThread() {
  Logger::info(LogCategory::MONITORING, "PostgreSQL transfer thread started");
  while (running) {
    try {
      Logger::info(LogCategory::MONITORING,
                   "Starting PostgreSQL transfer cycle - sync interval: " +
                       std::to_string(SyncConfig::getSyncInterval()) +
                       " seconds");

      auto startTime = std::chrono::high_resolution_clock::now();
      postgresToPg.transferDataPostgreSQLToPostgresParallel();
      auto endTime = std::chrono::high_resolution_clock::now();

      auto duration =
          std::chrono::duration_cast<std::chrono::seconds>(endTime - startTime);
      Logger::info(LogCategory::MONITORING,
                   "PostgreSQL transfer cycle completed successfully in " +
                       std::to_string(duration.count()) + " seconds");
    } catch (const std::exception &e) {
      Logger::error(LogCategory::MONITORING, "postgresTransferThread",
                    "Error in PostgreSQL transfer cycle: " +
                        std::string(e.what()));
    }

    std::this_thread::sleep_for(
        std::chrono::seconds(SyncConfig::getSyncInterval()));
  }
  Logger::info(LogCategory::MONITORING, "PostgreSQL transfer thread stopped");
}

// API transfer thread that runs continuously while the system is running.
// Performs periodic data synchronization from APIs to target databases.
// Measures and logs transfer duration. Sleeps for max(5, sync_interval/4)
// seconds between cycles to avoid overwhelming the system. Handles exceptions
// by logging errors and continuing to the next cycle. This thread is
// responsible for keeping API data synchronized with target databases.
void StreamingData::apiTransferThread() {
  Logger::info(LogCategory::MONITORING, "API transfer thread started");
  while (running) {
    try {
      Logger::info(LogCategory::MONITORING,
                   "Starting API transfer cycle - sync interval: " +
                       std::to_string(SyncConfig::getSyncInterval()) +
                       " seconds");

      auto startTime = std::chrono::high_resolution_clock::now();
      apiToDb.syncAllAPIs();
      auto endTime = std::chrono::high_resolution_clock::now();

      auto duration =
          std::chrono::duration_cast<std::chrono::seconds>(endTime - startTime);
      Logger::info(LogCategory::MONITORING,
                   "API transfer cycle completed successfully in " +
                       std::to_string(duration.count()) + " seconds");
    } catch (const std::exception &e) {
      Logger::error(
          LogCategory::MONITORING, "apiTransferThread",
          "CRITICAL ERROR in API transfer cycle: " + std::string(e.what()) +
              " - API data sync failed, retrying in " +
              std::to_string(SyncConfig::getSyncInterval()) + " seconds");
    }

    size_t interval = SyncConfig::getSyncInterval();
    size_t sleepSeconds = (interval > 0 && interval >= 4) ? (interval / 4) : 5;
    if (sleepSeconds < 5)
      sleepSeconds = 5;
    std::this_thread::sleep_for(std::chrono::seconds(sleepSeconds));
  }
  Logger::info(LogCategory::MONITORING, "API transfer thread stopped");
}

void StreamingData::csvTransferThread() {
  Logger::info(LogCategory::MONITORING, "CSV transfer thread started");
  while (running) {
    try {
      Logger::info(LogCategory::MONITORING,
                   "Starting CSV transfer cycle - sync interval: " +
                       std::to_string(SyncConfig::getSyncInterval()) +
                       " seconds");

      auto startTime = std::chrono::high_resolution_clock::now();
      csvToDb.syncAllCSVs();
      auto endTime = std::chrono::high_resolution_clock::now();

      auto duration =
          std::chrono::duration_cast<std::chrono::seconds>(endTime - startTime);
      Logger::info(LogCategory::MONITORING,
                   "CSV transfer cycle completed successfully in " +
                       std::to_string(duration.count()) + " seconds");
    } catch (const std::exception &e) {
      Logger::error(
          LogCategory::MONITORING, "csvTransferThread",
          "CRITICAL ERROR in CSV transfer cycle: " + std::string(e.what()) +
              " - CSV data sync failed, retrying in " +
              std::to_string(SyncConfig::getSyncInterval()) + " seconds");
    }

    size_t interval = SyncConfig::getSyncInterval();
    size_t sleepSeconds = (interval > 0 && interval >= 4) ? (interval / 4) : 5;
    if (sleepSeconds < 5)
      sleepSeconds = 5;
    std::this_thread::sleep_for(std::chrono::seconds(sleepSeconds));
  }
  Logger::info(LogCategory::MONITORING, "CSV transfer thread stopped");
}

void StreamingData::googleSheetsTransferThread() {
  Logger::info(LogCategory::MONITORING,
               "Google Sheets transfer thread started");
  while (running) {
    try {
      Logger::info(LogCategory::MONITORING,
                   "Starting Google Sheets transfer cycle - sync interval: " +
                       std::to_string(SyncConfig::getSyncInterval()) +
                       " seconds");

      auto startTime = std::chrono::high_resolution_clock::now();
      sheetsToDb.syncAllGoogleSheets();
      auto endTime = std::chrono::high_resolution_clock::now();

      auto duration =
          std::chrono::duration_cast<std::chrono::seconds>(endTime - startTime);
      Logger::info(LogCategory::MONITORING,
                   "Google Sheets transfer cycle completed successfully in " +
                       std::to_string(duration.count()) + " seconds");
    } catch (const std::exception &e) {
      Logger::error(LogCategory::MONITORING, "googleSheetsTransferThread",
                    "CRITICAL ERROR in Google Sheets transfer cycle: " +
                        std::string(e.what()) +
                        " - Google Sheets data sync failed, retrying in " +
                        std::to_string(SyncConfig::getSyncInterval()) +
                        " seconds");
    }

    size_t interval = SyncConfig::getSyncInterval();
    size_t sleepSeconds = (interval > 0 && interval >= 4) ? (interval / 4) : 5;
    if (sleepSeconds < 5)
      sleepSeconds = 5;
    std::this_thread::sleep_for(std::chrono::seconds(sleepSeconds));
  }
  Logger::info(LogCategory::MONITORING,
               "Google Sheets transfer thread stopped");
}

void StreamingData::customJobsSchedulerThread() {
  Logger::info(LogCategory::MONITORING, "Custom jobs scheduler thread started");
  while (running) {
    try {
      if (!customJobExecutor) {
        std::this_thread::sleep_for(std::chrono::minutes(1));
        continue;
      }

      CustomJobsRepository repo(DatabaseConfig::getPostgresConnectionString());
      std::vector<CustomJob> scheduledJobs = repo.getScheduledJobs();
      std::vector<CustomJob> activeJobs = repo.getActiveJobs();

      auto now = std::chrono::system_clock::now();
      auto timeT = std::chrono::system_clock::to_time_t(now);
      std::tm *tm = std::gmtime(&timeT);
      int currentMinute = tm->tm_min;
      int currentHour = tm->tm_hour;
      int currentDay = tm->tm_mday;
      int currentMonth = tm->tm_mon + 1;
      int currentDow = tm->tm_wday;

      for (const auto &job : activeJobs) {
        if (job.metadata.contains("execute_now") &&
            job.metadata["execute_now"].get<bool>()) {
          if (job.schedule_cron.empty()) {
            Logger::info(LogCategory::MONITORING, "customJobsSchedulerThread",
                         "Executing manual job: " + job.job_name);
          } else {
            Logger::info(LogCategory::MONITORING, "customJobsSchedulerThread",
                         "Executing scheduled job manually triggered: " +
                             job.job_name);
          }
          try {
            customJobExecutor->executeJob(job.job_name);
            pqxx::connection conn(
                DatabaseConfig::getPostgresConnectionString());
            pqxx::work txn(conn);
            json updatedMetadata = job.metadata;
            updatedMetadata["execute_now"] = false;
            updatedMetadata.erase("execute_timestamp");
            txn.exec_params("UPDATE metadata.custom_jobs SET metadata = "
                            "$1::jsonb WHERE job_name = $2",
                            updatedMetadata.dump(), job.job_name);
            txn.commit();
          } catch (const std::exception &e) {
            Logger::error(LogCategory::MONITORING, "customJobsSchedulerThread",
                          "Error executing job " + job.job_name + ": " +
                              std::string(e.what()));
          }
        }
      }

      for (const auto &job : scheduledJobs) {
        if (job.schedule_cron.empty()) {
          continue;
        }

        if (job.metadata.contains("execute_now") &&
            job.metadata["execute_now"].get<bool>()) {
          continue;
        }

        std::istringstream cronStream(job.schedule_cron);
        std::string minute, hour, day, month, dow;
        cronStream >> minute >> hour >> day >> month >> dow;

        auto matchesCronField = [](const std::string &field,
                                   int currentValue) -> bool {
          if (field == "*") {
            return true;
          }

          size_t dashPos = field.find('-');
          size_t commaPos = field.find(',');
          size_t slashPos = field.find('/');

          if (dashPos != std::string::npos) {
            try {
              int start = std::stoi(field.substr(0, dashPos));
              int end = std::stoi(field.substr(dashPos + 1));
              return currentValue >= start && currentValue <= end;
            } catch (...) {
              return false;
            }
          }

          if (commaPos != std::string::npos) {
            std::istringstream listStream(field);
            std::string item;
            while (std::getline(listStream, item, ',')) {
              try {
                if (std::stoi(item) == currentValue) {
                  return true;
                }
              } catch (...) {
                continue;
              }
            }
            return false;
          }

          if (slashPos != std::string::npos) {
            try {
              std::string base = field.substr(0, slashPos);
              int step = std::stoi(field.substr(slashPos + 1));
              if (base == "*") {
                return (currentValue % step) == 0;
              } else {
                int start = std::stoi(base);
                return ((currentValue - start) % step) == 0 &&
                       currentValue >= start;
              }
            } catch (...) {
              return false;
            }
          }

          try {
            return std::stoi(field) == currentValue;
          } catch (...) {
            return false;
          }
        };

        bool shouldRun = matchesCronField(minute, currentMinute) &&
                         matchesCronField(hour, currentHour) &&
                         matchesCronField(day, currentDay) &&
                         matchesCronField(month, currentMonth) &&
                         matchesCronField(dow, currentDow);

        if (shouldRun) {
          Logger::info(LogCategory::MONITORING, "customJobsSchedulerThread",
                       "Executing scheduled job: " + job.job_name);
          try {
            customJobExecutor->executeJob(job.job_name);
          } catch (const std::exception &e) {
            Logger::error(LogCategory::MONITORING, "customJobsSchedulerThread",
                          "Error executing scheduled job " + job.job_name +
                              ": " + std::string(e.what()));
          }
        }
      }
    } catch (const std::exception &e) {
      Logger::error(LogCategory::MONITORING, "customJobsSchedulerThread",
                    "Error in custom jobs scheduler: " + std::string(e.what()));
    }

    std::this_thread::sleep_for(std::chrono::minutes(1));
  }
  Logger::info(LogCategory::MONITORING, "Custom jobs scheduler thread stopped");
}

void StreamingData::warehouseBuilderThread() {
  Logger::info(LogCategory::MONITORING, "Warehouse builder thread started");
  while (running) {
    try {
      if (!warehouseBuilder) {
        std::this_thread::sleep_for(std::chrono::seconds(60));
        continue;
      }

      std::string connStr = DatabaseConfig::getPostgresConnectionString();
      pqxx::connection conn(connStr);
      pqxx::work txn(conn);

      auto warehousesResult = txn.exec(
          "SELECT warehouse_name, schedule_cron, metadata, active, enabled "
          "FROM metadata.data_warehouse_catalog "
          "WHERE active = true AND enabled = true");

      std::vector<DataWarehouseModel> manualBuilds;
      std::vector<DataWarehouseModel> scheduledBuilds;

      auto now = std::chrono::system_clock::now();
      auto timeT = std::chrono::system_clock::to_time_t(now);
      struct tm *tm = std::gmtime(&timeT);
      int currentMinute = tm->tm_min;
      int currentHour = tm->tm_hour;
      int currentDay = tm->tm_mday;
      int currentMonth = tm->tm_mon + 1;
      int currentDow = tm->tm_wday;

      for (const auto &row : warehousesResult) {
        std::string warehouseName = row["warehouse_name"].as<std::string>();
        std::string scheduleCron = row["schedule_cron"].is_null()
                                       ? ""
                                       : row["schedule_cron"].as<std::string>();
        std::string metadataStr = row["metadata"].is_null()
                                      ? "{}"
                                      : row["metadata"].as<std::string>();

        json metadata = json::parse(metadataStr);

        if (metadata.contains("build_now") &&
            metadata["build_now"].get<bool>()) {
          try {
            DataWarehouseModel warehouse =
                warehouseBuilder->getWarehouse(warehouseName);
            manualBuilds.push_back(warehouse);
          } catch (const std::exception &e) {
            Logger::error(LogCategory::MONITORING, "warehouseBuilderThread",
                          "Error getting warehouse " + warehouseName + ": " +
                              std::string(e.what()));
          }
        } else if (!scheduleCron.empty()) {
          try {
            DataWarehouseModel warehouse =
                warehouseBuilder->getWarehouse(warehouseName);
            scheduledBuilds.push_back(warehouse);
          } catch (const std::exception &e) {
            Logger::error(LogCategory::MONITORING, "warehouseBuilderThread",
                          "Error getting warehouse " + warehouseName + ": " +
                              std::string(e.what()));
          }
        }
      }

      txn.commit();

      for (const auto &warehouse : manualBuilds) {
        Logger::info(LogCategory::MONITORING, "warehouseBuilderThread",
                     "Executing manual build: " + warehouse.warehouse_name);
        try {
          warehouseBuilder->buildWarehouse(warehouse.warehouse_name);
          pqxx::connection conn2(connStr);
          pqxx::work txn2(conn2);
          json updatedMetadata = warehouse.metadata;
          updatedMetadata["build_now"] = false;
          updatedMetadata.erase("build_timestamp");
          txn2.exec_params(
              "UPDATE metadata.data_warehouse_catalog SET metadata = "
              "$1::jsonb WHERE warehouse_name = $2",
              updatedMetadata.dump(), warehouse.warehouse_name);
          txn2.commit();
        } catch (const std::exception &e) {
          Logger::error(LogCategory::MONITORING, "warehouseBuilderThread",
                        "Error building warehouse " + warehouse.warehouse_name +
                            ": " + std::string(e.what()));
        }
      }

      auto matchesCronField = [](const std::string &field,
                                 int currentValue) -> bool {
        if (field == "*") {
          return true;
        }

        size_t dashPos = field.find('-');
        size_t commaPos = field.find(',');
        size_t slashPos = field.find('/');

        if (dashPos != std::string::npos) {
          try {
            int start = std::stoi(field.substr(0, dashPos));
            int end = std::stoi(field.substr(dashPos + 1));
            return currentValue >= start && currentValue <= end;
          } catch (...) {
            return false;
          }
        }

        if (commaPos != std::string::npos) {
          std::istringstream listStream(field);
          std::string item;
          while (std::getline(listStream, item, ',')) {
            try {
              if (std::stoi(item) == currentValue) {
                return true;
              }
            } catch (...) {
              continue;
            }
          }
          return false;
        }

        if (slashPos != std::string::npos) {
          try {
            std::string base = field.substr(0, slashPos);
            int step = std::stoi(field.substr(slashPos + 1));
            if (base == "*") {
              return currentValue % step == 0;
            }
            int baseValue = std::stoi(base);
            return (currentValue - baseValue) % step == 0;
          } catch (...) {
            return false;
          }
        }

        try {
          return std::stoi(field) == currentValue;
        } catch (...) {
          return false;
        }
      };

      for (const auto &warehouse : scheduledBuilds) {
        if (warehouse.schedule_cron.empty()) {
          continue;
        }

        if (warehouse.metadata.contains("build_now") &&
            warehouse.metadata["build_now"].get<bool>()) {
          continue;
        }

        std::istringstream cronStream(warehouse.schedule_cron);
        std::string minute, hour, day, month, dow;
        cronStream >> minute >> hour >> day >> month >> dow;

        bool shouldRun = matchesCronField(minute, currentMinute) &&
                         matchesCronField(hour, currentHour) &&
                         matchesCronField(day, currentDay) &&
                         matchesCronField(month, currentMonth) &&
                         matchesCronField(dow, currentDow);

        if (shouldRun) {
          Logger::info(LogCategory::MONITORING, "warehouseBuilderThread",
                       "Executing scheduled build: " +
                           warehouse.warehouse_name);
          try {
            warehouseBuilder->buildWarehouse(warehouse.warehouse_name);
          } catch (const std::exception &e) {
            Logger::error(LogCategory::MONITORING, "warehouseBuilderThread",
                          "Error building warehouse " +
                              warehouse.warehouse_name + ": " +
                              std::string(e.what()));
          }
        }
      }

    } catch (const std::exception &e) {
      Logger::error(LogCategory::MONITORING, "warehouseBuilderThread",
                    "Error in warehouse build cycle: " + std::string(e.what()));
    }

    std::this_thread::sleep_for(std::chrono::seconds(60));
  }
  Logger::info(LogCategory::MONITORING, "Warehouse builder thread stopped");
}

void StreamingData::executeJob(const std::string &jobName) {
  if (!customJobExecutor) {
    throw std::runtime_error("Custom job executor not initialized");
  }
  customJobExecutor->executeJob(jobName);
}

// Data quality validation thread that runs continuously while the system is
// running. Validates tables for MariaDB, MSSQL, and PostgreSQL engines by
// calling validateTablesForEngine for each. Creates a PostgreSQL connection
// with 30s statement timeout and 10s lock timeout. Sleeps for
// sync_interval * 2 seconds between cycles. Handles connection errors by
// sleeping and retrying. Handles exceptions by logging errors and continuing
// to the next cycle. This thread ensures data quality metrics are regularly
// updated.
void StreamingData::qualityThread() {
  Logger::info(LogCategory::MONITORING, "Data quality thread started");
  while (running) {
    std::unique_ptr<pqxx::connection> pgConn;

    try {
      Logger::info(LogCategory::MONITORING,
                   "Starting data quality validation cycle");

      std::string connStr = DatabaseConfig::getPostgresConnectionString();
      if (connStr.empty()) {
        Logger::error(LogCategory::MONITORING, "qualityThread",
                      "CRITICAL ERROR: Empty PostgreSQL connection string");
        std::this_thread::sleep_for(
            std::chrono::seconds(SyncConfig::getSyncInterval() * 2));
        continue;
      }

      pgConn = std::make_unique<pqxx::connection>(connStr);

      pgConn->set_session_var("statement_timeout", "30000");
      pgConn->set_session_var("lock_timeout", "10000");

      if (!pgConn->is_open()) {
        Logger::error(LogCategory::MONITORING, "qualityThread",
                      "CRITICAL ERROR: Cannot establish PostgreSQL "
                      "connection for data quality validation");
        pgConn.reset();
        std::this_thread::sleep_for(
            std::chrono::seconds(SyncConfig::getSyncInterval() * 2));
        continue;
      }

      pgConn->set_session_var("statement_timeout", "30000");
      pgConn->set_session_var("lock_timeout", "10000");

      validateTablesForEngine(*pgConn, "MariaDB");
      validateTablesForEngine(*pgConn, "MSSQL");
      validateTablesForEngine(*pgConn, "PostgreSQL");
      validateTablesForEngine(*pgConn, "MongoDB");

      Logger::info(LogCategory::MONITORING,
                   "Data quality validation cycle completed successfully");
    } catch (const std::exception &e) {
      Logger::error(LogCategory::MONITORING, "qualityThread",
                    "CRITICAL ERROR in data quality validation cycle: " +
                        std::string(e.what()) +
                        " - Data quality validation completely failed");
    }

    pgConn.reset();

    std::this_thread::sleep_for(
        std::chrono::seconds(SyncConfig::getSyncInterval() * 2));
  }
  Logger::info(LogCategory::MONITORING, "Data quality thread stopped");
}

// Maintenance thread that runs continuously while the system is running.
// Performs periodic maintenance tasks: MariaDB table setup, MSSQL catalog
// sync, catalog cleanup, no-data table deactivation, and metrics collection.
// Measures and logs maintenance cycle duration. Each maintenance task is
// wrapped in try-catch to prevent one failure from stopping the entire cycle.
// Sleeps for sync_interval * 4 seconds between cycles. This thread ensures
// the system stays healthy and metrics are current.
void StreamingData::maintenanceThread() {
  Logger::info(LogCategory::MONITORING, "Maintenance thread started");
  while (running) {
    try {
      Logger::info(LogCategory::MONITORING,
                   "Starting periodic maintenance cycle");
      auto cycleStartTime = std::chrono::high_resolution_clock::now();

      try {
        Logger::info(LogCategory::MONITORING,
                     "Performing MariaDB table maintenance setup");
        mariaToPg.setupTableTargetMariaDBToPostgres();
        Logger::info(LogCategory::MONITORING,
                     "MariaDB table maintenance setup completed");
      } catch (const std::exception &e) {
        Logger::error(LogCategory::MONITORING, "maintenanceThread",
                      "ERROR in MariaDB table maintenance setup: " +
                          std::string(e.what()) +
                          " - MariaDB tables may not be properly maintained");
      }

      try {
        Logger::info(LogCategory::MONITORING,
                     "Performing MSSQL catalog sync maintenance");
        catalogManager.syncCatalogMSSQLToPostgres();
        Logger::info(LogCategory::MONITORING,
                     "MSSQL catalog sync maintenance completed");
      } catch (const std::exception &e) {
        Logger::error(LogCategory::MONITORING, "maintenanceThread",
                      "ERROR in MSSQL catalog sync maintenance: " +
                          std::string(e.what()) +
                          " - MSSQL catalog may be out of sync");
      }

      try {
        Logger::info(LogCategory::MONITORING,
                     "Performing catalog cleanup maintenance");
        catalogManager.cleanCatalog();
        Logger::info(LogCategory::MONITORING,
                     "Catalog cleanup maintenance completed");
      } catch (const std::exception &e) {
        Logger::error(
            LogCategory::MONITORING, "maintenanceThread",
            "ERROR in catalog cleanup maintenance: " + std::string(e.what()) +
                " - Catalog may contain stale entries");
      }

      try {
        Logger::info(LogCategory::MONITORING,
                     "Performing no-data table deactivation maintenance");
        catalogManager.deactivateNoDataTables();
        Logger::info(LogCategory::MONITORING,
                     "No-data table deactivation maintenance completed");
      } catch (const std::exception &e) {
        Logger::error(LogCategory::MONITORING, "maintenanceThread",
                      "ERROR in no-data table deactivation maintenance: " +
                          std::string(e.what()) +
                          " - Inactive tables may not be properly marked");
      }

      try {
        Logger::info(LogCategory::MONITORING,
                     "Performing metrics collection maintenance");
        MetricsCollector metricsCollector;
        metricsCollector.collectAllMetrics();
        Logger::info(LogCategory::MONITORING,
                     "Metrics collection maintenance completed");
      } catch (const std::exception &e) {
        Logger::error(LogCategory::MONITORING, "maintenanceThread",
                      "ERROR in metrics collection maintenance: " +
                          std::string(e.what()) +
                          " - System metrics may not be current");
      }

      try {
        Logger::info(LogCategory::MONITORING,
                     "Performing database maintenance detection and execution");
        MaintenanceManager maintenanceManager(
            DatabaseConfig::getPostgresConnectionString());
        maintenanceManager.detectMaintenanceNeeds();
        maintenanceManager.executeMaintenance();
        maintenanceManager.generateReport();
        Logger::info(LogCategory::MONITORING, "Database maintenance completed");
      } catch (const std::exception &e) {
        Logger::error(
            LogCategory::MONITORING, "maintenanceThread",
            "ERROR in database maintenance: " + std::string(e.what()) +
                " - Database maintenance may not be current");
      }

      auto cycleEndTime = std::chrono::high_resolution_clock::now();
      auto cycleDuration = std::chrono::duration_cast<std::chrono::seconds>(
          cycleEndTime - cycleStartTime);
      Logger::info(LogCategory::MONITORING,
                   "Periodic maintenance cycle completed successfully in " +
                       std::to_string(cycleDuration.count()) + " seconds");
    } catch (const std::exception &e) {
      Logger::error(LogCategory::MONITORING, "maintenanceThread",
                    "CRITICAL ERROR in periodic maintenance cycle: " +
                        std::string(e.what()) +
                        " - Maintenance cycle completely failed");
    }

    std::this_thread::sleep_for(
        std::chrono::seconds(SyncConfig::getSyncInterval() * 4));
  }
  Logger::info(LogCategory::MONITORING, "Maintenance thread stopped");
}

// Monitoring thread that runs continuously while the system is running.
// Loads configuration from the database on each cycle. Creates a PostgreSQL
// connection with 30s statement timeout and 10s lock timeout. Measures and
// logs monitoring cycle duration. Sleeps for sync_interval seconds between
// cycles. Handles connection errors by sleeping and retrying. Handles
// exceptions by logging errors and continuing to the next cycle. This thread
// ensures system configuration stays current and system health is monitored.
void StreamingData::monitoringThread() {
  Logger::info(LogCategory::MONITORING, "Monitoring thread started");
  while (running) {
    std::unique_ptr<pqxx::connection> pgConn;

    try {
      Logger::info(LogCategory::MONITORING, "Starting monitoring cycle");
      auto monitoringStartTime = std::chrono::high_resolution_clock::now();

      std::string connStr = DatabaseConfig::getPostgresConnectionString();
      if (connStr.empty()) {
        Logger::error(LogCategory::MONITORING, "monitoringThread",
                      "CRITICAL ERROR: Empty PostgreSQL connection string "
                      "for monitoring");
        std::this_thread::sleep_for(
            std::chrono::seconds(SyncConfig::getSyncInterval()));
        continue;
      }

      pgConn = std::make_unique<pqxx::connection>(connStr);

      if (!pgConn->is_open()) {
        Logger::error(
            LogCategory::MONITORING, "monitoringThread",
            "CRITICAL ERROR: Cannot establish PostgreSQL connection for "
            "monitoring - system health cannot be monitored");
        pgConn.reset();
        std::this_thread::sleep_for(
            std::chrono::seconds(SyncConfig::getSyncInterval()));
        continue;
      }

      pgConn->set_session_var("statement_timeout", "30000");
      pgConn->set_session_var("lock_timeout", "10000");

      try {
        Logger::info(LogCategory::MONITORING,
                     "Loading configuration from database");
        loadConfigFromDatabase(*pgConn);
      } catch (const std::exception &e) {
        Logger::error(LogCategory::MONITORING, "monitoringThread",
                      "ERROR loading configuration in monitoring cycle: " +
                          std::string(e.what()) +
                          " - Configuration may not be current");
      }

      Logger::info(
          LogCategory::MONITORING,
          "Monitoring cycle completed - using web dashboard for reporting");

      auto monitoringEndTime = std::chrono::high_resolution_clock::now();
      auto monitoringDuration =
          std::chrono::duration_cast<std::chrono::milliseconds>(
              monitoringEndTime - monitoringStartTime);
      Logger::info(LogCategory::MONITORING,
                   "Monitoring cycle completed successfully in " +
                       std::to_string(monitoringDuration.count()) +
                       " milliseconds");
    } catch (const std::exception &e) {
      Logger::error(
          LogCategory::MONITORING, "monitoringThread",
          "CRITICAL ERROR in monitoring cycle: " + std::string(e.what()) +
              " - System monitoring completely failed");
    }

    pgConn.reset();

    std::this_thread::sleep_for(
        std::chrono::seconds(SyncConfig::getSyncInterval()));
  }
  Logger::info(LogCategory::MONITORING, "Monitoring thread stopped");
}

// Validates tables for a specific database engine by querying metadata.catalog
// for tables with status 'LISTENING_CHANGES'. For each table, calls
// dataQuality.validateTable to perform quality checks. Uses txn.quote() to
// prevent SQL injection when constructing the query. Logs validation start,
// progress, and completion. Handles exceptions for individual table
// validations, allowing the process to continue even if one table fails. Used
// by qualityThread to validate tables for all engines.
void StreamingData::validateTablesForEngine(pqxx::connection &pgConn,
                                            const std::string &dbEngine) {
  try {
    if (!pgConn.is_open()) {
      Logger::error(LogCategory::MONITORING, "validateTablesForEngine",
                    "Connection is not open for " + dbEngine + " validation");
      return;
    }

    Logger::info(LogCategory::MONITORING,
                 "Starting " + dbEngine + " table validation");
    pqxx::work txn(pgConn);
    auto tables =
        txn.exec("SELECT schema_name, table_name FROM metadata.catalog WHERE "
                 "db_engine = " +
                 txn.quote(dbEngine) + " AND status = 'LISTENING_CHANGES'");
    txn.commit();

    for (const auto &row : tables) {
      try {
        std::string schema = row[0].as<std::string>();
        std::string table = row[1].as<std::string>();
        Logger::info(LogCategory::MONITORING, "Validating " + dbEngine +
                                                  " table: " + schema + "." +
                                                  table);
        dataQuality.validateTable(pgConn, schema, table, dbEngine);
      } catch (const std::exception &e) {
        Logger::error(LogCategory::MONITORING, "validateTablesForEngine",
                      "ERROR validating " + dbEngine + " table " +
                          row[0].as<std::string>() + "." +
                          row[1].as<std::string>() + ": " +
                          std::string(e.what()));
      }
    }
    Logger::info(LogCategory::MONITORING,
                 dbEngine + " table validation completed");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::MONITORING, "validateTablesForEngine",
                  "CRITICAL ERROR in " + dbEngine +
                      " table validation: " + std::string(e.what()) + " - " +
                      dbEngine + " data quality checks failed");
  }
}

void StreamingData::datalakeSchedulerThread() {
  Logger::info(LogCategory::MONITORING, "Datalake scheduler thread started");
  while (running) {
    try {
      Logger::info(LogCategory::MONITORING,
                   "Starting datalake scheduled tables check cycle");

      processScheduledTables("MariaDB");
      processScheduledTables("MSSQL");
      processScheduledTables("MongoDB");
      processScheduledTables("Oracle");

      Logger::info(LogCategory::MONITORING,
                   "Datalake scheduled tables check cycle completed");
    } catch (const std::exception &e) {
      Logger::error(LogCategory::MONITORING, "datalakeSchedulerThread",
                    "CRITICAL ERROR in datalake scheduler cycle: " +
                        std::string(e.what()) +
                        " - Scheduled table processing failed");
    }

    std::this_thread::sleep_for(std::chrono::seconds(60));
  }
  Logger::info(LogCategory::MONITORING, "Datalake scheduler thread stopped");
}

void StreamingData::processScheduledTables(const std::string &dbEngine) {
  try {
    pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());
    if (!pgConn.is_open()) {
      Logger::error(LogCategory::MONITORING, "processScheduledTables",
                    "Cannot establish PostgreSQL connection for " + dbEngine);
      return;
    }

    pqxx::work txn(pgConn);
    std::string query =
        "SELECT schema_name, table_name, connection_string, "
        "COALESCE(pk_strategy, '') as pk_strategy, "
        "COALESCE(status, '') as status, cron_schedule "
        "FROM metadata.catalog "
        "WHERE db_engine = $1 "
        "AND active = true "
        "AND cron_schedule IS NOT NULL "
        "AND cron_schedule != '' "
        "AND (next_sync_time IS NULL OR next_sync_time <= NOW()) "
        "AND status IN ('FULL_LOAD', 'SYNC', 'IN_PROGRESS', "
        "'LISTENING_CHANGES') "
        "ORDER BY next_sync_time NULLS FIRST "
        "LIMIT 50";

    auto result = txn.exec_params(query, dbEngine);
    txn.commit();

    if (result.empty()) {
      return;
    }

    Logger::info(LogCategory::MONITORING, "processScheduledTables",
                 "Found " + std::to_string(result.size()) + " scheduled " +
                     dbEngine + " tables to process");

    auto currentTime = std::chrono::system_clock::now();
    auto timeT = std::chrono::system_clock::to_time_t(currentTime);
    std::tm *tm = std::gmtime(&timeT);
    int currentMinute = tm->tm_min;
    int currentHour = tm->tm_hour;
    int currentDay = tm->tm_mday;
    int currentMonth = tm->tm_mon + 1;
    int currentDow = tm->tm_wday;

    for (const auto &row : result) {
      std::string schema = row[0].as<std::string>();
      std::string table = row[1].as<std::string>();
      std::string connStr = row[2].as<std::string>();
      std::string pkStrategy = row[3].is_null() ? "" : row[3].as<std::string>();
      std::string status = row[4].is_null() ? "" : row[4].as<std::string>();
      std::string cronSchedule = row[5].as<std::string>();

      std::istringstream cronStream(cronSchedule);
      std::string minute, hour, day, month, dow;
      cronStream >> minute >> hour >> day >> month >> dow;

      bool shouldRun = matchesCronField(minute, currentMinute) &&
                       matchesCronField(hour, currentHour) &&
                       matchesCronField(day, currentDay) &&
                       matchesCronField(month, currentMonth) &&
                       matchesCronField(dow, currentDow);

      if (!shouldRun) {
        continue;
      }

      Logger::info(LogCategory::MONITORING, "processScheduledTables",
                   "Processing scheduled table: " + schema + "." + table +
                       " (cron: " + cronSchedule + ")");

      try {
        DatabaseToPostgresSync::TableInfo tableInfo;
        tableInfo.schema_name = schema;
        tableInfo.table_name = table;
        tableInfo.connection_string = connStr;
        tableInfo.db_engine = dbEngine;
        tableInfo.status = status;
        tableInfo.pk_strategy = pkStrategy;

        pqxx::connection tablePgConn(
            DatabaseConfig::getPostgresConnectionString());

        try {
          if (dbEngine == "MariaDB") {
            mariaToPg.processTableParallel(tableInfo, tablePgConn);
          } else if (dbEngine == "MSSQL") {
            mssqlToPg.processTableParallel(tableInfo, tablePgConn);
          } else if (dbEngine == "MongoDB") {
            std::string effectiveStrategy =
                pkStrategy.empty() ? "CDC" : pkStrategy;
            if (effectiveStrategy == "CDC" && status != "FULL_LOAD") {
              mongoToPg.processTableCDC(tableInfo, tablePgConn);
            } else {
              Logger::warning(LogCategory::MONITORING, "processScheduledTables",
                              "MongoDB table " + schema + "." + table +
                                  " requires full sync, skipping scheduled "
                                  "processing (use regular transfer thread)");
              continue;
            }
          } else if (dbEngine == "Oracle") {
            oracleToPg.processTableParallel(tableInfo, tablePgConn);
          }
        } catch (const std::exception &e) {
          Logger::error(LogCategory::MONITORING, "processScheduledTables",
                        "Error processing table " + schema + "." + table +
                            ": " + std::string(e.what()));
          throw;
        }

        auto nextSync = calculateNextSyncTime(cronSchedule);
        auto nextSyncTimeT = std::chrono::system_clock::to_time_t(nextSync);

        pqxx::work updateTxn(pgConn);
        std::string updateQuery =
            "UPDATE metadata.catalog "
            "SET next_sync_time = TO_TIMESTAMP(" +
            std::to_string(nextSyncTimeT) +
            ") "
            "WHERE schema_name = " +
            updateTxn.quote(schema) +
            " AND table_name = " + updateTxn.quote(table) +
            " AND db_engine = " + updateTxn.quote(dbEngine);
        updateTxn.exec(updateQuery);
        updateTxn.commit();

        Logger::info(LogCategory::MONITORING, "processScheduledTables",
                     "Successfully processed and scheduled next sync for " +
                         schema + "." + table);

      } catch (const std::exception &e) {
        Logger::error(LogCategory::MONITORING, "processScheduledTables",
                      "Error processing scheduled table " + schema + "." +
                          table + ": " + std::string(e.what()));
      }
    }

  } catch (const std::exception &e) {
    Logger::error(LogCategory::MONITORING, "processScheduledTables",
                  "Error processing scheduled tables for " + dbEngine + ": " +
                      std::string(e.what()));
  }
}

bool StreamingData::matchesCronField(const std::string &field,
                                     int currentValue) {
  if (field == "*") {
    return true;
  }

  size_t dashPos = field.find('-');
  size_t commaPos = field.find(',');
  size_t slashPos = field.find('/');

  if (dashPos != std::string::npos) {
    try {
      int start = std::stoi(field.substr(0, dashPos));
      int end = std::stoi(field.substr(dashPos + 1));
      return currentValue >= start && currentValue <= end;
    } catch (...) {
      return false;
    }
  }

  if (commaPos != std::string::npos) {
    std::istringstream listStream(field);
    std::string item;
    while (std::getline(listStream, item, ',')) {
      try {
        if (std::stoi(item) == currentValue) {
          return true;
        }
      } catch (...) {
        continue;
      }
    }
    return false;
  }

  if (slashPos != std::string::npos) {
    try {
      std::string base = field.substr(0, slashPos);
      int step = std::stoi(field.substr(slashPos + 1));
      if (base == "*") {
        return (currentValue % step) == 0;
      } else {
        int start = std::stoi(base);
        return ((currentValue - start) % step) == 0 && currentValue >= start;
      }
    } catch (...) {
      return false;
    }
  }

  try {
    return std::stoi(field) == currentValue;
  } catch (...) {
    return false;
  }
}

std::chrono::system_clock::time_point
StreamingData::calculateNextSyncTime(const std::string &cronSchedule) {
  auto now = std::chrono::system_clock::now();
  auto timeT = std::chrono::system_clock::to_time_t(now);
  std::tm *tm = std::gmtime(&timeT);
  std::tm nextTm = *tm;
  nextTm.tm_sec = 0;
  nextTm.tm_min++;

  std::istringstream cronStream(cronSchedule);
  std::string minute, hour, day, month, dow;
  cronStream >> minute >> hour >> day >> month >> dow;

  bool found = false;
  int maxIterations = 366 * 24 * 60;
  int iterations = 0;

  while (!found && iterations < maxIterations) {
    iterations++;

    bool minuteMatch = matchesCronField(minute, nextTm.tm_min);
    bool hourMatch = matchesCronField(hour, nextTm.tm_hour);
    bool dayMatch = matchesCronField(day, nextTm.tm_mday);
    bool monthMatch = matchesCronField(month, nextTm.tm_mon + 1);
    bool dowMatch = matchesCronField(dow, nextTm.tm_wday);

    if (minuteMatch && hourMatch && dayMatch && monthMatch && dowMatch) {
      std::time_t nextTimeT = std::mktime(&nextTm);
      if (nextTimeT > timeT) {
        found = true;
        break;
      }
    }

    nextTm.tm_min++;
    if (nextTm.tm_min >= 60) {
      nextTm.tm_min = 0;
      nextTm.tm_hour++;
      if (nextTm.tm_hour >= 24) {
        nextTm.tm_hour = 0;
        nextTm.tm_mday++;
        int daysInMonth = 31;
        if (nextTm.tm_mon == 1) {
          daysInMonth = 28;
        } else if (nextTm.tm_mon == 3 || nextTm.tm_mon == 5 ||
                   nextTm.tm_mon == 8 || nextTm.tm_mon == 10) {
          daysInMonth = 30;
        }
        if (nextTm.tm_mday > daysInMonth) {
          nextTm.tm_mday = 1;
          nextTm.tm_mon++;
          if (nextTm.tm_mon >= 12) {
            nextTm.tm_mon = 0;
            nextTm.tm_year++;
          }
        }
      }
    }
  }

  if (!found) {
    nextTm.tm_min += 60;
    if (nextTm.tm_min >= 60) {
      nextTm.tm_min -= 60;
      nextTm.tm_hour++;
      if (nextTm.tm_hour >= 24) {
        nextTm.tm_hour = 0;
        nextTm.tm_mday++;
      }
    }
  }

  std::time_t nextTimeT = std::mktime(&nextTm);
  return std::chrono::system_clock::from_time_t(nextTimeT);
}
