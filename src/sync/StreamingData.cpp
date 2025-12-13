#include "sync/StreamingData.h"
#include "core/database_config.h"
#include "governance/QueryActivityLogger.h"
#include "governance/QueryStoreCollector.h"

// Destructor automatically shuts down the system, ensuring all threads are
// properly joined and resources are cleaned up.
StreamingData::~StreamingData() { shutdown(); }

// Initializes the DataSync system components. Currently performs minimal
// initialization, logging that database connections will be created as needed.
// This is a placeholder for future initialization logic. Should be called
// before run() to prepare the system.
void StreamingData::initialize() {
  Logger::info(LogCategory::MONITORING,
               "Initializing DataSync system components");

  Logger::info(LogCategory::MONITORING,
               "Database connections will be created as needed");

  Logger::info(LogCategory::MONITORING,
               "System initialization completed successfully");
}

// Main entry point that starts the multi-threaded DataSync system. Launches
// all core threads (initialization, catalog sync, monitoring, quality,
// maintenance) and transfer threads (MariaDB, MSSQL). Waits for all threads to
// complete before returning. Handles exceptions when joining threads. This
// method blocks until all threads finish execution. Should be called after
// initialize().
void StreamingData::run() {
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
               "Launching transfer threads (MariaDB, MSSQL)");
  threads.emplace_back(&StreamingData::mariaTransferThread, this);
  threads.emplace_back(&StreamingData::mssqlTransferThread, this);
  threads.emplace_back(&StreamingData::mongoTransferThread, this);
  threads.emplace_back(&StreamingData::oracleTransferThread, this);

  Logger::info(LogCategory::MONITORING,
               "Transfer threads launched successfully");

  Logger::info(LogCategory::MONITORING,
               "All threads launched successfully - System running");

  Logger::info(LogCategory::MONITORING, "Waiting for all threads to complete");
  for (auto &thread : threads) {
    if (thread.joinable()) {
      try {
        thread.join();
      } catch (const std::exception &e) {
        Logger::error(LogCategory::MONITORING, "run",
                      "Error joining thread in run(): " +
                          std::string(e.what()));
      }
    }
  }
  Logger::info(LogCategory::MONITORING, "All threads completed");
}

// Shuts down the DataSync system gracefully. Sets the running flag to false
// to signal all threads to stop, then waits for all threads to finish by
// joining them. Clears the threads vector after all threads are joined.
// Handles exceptions when joining threads. Idempotent - can be called
// multiple times safely. Should be called before destroying the object.
void StreamingData::shutdown() {
  Logger::info(LogCategory::MONITORING, "Shutting down DataSync system");
  running = false;

  Logger::info(LogCategory::MONITORING, "Waiting for all threads to finish");
  for (auto &thread : threads) {
    if (thread.joinable()) {
      try {
        thread.join();
      } catch (const std::exception &e) {
        Logger::error(LogCategory::MONITORING, "shutdown",
                      "Error joining thread: " + std::string(e.what()));
      }
    }
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
          size_t newSize = std::stoul(value);
          if (newSize >= 1 && newSize <= 1024 * 1024 * 1024 &&
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
          size_t newInterval = std::stoul(value);
          if (newInterval >= 5 && newInterval <= 3600 &&
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
          size_t v = std::stoul(value);
          if (v >= 1 && v <= 128 && v != SyncConfig::getMaxWorkers()) {
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
          size_t v = std::stoul(value);
          if (v >= 1 && v <= 1000000 &&
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

// Initialization thread that runs once at system startup. Initializes
// DataGovernance component, runs discovery, and generates a report.
// Initializes MetricsCollector and collects all metrics. Sets up MariaDB and
// MSSQL target tables. Each component initialization is wrapped in try-catch
// to prevent one failure from stopping the entire initialization. Logs all
// operations and errors. This thread completes after all initialization
// tasks are done.
void StreamingData::initializationThread() {
  try {
    Logger::info(LogCategory::MONITORING,
                 "Starting system initialization thread");

    try {
      Logger::info(LogCategory::MONITORING,
                   "Initializing DataGovernance component");
      DataGovernance dg;
      dg.initialize();
      Logger::info(LogCategory::MONITORING,
                   "DataGovernance initialized successfully");

      dg.runDiscovery();
      Logger::info(LogCategory::MONITORING,
                   "DataGovernance discovery completed");

      dg.generateReport();
      Logger::info(LogCategory::MONITORING, "DataGovernance report generated");
    } catch (const std::exception &e) {
      Logger::error(LogCategory::MONITORING, "initializationThread",
                    "CRITICAL ERROR in DataGovernance initialization: " +
                        std::string(e.what()) +
                        " - System may not function properly");
    }

    try {
      Logger::info(LogCategory::MONITORING,
                   "Initializing MetricsCollector component");
      MetricsCollector metricsCollector;
      metricsCollector.collectAllMetrics();
      Logger::info(LogCategory::MONITORING,
                   "MetricsCollector completed successfully");
    } catch (const std::exception &e) {
      Logger::error(LogCategory::MONITORING, "initializationThread",
                    "CRITICAL ERROR in MetricsCollector: " +
                        std::string(e.what()) + " - Metrics collection failed");
    }

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
          LogCategory::MONITORING, "initializationThread",
          "CRITICAL ERROR in QueryStoreCollector: " + std::string(e.what()) +
              " - Query snapshot collection failed");
    }

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
          LogCategory::MONITORING, "initializationThread",
          "CRITICAL ERROR in QueryActivityLogger: " + std::string(e.what()) +
              " - Query activity logging failed");
    }

    try {
      Logger::info(LogCategory::MONITORING, "Setting up MariaDB target tables");
      mariaToPg.setupTableTargetMariaDBToPostgres();
      Logger::info(LogCategory::MONITORING,
                   "MariaDB target tables setup completed");
    } catch (const std::exception &e) {
      Logger::error(LogCategory::MONITORING, "initializationThread",
                    "CRITICAL ERROR in MariaDB table setup: " +
                        std::string(e.what()) + " - MariaDB sync may fail");
    }

    try {
      Logger::info(LogCategory::MONITORING, "Setting up MSSQL target tables");
      mssqlToPg.setupTableTargetMSSQLToPostgres();
      Logger::info(LogCategory::MONITORING,
                   "MSSQL target tables setup completed");
    } catch (const std::exception &e) {
      Logger::error(LogCategory::MONITORING, "initializationThread",
                    "CRITICAL ERROR in MSSQL table setup: " +
                        std::string(e.what()) + " - MSSQL sync may fail");
    }

    try {
      Logger::info(LogCategory::MONITORING, "Setting up Oracle target tables");
      oracleToPg.setupTableTargetOracleToPostgres();
      Logger::info(LogCategory::MONITORING,
                   "Oracle target tables setup completed");
    } catch (const std::exception &e) {
      Logger::error(LogCategory::MONITORING, "initializationThread",
                    "CRITICAL ERROR in Oracle table setup: " +
                        std::string(e.what()) + " - Oracle sync may fail");
    }

    Logger::info(LogCategory::MONITORING,
                 "System initialization thread completed successfully");
  } catch (const std::exception &e) {
    Logger::error(
        LogCategory::MONITORING, "initializationThread",
        "CRITICAL ERROR in initializationThread: " + std::string(e.what()) +
            " - System initialization failed completely");
  }
}

// Catalog synchronization thread that runs continuously while the system is
// running. Performs periodic catalog synchronization for MariaDB and MSSQL
// in parallel using separate threads. After sync, performs catalog cleanup
// and deactivates tables with no data. Sleeps for SyncConfig::getSyncInterval()
// seconds between cycles. Handles exceptions for each operation
// independently, allowing the thread to continue even if one operation fails.
// Logs all operations and errors.
void StreamingData::catalogSyncThread() {
  Logger::info(LogCategory::MONITORING, "Catalog sync thread started");
  while (running) {
    try {
      Logger::info(LogCategory::MONITORING,
                   "Starting catalog synchronization cycle");

      std::vector<std::thread> syncThreads;
      std::vector<std::exception_ptr> exceptions;
      std::mutex exceptionMutex;

      syncThreads.emplace_back([this, &exceptions, &exceptionMutex]() {
        try {
          Logger::info(LogCategory::MONITORING,
                       "Starting MariaDB catalog sync");
          catalogManager.syncCatalogMariaDBToPostgres();
          Logger::info(LogCategory::MONITORING,
                       "MariaDB catalog sync completed successfully");
        } catch (const std::exception &e) {
          Logger::error(
              LogCategory::MONITORING, "catalogSyncThread",
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
          Logger::error(
              LogCategory::MONITORING, "catalogSyncThread",
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
          Logger::error(
              LogCategory::MONITORING, "catalogSyncThread",
              "ERROR in Oracle catalog sync: " + std::string(e.what()) +
                  " - Oracle catalog may be out of sync");
          std::lock_guard<std::mutex> lock(exceptionMutex);
          exceptions.push_back(std::current_exception());
        }
      });

      syncThreads.emplace_back([this, &exceptions, &exceptionMutex]() {
        try {
          Logger::info(LogCategory::MONITORING,
                       "Starting MongoDB catalog sync");
          catalogManager.syncCatalogMongoDBToPostgres();
          Logger::info(LogCategory::MONITORING,
                       "MongoDB catalog sync completed successfully");
        } catch (const std::exception &e) {
          Logger::error(
              LogCategory::MONITORING, "catalogSyncThread",
              "ERROR in MongoDB catalog sync: " + std::string(e.what()) +
                  " - MongoDB catalog may be out of sync");
          std::lock_guard<std::mutex> lock(exceptionMutex);
          exceptions.push_back(std::current_exception());
        }
      });

      for (auto &thread : syncThreads) {
        thread.join();
      }

      if (!exceptions.empty()) {
        Logger::error(LogCategory::MONITORING, "catalogSyncThread",
                      "CRITICAL: " + std::to_string(exceptions.size()) +
                          " catalog sync operations failed - system may be in "
                          "inconsistent state");
      }

      try {
        Logger::info(LogCategory::MONITORING, "Starting catalog cleanup");
        catalogManager.cleanCatalog();
        Logger::info(LogCategory::MONITORING,
                     "Catalog cleanup completed successfully");
      } catch (const std::exception &e) {
        Logger::error(LogCategory::MONITORING, "catalogSyncThread",
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
            LogCategory::MONITORING, "catalogSyncThread",
            "ERROR in no-data table deactivation: " + std::string(e.what()) +
                " - Inactive tables may not be properly marked");
      }

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

    std::this_thread::sleep_for(std::chrono::seconds(
        std::max(5, static_cast<int>(SyncConfig::getSyncInterval() / 4))));
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

    std::this_thread::sleep_for(std::chrono::seconds(
        std::max(5, static_cast<int>(SyncConfig::getSyncInterval() / 4))));
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
                   "Oracle transfer cycle completed successfully in " +
                       std::to_string(duration.count()) + " seconds");
    } catch (const std::exception &e) {
      Logger::error(
          LogCategory::MONITORING, "oracleTransferThread",
          "CRITICAL ERROR in Oracle transfer cycle: " + std::string(e.what()) +
              " - Oracle data sync failed, retrying in " +
              std::to_string(SyncConfig::getSyncInterval()) + " seconds");
    }

    std::this_thread::sleep_for(std::chrono::seconds(
        std::max(5, static_cast<int>(SyncConfig::getSyncInterval() / 4))));
  }
  Logger::info(LogCategory::MONITORING, "Oracle transfer thread stopped");
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
        std::this_thread::sleep_for(
            std::chrono::seconds(SyncConfig::getSyncInterval() * 2));
        continue;
      }

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

      pgConn->set_session_var("statement_timeout", "30000");
      pgConn->set_session_var("lock_timeout", "10000");

      if (!pgConn->is_open()) {
        Logger::error(
            LogCategory::MONITORING, "monitoringThread",
            "CRITICAL ERROR: Cannot establish PostgreSQL connection for "
            "monitoring - system health cannot be monitored");
        std::this_thread::sleep_for(
            std::chrono::seconds(SyncConfig::getSyncInterval()));
        continue;
      }

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
