#ifndef STREAMINGDATA_H
#define STREAMINGDATA_H

#include "Config.h"
#include "DDLExporter.h"
#include "DataGovernance.h"
#include "DataQuality.h"
#include "MSSQLToPostgres.h"
#include "MariaDBToPostgres.h"
#include "MetricsCollector.h"
#include "PostgresToPostgres.h"
#include "catalog_manager.h"
#include "logger.h"
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <future>
#include <iostream>
#include <mutex>
#include <pqxx/pqxx>
#include <thread>
#include <vector>

class StreamingData {
public:
  StreamingData() = default;
  ~StreamingData() { shutdown(); }

  void initialize() {
    Logger::info("Initializing DataSync system components");

    // Database connections will be created as needed
    Logger::info("Database connections will be created as needed");

    Logger::info("System initialization completed successfully");
  }

  void run() {
    Logger::info("Starting multi-threaded DataSync system");

    // Launch core threads
    Logger::info(
        "Launching core threads (init, sync, monitor, quality, maintenance)");
    threads.emplace_back(&StreamingData::initializationThread, this);
    threads.emplace_back(&StreamingData::catalogSyncThread, this);
    threads.emplace_back(&StreamingData::monitoringThread, this);
    threads.emplace_back(&StreamingData::qualityThread, this);
    threads.emplace_back(&StreamingData::maintenanceThread, this);
    Logger::info("Core threads launched successfully");

    // Launch transfer threads immediately
    Logger::info("Launching transfer threads (MariaDB, MSSQL, PostgreSQL)");
    threads.emplace_back(&StreamingData::mariaTransferThread, this);
    threads.emplace_back(&StreamingData::mssqlTransferThread, this);
    threads.emplace_back(&StreamingData::postgresTransferThread, this);
    Logger::info("Transfer threads launched successfully");

    Logger::info("All threads launched successfully - System running");

    // Wait for all threads to complete
    Logger::info("Waiting for all threads to complete");
    for (auto &thread : threads) {
      if (thread.joinable()) {
        thread.join();
      }
    }
    Logger::info("All threads completed");
  }

  void shutdown() {
    Logger::info("Shutting down DataSync system");
    running = false;

    // Wait for all threads to finish
    Logger::info("Waiting for all threads to finish");
    for (auto &thread : threads) {
      if (thread.joinable()) {
        thread.join();
      }
    }
    Logger::info("All threads finished successfully");

    // Database connections are cleaned up automatically

    Logger::info("Shutdown completed successfully");
  }

private:
  // Thread control
  std::atomic<bool> running{true};
  std::vector<std::thread> threads;
  std::mutex configMutex;
  std::condition_variable configCV;

  // Database objects
  MariaDBToPostgres mariaToPg;
  MSSQLToPostgres mssqlToPg;
  PostgresToPostgres pgToPg;
  CatalogManager catalogManager;
  DataQuality dataQuality;

  void loadConfigFromDatabase(pqxx::connection &pgConn) {
    try {
      Logger::info("Starting configuration load from database");

      if (!pgConn.is_open()) {
        Logger::error(
            "Database connection is not open in loadConfigFromDatabase");
        return;
      }

      pqxx::work txn(pgConn);
      auto results =
          txn.exec("SELECT key, value FROM metadata.config WHERE key IN "
                   "('chunk_size', 'sync_interval');");

      Logger::info("Configuration query executed, found " +
                   std::to_string(results.size()) + " config entries");

      txn.commit();

      for (const auto &row : results) {
        if (row.size() < 2) {
          Logger::error("Invalid configuration row - insufficient columns: " +
                        std::to_string(row.size()));
          continue;
        }

        std::string key = row[0].as<std::string>();
        std::string value = row[1].as<std::string>();

        Logger::info("Processing config key: " + key + " = " + value);

        if (key == "chunk_size") {
          try {
            size_t newSize = std::stoul(value);
            if (newSize > 0 && newSize != SyncConfig::getChunkSize()) {
              Logger::info("Updating chunk_size from " +
                           std::to_string(SyncConfig::getChunkSize()) + " to " +
                           std::to_string(newSize));
              SyncConfig::setChunkSize(newSize);
            }
          } catch (const std::exception &e) {
            Logger::error("Failed to parse chunk_size value '" + value +
                          "': " + std::string(e.what()));
          }
        } else if (key == "sync_interval") {
          try {
            size_t newInterval = std::stoul(value);
            if (newInterval > 0 &&
                newInterval != SyncConfig::getSyncInterval()) {
              Logger::info("Updating sync_interval from " +
                           std::to_string(SyncConfig::getSyncInterval()) +
                           " to " + std::to_string(newInterval));
              SyncConfig::setSyncInterval(newInterval);
            }
          } catch (const std::exception &e) {
            Logger::error("Failed to parse sync_interval value '" + value +
                          "': " + std::string(e.what()));
          }
        } else {
          Logger::warning("Unknown configuration key: " + key);
        }
      }

      Logger::info("Configuration load completed successfully");
    } catch (const std::exception &e) {
      Logger::error(
          "CRITICAL ERROR in loadConfigFromDatabase: " + std::string(e.what()) +
          " - Configuration may not be applied");
    }
  }

  // Thread implementations
  void initializationThread() {
    try {
      Logger::info("Starting system initialization thread");

      // DataGovernance
      try {
        Logger::info("Initializing DataGovernance component");
        DataGovernance dg;
        dg.initialize();
        Logger::info("DataGovernance initialized successfully");

        dg.runDiscovery();
        Logger::info("DataGovernance discovery completed");

        dg.generateReport();
        Logger::info("DataGovernance report generated");
      } catch (const std::exception &e) {
        Logger::error("CRITICAL ERROR in DataGovernance initialization: " +
                      std::string(e.what()) +
                      " - System may not function properly");
      }

      // DDLExporter
      try {
        Logger::info("Initializing DDLExporter component");
        DDLExporter ddlExporter;
        ddlExporter.exportAllDDL();
        Logger::info("DDLExporter completed successfully");
      } catch (const std::exception &e) {
        Logger::error(
            "CRITICAL ERROR in DDLExporter: " + std::string(e.what()) +
            " - Schema exports may be incomplete");
      }

      // MetricsCollector
      try {
        Logger::info("Initializing MetricsCollector component");
        MetricsCollector metricsCollector;
        metricsCollector.collectAllMetrics();
        Logger::info("MetricsCollector completed successfully");
      } catch (const std::exception &e) {
        Logger::error("CRITICAL ERROR in MetricsCollector: " +
                      std::string(e.what()) + " - Metrics collection failed");
      }

      // Setup initial tables
      try {
        Logger::info("Setting up MariaDB target tables");
        mariaToPg.setupTableTargetMariaDBToPostgres();
        Logger::info("MariaDB target tables setup completed");
      } catch (const std::exception &e) {
        Logger::error("CRITICAL ERROR in MariaDB table setup: " +
                      std::string(e.what()) + " - MariaDB sync may fail");
      }

      try {
        Logger::info("Setting up MSSQL target tables");
        mssqlToPg.setupTableTargetMSSQLToPostgres();
        Logger::info("MSSQL target tables setup completed");
      } catch (const std::exception &e) {
        Logger::error("CRITICAL ERROR in MSSQL table setup: " +
                      std::string(e.what()) + " - MSSQL sync may fail");
      }

      try {
        Logger::info("Setting up PostgreSQL target tables");
        pgToPg.setupTableTargetPostgresToPostgres();
        Logger::info("PostgreSQL target tables setup completed");
      } catch (const std::exception &e) {
        Logger::error("CRITICAL ERROR in PostgreSQL table setup: " +
                      std::string(e.what()) + " - PostgreSQL sync may fail");
      }

      Logger::info("System initialization thread completed successfully");
    } catch (const std::exception &e) {
      Logger::error(
          "CRITICAL ERROR in initializationThread: " + std::string(e.what()) +
          " - System initialization failed completely");
    }
  }

  void catalogSyncThread() {
    Logger::info("Catalog sync thread started");
    while (running) {
      try {
        Logger::info("Starting catalog synchronization cycle");

        // Launch all catalog syncs in parallel
        std::vector<std::thread> syncThreads;
        std::vector<std::exception_ptr> exceptions;
        std::mutex exceptionMutex;

        syncThreads.emplace_back([this, &exceptions, &exceptionMutex]() {
          try {
            Logger::info("Starting MariaDB catalog sync");
            catalogManager.syncCatalogMariaDBToPostgres();
            Logger::info("MariaDB catalog sync completed successfully");
          } catch (const std::exception &e) {
            Logger::error(
                "ERROR in MariaDB catalog sync: " + std::string(e.what()) +
                " - MariaDB catalog may be out of sync");
            std::lock_guard<std::mutex> lock(exceptionMutex);
            exceptions.push_back(std::current_exception());
          }
        });

        syncThreads.emplace_back([this, &exceptions, &exceptionMutex]() {
          try {
            Logger::info("Starting MSSQL catalog sync");
            catalogManager.syncCatalogMSSQLToPostgres();
            Logger::info("MSSQL catalog sync completed successfully");
          } catch (const std::exception &e) {
            Logger::error(
                "ERROR in MSSQL catalog sync: " + std::string(e.what()) +
                " - MSSQL catalog may be out of sync");
            std::lock_guard<std::mutex> lock(exceptionMutex);
            exceptions.push_back(std::current_exception());
          }
        });

        syncThreads.emplace_back([this, &exceptions, &exceptionMutex]() {
          try {
            Logger::info("Starting PostgreSQL catalog sync");
            catalogManager.syncCatalogPostgresToPostgres();
            Logger::info("PostgreSQL catalog sync completed successfully");
          } catch (const std::exception &e) {
            Logger::error(
                "ERROR in PostgreSQL catalog sync: " + std::string(e.what()) +
                " - PostgreSQL catalog may be out of sync");
            std::lock_guard<std::mutex> lock(exceptionMutex);
            exceptions.push_back(std::current_exception());
          }
        });

        // Wait for all sync threads to complete
        for (auto &thread : syncThreads) {
          thread.join();
        }

        if (!exceptions.empty()) {
          Logger::error("CRITICAL: " + std::to_string(exceptions.size()) +
                        " catalog sync operations failed - system may be in "
                        "inconsistent state");
        }

        try {
          Logger::info("Starting catalog cleanup");
          catalogManager.cleanCatalog();
          Logger::info("Catalog cleanup completed successfully");
        } catch (const std::exception &e) {
          Logger::error("ERROR in catalog cleanup: " + std::string(e.what()) +
                        " - Catalog may contain stale data");
        }

        try {
          Logger::info("Starting no-data table deactivation");
          catalogManager.deactivateNoDataTables();
          Logger::info("No-data table deactivation completed successfully");
        } catch (const std::exception &e) {
          Logger::error(
              "ERROR in no-data table deactivation: " + std::string(e.what()) +
              " - Inactive tables may not be properly marked");
        }

        Logger::info("Catalog synchronization cycle completed");
      } catch (const std::exception &e) {
        Logger::error("CRITICAL ERROR in catalog synchronization cycle: " +
                      std::string(e.what()) +
                      " - Catalog sync completely failed");
      }

      std::this_thread::sleep_for(std::chrono::seconds(30));
    }
    Logger::info("Catalog sync thread stopped");
  }

  void mariaTransferThread() {
    Logger::info("MariaDB transfer thread started");
    while (running) {
      try {
        Logger::info("Starting MariaDB transfer cycle - sync interval: " +
                     std::to_string(SyncConfig::getSyncInterval()) +
                     " seconds");

        auto startTime = std::chrono::high_resolution_clock::now();
        mariaToPg.transferDataMariaDBToPostgres();
        auto endTime = std::chrono::high_resolution_clock::now();

        auto duration = std::chrono::duration_cast<std::chrono::seconds>(
            endTime - startTime);
        Logger::info("MariaDB transfer cycle completed successfully in " +
                     std::to_string(duration.count()) + " seconds");
      } catch (const std::exception &e) {
        Logger::error("CRITICAL ERROR in MariaDB transfer cycle: " +
                      std::string(e.what()) +
                      " - MariaDB data sync failed, retrying in " +
                      std::to_string(SyncConfig::getSyncInterval()) +
                      " seconds");
      }

      std::this_thread::sleep_for(
          std::chrono::seconds(SyncConfig::getSyncInterval()));
    }
    Logger::info("MariaDB transfer thread stopped");
  }

  void mssqlTransferThread() {
    Logger::info("MSSQL transfer thread started");
    while (running) {
      try {
        Logger::info("Starting MSSQL transfer cycle - sync interval: " +
                     std::to_string(SyncConfig::getSyncInterval()) +
                     " seconds");

        auto startTime = std::chrono::high_resolution_clock::now();
        mssqlToPg.transferDataMSSQLToPostgres();
        auto endTime = std::chrono::high_resolution_clock::now();

        auto duration = std::chrono::duration_cast<std::chrono::seconds>(
            endTime - startTime);
        Logger::info("MSSQL transfer cycle completed successfully in " +
                     std::to_string(duration.count()) + " seconds");
      } catch (const std::exception &e) {
        Logger::error(
            "CRITICAL ERROR in MSSQL transfer cycle: " + std::string(e.what()) +
            " - MSSQL data sync failed, retrying in " +
            std::to_string(SyncConfig::getSyncInterval()) + " seconds");
      }

      std::this_thread::sleep_for(
          std::chrono::seconds(SyncConfig::getSyncInterval()));
    }
    Logger::info("MSSQL transfer thread stopped");
  }

  void postgresTransferThread() {
    Logger::info("PostgreSQL transfer thread started");
    while (running) {
      try {
        Logger::info("Starting PostgreSQL transfer cycle - sync interval: " +
                     std::to_string(SyncConfig::getSyncInterval()) +
                     " seconds");

        auto startTime = std::chrono::high_resolution_clock::now();
        pgToPg.transferDataPostgresToPostgres();
        auto endTime = std::chrono::high_resolution_clock::now();

        auto duration = std::chrono::duration_cast<std::chrono::seconds>(
            endTime - startTime);
        Logger::info("PostgreSQL transfer cycle completed successfully in " +
                     std::to_string(duration.count()) + " seconds");
      } catch (const std::exception &e) {
        Logger::error("CRITICAL ERROR in PostgreSQL transfer cycle: " +
                      std::string(e.what()) +
                      " - PostgreSQL data sync failed, retrying in " +
                      std::to_string(SyncConfig::getSyncInterval()) +
                      " seconds");
      }

      std::this_thread::sleep_for(
          std::chrono::seconds(SyncConfig::getSyncInterval()));
    }
    Logger::info("PostgreSQL transfer thread stopped");
  }

  void qualityThread() {
    Logger::info("Data quality thread started");
    while (running) {
      try {
        Logger::info("Starting data quality validation cycle");

        pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());

        if (!pgConn.is_open()) {
          Logger::error("CRITICAL ERROR: Cannot establish PostgreSQL "
                        "connection for data quality validation");
          std::this_thread::sleep_for(std::chrono::seconds(60));
          continue;
        }

        // Validate MariaDB tables
        try {
          Logger::info("Starting MariaDB table validation");
          pqxx::work txn(pgConn);
          auto mariaTables = txn.exec(
              "SELECT schema_name, table_name FROM metadata.catalog WHERE "
              "db_engine = 'MariaDB' AND status = 'PERFECT_MATCH'");
          txn.commit();

          Logger::info("Found " + std::to_string(mariaTables.size()) +
                       " MariaDB tables to validate");

          for (const auto &row : mariaTables) {
            try {
              std::string schema = row[0].as<std::string>();
              std::string table = row[1].as<std::string>();
              Logger::info("Validating MariaDB table: " + schema + "." + table);
              dataQuality.validateTable(pgConn, schema, table, "MariaDB");
            } catch (const std::exception &e) {
              Logger::error("ERROR validating MariaDB table " +
                            row[0].as<std::string>() + "." +
                            row[1].as<std::string>() + ": " +
                            std::string(e.what()));
            }
          }
          Logger::info("MariaDB table validation completed");
        } catch (const std::exception &e) {
          Logger::error("CRITICAL ERROR in MariaDB table validation: " +
                        std::string(e.what()) +
                        " - MariaDB data quality checks failed");
        }

        // Validate MSSQL tables
        try {
          Logger::info("Starting MSSQL table validation");
          pqxx::work txn(pgConn);
          auto mssqlTables = txn.exec(
              "SELECT schema_name, table_name FROM metadata.catalog "
              "WHERE db_engine = 'MSSQL' AND status = 'PERFECT_MATCH'");
          txn.commit();

          Logger::info("Found " + std::to_string(mssqlTables.size()) +
                       " MSSQL tables to validate");

          for (const auto &row : mssqlTables) {
            try {
              std::string schema = row[0].as<std::string>();
              std::string table = row[1].as<std::string>();
              Logger::info("Validating MSSQL table: " + schema + "." + table);
              dataQuality.validateTable(pgConn, schema, table, "MSSQL");
            } catch (const std::exception &e) {
              Logger::error("ERROR validating MSSQL table " +
                            row[0].as<std::string>() + "." +
                            row[1].as<std::string>() + ": " +
                            std::string(e.what()));
            }
          }
          Logger::info("MSSQL table validation completed");
        } catch (const std::exception &e) {
          Logger::error("CRITICAL ERROR in MSSQL table validation: " +
                        std::string(e.what()) +
                        " - MSSQL data quality checks failed");
        }

        // Validate PostgreSQL tables
        try {
          Logger::info("Starting PostgreSQL table validation");
          pqxx::work txn(pgConn);
          auto pgTables = txn.exec(
              "SELECT schema_name, table_name FROM metadata.catalog WHERE "
              "db_engine = 'PostgreSQL' AND status = 'PERFECT_MATCH'");
          txn.commit();

          Logger::info("Found " + std::to_string(pgTables.size()) +
                       " PostgreSQL tables to validate");

          for (const auto &row : pgTables) {
            try {
              std::string schema = row[0].as<std::string>();
              std::string table = row[1].as<std::string>();
              Logger::info("Validating PostgreSQL table: " + schema + "." +
                           table);
              dataQuality.validateTable(pgConn, schema, table, "PostgreSQL");
            } catch (const std::exception &e) {
              Logger::error("ERROR validating PostgreSQL table " +
                            row[0].as<std::string>() + "." +
                            row[1].as<std::string>() + ": " +
                            std::string(e.what()));
            }
          }
          Logger::info("PostgreSQL table validation completed");
        } catch (const std::exception &e) {
          Logger::error("CRITICAL ERROR in PostgreSQL table validation: " +
                        std::string(e.what()) +
                        " - PostgreSQL data quality checks failed");
        }

        Logger::info("Data quality validation cycle completed successfully");
      } catch (const std::exception &e) {
        Logger::error("CRITICAL ERROR in data quality validation cycle: " +
                      std::string(e.what()) +
                      " - Data quality validation completely failed");
      }

      std::this_thread::sleep_for(std::chrono::seconds(60));
    }
    Logger::info("Data quality thread stopped");
  }

  void maintenanceThread() {
    Logger::info("Maintenance thread started");
    while (running) {
      try {
        Logger::info("Starting periodic maintenance cycle");
        auto cycleStartTime = std::chrono::high_resolution_clock::now();

        // Setup tables
        try {
          Logger::info("Performing MariaDB table maintenance setup");
          mariaToPg.setupTableTargetMariaDBToPostgres();
          Logger::info("MariaDB table maintenance setup completed");
        } catch (const std::exception &e) {
          Logger::error("ERROR in MariaDB table maintenance setup: " +
                        std::string(e.what()) +
                        " - MariaDB tables may not be properly maintained");
        }

        // Sync catalogs
        try {
          Logger::info("Performing MSSQL catalog sync maintenance");
          catalogManager.syncCatalogMSSQLToPostgres();
          Logger::info("MSSQL catalog sync maintenance completed");
        } catch (const std::exception &e) {
          Logger::error("ERROR in MSSQL catalog sync maintenance: " +
                        std::string(e.what()) +
                        " - MSSQL catalog may be out of sync");
        }

        try {
          Logger::info("Performing PostgreSQL catalog sync maintenance");
          catalogManager.syncCatalogPostgresToPostgres();
          Logger::info("PostgreSQL catalog sync maintenance completed");
        } catch (const std::exception &e) {
          Logger::error("ERROR in PostgreSQL catalog sync maintenance: " +
                        std::string(e.what()) +
                        " - PostgreSQL catalog may be out of sync");
        }

        // Cleanup
        try {
          Logger::info("Performing catalog cleanup maintenance");
          catalogManager.cleanCatalog();
          Logger::info("Catalog cleanup maintenance completed");
        } catch (const std::exception &e) {
          Logger::error(
              "ERROR in catalog cleanup maintenance: " + std::string(e.what()) +
              " - Catalog may contain stale entries");
        }

        try {
          Logger::info("Performing no-data table deactivation maintenance");
          catalogManager.deactivateNoDataTables();
          Logger::info("No-data table deactivation maintenance completed");
        } catch (const std::exception &e) {
          Logger::error("ERROR in no-data table deactivation maintenance: " +
                        std::string(e.what()) +
                        " - Inactive tables may not be properly marked");
        }

        // Metrics
        try {
          Logger::info("Performing metrics collection maintenance");
          MetricsCollector metricsCollector;
          metricsCollector.collectAllMetrics();
          Logger::info("Metrics collection maintenance completed");
        } catch (const std::exception &e) {
          Logger::error("ERROR in metrics collection maintenance: " +
                        std::string(e.what()) +
                        " - System metrics may not be current");
        }

        auto cycleEndTime = std::chrono::high_resolution_clock::now();
        auto cycleDuration = std::chrono::duration_cast<std::chrono::seconds>(
            cycleEndTime - cycleStartTime);
        Logger::info("Periodic maintenance cycle completed successfully in " +
                     std::to_string(cycleDuration.count()) + " seconds");
      } catch (const std::exception &e) {
        Logger::error("CRITICAL ERROR in periodic maintenance cycle: " +
                      std::string(e.what()) +
                      " - Maintenance cycle completely failed");
      }

      std::this_thread::sleep_for(std::chrono::seconds(120));
    }
    Logger::info("Maintenance thread stopped");
  }

  void monitoringThread() {
    Logger::info("Monitoring thread started");
    while (running) {
      try {
        Logger::info("Starting monitoring cycle");
        auto monitoringStartTime = std::chrono::high_resolution_clock::now();

        pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());

        if (!pgConn.is_open()) {
          Logger::error(
              "CRITICAL ERROR: Cannot establish PostgreSQL connection for "
              "monitoring - system health cannot be monitored");
          std::this_thread::sleep_for(std::chrono::seconds(30));
          continue;
        }

        // Load configuration
        try {
          Logger::info("Loading configuration from database");
          loadConfigFromDatabase(pgConn);
        } catch (const std::exception &e) {
          Logger::error("ERROR loading configuration in monitoring cycle: " +
                        std::string(e.what()) +
                        " - Configuration may not be current");
        }

        // Report generation removed - using web dashboard instead
        Logger::info(
            "Monitoring cycle completed - using web dashboard for reporting");

        auto monitoringEndTime = std::chrono::high_resolution_clock::now();
        auto monitoringDuration =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                monitoringEndTime - monitoringStartTime);
        Logger::info("Monitoring cycle completed successfully in " +
                     std::to_string(monitoringDuration.count()) +
                     " milliseconds");
      } catch (const std::exception &e) {
        Logger::error(
            "CRITICAL ERROR in monitoring cycle: " + std::string(e.what()) +
            " - System monitoring completely failed");
      }

      std::this_thread::sleep_for(std::chrono::seconds(30));
    }
    Logger::info("Monitoring thread stopped");
  }
};

#endif // STREAMINGDATA_H