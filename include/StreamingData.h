#ifndef STREAMINGDATA_H
#define STREAMINGDATA_H

#include "Config.h"
#include "DDLExporter.h"
#include "DataGovernance.h"
#include "DataQuality.h"
#include "MSSQLToPostgres.h"
#include "MariaDBToPostgres.h"
#include "MetricsCollector.h"
#include "MongoToPostgres.h"
#include "PostgresToPostgres.h"
#include "SyncReporter.h"
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
    Logger::info("StreamingData", "Initializing DataSync system components");

    // Initialize MongoDB
    Logger::info("StreamingData", "Initializing MongoDB driver");
    mongoc_init();
    Logger::info("StreamingData", "MongoDB driver initialized successfully");

    // Database connections will be created as needed
    Logger::info("StreamingData", "Database connections will be created as needed");

    Logger::info("StreamingData",
                 "System initialization completed successfully");
  }

  void run() {
    Logger::info("StreamingData", "Starting multi-threaded DataSync system");

    // Launch core threads
    Logger::info(
        "StreamingData",
        "Launching core threads (init, sync, monitor, quality, maintenance)");
    threads.emplace_back(&StreamingData::initializationThread, this);
    threads.emplace_back(&StreamingData::catalogSyncThread, this);
    threads.emplace_back(&StreamingData::monitoringThread, this);
    threads.emplace_back(&StreamingData::qualityThread, this);
    threads.emplace_back(&StreamingData::maintenanceThread, this);
    Logger::info("StreamingData", "Core threads launched successfully");

    // Wait for initialization to complete
    Logger::info("StreamingData",
                 "Waiting 60 seconds for initialization to complete");
    std::this_thread::sleep_for(std::chrono::seconds(60));

    // Launch transfer threads after initialization
    Logger::info(
        "StreamingData",
        "Launching transfer threads (MariaDB, MSSQL, PostgreSQL, MongoDB)");
    threads.emplace_back(&StreamingData::mariaTransferThread, this);
    threads.emplace_back(&StreamingData::mssqlTransferThread, this);
    threads.emplace_back(&StreamingData::postgresTransferThread, this);
    threads.emplace_back(&StreamingData::mongoTransferThread, this);
    Logger::info("StreamingData", "Transfer threads launched successfully");

    Logger::info("StreamingData",
                 "All threads launched successfully - System running");

    // Wait for all threads to complete
    Logger::info("StreamingData", "Waiting for all threads to complete");
    for (auto &thread : threads) {
      if (thread.joinable()) {
        thread.join();
      }
    }
    Logger::info("StreamingData", "All threads completed");
  }

  void shutdown() {
    Logger::info("StreamingData", "Shutting down DataSync system");
    running = false;

    // Wait for all threads to finish
    Logger::info("StreamingData", "Waiting for all threads to finish");
    for (auto &thread : threads) {
      if (thread.joinable()) {
        thread.join();
      }
    }
    Logger::info("StreamingData", "All threads finished successfully");

    // Database connections are cleaned up automatically

    // Cleanup MongoDB
    Logger::info("StreamingData", "Cleaning up MongoDB driver");
    mongoc_cleanup();

    Logger::info("StreamingData", "Shutdown completed successfully");
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
  MongoToPostgres mongoToPg;
  SyncReporter reporter;
  CatalogManager catalogManager;
  DataQuality dataQuality;

  void loadConfigFromDatabase(pqxx::connection &pgConn) {
    try {
      pqxx::work txn(pgConn);
      auto results =
          txn.exec("SELECT key, value FROM metadata.config WHERE key IN "
                   "('chunk_size', 'sync_interval');");
      txn.commit();

      for (const auto &row : results) {
        if (row.size() < 2)
          continue;
        std::string key = row[0].as<std::string>();
        std::string value = row[1].as<std::string>();

        if (key == "chunk_size") {
          size_t newSize = std::stoul(value);
          if (newSize > 0 && newSize != SyncConfig::getChunkSize()) {
            Logger::info("loadConfigFromDatabase",
                         "Updating chunk_size from " +
                             std::to_string(SyncConfig::getChunkSize()) +
                             " to " + std::to_string(newSize));
            SyncConfig::setChunkSize(newSize);
          }
        } else if (key == "sync_interval") {
          size_t newInterval = std::stoul(value);
          if (newInterval > 0 && newInterval != SyncConfig::getSyncInterval()) {
            Logger::info("loadConfigFromDatabase",
                         "Updating sync_interval from " +
                             std::to_string(SyncConfig::getSyncInterval()) +
                             " to " + std::to_string(newInterval));
            SyncConfig::setSyncInterval(newInterval);
          }
        }
      }
    } catch (const std::exception &e) {
      Logger::warning("loadConfigFromDatabase",
                      "Could not load configuration: " + std::string(e.what()));
    }
  }

  // Thread implementations
  void initializationThread() {
    try {
      Logger::info("initializationThread", "Starting system initialization");

      // DataGovernance
      DataGovernance dg;
      dg.initialize();
      dg.runDiscovery();
      dg.generateReport();

      // DDLExporter
      DDLExporter ddlExporter;
      ddlExporter.exportAllDDL();

      // MetricsCollector
      MetricsCollector metricsCollector;
      metricsCollector.collectAllMetrics();

      // Setup initial tables
      mariaToPg.setupTableTargetMariaDBToPostgres();
      mssqlToPg.setupTableTargetMSSQLToPostgres();
      pgToPg.setupTableTargetPostgresToPostgres();
      mongoToPg.setupTableTargetMongoToPostgres();

      Logger::info("initializationThread", "Initialization completed");
    } catch (const std::exception &e) {
      Logger::error("initializationThread", "Error: " + std::string(e.what()));
    }
  }

  void catalogSyncThread() {
    Logger::info("catalogSyncThread", "Catalog sync thread started");
    while (running) {
      try {
        Logger::info("catalogSyncThread",
                     "Starting catalog synchronization cycle");

        Logger::debug("catalogSyncThread", "Syncing MariaDB catalog");
        catalogManager.syncCatalogMariaDBToPostgres();
        Logger::debug("catalogSyncThread", "MariaDB catalog sync completed");

        Logger::debug("catalogSyncThread", "Syncing MSSQL catalog");
        catalogManager.syncCatalogMSSQLToPostgres();
        Logger::debug("catalogSyncThread", "MSSQL catalog sync completed");

        Logger::debug("catalogSyncThread", "Syncing PostgreSQL catalog");
        catalogManager.syncCatalogPostgresToPostgres();
        Logger::debug("catalogSyncThread", "PostgreSQL catalog sync completed");

        Logger::debug("catalogSyncThread", "Syncing MongoDB catalog");
        catalogManager.syncCatalogMongoToPostgres();
        Logger::debug("catalogSyncThread", "MongoDB catalog sync completed");

        Logger::debug("catalogSyncThread", "Cleaning catalog");
        catalogManager.cleanCatalog();
        Logger::debug("catalogSyncThread", "Catalog cleanup completed");

        Logger::debug("catalogSyncThread", "Deactivating NO_DATA tables");
        catalogManager.deactivateNoDataTables();
        Logger::debug("catalogSyncThread",
                      "NO_DATA tables deactivation completed");

        Logger::info("catalogSyncThread",
                     "Catalog synchronization cycle completed successfully");
      } catch (const std::exception &e) {
        Logger::error("catalogSyncThread",
                      "Error in catalog synchronization: " +
                          std::string(e.what()));
      }

      Logger::debug("catalogSyncThread", "Sleeping for 30 seconds");
      std::this_thread::sleep_for(std::chrono::seconds(30));
    }
    Logger::info("catalogSyncThread", "Catalog sync thread stopped");
  }

  void mariaTransferThread() {
    Logger::info("mariaTransferThread", "MariaDB transfer thread started");
    while (running) {
      try {
        Logger::info("mariaTransferThread", "Starting MariaDB transfer cycle");
        mariaToPg.transferDataMariaDBToPostgres();
        Logger::info("mariaTransferThread",
                     "MariaDB transfer cycle completed successfully");
      } catch (const std::exception &e) {
        Logger::error("mariaTransferThread",
                      "Error in MariaDB transfer: " + std::string(e.what()));
      }

      Logger::debug("mariaTransferThread",
                    "Sleeping for " +
                        std::to_string(SyncConfig::getSyncInterval()) +
                        " seconds");
      std::this_thread::sleep_for(
          std::chrono::seconds(SyncConfig::getSyncInterval()));
    }
    Logger::info("mariaTransferThread", "MariaDB transfer thread stopped");
  }

  void mssqlTransferThread() {
    Logger::info("mssqlTransferThread", "MSSQL transfer thread started");
    while (running) {
      try {
        Logger::info("mssqlTransferThread", "Starting MSSQL transfer cycle");
        mssqlToPg.transferDataMSSQLToPostgres();
        Logger::info("mssqlTransferThread",
                     "MSSQL transfer cycle completed successfully");
      } catch (const std::exception &e) {
        Logger::error("mssqlTransferThread",
                      "Error in MSSQL transfer: " + std::string(e.what()));
      }

      Logger::debug("mssqlTransferThread",
                    "Sleeping for " +
                        std::to_string(SyncConfig::getSyncInterval()) +
                        " seconds");
      std::this_thread::sleep_for(
          std::chrono::seconds(SyncConfig::getSyncInterval()));
    }
    Logger::info("mssqlTransferThread", "MSSQL transfer thread stopped");
  }

  void postgresTransferThread() {
    Logger::info("postgresTransferThread",
                 "PostgreSQL transfer thread started");
    while (running) {
      try {
        Logger::info("postgresTransferThread",
                     "Starting PostgreSQL transfer cycle");
        pgToPg.transferDataPostgresToPostgres();
        Logger::info("postgresTransferThread",
                     "PostgreSQL transfer cycle completed successfully");
      } catch (const std::exception &e) {
        Logger::error("postgresTransferThread",
                      "Error in PostgreSQL transfer: " + std::string(e.what()));
      }

      Logger::debug("postgresTransferThread",
                    "Sleeping for " +
                        std::to_string(SyncConfig::getSyncInterval()) +
                        " seconds");
      std::this_thread::sleep_for(
          std::chrono::seconds(SyncConfig::getSyncInterval()));
    }
    Logger::info("postgresTransferThread",
                 "PostgreSQL transfer thread stopped");
  }

  void mongoTransferThread() {
    Logger::info("mongoTransferThread", "MongoDB transfer thread started");
    while (running) {
      try {
        Logger::info("mongoTransferThread", "Starting MongoDB transfer cycle");
        mongoToPg.transferDataMongoToPostgres();
        Logger::info("mongoTransferThread",
                     "MongoDB transfer cycle completed successfully");
      } catch (const std::exception &e) {
        Logger::error("mongoTransferThread",
                      "Error in MongoDB transfer: " + std::string(e.what()));
      }

      Logger::debug("mongoTransferThread",
                    "Sleeping for " +
                        std::to_string(SyncConfig::getSyncInterval()) +
                        " seconds");
      std::this_thread::sleep_for(
          std::chrono::seconds(SyncConfig::getSyncInterval()));
    }
    Logger::info("mongoTransferThread", "MongoDB transfer thread stopped");
  }

  void qualityThread() {
    Logger::info("qualityThread", "Data quality thread started");
    while (running) {
      try {
        Logger::info("qualityThread", "Starting data quality validation cycle");

        pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());

        // Validate MariaDB tables
        Logger::debug("qualityThread", "Validating MariaDB tables");
        {
          pqxx::work txn(pgConn);
          auto mariaTables = txn.exec(
              "SELECT schema_name, table_name FROM metadata.catalog WHERE "
              "db_engine = 'MariaDB' AND status = 'PERFECT_MATCH'");
          txn.commit();

          for (const auto &row : mariaTables) {
            std::string schema = row[0].as<std::string>();
            std::string table = row[1].as<std::string>();
            dataQuality.validateTable(pgConn, schema, table, "MariaDB");
          }
        }
        Logger::debug("qualityThread", "MariaDB tables validation completed");

        // Validate MSSQL tables
        Logger::debug("qualityThread", "Validating MSSQL tables");
        {
          pqxx::work txn(pgConn);
          auto mssqlTables = txn.exec(
              "SELECT schema_name, table_name FROM metadata.catalog "
              "WHERE db_engine = 'MSSQL' AND status = 'PERFECT_MATCH'");
          txn.commit();

          for (const auto &row : mssqlTables) {
            std::string schema = row[0].as<std::string>();
            std::string table = row[1].as<std::string>();
            dataQuality.validateTable(pgConn, schema, table, "MSSQL");
          }
        }
        Logger::debug("qualityThread", "MSSQL tables validation completed");

        // Validate PostgreSQL tables
        Logger::debug("qualityThread", "Validating PostgreSQL tables");
        {
          pqxx::work txn(pgConn);
          auto pgTables = txn.exec(
              "SELECT schema_name, table_name FROM metadata.catalog WHERE "
              "db_engine = 'PostgreSQL' AND status = 'PERFECT_MATCH'");
          txn.commit();

          for (const auto &row : pgTables) {
            std::string schema = row[0].as<std::string>();
            std::string table = row[1].as<std::string>();
            dataQuality.validateTable(pgConn, schema, table, "PostgreSQL");
          }
        }
        Logger::debug("qualityThread",
                      "PostgreSQL tables validation completed");

        // Validate MongoDB tables
        Logger::debug("qualityThread", "Validating MongoDB tables");
        {
          pqxx::work txn(pgConn);
          auto mongoTables = txn.exec(
              "SELECT schema_name, table_name FROM metadata.catalog WHERE "
              "db_engine = 'MongoDB' AND status = 'PERFECT_MATCH'");
          txn.commit();

          for (const auto &row : mongoTables) {
            std::string schema = row[0].as<std::string>();
            std::string table = row[1].as<std::string>();
            dataQuality.validateTable(pgConn, schema, table, "MongoDB");
          }
        }
        Logger::debug("qualityThread", "MongoDB tables validation completed");

        Logger::info("qualityThread",
                     "Data quality validation cycle completed successfully");
      } catch (const std::exception &e) {
        Logger::error("qualityThread", "Error in data quality validation: " +
                                           std::string(e.what()));
      }

      Logger::debug("qualityThread", "Sleeping for 60 seconds");
      std::this_thread::sleep_for(std::chrono::seconds(60));
    }
    Logger::info("qualityThread", "Data quality thread stopped");
  }

  void maintenanceThread() {
    Logger::info("maintenanceThread", "Maintenance thread started");
    while (running) {
      try {
        Logger::info("maintenanceThread",
                     "Starting periodic maintenance cycle");

        // Setup tables
        Logger::debug("maintenanceThread", "Setting up MariaDB target tables");
        mariaToPg.setupTableTargetMariaDBToPostgres();
        Logger::debug("maintenanceThread",
                      "MariaDB target tables setup completed");

        // Sync catalogs
        Logger::debug("maintenanceThread", "Syncing MSSQL catalog");
        catalogManager.syncCatalogMSSQLToPostgres();
        Logger::debug("maintenanceThread", "MSSQL catalog sync completed");

        Logger::debug("maintenanceThread", "Syncing PostgreSQL catalog");
        catalogManager.syncCatalogPostgresToPostgres();
        Logger::debug("maintenanceThread", "PostgreSQL catalog sync completed");

        Logger::debug("maintenanceThread", "Syncing MongoDB catalog");
        catalogManager.syncCatalogMongoToPostgres();
        Logger::debug("maintenanceThread", "MongoDB catalog sync completed");

        // Cleanup
        Logger::debug("maintenanceThread", "Cleaning catalog");
        catalogManager.cleanCatalog();
        Logger::debug("maintenanceThread", "Catalog cleanup completed");

        Logger::debug("maintenanceThread", "Deactivating NO_DATA tables");
        catalogManager.deactivateNoDataTables();
        Logger::debug("maintenanceThread",
                      "NO_DATA tables deactivation completed");

        // Metrics
        Logger::debug("maintenanceThread", "Collecting system metrics");
        MetricsCollector metricsCollector;
        metricsCollector.collectAllMetrics();
        Logger::debug("maintenanceThread",
                      "System metrics collection completed");

        Logger::info("maintenanceThread",
                     "Periodic maintenance cycle completed successfully");
      } catch (const std::exception &e) {
        Logger::error("maintenanceThread", "Error in periodic maintenance: " +
                                               std::string(e.what()));
      }

      Logger::debug("maintenanceThread", "Sleeping for 120 seconds");
      std::this_thread::sleep_for(std::chrono::seconds(120));
    }
    Logger::info("maintenanceThread", "Maintenance thread stopped");
  }

  void monitoringThread() {
    Logger::info("monitoringThread", "Monitoring thread started");
    while (running) {
      try {
        Logger::info("monitoringThread", "Starting monitoring cycle");

        pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());

        // Load configuration
        Logger::debug("monitoringThread",
                      "Loading configuration from database");
        loadConfigFromDatabase(pgConn);
        Logger::debug("monitoringThread", "Configuration loaded successfully");

        // Generate report
        Logger::debug("monitoringThread", "Generating full report");
        reporter.generateFullReport(pgConn);
        Logger::debug("monitoringThread", "Full report generated successfully");

        Logger::info("monitoringThread",
                     "Monitoring cycle completed successfully");
      } catch (const std::exception &e) {
        Logger::error("monitoringThread",
                      "Error in monitoring cycle: " + std::string(e.what()));
      }

      Logger::debug("monitoringThread", "Sleeping for 30 seconds");
      std::this_thread::sleep_for(std::chrono::seconds(30));
    }
    Logger::info("monitoringThread", "Monitoring thread stopped");
  }
};

#endif // STREAMINGDATA_H