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
            Logger::info("Updating chunk_size from " +
                         std::to_string(SyncConfig::getChunkSize()) + " to " +
                         std::to_string(newSize));
            SyncConfig::setChunkSize(newSize);
          }
        } else if (key == "sync_interval") {
          size_t newInterval = std::stoul(value);
          if (newInterval > 0 && newInterval != SyncConfig::getSyncInterval()) {
            Logger::info("Updating sync_interval from " +
                         std::to_string(SyncConfig::getSyncInterval()) +
                         " to " + std::to_string(newInterval));
            SyncConfig::setSyncInterval(newInterval);
          }
        }
      }
    } catch (const std::exception &e) {
      Logger::warning("Could not load configuration: " + std::string(e.what()));
    }
  }

  // Thread implementations
  void initializationThread() {
    try {
      Logger::info("Starting system initialization");

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

      Logger::info("Initialization completed");
    } catch (const std::exception &e) {
      Logger::error("Error: " + std::string(e.what()));
    }
  }

  void catalogSyncThread() {
    Logger::info("Catalog sync thread started");
    while (running) {
      try {
        Logger::info("Starting catalog synchronization cycle");

        // Launch all catalog syncs in parallel
        std::vector<std::thread> syncThreads;

        syncThreads.emplace_back(
            [this]() { catalogManager.syncCatalogMariaDBToPostgres(); });

        syncThreads.emplace_back(
            [this]() { catalogManager.syncCatalogMSSQLToPostgres(); });

        syncThreads.emplace_back(
            [this]() { catalogManager.syncCatalogPostgresToPostgres(); });

        // Wait for all sync threads to complete
        for (auto &thread : syncThreads) {
          thread.join();
        }

        catalogManager.cleanCatalog();

        catalogManager.deactivateNoDataTables();

        Logger::info("Catalog synchronization cycle completed successfully");
      } catch (const std::exception &e) {
        Logger::error("Error in catalog synchronization: " +
                      std::string(e.what()));
      }

      std::this_thread::sleep_for(std::chrono::seconds(30));
    }
    Logger::info("Catalog sync thread stopped");
  }

  void mariaTransferThread() {
    Logger::info("MariaDB transfer thread started");
    while (running) {
      try {
        Logger::info("Starting MariaDB transfer cycle");
        mariaToPg.transferDataMariaDBToPostgres();
        Logger::info("MariaDB transfer cycle completed successfully");
      } catch (const std::exception &e) {
        Logger::error("Error in MariaDB transfer: " + std::string(e.what()));
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
        Logger::info("Starting MSSQL transfer cycle");
        mssqlToPg.transferDataMSSQLToPostgres();
        Logger::info("MSSQL transfer cycle completed successfully");
      } catch (const std::exception &e) {
        Logger::error("Error in MSSQL transfer: " + std::string(e.what()));
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
        Logger::info("Starting PostgreSQL transfer cycle");
        pgToPg.transferDataPostgresToPostgres();
        Logger::info("PostgreSQL transfer cycle completed successfully");
      } catch (const std::exception &e) {
        Logger::error("Error in PostgreSQL transfer: " + std::string(e.what()));
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

        // Validate MariaDB tables
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

        // Validate MSSQL tables
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

        // Validate PostgreSQL tables
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

        Logger::info("Data quality validation cycle completed successfully");
      } catch (const std::exception &e) {
        Logger::error("Error in data quality validation: " +
                      std::string(e.what()));
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

        // Setup tables
        mariaToPg.setupTableTargetMariaDBToPostgres();

        // Sync catalogs
        catalogManager.syncCatalogMSSQLToPostgres();

        catalogManager.syncCatalogPostgresToPostgres();

        // Cleanup
        catalogManager.cleanCatalog();

        catalogManager.deactivateNoDataTables();

        // Metrics
        MetricsCollector metricsCollector;
        metricsCollector.collectAllMetrics();

        Logger::info("Periodic maintenance cycle completed successfully");
      } catch (const std::exception &e) {
        Logger::error("Error in periodic maintenance: " +
                      std::string(e.what()));
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

        pqxx::connection pgConn(DatabaseConfig::getPostgresConnectionString());

        // Load configuration
        loadConfigFromDatabase(pgConn);

        // Report generation removed - using web dashboard instead

        Logger::info("Monitoring cycle completed successfully");
      } catch (const std::exception &e) {
        Logger::error("Error in monitoring cycle: " + std::string(e.what()));
      }

      std::this_thread::sleep_for(std::chrono::seconds(30));
    }
    Logger::info("Monitoring thread stopped");
  }
};

#endif // STREAMINGDATA_H