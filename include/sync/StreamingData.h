#ifndef STREAMINGDATA_H
#define STREAMINGDATA_H

#include "catalog/catalog_manager.h"
#include "core/Config.h"
#include "core/logger.h"
#include "governance/DataGovernance.h"
#include "governance/DataQuality.h"
#include "governance/MaintenanceManager.h"
#include "metrics/MetricsCollector.h"
#include "sync/APIToDatabaseSync.h"
#include "sync/CustomJobExecutor.h"
#include "sync/MSSQLToPostgres.h"
#include "sync/MariaDBToPostgres.h"
#include "sync/MongoDBToPostgres.h"
#include "sync/OracleToPostgres.h"
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <future>
#include <memory>
#include <mutex>
#include <pqxx/pqxx>
#include <thread>
#include <vector>

class StreamingData {
public:
  StreamingData();
  ~StreamingData();

  void initialize();
  void run();
  void shutdown();
  void executeJob(const std::string &jobName);

private:
  std::atomic<bool> running{true};
  std::vector<std::thread> threads;

  MariaDBToPostgres mariaToPg;
  MSSQLToPostgres mssqlToPg;
  MongoDBToPostgres mongoToPg;
  OracleToPostgres oracleToPg;
  APIToDatabaseSync apiToDb;
  std::unique_ptr<CustomJobExecutor> customJobExecutor;
  CatalogManager catalogManager;
  DataQuality dataQuality;

  void loadConfigFromDatabase(pqxx::connection &pgConn);
  void initializationThread();
  void catalogSyncThread();
  void mariaTransferThread();
  void mssqlTransferThread();
  void mongoTransferThread();
  void oracleTransferThread();
  void apiTransferThread();
  void customJobsSchedulerThread();
  void qualityThread();
  void maintenanceThread();
  void monitoringThread();
  void validateTablesForEngine(pqxx::connection &pgConn,
                               const std::string &dbEngine);

  void initializeDataGovernance();
  void initializeMetricsCollector();
  void initializeQueryStoreCollector();
  void initializeQueryActivityLogger();
  void initializeDatabaseTables();
  void performCatalogSyncs(std::vector<std::exception_ptr> &exceptions,
                           std::mutex &exceptionMutex);
  void processCatalogSyncExceptions(
      const std::vector<std::exception_ptr> &exceptions);
  void performCatalogMaintenance();
};

#endif
