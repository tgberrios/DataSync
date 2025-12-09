#ifndef STREAMINGDATA_H
#define STREAMINGDATA_H

#include "catalog/catalog_manager.h"
#include "core/Config.h"
#include "core/logger.h"
#include "governance/DataGovernance.h"
#include "governance/DataQuality.h"
#include "metrics/MetricsCollector.h"
#include "sync/MSSQLToPostgres.h"
#include "sync/MariaDBToPostgres.h"
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <future>
#include <mutex>
#include <pqxx/pqxx>
#include <thread>
#include <vector>

class StreamingData {
public:
  StreamingData() = default;
  ~StreamingData();

  void initialize();
  void run();
  void shutdown();

private:
  std::atomic<bool> running{true};
  std::vector<std::thread> threads;
  std::mutex configMutex;
  std::condition_variable configCV;

  MariaDBToPostgres mariaToPg;
  MSSQLToPostgres mssqlToPg;
  CatalogManager catalogManager;
  DataQuality dataQuality;

  void loadConfigFromDatabase(pqxx::connection &pgConn);
  void initializationThread();
  void catalogSyncThread();
  void mariaTransferThread();
  void mssqlTransferThread();
  void qualityThread();
  void maintenanceThread();
  void monitoringThread();
  void validateTablesForEngine(pqxx::connection &pgConn,
                               const std::string &dbEngine);
};

#endif
