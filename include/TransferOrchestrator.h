#ifndef TRANSFERORCHESTRATOR_H
#define TRANSFERORCHESTRATOR_H

#include "Config.h"
#include "logger.h"
#include <atomic>
#include <memory>

// Forward declarations
class MSSQLToPostgres;
class MariaDBToPostgres;
class PostgresToPostgres;
class CatalogManager;

class TransferOrchestrator {
public:
  TransferOrchestrator();
  ~TransferOrchestrator();

  // Main orchestration
  void initialize();
  void runTransfers();
  void stop();

  // Individual transfers
  void runMariaDBTransfer();
  void runMSSQLTransfer();
  void runPostgreSQLTransfer();

  // Setup operations
  void setupAllTables();
  void setupMariaDBTables();
  void setupMSSQLTables();
  void setupPostgreSQLTables();

  // Status
  bool isRunning() const;

private:
  std::atomic<bool> running{true};

  // Transfer components
  std::unique_ptr<MariaDBToPostgres> mariaToPg;
  std::unique_ptr<MSSQLToPostgres> mssqlToPg;
  std::unique_ptr<PostgresToPostgres> pgToPg;
  std::unique_ptr<CatalogManager> catalogManager;

  // Helper methods
  void logTransferStart(const std::string &engine);
  void logTransferComplete(const std::string &engine, int durationSeconds);
  void logTransferError(const std::string &engine, const std::string &error);
  void sleepBetweenTransfers();
};

#endif // TRANSFERORCHESTRATOR_H
