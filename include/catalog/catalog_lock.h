#ifndef CATALOG_LOCK_H
#define CATALOG_LOCK_H

#include <chrono>
#include <pqxx/pqxx>
#include <string>

class CatalogLock {
  std::string connectionString_;
  std::string lockName_;
  std::string sessionId_;
  bool acquired_;
  int lockTimeoutSeconds_;

public:
  explicit CatalogLock(std::string connectionString, std::string lockName,
                       int lockTimeoutSeconds = 300);

  ~CatalogLock();

  CatalogLock(const CatalogLock &) = delete;
  CatalogLock &operator=(const CatalogLock &) = delete;
  CatalogLock(CatalogLock &&) = delete;
  CatalogLock &operator=(CatalogLock &&) = delete;

  bool tryAcquire(int maxWaitSeconds = 30);
  void release();
  bool isAcquired() const { return acquired_; }

private:
  std::string generateSessionId();
  std::string getHostname();
  void cleanExpiredLocks(pqxx::work &txn);
};

#endif
