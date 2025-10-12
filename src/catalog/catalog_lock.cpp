#include "catalog/catalog_lock.h"
#include "core/logger.h"
#include <chrono>
#include <iomanip>
#include <random>
#include <sstream>
#include <thread>
#include <unistd.h>

CatalogLock::CatalogLock(std::string connectionString, std::string lockName,
                         int lockTimeoutSeconds)
    : connectionString_(std::move(connectionString)),
      lockName_(std::move(lockName)), sessionId_(generateSessionId()),
      acquired_(false), lockTimeoutSeconds_(lockTimeoutSeconds) {}

CatalogLock::~CatalogLock() {
  if (acquired_) {
    try {
      release();
    } catch (const std::exception &e) {
      Logger::error(LogCategory::DATABASE, "CatalogLock",
                    "Error releasing lock in destructor: " +
                        std::string(e.what()));
    }
  }
}

bool CatalogLock::tryAcquire(int maxWaitSeconds) {
  auto startTime = std::chrono::steady_clock::now();
  std::string hostname = getHostname();

  while (true) {
    try {
      pqxx::connection conn(connectionString_);
      pqxx::work txn(conn);

      cleanExpiredLocks(txn);

      auto expiresAt = std::chrono::system_clock::now() +
                       std::chrono::seconds(lockTimeoutSeconds_);
      auto time_t_expires = std::chrono::system_clock::to_time_t(expiresAt);
      std::tm tm_expires;
      localtime_r(&time_t_expires, &tm_expires);
      std::ostringstream oss;
      oss << std::put_time(&tm_expires, "%Y-%m-%d %H:%M:%S");

      auto result = txn.exec_params(
          "INSERT INTO metadata.catalog_locks (lock_name, acquired_by, "
          "expires_at, session_id) "
          "VALUES ($1, $2, $3::timestamp, $4) "
          "ON CONFLICT (lock_name) DO NOTHING "
          "RETURNING lock_name",
          lockName_, hostname, oss.str(), sessionId_);

      if (!result.empty()) {
        txn.commit();
        acquired_ = true;
        Logger::info(LogCategory::DATABASE, "CatalogLock",
                     "Acquired lock: " + lockName_);
        return true;
      }

      txn.commit();

      auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                         std::chrono::steady_clock::now() - startTime)
                         .count();
      if (elapsed >= maxWaitSeconds) {
        Logger::warning(LogCategory::DATABASE, "CatalogLock",
                        "Failed to acquire lock after " +
                            std::to_string(elapsed) + " seconds: " + lockName_);
        return false;
      }

      std::this_thread::sleep_for(std::chrono::milliseconds(500));

    } catch (const std::exception &e) {
      Logger::error(LogCategory::DATABASE, "CatalogLock",
                    "Error trying to acquire lock: " + std::string(e.what()));

      auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                         std::chrono::steady_clock::now() - startTime)
                         .count();
      if (elapsed >= maxWaitSeconds) {
        return false;
      }

      std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
  }
}

void CatalogLock::release() {
  if (!acquired_) {
    return;
  }

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    txn.exec_params("DELETE FROM metadata.catalog_locks "
                    "WHERE lock_name = $1 AND session_id = $2",
                    lockName_, sessionId_);

    txn.commit();
    acquired_ = false;
    Logger::info(LogCategory::DATABASE, "CatalogLock",
                 "Released lock: " + lockName_);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "CatalogLock",
                  "Error releasing lock: " + std::string(e.what()));
    throw;
  }
}

std::string CatalogLock::generateSessionId() {
  std::random_device rd;
  std::mt19937_64 gen(rd());
  std::uniform_int_distribution<uint64_t> dis;

  uint64_t random_value = dis(gen);
  std::ostringstream oss;
  oss << std::hex << random_value;
  return oss.str();
}

std::string CatalogLock::getHostname() {
  char hostname[256];
  if (gethostname(hostname, sizeof(hostname)) == 0) {
    return std::string(hostname);
  }
  return "unknown";
}

void CatalogLock::cleanExpiredLocks(pqxx::work &txn) {
  try {
    auto result = txn.exec("DELETE FROM metadata.catalog_locks "
                           "WHERE expires_at < NOW()");

    if (result.affected_rows() > 0) {
      Logger::info(LogCategory::DATABASE, "CatalogLock",
                   "Cleaned " + std::to_string(result.affected_rows()) +
                       " expired locks");
    }
  } catch (const std::exception &e) {
    Logger::warning(LogCategory::DATABASE, "CatalogLock",
                    "Error cleaning expired locks: " + std::string(e.what()));
  }
}
