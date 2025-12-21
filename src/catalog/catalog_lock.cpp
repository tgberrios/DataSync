#include "catalog/catalog_lock.h"
#include "core/logger.h"
#include <cerrno>
#include <chrono>
#include <iomanip>
#include <random>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <unistd.h>

namespace {
constexpr int DEFAULT_LOCK_RETRY_SLEEP_MS = 500;
constexpr int MIN_LOCK_RETRY_SLEEP_MS = 100;
constexpr int MAX_LOCK_RETRY_SLEEP_MS = 10000;
constexpr int MIN_MAX_WAIT_SECONDS = 1;
constexpr int MAX_MAX_WAIT_SECONDS = 3600;
constexpr int MIN_LOCK_TIMEOUT_SECONDS = 1;
constexpr int MAX_LOCK_TIMEOUT_SECONDS = 3600;
} // namespace

int CatalogLock::getRetrySleepMs(pqxx::work &txn) {
  try {
    auto result = txn.exec(
        "SELECT value FROM metadata.config WHERE key = 'lock_retry_sleep_ms'");
    if (!result.empty()) {
      std::string valueStr = result[0][0].as<std::string>();
      int value = std::stoi(valueStr);
      if (value >= MIN_LOCK_RETRY_SLEEP_MS &&
          value <= MAX_LOCK_RETRY_SLEEP_MS) {
        return value;
      }
      Logger::error(LogCategory::DATABASE, "CatalogLock",
                    "Invalid lock_retry_sleep_ms value from config: " +
                        valueStr + ", using default: " +
                        std::to_string(DEFAULT_LOCK_RETRY_SLEEP_MS));
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "CatalogLock",
                  "Error reading lock_retry_sleep_ms from config: " +
                      std::string(e.what()) + ", using default: " +
                      std::to_string(DEFAULT_LOCK_RETRY_SLEEP_MS));
  }
  return DEFAULT_LOCK_RETRY_SLEEP_MS;
}

CatalogLock::CatalogLock(std::string connectionString, std::string lockName,
                         int lockTimeoutSeconds)
    : connectionString_(std::move(connectionString)),
      lockName_(std::move(lockName)), sessionId_(generateSessionId()),
      acquired_(false), lockTimeoutSeconds_(lockTimeoutSeconds) {
  if (lockTimeoutSeconds_ < MIN_LOCK_TIMEOUT_SECONDS ||
      lockTimeoutSeconds_ > MAX_LOCK_TIMEOUT_SECONDS) {
    Logger::error(
        LogCategory::DATABASE, "CatalogLock",
        "Invalid lockTimeoutSeconds: " + std::to_string(lockTimeoutSeconds_) +
            ", must be between " + std::to_string(MIN_LOCK_TIMEOUT_SECONDS) +
            " and " + std::to_string(MAX_LOCK_TIMEOUT_SECONDS));
  }
}

CatalogLock::~CatalogLock() {
  if (acquired_) {
    try {
      release();
    } catch (const std::exception &e) {
      Logger::error(
          LogCategory::DATABASE, "CatalogLock",
          "Error releasing lock in destructor for lock: " + lockName_ +
              ", session: " + sessionId_ + ", error: " + std::string(e.what()));
    }
  }
}

bool CatalogLock::tryAcquire(int maxWaitSeconds) {
  if (maxWaitSeconds < MIN_MAX_WAIT_SECONDS ||
      maxWaitSeconds > MAX_MAX_WAIT_SECONDS) {
    Logger::error(LogCategory::DATABASE, "CatalogLock",
                  "Invalid maxWaitSeconds: " + std::to_string(maxWaitSeconds) +
                      ", must be between " +
                      std::to_string(MIN_MAX_WAIT_SECONDS) + " and " +
                      std::to_string(MAX_MAX_WAIT_SECONDS));
    return false;
  }
  if (lockTimeoutSeconds_ < MIN_LOCK_TIMEOUT_SECONDS ||
      lockTimeoutSeconds_ > MAX_LOCK_TIMEOUT_SECONDS) {
    Logger::error(
        LogCategory::DATABASE, "CatalogLock",
        "Invalid lockTimeoutSeconds: " + std::to_string(lockTimeoutSeconds_) +
            ", must be between " + std::to_string(MIN_LOCK_TIMEOUT_SECONDS) +
            " and " + std::to_string(MAX_LOCK_TIMEOUT_SECONDS));
    return false;
  }

  auto startTime = std::chrono::steady_clock::now();
  std::string hostname = getHostname();
  int retrySleepMs = DEFAULT_LOCK_RETRY_SLEEP_MS;

  Logger::info(LogCategory::DATABASE, "CatalogLock",
               "Attempting to acquire lock: " + lockName_ +
                   ", maxWaitSeconds: " + std::to_string(maxWaitSeconds) +
                   ", sessionId: " + sessionId_);

  while (true) {
    try {
      pqxx::connection conn(connectionString_);
      if (!conn.is_open()) {
        Logger::error(LogCategory::DATABASE, "CatalogLock",
                      "Database connection failed for lock: " + lockName_);
        auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                           std::chrono::steady_clock::now() - startTime)
                           .count();
        if (elapsed >= maxWaitSeconds) {
          Logger::error(LogCategory::DATABASE, "CatalogLock",
                        "Failed to acquire lock after " +
                            std::to_string(elapsed) +
                            " seconds due to connection errors: " + lockName_);
          return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(retrySleepMs));
        continue;
      }

      pqxx::work txn(conn);
      retrySleepMs = getRetrySleepMs(txn);

      cleanExpiredLocks(txn);

      auto expiresAt = std::chrono::system_clock::now() +
                       std::chrono::seconds(lockTimeoutSeconds_);
      auto time_t_expires = std::chrono::system_clock::to_time_t(expiresAt);
      std::tm tm_expires;
      if (localtime_r(&time_t_expires, &tm_expires) == nullptr) {
        Logger::error(
            LogCategory::DATABASE, "CatalogLock",
            "Error converting expiration time to local time for lock: " +
                lockName_);
        txn.commit();
        auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                           std::chrono::steady_clock::now() - startTime)
                           .count();
        if (elapsed >= maxWaitSeconds) {
          Logger::error(LogCategory::DATABASE, "CatalogLock",
                        "Failed to acquire lock after " +
                            std::to_string(elapsed) + " seconds: " + lockName_);
          return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(retrySleepMs));
        continue;
      }

      std::ostringstream oss;
      oss << std::put_time(&tm_expires, "%Y-%m-%d %H:%M:%S");
      std::string expiresAtStr = oss.str();

      auto result = txn.exec_params(
          "INSERT INTO metadata.catalog_locks (lock_name, acquired_by, "
          "expires_at, session_id) "
          "SELECT $1::text, $2::text, $3::timestamp, $4::text "
          "WHERE NOT EXISTS (SELECT 1 FROM metadata.catalog_locks WHERE "
          "lock_name = $1::text AND expires_at >= NOW()) "
          "RETURNING lock_name",
          lockName_, hostname, expiresAtStr, sessionId_);

      if (!result.empty()) {
        txn.commit();
        acquired_ = true;
        Logger::info(
            LogCategory::DATABASE, "CatalogLock",
            "Acquired lock: " + lockName_ + ", sessionId: " + sessionId_ +
                ", hostname: " + hostname + ", expiresAt: " + expiresAtStr);
        return true;
      }

      txn.commit();

      auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                         std::chrono::steady_clock::now() - startTime)
                         .count();
      if (elapsed >= maxWaitSeconds) {
        Logger::error(LogCategory::DATABASE, "CatalogLock",
                      "Failed to acquire lock after " +
                          std::to_string(elapsed) + " seconds: " + lockName_ +
                          ", sessionId: " + sessionId_);
        return false;
      }

      std::this_thread::sleep_for(std::chrono::milliseconds(retrySleepMs));

    } catch (const std::exception &e) {
      Logger::error(LogCategory::DATABASE, "CatalogLock",
                    "Error trying to acquire lock: " + lockName_ +
                        ", sessionId: " + sessionId_ +
                        ", error: " + std::string(e.what()));

      auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                         std::chrono::steady_clock::now() - startTime)
                         .count();
      if (elapsed >= maxWaitSeconds) {
        Logger::error(LogCategory::DATABASE, "CatalogLock",
                      "Failed to acquire lock after " +
                          std::to_string(elapsed) + " seconds due to errors: " +
                          lockName_ + ", sessionId: " + sessionId_);
        return false;
      }

      std::this_thread::sleep_for(std::chrono::milliseconds(retrySleepMs));
    }
  }
}

void CatalogLock::release() {
  if (!acquired_) {
    Logger::error(LogCategory::DATABASE, "CatalogLock",
                  "Attempted to release lock that was not acquired: " +
                      lockName_ + ", sessionId: " + sessionId_);
    return;
  }

  try {
    pqxx::connection conn(connectionString_);
    if (!conn.is_open()) {
      Logger::error(LogCategory::DATABASE, "CatalogLock",
                    "Database connection failed when releasing lock: " +
                        lockName_ + ", sessionId: " + sessionId_);
      throw std::runtime_error("Database connection failed");
    }

    pqxx::work txn(conn);

    auto result =
        txn.exec_params("DELETE FROM metadata.catalog_locks "
                        "WHERE lock_name = $1::text AND session_id = $2::text",
                        lockName_, sessionId_);

    if (result.affected_rows() == 0) {
      Logger::error(LogCategory::DATABASE, "CatalogLock",
                    "Lock not found when releasing: " + lockName_ +
                        ", sessionId: " + sessionId_ +
                        ". Lock may have already been released or expired.");
    }

    txn.commit();
    acquired_ = false;
    Logger::info(LogCategory::DATABASE, "CatalogLock",
                 "Released lock: " + lockName_ + ", sessionId: " + sessionId_);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "CatalogLock",
                  "Error releasing lock: " + lockName_ + ", sessionId: " +
                      sessionId_ + ", error: " + std::string(e.what()));
    throw;
  }
}

std::string CatalogLock::generateSessionId() {
  try {
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<uint64_t> dis;

    uint64_t random_value = dis(gen);
    std::ostringstream oss;
    oss << std::hex << random_value;
    return oss.str();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "CatalogLock",
                  "Error generating session ID: " + std::string(e.what()));
    auto fallback = std::chrono::system_clock::now().time_since_epoch().count();
    return std::to_string(fallback);
  }
}

std::string CatalogLock::getHostname() {
  char hostname[256];
  hostname[255] = '\0';
  if (gethostname(hostname, sizeof(hostname) - 1) == 0) {
    hostname[255] = '\0';
    std::string hostnameStr(hostname);
    if (hostnameStr.empty()) {
      Logger::error(LogCategory::DATABASE, "CatalogLock",
                    "Empty hostname retrieved, using 'unknown'");
      return "unknown";
    }
    return hostnameStr;
  }
  Logger::error(LogCategory::DATABASE, "CatalogLock",
                "Failed to retrieve hostname, error code: " +
                    std::to_string(errno) + ", using 'unknown'");
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
    Logger::error(LogCategory::DATABASE, "CatalogLock",
                  "Error cleaning expired locks: " + std::string(e.what()) +
                      ". Lock acquisition may fail if stale locks remain.");
  }
}
