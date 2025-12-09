#include "catalog/catalog_lock.h"
#include "core/logger.h"
#include <chrono>
#include <iomanip>
#include <random>
#include <sstream>
#include <thread>
#include <unistd.h>

// Constructor for CatalogLock. Initializes the lock with a connection string,
// lock name, and timeout duration. Automatically generates a unique session ID
// for this lock instance. The lock starts in a non-acquired state and must be
// explicitly acquired using tryAcquire(). The lockTimeoutSeconds parameter
// determines how long the lock will be held once acquired before expiring.
CatalogLock::CatalogLock(std::string connectionString, std::string lockName,
                         int lockTimeoutSeconds)
    : connectionString_(std::move(connectionString)),
      lockName_(std::move(lockName)), sessionId_(generateSessionId()),
      acquired_(false), lockTimeoutSeconds_(lockTimeoutSeconds) {}

// Destructor for CatalogLock. Automatically releases the lock if it was
// acquired and not previously released. This ensures that locks are always
// cleaned up even if release() is not explicitly called, preventing lock leaks.
// Errors during release in the destructor are logged but do not throw
// exceptions.
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

// Attempts to acquire the lock with a maximum wait time. This function uses
// a database-backed locking mechanism stored in metadata.catalog_locks table.
// It first cleans expired locks, then attempts to insert a new lock entry.
// If the lock is already held by another instance, it retries every 500ms
// until either the lock is acquired or maxWaitSeconds is reached. The lock
// expires after lockTimeoutSeconds from the time of acquisition. Returns true
// if the lock was successfully acquired, false if the maximum wait time was
// exceeded or an error occurred. If the lock cannot be acquired within
// maxWaitSeconds, the function returns false and logs a warning. If the lock
// is acquired but the operation fails, the destructor automatically releases
// the lock to prevent lock leaks. If the operation is successful, the lock
// should be explicitly released using release() or it will be automatically
// released when the CatalogLock object is destroyed.
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

// Releases the lock if it was previously acquired. This function removes the
// lock entry from the database using both the lock name and session ID to
// ensure only the owning instance can release its own lock. If the lock was
// not acquired, the function returns immediately without performing any
// operations. Throws an exception if an error occurs during release.
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

// Generates a unique session ID for this lock instance using a
// cryptographically secure random number generator. The session ID is a
// hexadecimal string that uniquely identifies this lock instance, allowing
// multiple locks with the same name to be distinguished and ensuring only the
// owning instance can release its lock. This prevents accidental lock releases
// by other processes.
std::string CatalogLock::generateSessionId() {
  std::random_device rd;
  std::mt19937_64 gen(rd());
  std::uniform_int_distribution<uint64_t> dis;

  uint64_t random_value = dis(gen);
  std::ostringstream oss;
  oss << std::hex << random_value;
  return oss.str();
}

// Retrieves the hostname of the current machine using the system gethostname()
// function. This hostname is stored with the lock entry to identify which
// machine/instance acquired the lock, which is useful for debugging and
// monitoring. Returns "unknown" if the hostname cannot be retrieved.
std::string CatalogLock::getHostname() {
  char hostname[256];
  if (gethostname(hostname, sizeof(hostname)) == 0) {
    return std::string(hostname);
  }
  return "unknown";
}

// Cleans expired locks from the metadata.catalog_locks table. This function
// removes all lock entries where the expires_at timestamp is in the past.
// It is called automatically before attempting to acquire a lock to ensure
// stale locks don't prevent new lock acquisitions. Errors during cleanup are
// logged as warnings but do not prevent the lock acquisition process from
// continuing. The function logs the number of expired locks cleaned if any
// were found.
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
