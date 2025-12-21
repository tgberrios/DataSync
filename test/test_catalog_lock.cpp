#include "catalog/catalog_lock.h"
#include "core/logger.h"
#include <atomic>
#include <chrono>
#include <functional>
#include <iostream>
#include <pqxx/pqxx>
#include <sstream>
#include <thread>
#include <vector>

class TestRunner {
private:
  int testsPassed = 0;
  int testsFailed = 0;
  std::string currentTest = "";

public:
  void assertTrue(bool condition, const std::string &message) {
    if (!condition) {
      std::cerr << "  [FAIL] " << currentTest << ": " << message << std::endl;
      testsFailed++;
      return;
    }
    testsPassed++;
  }

  void assertFalse(bool condition, const std::string &message) {
    assertTrue(!condition, message);
  }

  void assertEquals(const std::string &expected, const std::string &actual,
                    const std::string &message) {
    if (expected != actual) {
      std::cerr << "  [FAIL] " << currentTest << ": " << message << std::endl;
      std::cerr << "    Expected: '" << expected << "'" << std::endl;
      std::cerr << "    Actual: '" << actual << "'" << std::endl;
      testsFailed++;
      return;
    }
    testsPassed++;
  }

  void assertEquals(int expected, int actual, const std::string &message) {
    if (expected != actual) {
      std::cerr << "  [FAIL] " << currentTest << ": " << message << std::endl;
      std::cerr << "    Expected: " << expected << std::endl;
      std::cerr << "    Actual: " << actual << std::endl;
      testsFailed++;
      return;
    }
    testsPassed++;
  }

  void assertEquals(bool expected, bool actual, const std::string &message) {
    if (expected != actual) {
      std::cerr << "  [FAIL] " << currentTest << ": " << message << std::endl;
      std::cerr << "    Expected: " << (expected ? "true" : "false")
                << std::endl;
      std::cerr << "    Actual: " << (actual ? "true" : "false") << std::endl;
      testsFailed++;
      return;
    }
    testsPassed++;
  }

  void assertNotEmpty(const std::string &str, const std::string &message) {
    if (str.empty()) {
      std::cerr << "  [FAIL] " << currentTest << ": " << message << std::endl;
      testsFailed++;
      return;
    }
    testsPassed++;
  }

  void assertGreaterOrEqual(int expected, int actual,
                            const std::string &message) {
    if (actual < expected) {
      std::cerr << "  [FAIL] " << currentTest << ": " << message << std::endl;
      std::cerr << "    Expected at least: " << expected << std::endl;
      std::cerr << "    Actual: " << actual << std::endl;
      testsFailed++;
      return;
    }
    testsPassed++;
  }

  void runTest(const std::string &testName,
               std::function<void()> testFunction) {
    currentTest = testName;
    std::cout << "[TEST] " << testName << std::endl;
    try {
      testFunction();
      std::cout << "  [PASS]" << std::endl;
    } catch (const std::exception &e) {
      std::cerr << "  [FAIL] Exception: " << e.what() << std::endl;
      testsFailed++;
    } catch (...) {
      std::cerr << "  [FAIL] Unknown exception" << std::endl;
      testsFailed++;
    }
  }

  void printSummary() {
    std::cout << "\n========================================" << std::endl;
    std::cout << "TEST SUMMARY" << std::endl;
    std::cout << "========================================" << std::endl;
    std::cout << "Passed: " << testsPassed << std::endl;
    std::cout << "Failed: " << testsFailed << std::endl;
    std::cout << "Total: " << (testsPassed + testsFailed) << std::endl;
    std::cout << "========================================\n" << std::endl;

    if (testsFailed == 0) {
      std::cout << "✓ ALL TESTS PASSED!" << std::endl;
      exit(0);
    } else {
      std::cout << "✗ SOME TESTS FAILED!" << std::endl;
      exit(1);
    }
  }
};

class TestDatabaseSetup {
private:
  std::string connectionString;

public:
  TestDatabaseSetup(const std::string &connStr) : connectionString(connStr) {
    setupDatabase();
  }

  ~TestDatabaseSetup() { cleanupDatabase(); }

  void setupDatabase() {
    try {
      pqxx::connection conn(connectionString);
      pqxx::work txn(conn);

      txn.exec("CREATE SCHEMA IF NOT EXISTS metadata");

      txn.exec("CREATE TABLE IF NOT EXISTS metadata.catalog_locks ("
               "id SERIAL PRIMARY KEY,"
               "lock_name VARCHAR(255) NOT NULL UNIQUE,"
               "acquired_at TIMESTAMP NOT NULL DEFAULT NOW(),"
               "acquired_by VARCHAR(255) NOT NULL,"
               "expires_at TIMESTAMP NOT NULL,"
               "session_id VARCHAR(255) NOT NULL"
               ")");

      txn.exec("CREATE INDEX IF NOT EXISTS idx_catalog_locks_name "
               "ON metadata.catalog_locks (lock_name)");
      txn.exec("CREATE INDEX IF NOT EXISTS idx_catalog_locks_expires "
               "ON metadata.catalog_locks (expires_at)");

      txn.exec("CREATE TABLE IF NOT EXISTS metadata.config ("
               "key VARCHAR(255) PRIMARY KEY,"
               "value TEXT NOT NULL"
               ")");

      txn.commit();
    } catch (const std::exception &e) {
      std::cerr << "Error setting up test database: " << e.what() << std::endl;
    }
  }

  void cleanupDatabase() {
    try {
      pqxx::connection conn(connectionString);
      pqxx::work txn(conn);
      txn.exec("TRUNCATE TABLE metadata.catalog_locks CASCADE");
      txn.exec("TRUNCATE TABLE metadata.config CASCADE");
      txn.commit();
    } catch (const std::exception &e) {
      std::cerr << "Error cleaning up test database: " << e.what() << std::endl;
    }
  }

  void clearData() { cleanupDatabase(); }

  int countLocks() {
    try {
      pqxx::connection conn(connectionString);
      pqxx::work txn(conn);
      auto result = txn.exec("SELECT COUNT(*) FROM metadata.catalog_locks");
      return result[0][0].as<int>();
    } catch (const std::exception &e) {
      return 0;
    }
  }

  int countLocksByName(const std::string &lockName) {
    try {
      pqxx::connection conn(connectionString);
      pqxx::work txn(conn);
      auto result = txn.exec_params(
          "SELECT COUNT(*) FROM metadata.catalog_locks WHERE lock_name = $1",
          lockName);
      return result[0][0].as<int>();
    } catch (const std::exception &e) {
      return 0;
    }
  }

  void insertExpiredLock(const std::string &lockName,
                         const std::string &sessionId) {
    try {
      pqxx::connection conn(connectionString);
      pqxx::work txn(conn);
      txn.exec_params(
          "INSERT INTO metadata.catalog_locks (lock_name, acquired_by, "
          "expires_at, session_id) "
          "VALUES ($1, 'test_host', NOW() - INTERVAL '1 hour', $2) "
          "ON CONFLICT (lock_name) DO UPDATE SET expires_at = NOW() - INTERVAL "
          "'1 hour'",
          lockName, sessionId);
      txn.commit();
    } catch (const std::exception &e) {
      std::cerr << "Error inserting expired lock: " << e.what() << std::endl;
    }
  }

  void insertActiveLock(const std::string &lockName,
                        const std::string &sessionId, int timeoutSeconds) {
    try {
      pqxx::connection conn(connectionString);
      pqxx::work txn(conn);
      txn.exec_params(
          "INSERT INTO metadata.catalog_locks (lock_name, acquired_by, "
          "expires_at, session_id) "
          "VALUES ($1, 'test_host', NOW() + INTERVAL '1 second' * $2, $3) "
          "ON CONFLICT (lock_name) DO UPDATE SET expires_at = NOW() + INTERVAL "
          "'1 second' * $2, session_id = $3",
          lockName, timeoutSeconds, sessionId);
      txn.commit();
    } catch (const std::exception &e) {
      std::cerr << "Error inserting active lock: " << e.what() << std::endl;
    }
  }

  void setConfigValue(const std::string &key, const std::string &value) {
    try {
      pqxx::connection conn(connectionString);
      pqxx::work txn(conn);
      txn.exec_params(
          "INSERT INTO metadata.config (key, value) VALUES ($1, $2) "
          "ON CONFLICT (key) DO UPDATE SET value = $2",
          key, value);
      txn.commit();
    } catch (const std::exception &e) {
      std::cerr << "Error setting config value: " << e.what() << std::endl;
    }
  }
};

int main(int argc, char *argv[]) {
  if (argc < 2) {
    std::cerr << "Usage: " << argv[0] << " <postgresql_connection_string>"
              << std::endl;
    std::cerr << "Example: " << argv[0]
              << " postgresql://user:password@localhost/database" << std::endl;
    return 1;
  }

  std::string connectionString = argv[1];
  TestRunner runner;
  TestDatabaseSetup dbSetup(connectionString);

  Logger::initialize();

  std::cout << "\n========================================" << std::endl;
  std::cout << "CATALOG LOCK - EXHAUSTIVE TESTS" << std::endl;
  std::cout << "========================================\n" << std::endl;

  runner.runTest("Constructor with valid parameters", [&]() {
    CatalogLock lock(connectionString, "test_lock", 300);
    runner.assertFalse(lock.isAcquired(),
                       "Lock should not be acquired initially");
  });

  runner.runTest("Constructor with default timeout", [&]() {
    CatalogLock lock(connectionString, "test_lock_default");
    runner.assertFalse(lock.isAcquired(),
                       "Lock should not be acquired initially");
  });

  runner.runTest("Constructor with minimum timeout", [&]() {
    CatalogLock lock(connectionString, "test_lock_min", 1);
    runner.assertFalse(lock.isAcquired(),
                       "Lock should not be acquired initially");
  });

  runner.runTest("Constructor with maximum timeout", [&]() {
    CatalogLock lock(connectionString, "test_lock_max", 3600);
    runner.assertFalse(lock.isAcquired(),
                       "Lock should not be acquired initially");
  });

  runner.runTest("tryAcquire with available lock", [&]() {
    dbSetup.clearData();
    CatalogLock lock(connectionString, "available_lock", 300);
    bool acquired = lock.tryAcquire(5);
    runner.assertTrue(acquired, "Should acquire available lock");
    runner.assertTrue(lock.isAcquired(), "Lock should be marked as acquired");
    runner.assertEquals(1, dbSetup.countLocksByName("available_lock"),
                        "Should have one lock in database");
  });

  runner.runTest("tryAcquire with default maxWaitSeconds", [&]() {
    dbSetup.clearData();
    CatalogLock lock(connectionString, "default_wait_lock", 300);
    bool acquired = lock.tryAcquire();
    runner.assertTrue(acquired, "Should acquire lock with default wait time");
    runner.assertTrue(lock.isAcquired(), "Lock should be marked as acquired");
  });

  runner.runTest("tryAcquire when lock is already held", [&]() {
    dbSetup.clearData();
    CatalogLock lock1(connectionString, "held_lock", 300);
    bool acquired1 = lock1.tryAcquire(5);
    runner.assertTrue(acquired1, "First lock should be acquired");

    CatalogLock lock2(connectionString, "held_lock", 300);
    bool acquired2 = lock2.tryAcquire(2);
    runner.assertFalse(acquired2, "Second lock should not be acquired");
    runner.assertFalse(lock2.isAcquired(),
                       "Second lock should not be marked as acquired");
  });

  runner.runTest("tryAcquire with expired lock", [&]() {
    dbSetup.clearData();
    dbSetup.insertExpiredLock("expired_lock", "old_session");

    CatalogLock lock(connectionString, "expired_lock", 300);
    bool acquired = lock.tryAcquire(5);
    runner.assertTrue(acquired,
                      "Should acquire lock after expired lock is cleaned");
    runner.assertTrue(lock.isAcquired(), "Lock should be marked as acquired");
  });

  runner.runTest("tryAcquire with timeout", [&]() {
    dbSetup.clearData();
    CatalogLock lock1(connectionString, "timeout_lock", 300);
    bool acquired1 = lock1.tryAcquire(5);
    runner.assertTrue(acquired1, "First lock should be acquired");

    CatalogLock lock2(connectionString, "timeout_lock", 300);
    auto start = std::chrono::steady_clock::now();
    bool acquired2 = lock2.tryAcquire(1);
    auto end = std::chrono::steady_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    runner.assertFalse(acquired2, "Second lock should not be acquired");
    runner.assertGreaterOrEqual(1000, static_cast<int>(duration.count()),
                                "Should wait at least 1 second before timeout");
  });

  runner.runTest("tryAcquire with very short timeout", [&]() {
    dbSetup.clearData();
    CatalogLock lock1(connectionString, "short_timeout_lock", 300);
    bool acquired1 = lock1.tryAcquire(5);
    runner.assertTrue(acquired1, "First lock should be acquired");

    CatalogLock lock2(connectionString, "short_timeout_lock", 300);
    bool acquired2 = lock2.tryAcquire(1);
    runner.assertFalse(acquired2, "Second lock should timeout quickly");
  });

  runner.runTest("release acquired lock", [&]() {
    dbSetup.clearData();
    CatalogLock lock(connectionString, "release_lock", 300);
    bool acquired = lock.tryAcquire(5);
    runner.assertTrue(acquired, "Should acquire lock");

    lock.release();
    runner.assertFalse(lock.isAcquired(),
                       "Lock should not be acquired after release");
    runner.assertEquals(0, dbSetup.countLocksByName("release_lock"),
                        "Lock should be removed from database");
  });

  runner.runTest("release without acquiring", [&]() {
    dbSetup.clearData();
    CatalogLock lock(connectionString, "no_acquire_lock", 300);
    lock.release();
    runner.assertFalse(lock.isAcquired(), "Lock should remain not acquired");
    runner.assertTrue(true, "Should not throw exception");
  });

  runner.runTest("release after timeout expires", [&]() {
    dbSetup.clearData();
    CatalogLock lock(connectionString, "timeout_expire_lock", 1);
    bool acquired = lock.tryAcquire(5);
    runner.assertTrue(acquired, "Should acquire lock");

    std::this_thread::sleep_for(std::chrono::seconds(2));

    lock.release();
    runner.assertFalse(lock.isAcquired(), "Lock should be released");
    runner.assertEquals(0, dbSetup.countLocksByName("timeout_expire_lock"),
                        "Lock should be removed from database");
  });

  runner.runTest("Destructor releases lock automatically", [&]() {
    dbSetup.clearData();
    {
      CatalogLock lock(connectionString, "destructor_lock", 300);
      bool acquired = lock.tryAcquire(5);
      runner.assertTrue(acquired, "Should acquire lock");
      runner.assertEquals(1, dbSetup.countLocksByName("destructor_lock"),
                          "Lock should exist in database");
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    runner.assertEquals(0, dbSetup.countLocksByName("destructor_lock"),
                        "Lock should be released in destructor");
  });

  runner.runTest("Multiple locks with different names", [&]() {
    dbSetup.clearData();
    CatalogLock lock1(connectionString, "lock1", 300);
    CatalogLock lock2(connectionString, "lock2", 300);
    CatalogLock lock3(connectionString, "lock3", 300);

    bool acquired1 = lock1.tryAcquire(5);
    bool acquired2 = lock2.tryAcquire(5);
    bool acquired3 = lock3.tryAcquire(5);

    runner.assertTrue(acquired1, "Lock1 should be acquired");
    runner.assertTrue(acquired2, "Lock2 should be acquired");
    runner.assertTrue(acquired3, "Lock3 should be acquired");
    runner.assertEquals(3, dbSetup.countLocks(),
                        "Should have 3 locks in database");
  });

  runner.runTest("Concurrent acquisition attempts", [&]() {
    dbSetup.clearData();
    std::atomic<int> successCount(0);
    std::vector<std::thread> threads;
    int numThreads = 10;

    for (int i = 0; i < numThreads; i++) {
      threads.emplace_back([&connectionString, &successCount, i]() {
        CatalogLock lock(connectionString, "concurrent_lock", 300);
        if (lock.tryAcquire(10)) {
          successCount++;
          std::this_thread::sleep_for(std::chrono::milliseconds(100));
          lock.release();
        }
      });
    }

    for (auto &thread : threads) {
      thread.join();
    }

    runner.assertEquals(10, successCount.load(),
                        "All threads should eventually acquire lock");
    runner.assertEquals(0, dbSetup.countLocks(),
                        "All locks should be released");
  });

  runner.runTest("Concurrent acquisition with only one success", [&]() {
    dbSetup.clearData();
    std::atomic<int> successCount(0);
    std::atomic<int> failCount(0);
    std::vector<std::thread> threads;
    int numThreads = 5;

    for (int i = 0; i < numThreads; i++) {
      threads.emplace_back([&connectionString, &successCount, &failCount, i]() {
        CatalogLock lock(connectionString, "single_lock", 300);
        if (lock.tryAcquire(1)) {
          successCount++;
          std::this_thread::sleep_for(std::chrono::seconds(2));
          lock.release();
        } else {
          failCount++;
        }
      });
    }

    for (auto &thread : threads) {
      thread.join();
    }

    runner.assertEquals(1, successCount.load(),
                        "Only one thread should acquire lock");
    runner.assertGreaterOrEqual(4, failCount.load(),
                                "Other threads should fail");
  });

  runner.runTest("tryAcquire with invalid maxWaitSeconds (too low)", [&]() {
    dbSetup.clearData();
    CatalogLock lock(connectionString, "invalid_wait_lock", 300);
    bool acquired = lock.tryAcquire(0);
    runner.assertFalse(acquired,
                       "Should not acquire with invalid maxWaitSeconds");
  });

  runner.runTest("tryAcquire with invalid maxWaitSeconds (too high)", [&]() {
    dbSetup.clearData();
    CatalogLock lock(connectionString, "invalid_wait_high_lock", 300);
    bool acquired = lock.tryAcquire(4000);
    runner.assertFalse(acquired,
                       "Should not acquire with maxWaitSeconds > 3600");
  });

  runner.runTest(
      "tryAcquire with invalid lockTimeoutSeconds in constructor", [&]() {
        dbSetup.clearData();
        CatalogLock lock(connectionString, "invalid_timeout_lock", 0);
        bool acquired = lock.tryAcquire(5);
        runner.assertFalse(
            acquired, "Should not acquire with invalid lockTimeoutSeconds");
      });

  runner.runTest(
      "tryAcquire with invalid lockTimeoutSeconds (too high)", [&]() {
        dbSetup.clearData();
        CatalogLock lock(connectionString, "invalid_timeout_high_lock", 4000);
        bool acquired = lock.tryAcquire(5);
        runner.assertFalse(acquired,
                           "Should not acquire with lockTimeoutSeconds > 3600");
      });

  runner.runTest("Custom retry sleep from config", [&]() {
    dbSetup.clearData();
    dbSetup.setConfigValue("lock_retry_sleep_ms", "200");

    CatalogLock lock1(connectionString, "config_retry_lock", 300);
    bool acquired1 = lock1.tryAcquire(5);
    runner.assertTrue(acquired1, "First lock should be acquired");

    CatalogLock lock2(connectionString, "config_retry_lock", 300);
    auto start = std::chrono::steady_clock::now();
    bool acquired2 = lock2.tryAcquire(1);
    auto end = std::chrono::steady_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    runner.assertFalse(acquired2, "Second lock should not be acquired");
    runner.assertGreaterOrEqual(200, static_cast<int>(duration.count()),
                                "Should use custom retry sleep from config");
  });

  runner.runTest("Invalid retry sleep in config (too low)", [&]() {
    dbSetup.clearData();
    dbSetup.setConfigValue("lock_retry_sleep_ms", "50");

    CatalogLock lock(connectionString, "invalid_retry_low_lock", 300);
    bool acquired = lock.tryAcquire(5);
    runner.assertTrue(acquired,
                      "Should still work with invalid low retry sleep");
  });

  runner.runTest("Invalid retry sleep in config (too high)", [&]() {
    dbSetup.clearData();
    dbSetup.setConfigValue("lock_retry_sleep_ms", "20000");

    CatalogLock lock(connectionString, "invalid_retry_high_lock", 300);
    bool acquired = lock.tryAcquire(5);
    runner.assertTrue(acquired,
                      "Should still work with invalid high retry sleep");
  });

  runner.runTest("Lock expiration and automatic cleanup", [&]() {
    dbSetup.clearData();
    CatalogLock lock1(connectionString, "expire_cleanup_lock", 1);
    bool acquired1 = lock1.tryAcquire(5);
    runner.assertTrue(acquired1, "First lock should be acquired");

    std::this_thread::sleep_for(std::chrono::seconds(2));

    CatalogLock lock2(connectionString, "expire_cleanup_lock", 300);
    bool acquired2 = lock2.tryAcquire(5);
    runner.assertTrue(acquired2,
                      "Should acquire after expired lock is cleaned");
  });

  runner.runTest("Multiple release attempts", [&]() {
    dbSetup.clearData();
    CatalogLock lock(connectionString, "multiple_release_lock", 300);
    bool acquired = lock.tryAcquire(5);
    runner.assertTrue(acquired, "Should acquire lock");

    lock.release();
    lock.release();
    lock.release();

    runner.assertFalse(lock.isAcquired(), "Lock should remain released");
    runner.assertEquals(0, dbSetup.countLocksByName("multiple_release_lock"),
                        "Lock should not exist after release");
  });

  runner.runTest("Lock with very long timeout", [&]() {
    dbSetup.clearData();
    CatalogLock lock(connectionString, "long_timeout_lock", 3600);
    bool acquired = lock.tryAcquire(5);
    runner.assertTrue(acquired, "Should acquire lock with long timeout");
    runner.assertTrue(lock.isAcquired(), "Lock should be marked as acquired");
  });

  runner.runTest("Lock with minimum timeout", [&]() {
    dbSetup.clearData();
    CatalogLock lock(connectionString, "min_timeout_lock", 1);
    bool acquired = lock.tryAcquire(5);
    runner.assertTrue(acquired, "Should acquire lock with minimum timeout");
  });

  runner.runTest("Acquire and release cycle multiple times", [&]() {
    dbSetup.clearData();
    CatalogLock lock(connectionString, "cycle_lock", 300);

    for (int i = 0; i < 5; i++) {
      bool acquired = lock.tryAcquire(5);
      runner.assertTrue(acquired,
                        "Should acquire lock in cycle " + std::to_string(i));
      runner.assertTrue(lock.isAcquired(), "Lock should be acquired");
      lock.release();
      runner.assertFalse(lock.isAcquired(), "Lock should be released");
    }
  });

  runner.runTest("Lock name with special characters", [&]() {
    dbSetup.clearData();
    std::string specialName = "lock_with_underscores_123";
    CatalogLock lock(connectionString, specialName, 300);
    bool acquired = lock.tryAcquire(5);
    runner.assertTrue(acquired,
                      "Should acquire lock with special characters in name");
    runner.assertEquals(1, dbSetup.countLocksByName(specialName),
                        "Lock should exist in database");
  });

  runner.runTest("Lock name with Unicode characters", [&]() {
    dbSetup.clearData();
    std::string unicodeName = "lock_测试_🎉";
    CatalogLock lock(connectionString, unicodeName, 300);
    bool acquired = lock.tryAcquire(5);
    runner.assertTrue(acquired, "Should acquire lock with Unicode characters");
    runner.assertEquals(1, dbSetup.countLocksByName(unicodeName),
                        "Lock should exist in database");
  });

  runner.runTest("Very long lock name", [&]() {
    dbSetup.clearData();
    std::string longName(255, 'a');
    CatalogLock lock(connectionString, longName, 300);
    bool acquired = lock.tryAcquire(5);
    runner.assertTrue(acquired, "Should acquire lock with very long name");
    runner.assertEquals(1, dbSetup.countLocksByName(longName),
                        "Lock should exist in database");
  });

  runner.runTest("Session ID uniqueness", [&]() {
    dbSetup.clearData();
    CatalogLock lock1(connectionString, "session_test1", 300);
    CatalogLock lock2(connectionString, "session_test2", 300);

    bool acquired1 = lock1.tryAcquire(5);
    bool acquired2 = lock2.tryAcquire(5);

    runner.assertTrue(acquired1, "Lock1 should be acquired");
    runner.assertTrue(acquired2, "Lock2 should be acquired");

    try {
      pqxx::connection conn(connectionString);
      pqxx::work txn(conn);
      auto result = txn.exec(
          "SELECT COUNT(DISTINCT session_id) FROM metadata.catalog_locks");
      int distinctSessions = result[0][0].as<int>();
      runner.assertEquals(2, distinctSessions,
                          "Each lock should have unique session ID");
    } catch (const std::exception &e) {
      runner.assertTrue(false,
                        "Error checking session IDs: " + std::string(e.what()));
    }
  });

  runner.runTest("Hostname is stored correctly", [&]() {
    dbSetup.clearData();
    CatalogLock lock(connectionString, "hostname_test", 300);
    bool acquired = lock.tryAcquire(5);
    runner.assertTrue(acquired, "Should acquire lock");

    try {
      pqxx::connection conn(connectionString);
      pqxx::work txn(conn);
      auto result = txn.exec_params(
          "SELECT acquired_by FROM metadata.catalog_locks WHERE lock_name = $1",
          "hostname_test");
      if (!result.empty()) {
        std::string hostname = result[0][0].as<std::string>();
        runner.assertNotEmpty(hostname, "Hostname should not be empty");
      }
    } catch (const std::exception &e) {
      runner.assertTrue(false, "Error checking hostname");
    }
  });

  runner.runTest("Expiration time is set correctly", [&]() {
    dbSetup.clearData();
    CatalogLock lock(connectionString, "expiration_test", 300);
    bool acquired = lock.tryAcquire(5);
    runner.assertTrue(acquired, "Should acquire lock");

    try {
      pqxx::connection conn(connectionString);
      pqxx::work txn(conn);
      auto result = txn.exec_params(
          "SELECT expires_at FROM metadata.catalog_locks WHERE lock_name = $1",
          "expiration_test");
      if (!result.empty()) {
        std::string expiresAt = result[0][0].as<std::string>();
        runner.assertNotEmpty(expiresAt, "Expiration time should be set");
      }
    } catch (const std::exception &e) {
      runner.assertTrue(false, "Error checking expiration time");
    }
  });

  runner.runTest("Release lock with wrong session ID", [&]() {
    dbSetup.clearData();
    CatalogLock lock1(connectionString, "wrong_session_lock", 300);
    bool acquired1 = lock1.tryAcquire(5);
    runner.assertTrue(acquired1, "First lock should be acquired");

    CatalogLock lock2(connectionString, "wrong_session_lock", 300);
    lock2.release();

    runner.assertEquals(1, dbSetup.countLocksByName("wrong_session_lock"),
                        "Lock should still exist (wrong session ID)");
    runner.assertTrue(lock1.isAcquired(),
                      "First lock should still be acquired");
  });

  runner.runTest("Performance test - 100 acquire/release cycles", [&]() {
    dbSetup.clearData();
    CatalogLock lock(connectionString, "perf_test_lock", 300);

    auto start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < 100; i++) {
      bool acquired = lock.tryAcquire(5);
      runner.assertTrue(acquired,
                        "Should acquire lock in cycle " + std::to_string(i));
      lock.release();
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    std::cout << "  Completed 100 acquire/release cycles in "
              << duration.count() << "ms" << std::endl;
    runner.assertTrue(true, "Performance test completed");
  });

  runner.printSummary();
  return 0;
}
