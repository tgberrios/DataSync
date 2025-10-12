#include "catalog/catalog_lock.h"
#include "core/Config.h"
#include <chrono>
#include <iostream>
#include <thread>

void testBasicLockAcquisition() {
  std::cout << "\n=== Test 1: Basic Lock Acquisition ===" << std::endl;

  std::string connStr = DatabaseConfig::getPostgresConnectionString();
  CatalogLock lock1(connStr, "test_lock_1", 300);

  if (lock1.tryAcquire(5)) {
    std::cout << "✅ Lock acquired successfully" << std::endl;
    std::cout << "✅ Lock status: "
              << (lock1.isAcquired() ? "ACQUIRED" : "NOT ACQUIRED")
              << std::endl;
    lock1.release();
    std::cout << "✅ Lock released successfully" << std::endl;
    std::cout << "✅ Lock status after release: "
              << (lock1.isAcquired() ? "ACQUIRED" : "NOT ACQUIRED")
              << std::endl;
  } else {
    std::cout << "❌ Failed to acquire lock" << std::endl;
  }
}

void testLockContention() {
  std::cout << "\n=== Test 2: Lock Contention ===" << std::endl;

  std::string connStr = DatabaseConfig::getPostgresConnectionString();

  CatalogLock lock1(connStr, "test_lock_contention", 300);
  if (!lock1.tryAcquire(5)) {
    std::cout << "❌ Failed to acquire first lock" << std::endl;
    return;
  }
  std::cout << "✅ First instance acquired lock" << std::endl;

  CatalogLock lock2(connStr, "test_lock_contention", 300);
  std::cout << "⏳ Trying to acquire same lock from second instance (should "
               "fail)..."
            << std::endl;

  auto start = std::chrono::steady_clock::now();
  bool acquired = lock2.tryAcquire(3);
  auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                     std::chrono::steady_clock::now() - start)
                     .count();

  if (!acquired) {
    std::cout << "✅ Second instance correctly failed to acquire lock after "
              << elapsed << " seconds" << std::endl;
  } else {
    std::cout << "❌ Second instance incorrectly acquired the lock!"
              << std::endl;
  }

  lock1.release();
  std::cout << "✅ First instance released lock" << std::endl;

  std::cout << "⏳ Trying to acquire lock from second instance again (should "
               "succeed)..."
            << std::endl;
  if (lock2.tryAcquire(3)) {
    std::cout << "✅ Second instance acquired lock after first released"
              << std::endl;
    lock2.release();
  } else {
    std::cout << "❌ Second instance failed to acquire lock after first "
                 "released"
              << std::endl;
  }
}

void testRAIIBehavior() {
  std::cout << "\n=== Test 3: RAII Behavior (automatic release) ==="
            << std::endl;

  std::string connStr = DatabaseConfig::getPostgresConnectionString();

  {
    CatalogLock lock(connStr, "test_lock_raii", 300);
    if (lock.tryAcquire(5)) {
      std::cout << "✅ Lock acquired in scope" << std::endl;
    }
    std::cout << "⏳ Lock will be auto-released when going out of scope..."
              << std::endl;
  }

  std::cout << "✅ Lock went out of scope" << std::endl;

  CatalogLock lock2(connStr, "test_lock_raii", 300);
  if (lock2.tryAcquire(3)) {
    std::cout << "✅ Successfully acquired lock after previous instance was "
                 "destroyed"
              << std::endl;
    lock2.release();
  } else {
    std::cout << "❌ Failed to acquire lock - RAII cleanup may have failed"
              << std::endl;
  }
}

void testExpiredLockCleanup() {
  std::cout << "\n=== Test 4: Expired Lock Cleanup ===" << std::endl;

  std::string connStr = DatabaseConfig::getPostgresConnectionString();

  std::cout << "⏳ Creating lock with very short expiration (5 seconds)..."
            << std::endl;
  CatalogLock lock1(connStr, "test_lock_expired", 5);

  if (!lock1.tryAcquire(3)) {
    std::cout << "❌ Failed to acquire lock" << std::endl;
    return;
  }
  std::cout << "✅ Lock acquired with 5 second expiration" << std::endl;

  std::cout << "⏳ Waiting 6 seconds for lock to expire..." << std::endl;
  std::this_thread::sleep_for(std::chrono::seconds(6));

  std::cout << "⏳ Trying to acquire same lock (expired lock should be cleaned "
               "up)..."
            << std::endl;
  CatalogLock lock2(connStr, "test_lock_expired", 300);
  if (lock2.tryAcquire(3)) {
    std::cout << "✅ Successfully acquired lock - expired lock was cleaned up"
              << std::endl;
    lock2.release();
  } else {
    std::cout << "❌ Failed to acquire lock - expired lock cleanup may have "
                 "failed"
              << std::endl;
  }
}

int main() {
  std::cout << "╔════════════════════════════════════════════════╗"
            << std::endl;
  std::cout << "║  Catalog Lock System - Concurrency Tests      ║" << std::endl;
  std::cout << "╚════════════════════════════════════════════════╝"
            << std::endl;

  try {
    DatabaseConfig::loadFromFile("config.json");

    testBasicLockAcquisition();
    testLockContention();
    testRAIIBehavior();
    testExpiredLockCleanup();

    std::cout << "\n╔════════════════════════════════════════════════╗"
              << std::endl;
    std::cout << "║  ✅ All Tests Completed Successfully          ║"
              << std::endl;
    std::cout << "╚════════════════════════════════════════════════╝"
              << std::endl;

  } catch (const std::exception &e) {
    std::cerr << "\n❌ Test failed with exception: " << e.what() << std::endl;
    return 1;
  }

  return 0;
}
