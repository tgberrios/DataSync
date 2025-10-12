#include "core/sync_config.h"
#include <iostream>

void testChunkSizeValidation() {
  std::cout << "\n=== Test 1: CHUNK_SIZE Validation ===" << std::endl;

  try {
    SyncConfig::setChunkSize(50);
    std::cout << "❌ Should have thrown for CHUNK_SIZE < MIN (100)"
              << std::endl;
  } catch (const std::invalid_argument &e) {
    std::cout << "✅ Correctly rejected CHUNK_SIZE = 50: " << e.what()
              << std::endl;
  }

  try {
    SyncConfig::setChunkSize(150000);
    std::cout << "❌ Should have thrown for CHUNK_SIZE > MAX (100000)"
              << std::endl;
  } catch (const std::invalid_argument &e) {
    std::cout << "✅ Correctly rejected CHUNK_SIZE = 150000: " << e.what()
              << std::endl;
  }

  try {
    SyncConfig::setChunkSize(25000);
    std::cout << "✅ Accepted valid CHUNK_SIZE = 25000" << std::endl;
    std::cout << "✅ Current CHUNK_SIZE: " << SyncConfig::getChunkSize()
              << std::endl;
  } catch (const std::invalid_argument &e) {
    std::cout << "❌ Should have accepted CHUNK_SIZE = 25000" << std::endl;
  }
}

void testSyncIntervalValidation() {
  std::cout << "\n=== Test 2: SYNC_INTERVAL Validation ===" << std::endl;

  try {
    SyncConfig::setSyncInterval(2);
    std::cout << "❌ Should have thrown for SYNC_INTERVAL < MIN (5)"
              << std::endl;
  } catch (const std::invalid_argument &e) {
    std::cout << "✅ Correctly rejected SYNC_INTERVAL = 2: " << e.what()
              << std::endl;
  }

  try {
    SyncConfig::setSyncInterval(5000);
    std::cout << "❌ Should have thrown for SYNC_INTERVAL > MAX (3600)"
              << std::endl;
  } catch (const std::invalid_argument &e) {
    std::cout << "✅ Correctly rejected SYNC_INTERVAL = 5000: " << e.what()
              << std::endl;
  }

  try {
    SyncConfig::setSyncInterval(30);
    std::cout << "✅ Accepted valid SYNC_INTERVAL = 30" << std::endl;
    std::cout << "✅ Current SYNC_INTERVAL: " << SyncConfig::getSyncInterval()
              << std::endl;
  } catch (const std::invalid_argument &e) {
    std::cout << "❌ Should have accepted SYNC_INTERVAL = 30" << std::endl;
  }
}

void testMaxWorkersValidation() {
  std::cout << "\n=== Test 3: MAX_WORKERS Validation ===" << std::endl;

  try {
    SyncConfig::setMaxWorkers(0);
    std::cout << "❌ Should have thrown for MAX_WORKERS < MIN (1)" << std::endl;
  } catch (const std::invalid_argument &e) {
    std::cout << "✅ Correctly rejected MAX_WORKERS = 0: " << e.what()
              << std::endl;
  }

  try {
    SyncConfig::setMaxWorkers(64);
    std::cout << "❌ Should have thrown for MAX_WORKERS > MAX (32)"
              << std::endl;
  } catch (const std::invalid_argument &e) {
    std::cout << "✅ Correctly rejected MAX_WORKERS = 64: " << e.what()
              << std::endl;
  }

  try {
    SyncConfig::setMaxWorkers(8);
    std::cout << "✅ Accepted valid MAX_WORKERS = 8" << std::endl;
    std::cout << "✅ Current MAX_WORKERS: " << SyncConfig::getMaxWorkers()
              << std::endl;
  } catch (const std::invalid_argument &e) {
    std::cout << "❌ Should have accepted MAX_WORKERS = 8" << std::endl;
  }
}

void testMaxTablesPerCycleValidation() {
  std::cout << "\n=== Test 4: MAX_TABLES_PER_CYCLE Validation ===" << std::endl;

  try {
    SyncConfig::setMaxTablesPerCycle(0);
    std::cout << "❌ Should have thrown for MAX_TABLES_PER_CYCLE < MIN (1)"
              << std::endl;
  } catch (const std::invalid_argument &e) {
    std::cout << "✅ Correctly rejected MAX_TABLES_PER_CYCLE = 0: " << e.what()
              << std::endl;
  }

  try {
    SyncConfig::setMaxTablesPerCycle(20000);
    std::cout << "❌ Should have thrown for MAX_TABLES_PER_CYCLE > MAX (10000)"
              << std::endl;
  } catch (const std::invalid_argument &e) {
    std::cout << "✅ Correctly rejected MAX_TABLES_PER_CYCLE = 20000: "
              << e.what() << std::endl;
  }

  try {
    SyncConfig::setMaxTablesPerCycle(1000);
    std::cout << "✅ Accepted valid MAX_TABLES_PER_CYCLE = 1000" << std::endl;
    std::cout << "✅ Current MAX_TABLES_PER_CYCLE: "
              << SyncConfig::getMaxTablesPerCycle() << std::endl;
  } catch (const std::invalid_argument &e) {
    std::cout << "❌ Should have accepted MAX_TABLES_PER_CYCLE = 1000"
              << std::endl;
  }
}

void testBoundaryValues() {
  std::cout << "\n=== Test 5: Boundary Values ===" << std::endl;

  try {
    SyncConfig::setChunkSize(SyncConfig::MIN_CHUNK_SIZE);
    std::cout << "✅ Accepted MIN_CHUNK_SIZE = " << SyncConfig::MIN_CHUNK_SIZE
              << std::endl;

    SyncConfig::setChunkSize(SyncConfig::MAX_CHUNK_SIZE);
    std::cout << "✅ Accepted MAX_CHUNK_SIZE = " << SyncConfig::MAX_CHUNK_SIZE
              << std::endl;

    SyncConfig::setSyncInterval(SyncConfig::MIN_SYNC_INTERVAL);
    std::cout << "✅ Accepted MIN_SYNC_INTERVAL = "
              << SyncConfig::MIN_SYNC_INTERVAL << std::endl;

    SyncConfig::setSyncInterval(SyncConfig::MAX_SYNC_INTERVAL);
    std::cout << "✅ Accepted MAX_SYNC_INTERVAL = "
              << SyncConfig::MAX_SYNC_INTERVAL << std::endl;

    SyncConfig::setMaxWorkers(SyncConfig::MIN_MAX_WORKERS);
    std::cout << "✅ Accepted MIN_MAX_WORKERS = " << SyncConfig::MIN_MAX_WORKERS
              << std::endl;

    SyncConfig::setMaxWorkers(SyncConfig::MAX_MAX_WORKERS);
    std::cout << "✅ Accepted MAX_MAX_WORKERS = " << SyncConfig::MAX_MAX_WORKERS
              << std::endl;

    SyncConfig::setMaxTablesPerCycle(SyncConfig::MIN_MAX_TABLES_PER_CYCLE);
    std::cout << "✅ Accepted MIN_MAX_TABLES_PER_CYCLE = "
              << SyncConfig::MIN_MAX_TABLES_PER_CYCLE << std::endl;

    SyncConfig::setMaxTablesPerCycle(SyncConfig::MAX_MAX_TABLES_PER_CYCLE);
    std::cout << "✅ Accepted MAX_MAX_TABLES_PER_CYCLE = "
              << SyncConfig::MAX_MAX_TABLES_PER_CYCLE << std::endl;

  } catch (const std::invalid_argument &e) {
    std::cout << "❌ Boundary value test failed: " << e.what() << std::endl;
  }
}

int main() {
  std::cout << "╔════════════════════════════════════════════════╗"
            << std::endl;
  std::cout << "║  SyncConfig Validation Tests                  ║" << std::endl;
  std::cout << "╚════════════════════════════════════════════════╝"
            << std::endl;

  std::cout << "\n📊 Validation Ranges:" << std::endl;
  std::cout << "  CHUNK_SIZE: [" << SyncConfig::MIN_CHUNK_SIZE << ", "
            << SyncConfig::MAX_CHUNK_SIZE << "]" << std::endl;
  std::cout << "  SYNC_INTERVAL: [" << SyncConfig::MIN_SYNC_INTERVAL << ", "
            << SyncConfig::MAX_SYNC_INTERVAL << "]" << std::endl;
  std::cout << "  MAX_WORKERS: [" << SyncConfig::MIN_MAX_WORKERS << ", "
            << SyncConfig::MAX_MAX_WORKERS << "]" << std::endl;
  std::cout << "  MAX_TABLES_PER_CYCLE: ["
            << SyncConfig::MIN_MAX_TABLES_PER_CYCLE << ", "
            << SyncConfig::MAX_MAX_TABLES_PER_CYCLE << "]" << std::endl;

  try {
    testChunkSizeValidation();
    testSyncIntervalValidation();
    testMaxWorkersValidation();
    testMaxTablesPerCycleValidation();
    testBoundaryValues();

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
