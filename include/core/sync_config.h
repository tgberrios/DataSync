#ifndef SYNC_CONFIG_H
#define SYNC_CONFIG_H

#include <atomic>
#include <stdexcept>

struct SyncConfig {
  static std::atomic<size_t> CHUNK_SIZE;
  static std::atomic<size_t> SYNC_INTERVAL_SECONDS;
  static std::atomic<size_t> MAX_WORKERS;
  static std::atomic<size_t> MAX_TABLES_PER_CYCLE;

  static constexpr size_t DEFAULT_CHUNK_SIZE = 25000;
  static constexpr size_t DEFAULT_SYNC_INTERVAL = 30;
  static constexpr size_t DEFAULT_MAX_WORKERS = 4;
  static constexpr size_t DEFAULT_MAX_TABLES_PER_CYCLE = 1000;

  static constexpr size_t MIN_CHUNK_SIZE = 100;
  static constexpr size_t MAX_CHUNK_SIZE = 100000;
  static constexpr size_t MIN_SYNC_INTERVAL = 5;
  static constexpr size_t MAX_SYNC_INTERVAL = 3600;
  static constexpr size_t MIN_MAX_WORKERS = 1;
  static constexpr size_t MAX_MAX_WORKERS = 32;
  static constexpr size_t MIN_MAX_TABLES_PER_CYCLE = 1;
  static constexpr size_t MAX_MAX_TABLES_PER_CYCLE = 10000;

  static void setChunkSize(size_t newSize) {
    if (newSize < MIN_CHUNK_SIZE || newSize > MAX_CHUNK_SIZE) {
      throw std::invalid_argument("CHUNK_SIZE must be between " +
                                  std::to_string(MIN_CHUNK_SIZE) + " and " +
                                  std::to_string(MAX_CHUNK_SIZE));
    }
    CHUNK_SIZE = newSize;
  }

  static size_t getChunkSize() { return CHUNK_SIZE; }

  static void setSyncInterval(size_t newInterval) {
    if (newInterval < MIN_SYNC_INTERVAL || newInterval > MAX_SYNC_INTERVAL) {
      throw std::invalid_argument("SYNC_INTERVAL_SECONDS must be between " +
                                  std::to_string(MIN_SYNC_INTERVAL) + " and " +
                                  std::to_string(MAX_SYNC_INTERVAL));
    }
    SYNC_INTERVAL_SECONDS = newInterval;
  }

  static size_t getSyncInterval() { return SYNC_INTERVAL_SECONDS; }

  static void setMaxWorkers(size_t v) {
    if (v < MIN_MAX_WORKERS || v > MAX_MAX_WORKERS) {
      throw std::invalid_argument("MAX_WORKERS must be between " +
                                  std::to_string(MIN_MAX_WORKERS) + " and " +
                                  std::to_string(MAX_MAX_WORKERS));
    }
    MAX_WORKERS = v;
  }

  static size_t getMaxWorkers() { return MAX_WORKERS; }

  static void setMaxTablesPerCycle(size_t v) {
    if (v < MIN_MAX_TABLES_PER_CYCLE || v > MAX_MAX_TABLES_PER_CYCLE) {
      throw std::invalid_argument("MAX_TABLES_PER_CYCLE must be between " +
                                  std::to_string(MIN_MAX_TABLES_PER_CYCLE) +
                                  " and " +
                                  std::to_string(MAX_MAX_TABLES_PER_CYCLE));
    }
    MAX_TABLES_PER_CYCLE = v;
  }

  static size_t getMaxTablesPerCycle() { return MAX_TABLES_PER_CYCLE; }
};

#endif
