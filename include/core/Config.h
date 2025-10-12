#ifndef CONFIG_H
#define CONFIG_H

#include <atomic>
#include <string>

struct DatabaseConfig {
  static std::string POSTGRES_HOST;
  static std::string POSTGRES_DB;
  static std::string POSTGRES_USER;
  static std::string POSTGRES_PASSWORD;
  static std::string POSTGRES_PORT;

  static std::string getPostgresConnectionString() {
    return "host=" + POSTGRES_HOST + " dbname=" + POSTGRES_DB +
           " user=" + POSTGRES_USER + " password=" + POSTGRES_PASSWORD +
           " port=" + POSTGRES_PORT;
  }
};

struct SyncConfig {
  static std::atomic<size_t> CHUNK_SIZE;
  static std::atomic<size_t> SYNC_INTERVAL_SECONDS;
  static constexpr size_t DEFAULT_CHUNK_SIZE = 25000;
  static constexpr size_t DEFAULT_SYNC_INTERVAL = 30;
  static std::atomic<size_t> MAX_WORKERS;
  static std::atomic<size_t> MAX_TABLES_PER_CYCLE;
  static constexpr size_t DEFAULT_MAX_WORKERS = 4;
  static constexpr size_t DEFAULT_MAX_TABLES_PER_CYCLE = 1000;

  static void setChunkSize(size_t newSize) { CHUNK_SIZE = newSize; }

  static size_t getChunkSize() { return CHUNK_SIZE; }

  static void setSyncInterval(size_t newInterval) {
    SYNC_INTERVAL_SECONDS = newInterval;
  }

  static size_t getSyncInterval() { return SYNC_INTERVAL_SECONDS; }

  static void setMaxWorkers(size_t v) { MAX_WORKERS = v; }
  static size_t getMaxWorkers() { return MAX_WORKERS; }

  static void setMaxTablesPerCycle(size_t v) { MAX_TABLES_PER_CYCLE = v; }
  static size_t getMaxTablesPerCycle() { return MAX_TABLES_PER_CYCLE; }
};

namespace DatabaseDefaults {
constexpr int DEFAULT_MYSQL_PORT = 3306;
constexpr int MARIADB_TIMEOUT_SECONDS = 600;
constexpr int BUFFER_SIZE = 1024;
constexpr int DEFAULT_LOG_RETENTION_HOURS = 24;

constexpr const char *TIME_COLUMN_CANDIDATES[] = {
    "updated_at", "modified_at",  "last_modified", "updated_time",
    "created_at", "created_time", "timestamp"};
constexpr size_t TIME_COLUMN_COUNT = 7;
} // namespace DatabaseDefaults

#endif