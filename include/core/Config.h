#ifndef CONFIG_H
#define CONFIG_H

#include <atomic>
#include <string>

class DatabaseConfig {
private:
  static std::string postgres_host_;
  static std::string postgres_db_;
  static std::string postgres_user_;
  static std::string postgres_password_;
  static std::string postgres_port_;
  static bool initialized_;

public:
  static void loadFromFile(const std::string &configPath = "config.json");
  static void loadFromEnv();

  static const std::string &getPostgresHost() { return postgres_host_; }
  static const std::string &getPostgresDB() { return postgres_db_; }
  static const std::string &getPostgresUser() { return postgres_user_; }
  static const std::string &getPostgresPassword() { return postgres_password_; }
  static const std::string &getPostgresPort() { return postgres_port_; }

  static std::string getPostgresConnectionString() {
    return "host=" + postgres_host_ + " dbname=" + postgres_db_ +
           " user=" + postgres_user_ + " password=" + postgres_password_ +
           " port=" + postgres_port_;
  }

  static void setForTesting(const std::string &host, const std::string &db,
                            const std::string &user,
                            const std::string &password,
                            const std::string &port);
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