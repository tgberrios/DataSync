#include "ConfigManager.h"

void ConfigManager::loadFromDatabase(pqxx::connection &conn) {
  try {
    Logger::getInstance().info(LogCategory::MONITORING,
                               "Loading configuration from database");

    if (!conn.is_open()) {
      Logger::getInstance().error(LogCategory::MONITORING,
                                  "Database connection is not open");
      return;
    }

    pqxx::work txn(conn);
    auto results = txn.exec("SELECT key, value FROM metadata.config WHERE key "
                            "IN ('chunk_size', 'sync_interval')");
    txn.commit();

    Logger::getInstance().info(LogCategory::MONITORING,
                               "Found " + std::to_string(results.size()) +
                                   " configuration entries");

    for (const auto &row : results) {
      if (row.size() < 2) {
        Logger::getInstance().error(
            LogCategory::MONITORING,
            "Invalid configuration row - insufficient columns: " +
                std::to_string(row.size()));
        continue;
      }

      if (row[0].is_null() || row[1].is_null()) {
        Logger::getInstance().warning(
            LogCategory::MONITORING,
            "Skipping configuration row with null key or value");
        continue;
      }

      std::string key = row[0].as<std::string>();
      std::string value = row[1].as<std::string>();

      if (key.empty() || value.empty()) {
        Logger::getInstance().warning(
            LogCategory::MONITORING,
            "Skipping configuration row with empty key or value");
        continue;
      }

      Logger::getInstance().info(LogCategory::MONITORING,
                                 "Processing config key: " + key + " = " +
                                     value);

      validateAndSetConfig(key, value);
    }

    Logger::getInstance().info(LogCategory::MONITORING,
                               "Configuration loaded successfully");
  } catch (const pqxx::sql_error &e) {
    Logger::getInstance().error(
        LogCategory::MONITORING,
        "SQL error loading configuration: " + std::string(e.what()) +
            " [SQL State: " + e.sqlstate() + "]");
  } catch (const pqxx::broken_connection &e) {
    Logger::getInstance().error(LogCategory::MONITORING,
                                "Connection error loading configuration: " +
                                    std::string(e.what()));
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::MONITORING,
                                "Error loading configuration: " +
                                    std::string(e.what()));
  }
}

void ConfigManager::refreshConfig() {
  try {
    std::string connStr = DatabaseConfig::getPostgresConnectionString();
    if (connStr.empty()) {
      Logger::getInstance().error(LogCategory::MONITORING,
                                  "Empty PostgreSQL connection string");
      return;
    }

    pqxx::connection conn(connStr);
    conn.set_session_var("statement_timeout", "30000");
    conn.set_session_var("lock_timeout", "10000");

    if (!conn.is_open()) {
      Logger::getInstance().error(LogCategory::MONITORING,
                                  "Cannot establish database connection");
      return;
    }

    loadFromDatabase(conn);
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::MONITORING,
                                "Error refreshing configuration: " +
                                    std::string(e.what()));
  }
}

size_t ConfigManager::getChunkSize() const {
  std::lock_guard<std::mutex> lock(configMutex);
  return chunkSize;
}

size_t ConfigManager::getSyncInterval() const {
  std::lock_guard<std::mutex> lock(configMutex);
  return syncInterval;
}

bool ConfigManager::isValidChunkSize(size_t size) const {
  return size >= 1 && size <= 1024 * 1024 * 1024; // 1 to 1GB
}

bool ConfigManager::isValidSyncInterval(size_t interval) const {
  return interval >= 5 && interval <= 3600; // 5 seconds to 1 hour
}

void ConfigManager::updateChunkSize(size_t newSize) {
  std::lock_guard<std::mutex> lock(configMutex);
  if (newSize != chunkSize) {
    logConfigChange("chunk_size", std::to_string(chunkSize),
                    std::to_string(newSize));
    chunkSize = newSize;
    SyncConfig::setChunkSize(newSize);
  }
}

void ConfigManager::updateSyncInterval(size_t newInterval) {
  std::lock_guard<std::mutex> lock(configMutex);
  if (newInterval != syncInterval) {
    logConfigChange("sync_interval", std::to_string(syncInterval),
                    std::to_string(newInterval));
    syncInterval = newInterval;
    SyncConfig::setSyncInterval(newInterval);
  }
}

void ConfigManager::validateAndSetConfig(const std::string &key,
                                         const std::string &value) {
  try {
    if (key == "chunk_size") {
      size_t newSize = std::stoul(value);
      if (isValidChunkSize(newSize)) {
        updateChunkSize(newSize);
      } else {
        Logger::getInstance().warning(
            LogCategory::MONITORING,
            "Chunk size value out of range (1-1GB): " + value);
      }
    } else if (key == "sync_interval") {
      size_t newInterval = std::stoul(value);
      if (isValidSyncInterval(newInterval)) {
        updateSyncInterval(newInterval);
      } else {
        Logger::getInstance().warning(
            LogCategory::MONITORING,
            "Sync interval value out of range (5s-1h): " + value);
      }
    } else {
      Logger::getInstance().warning(LogCategory::MONITORING,
                                    "Unknown configuration key: " + key);
    }
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::MONITORING,
                                "Failed to parse configuration value '" +
                                    value + "' for key '" + key +
                                    "': " + std::string(e.what()));
  }
}

void ConfigManager::logConfigChange(const std::string &key,
                                    const std::string &oldValue,
                                    const std::string &newValue) {
  Logger::getInstance().info(LogCategory::MONITORING,
                             "Configuration updated: " + key + " from " +
                                 oldValue + " to " + newValue);
}
