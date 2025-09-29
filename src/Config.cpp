#include "Config.h"
#include "logger.h"

// Static member definitions
std::string DatabaseConfig::POSTGRES_HOST = "localhost";
std::string DatabaseConfig::POSTGRES_DB = "DataLake";
std::string DatabaseConfig::POSTGRES_USER = "tomy.berrios";
std::string DatabaseConfig::POSTGRES_PASSWORD = "Yucaquemada1";
std::string DatabaseConfig::POSTGRES_PORT = "5432";

size_t SyncConfig::CHUNK_SIZE = SyncConfig::DEFAULT_CHUNK_SIZE;
size_t SyncConfig::SYNC_INTERVAL_SECONDS = SyncConfig::DEFAULT_SYNC_INTERVAL;

void DatabaseConfig::loadFromConfig(const std::string &configPath) {
  try {
    std::ifstream configFile(configPath);
    if (!configFile.is_open()) {
      Logger::warning(LogCategory::SYSTEM, "loadFromConfig",
                      "Could not open config file: " + configPath +
                          " - using default values");
      return;
    }

    nlohmann::json config;
    configFile >> config;
    configFile.close();

    // Load database configuration
    if (config.contains("database") &&
        config["database"].contains("postgres")) {
      const auto &db = config["database"]["postgres"];

      if (db.contains("host"))
        POSTGRES_HOST = db["host"];
      if (db.contains("port"))
        POSTGRES_PORT = db["port"];
      if (db.contains("database"))
        POSTGRES_DB = db["database"];
      if (db.contains("user"))
        POSTGRES_USER = db["user"];
      if (db.contains("password"))
        POSTGRES_PASSWORD = db["password"];

      Logger::info(LogCategory::SYSTEM, "loadFromConfig",
                   "Database configuration loaded from: " + configPath);
    } else {
      Logger::warning(LogCategory::SYSTEM, "loadFromConfig",
                      "Database configuration not found in: " + configPath);
    }

  } catch (const std::exception &e) {
    Logger::error(LogCategory::SYSTEM, "loadFromConfig",
                  "Error loading config from " + configPath + ": " + e.what());
  }
}

void SyncConfig::loadFromConfig(const std::string &configPath) {
  // Sync configuration is loaded from metadata.config table in database
  // This function is kept for compatibility but does nothing
  // The actual sync config loading happens in StreamingData.cpp
  Logger::info(
      LogCategory::SYSTEM, "loadFromConfig",
      "Sync configuration will be loaded from database metadata.config table");
}
