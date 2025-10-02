#include "Config.h"
#include "logger.h"
#include <pqxx/pqxx>

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

void DatabaseConfig::loadFromDatabase() {
  try {
    // Use hardcoded connection string to connect to database
    std::string connectionString = "host=localhost dbname=DataLake user=tomy.berrios password=Yucaquemada1 port=5432";
    pqxx::connection conn(connectionString);
    
    if (!conn.is_open()) {
      Logger::warning(LogCategory::SYSTEM, "loadFromDatabase",
                      "Cannot connect to database - using default values");
      return;
    }

    pqxx::work txn(conn);
    auto result = txn.exec("SELECT key, value FROM metadata.config WHERE category = 'database'");
    txn.commit();

    for (const auto &row : result) {
      std::string key = row[0].as<std::string>();
      std::string value = row[1].as<std::string>();
      
      if (key == "postgres_host") {
        POSTGRES_HOST = value;
      } else if (key == "postgres_port") {
        POSTGRES_PORT = value;
      } else if (key == "postgres_database") {
        POSTGRES_DB = value;
      } else if (key == "postgres_user") {
        POSTGRES_USER = value;
      } else if (key == "postgres_password") {
        POSTGRES_PASSWORD = value;
      }
    }

    Logger::info(LogCategory::SYSTEM, "loadFromDatabase",
                 "Database configuration loaded from database");

  } catch (const std::exception &e) {
    Logger::warning(LogCategory::SYSTEM, "loadFromDatabase",
                    "Error loading config from database: " + std::string(e.what()) + 
                    " - using default values");
  }
}

void SyncConfig::loadFromDatabase() {
  try {
    // Use hardcoded connection string to connect to database
    std::string connectionString = "host=localhost dbname=DataLake user=tomy.berrios password=Yucaquemada1 port=5432";
    pqxx::connection conn(connectionString);
    
    if (!conn.is_open()) {
      Logger::warning(LogCategory::SYSTEM, "loadFromDatabase",
                      "Cannot connect to database - using default values");
      return;
    }

    pqxx::work txn(conn);
    auto result = txn.exec("SELECT key, value FROM metadata.config WHERE category = 'sync'");
    txn.commit();

    for (const auto &row : result) {
      std::string key = row[0].as<std::string>();
      std::string value = row[1].as<std::string>();
      
      if (key == "chunk_size") {
        try {
          CHUNK_SIZE = std::stoul(value);
        } catch (const std::exception &e) {
          Logger::warning(LogCategory::SYSTEM, "loadFromDatabase",
                          "Invalid chunk_size value: " + value + " - using default");
        }
      } else if (key == "sync_interval_seconds") {
        try {
          SYNC_INTERVAL_SECONDS = std::stoul(value);
        } catch (const std::exception &e) {
          Logger::warning(LogCategory::SYSTEM, "loadFromDatabase",
                          "Invalid sync_interval_seconds value: " + value + " - using default");
        }
      }
    }

    Logger::info(LogCategory::SYSTEM, "loadFromDatabase",
                 "Sync configuration loaded from database");

  } catch (const std::exception &e) {
    Logger::warning(LogCategory::SYSTEM, "loadFromDatabase",
                    "Error loading sync config from database: " + std::string(e.what()) + 
                    " - using default values");
  }
}
