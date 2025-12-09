#include "core/database_config.h"
#include "core/logger.h"
#include "third_party/json.hpp"
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>

using json = nlohmann::json;

// Static member initialization for DatabaseConfig. These default values are
// used when no configuration file or environment variables are provided.
// Defaults: localhost:5432, database "DataLake", user "postgres", empty
// password.

std::string DatabaseConfig::postgres_host_ = "localhost";
std::string DatabaseConfig::postgres_db_ = "DataLake";
std::string DatabaseConfig::postgres_user_ = "postgres";
std::string DatabaseConfig::postgres_password_ = "";
std::string DatabaseConfig::postgres_port_ = "5432";
bool DatabaseConfig::initialized_ = false;

// Loads PostgreSQL database configuration from a JSON file. The function
// expects a JSON structure with a "database" object containing a "postgres"
// object with keys: host, port, database, user, and password. If the file
// cannot be opened or parsed, the function falls back to loading from
// environment variables. The configPath parameter defaults to "config.json" if
// not specified. After successful loading, the initialized_ flag is set to
// true.
void DatabaseConfig::loadFromFile(const std::string &configPath) {
  try {
    std::ifstream configFile(configPath);
    if (!configFile.is_open()) {
      Logger::warning(LogCategory::CONFIG, "DatabaseConfig",
                      "Could not open config file '" + configPath +
                          "', using defaults or environment variables");
      loadFromEnv();
      return;
    }

    json config;
    configFile >> config;

    if (config.contains("database") &&
        config["database"].contains("postgres")) {
      auto pgConfig = config["database"]["postgres"];

      if (pgConfig.contains("host")) {
        std::string host = pgConfig["host"].get<std::string>();
        if (!host.empty())
          postgres_host_ = host;
      }
      if (pgConfig.contains("port")) {
        std::string port = pgConfig["port"].get<std::string>();
        if (!port.empty()) {
          try {
            int portNum = std::stoi(port);
            if (portNum > 0 && portNum <= 65535) {
              postgres_port_ = port;
            } else {
              Logger::warning(LogCategory::CONFIG, "DatabaseConfig",
                              "Invalid port number: " + port +
                                  ", using default: 5432");
            }
          } catch (const std::exception &) {
            Logger::warning(LogCategory::CONFIG, "DatabaseConfig",
                            "Port is not a valid number: " + port +
                                ", using default: 5432");
          }
        }
      }
      if (pgConfig.contains("database")) {
        std::string db = pgConfig["database"].get<std::string>();
        if (!db.empty())
          postgres_db_ = db;
      }
      if (pgConfig.contains("user")) {
        std::string user = pgConfig["user"].get<std::string>();
        if (!user.empty())
          postgres_user_ = user;
      }
      if (pgConfig.contains("password"))
        postgres_password_ = pgConfig["password"].get<std::string>();
    }

    initialized_ = true;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::CONFIG, "DatabaseConfig",
                  "Error loading config from file: " + std::string(e.what()) +
                      ", falling back to environment variables");
    loadFromEnv();
  }
}

// Loads PostgreSQL database configuration from environment variables. The
// function checks for the following environment variables: POSTGRES_HOST,
// POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, and POSTGRES_PASSWORD. If any
// variable is not set, the corresponding default value is retained. A warning
// is logged if POSTGRES_PASSWORD is not set, as database connections may fail
// without it. After loading, the initialized_ flag is set to true.
void DatabaseConfig::loadFromEnv() {
  const char *host = std::getenv("POSTGRES_HOST");
  const char *port = std::getenv("POSTGRES_PORT");
  const char *db = std::getenv("POSTGRES_DB");
  const char *user = std::getenv("POSTGRES_USER");
  const char *password = std::getenv("POSTGRES_PASSWORD");

  if (host && strlen(host) > 0)
    postgres_host_ = host;
  if (port && strlen(port) > 0) {
    try {
      int portNum = std::stoi(port);
      if (portNum > 0 && portNum <= 65535) {
        postgres_port_ = port;
      } else {
        Logger::warning(LogCategory::CONFIG, "DatabaseConfig",
                        "Invalid port number: " + std::string(port) +
                            ", using default: 5432");
      }
    } catch (const std::exception &) {
      Logger::warning(LogCategory::CONFIG, "DatabaseConfig",
                      "Port is not a valid number: " + std::string(port) +
                          ", using default: 5432");
    }
  }
  if (db && strlen(db) > 0)
    postgres_db_ = db;
  if (user && strlen(user) > 0)
    postgres_user_ = user;
  if (password)
    postgres_password_ = password;

  if (postgres_password_.empty()) {
    Logger::warning(LogCategory::CONFIG, "DatabaseConfig",
                    "POSTGRES_PASSWORD not set in config.json or environment. "
                    "Database connections may fail.");
  }

  initialized_ = true;
}
