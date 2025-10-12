#include "core/database_config.h"
#include "third_party/json.hpp"
#include <cstdlib>
#include <fstream>
#include <iostream>

using json = nlohmann::json;

std::string DatabaseConfig::postgres_host_ = "localhost";
std::string DatabaseConfig::postgres_db_ = "DataLake";
std::string DatabaseConfig::postgres_user_ = "postgres";
std::string DatabaseConfig::postgres_password_ = "";
std::string DatabaseConfig::postgres_port_ = "5432";
bool DatabaseConfig::initialized_ = false;

void DatabaseConfig::loadFromFile(const std::string &configPath) {
  try {
    std::ifstream configFile(configPath);
    if (!configFile.is_open()) {
      std::cerr << "Warning: Could not open config file '" << configPath
                << "', using defaults or environment variables" << std::endl;
      loadFromEnv();
      return;
    }

    json config;
    configFile >> config;

    if (config.contains("database") &&
        config["database"].contains("postgres")) {
      auto pgConfig = config["database"]["postgres"];

      if (pgConfig.contains("host"))
        postgres_host_ = pgConfig["host"].get<std::string>();
      if (pgConfig.contains("port"))
        postgres_port_ = pgConfig["port"].get<std::string>();
      if (pgConfig.contains("database"))
        postgres_db_ = pgConfig["database"].get<std::string>();
      if (pgConfig.contains("user"))
        postgres_user_ = pgConfig["user"].get<std::string>();
      if (pgConfig.contains("password"))
        postgres_password_ = pgConfig["password"].get<std::string>();
    }

    initialized_ = true;
  } catch (const std::exception &e) {
    std::cerr << "Error loading config from file: " << e.what()
              << ", falling back to environment variables" << std::endl;
    loadFromEnv();
  }
}

void DatabaseConfig::loadFromEnv() {
  const char *host = std::getenv("POSTGRES_HOST");
  const char *port = std::getenv("POSTGRES_PORT");
  const char *db = std::getenv("POSTGRES_DB");
  const char *user = std::getenv("POSTGRES_USER");
  const char *password = std::getenv("POSTGRES_PASSWORD");

  if (host)
    postgres_host_ = host;
  if (port)
    postgres_port_ = port;
  if (db)
    postgres_db_ = db;
  if (user)
    postgres_user_ = user;
  if (password)
    postgres_password_ = password;

  if (postgres_password_.empty()) {
    std::cerr << "WARNING: POSTGRES_PASSWORD not set in config.json or "
                 "environment. Database connections may fail."
              << std::endl;
  }

  initialized_ = true;
}

void DatabaseConfig::setForTesting(const std::string &host,
                                   const std::string &db,
                                   const std::string &user,
                                   const std::string &password,
                                   const std::string &port) {
  postgres_host_ = host;
  postgres_db_ = db;
  postgres_user_ = user;
  postgres_password_ = password;
  postgres_port_ = port;
  initialized_ = true;
}
