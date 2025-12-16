#ifndef DATABASE_CONFIG_H
#define DATABASE_CONFIG_H

#include <mutex>
#include <string>

class DatabaseConfig {
private:
  static std::string postgres_host_;
  static std::string postgres_db_;
  static std::string postgres_user_;
  static std::string postgres_password_;
  static std::string postgres_port_;
  static bool initialized_;
  static std::mutex configMutex_;

  static std::string escapeConnectionParam(const std::string &param);
  static void loadFromEnvUnlocked();

public:
  static void loadFromFile(const std::string &configPath = "config.json");
  static void loadFromEnv();

  static std::string getPostgresHost() {
    std::lock_guard<std::mutex> lock(configMutex_);
    return postgres_host_;
  }
  static std::string getPostgresDB() {
    std::lock_guard<std::mutex> lock(configMutex_);
    return postgres_db_;
  }
  static std::string getPostgresUser() {
    std::lock_guard<std::mutex> lock(configMutex_);
    return postgres_user_;
  }
  static std::string getPostgresPassword() {
    std::lock_guard<std::mutex> lock(configMutex_);
    return postgres_password_;
  }
  static std::string getPostgresPort() {
    std::lock_guard<std::mutex> lock(configMutex_);
    return postgres_port_;
  }

  static std::string getPostgresConnectionString() {
    std::lock_guard<std::mutex> lock(configMutex_);
    return "host=" + escapeConnectionParam(postgres_host_) +
           " dbname=" + escapeConnectionParam(postgres_db_) +
           " user=" + escapeConnectionParam(postgres_user_) +
           " password=" + escapeConnectionParam(postgres_password_) +
           " port=" + escapeConnectionParam(postgres_port_);
  }

  static std::string getPostgresConnectionStringForLogging() {
    std::lock_guard<std::mutex> lock(configMutex_);
    return "host=" + escapeConnectionParam(postgres_host_) +
           " dbname=" + escapeConnectionParam(postgres_db_) +
           " user=" + escapeConnectionParam(postgres_user_) +
           " password=*** port=" + escapeConnectionParam(postgres_port_);
  }

  static bool isInitialized() {
    std::lock_guard<std::mutex> lock(configMutex_);
    return initialized_;
  }
};

#endif
