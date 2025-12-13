#ifndef DATABASE_CONFIG_H
#define DATABASE_CONFIG_H

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

  static std::string getPostgresConnectionStringForLogging() {
    return "host=" + postgres_host_ + " dbname=" + postgres_db_ +
           " user=" + postgres_user_ + " password=*** port=" + postgres_port_;
  }
};

#endif
