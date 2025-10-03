#include "MariaDBConnectionManager.h"
#include <algorithm>
#include <sstream>

MYSQL *
MariaDBConnectionManager::getConnection(const std::string &connectionString) {
  if (connectionString.empty()) {
    Logger::getInstance().error(LogCategory::TRANSFER,
                                "MariaDBConnectionManager",
                                "Empty connection string provided");
    return nullptr;
  }

  ConnectionParams params = parseConnectionString(connectionString);
  if (!validateConnectionParams(params)) {
    return nullptr;
  }

  MYSQL *conn = mysql_init(nullptr);
  if (!conn) {
    Logger::getInstance().error(LogCategory::TRANSFER,
                                "MariaDBConnectionManager",
                                "mysql_init() failed");
    return nullptr;
  }

  unsigned int portNum = parsePort(params.port);

  if (mysql_real_connect(conn, params.host.c_str(), params.user.c_str(),
                         params.password.c_str(), params.db.c_str(), portNum,
                         nullptr, 0) == nullptr) {
    std::string errorMsg = mysql_error(conn);
    Logger::getInstance().error(
        LogCategory::TRANSFER, "MariaDBConnectionManager",
        "MariaDB connection failed: " + errorMsg + " (host: " + params.host +
            ", user: " + params.user + ", db: " + params.db +
            ", port: " + std::to_string(portNum) + ")");
    mysql_close(conn);
    return nullptr;
  }

  if (!testConnection(conn)) {
    mysql_close(conn);
    return nullptr;
  }

  setTimeouts(conn);
  return conn;
}

void MariaDBConnectionManager::closeConnection(MYSQL *conn) {
  if (conn) {
    mysql_close(conn);
  }
}

bool MariaDBConnectionManager::testConnection(MYSQL *conn) {
  if (mysql_query(conn, "SELECT 1")) {
    std::string errorMsg = mysql_error(conn);
    Logger::getInstance().error(LogCategory::TRANSFER,
                                "MariaDBConnectionManager",
                                "Connection test failed: " + errorMsg);
    return false;
  }

  MYSQL_RES *testResult = mysql_store_result(conn);
  if (testResult) {
    mysql_free_result(testResult);
  }
  return true;
}

void MariaDBConnectionManager::setTimeouts(MYSQL *conn) {
  std::string timeoutQuery = "SET SESSION wait_timeout = 600" +
                             std::string(", interactive_timeout = 600") +
                             std::string(", net_read_timeout = 600") +
                             std::string(", net_write_timeout = 600") +
                             std::string(", innodb_lock_wait_timeout = 600") +
                             std::string(", lock_wait_timeout = 600");
  mysql_query(conn, timeoutQuery.c_str());
}

MariaDBConnectionManager::ConnectionParams
MariaDBConnectionManager::parseConnectionString(
    const std::string &connectionString) {
  ConnectionParams params;
  std::istringstream ss(connectionString);
  std::string token;

  while (std::getline(ss, token, ';')) {
    auto pos = token.find('=');
    if (pos == std::string::npos)
      continue;

    std::string key = token.substr(0, pos);
    std::string value = token.substr(pos + 1);

    // Trim whitespace
    key.erase(0, key.find_first_not_of(" \t\r\n"));
    key.erase(key.find_last_not_of(" \t\r\n") + 1);
    value.erase(0, value.find_first_not_of(" \t\r\n"));
    value.erase(value.find_last_not_of(" \t\r\n") + 1);

    if (key == "host")
      params.host = value;
    else if (key == "user")
      params.user = value;
    else if (key == "password")
      params.password = value;
    else if (key == "db")
      params.db = value;
    else if (key == "port")
      params.port = value;
  }

  return params;
}

bool MariaDBConnectionManager::validateConnectionParams(
    const ConnectionParams &params) {
  if (params.host.empty() || params.user.empty() || params.db.empty()) {
    Logger::getInstance().error(
        LogCategory::TRANSFER, "MariaDBConnectionManager",
        "Missing required connection parameters (host, user, or db)");
    return false;
  }
  return true;
}

unsigned int MariaDBConnectionManager::parsePort(const std::string &portStr) {
  if (portStr.empty())
    return 3306;

  try {
    unsigned int portNum = std::stoul(portStr);
    if (portNum == 0 || portNum > 65535) {
      Logger::getInstance().warning(
          LogCategory::TRANSFER, "MariaDBConnectionManager",
          "Invalid port number " + portStr + ", using default 3306");
      return 3306;
    }
    return portNum;
  } catch (const std::exception &e) {
    Logger::getInstance().warning(
        LogCategory::TRANSFER, "MariaDBConnectionManager",
        "Could not parse port " + portStr + ": " + std::string(e.what()) +
            ", using default 3306");
    return 3306;
  }
}
