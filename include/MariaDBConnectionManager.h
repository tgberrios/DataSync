#ifndef MARIADBCONNECTIONMANAGER_H
#define MARIADBCONNECTIONMANAGER_H

#include "logger.h"
#include <mysql/mysql.h>
#include <string>

class MariaDBConnectionManager {
public:
  MariaDBConnectionManager() = default;
  ~MariaDBConnectionManager() = default;

  MYSQL *getConnection(const std::string &connectionString);
  void closeConnection(MYSQL *conn);
  bool testConnection(MYSQL *conn);
  void setTimeouts(MYSQL *conn);

private:
  struct ConnectionParams {
    std::string host;
    std::string user;
    std::string password;
    std::string db;
    std::string port;
  };

  ConnectionParams parseConnectionString(const std::string &connectionString);
  bool validateConnectionParams(const ConnectionParams &params);
  unsigned int parsePort(const std::string &portStr);
};

#endif // MARIADBCONNECTIONMANAGER_H
