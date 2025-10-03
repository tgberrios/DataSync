#ifndef DATABASECONNECTIONMANAGER_H
#define DATABASECONNECTIONMANAGER_H

#include <mysql/mysql.h>
#include <pqxx/pqxx>
#include <sql.h>
#include <sqlext.h>
#include <string>

struct ConnectionInfo {
  std::string host;
  std::string database;
  std::string username;
  std::string password;
  std::string port;
  std::string driver;
  bool trusted_connection = false;
};

class DatabaseConnectionManager {
public:
  DatabaseConnectionManager() = default;
  ~DatabaseConnectionManager() = default;

  // Connection parsing
  ConnectionInfo parseMariaDBConnection(const std::string &connectionString);
  ConnectionInfo parseMSSQLConnection(const std::string &connectionString);
  ConnectionInfo parsePostgreSQLConnection(const std::string &connectionString);

  // Connection establishment
  MYSQL* establishMariaDBConnection(const ConnectionInfo &info);
  SQLHDBC establishMSSQLConnection(const ConnectionInfo &info);
  pqxx::connection* establishPostgreSQLConnection(const ConnectionInfo &info);

  // Connection validation
  bool validateMariaDBConnection(const ConnectionInfo &info);
  bool validateMSSQLConnection(const ConnectionInfo &info);
  bool validatePostgreSQLConnection(const ConnectionInfo &info);

private:
  std::string trim(const std::string &str);
  void logConnectionError(const std::string &dbType, const std::string &error);
};

#endif // DATABASECONNECTIONMANAGER_H
