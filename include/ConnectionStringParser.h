#ifndef CONNECTION_STRING_PARSER_H
#define CONNECTION_STRING_PARSER_H

#include <algorithm>
#include <sstream>
#include <string>
#include <unordered_map>

namespace ConnectionParsing {

// Generic connection string parser
class ConnectionStringParser {
public:
  static std::unordered_map<std::string, std::string>
  parse(const std::string &connectionString, char delimiter = ';') {

    std::unordered_map<std::string, std::string> result;
    std::istringstream ss(connectionString);
    std::string token;

    while (std::getline(ss, token, delimiter)) {
      auto pos = token.find('=');
      if (pos == std::string::npos)
        continue;

      std::string key = trim(token.substr(0, pos));
      std::string value = trim(token.substr(pos + 1));

      if (!key.empty() && !value.empty()) {
        result[key] = value;
      }
    }

    return result;
  }

  // Parse space-separated connection strings (PostgreSQL style)
  static std::unordered_map<std::string, std::string>
  parseSpaceSeparated(const std::string &connectionString) {

    std::unordered_map<std::string, std::string> result;
    std::istringstream ss(connectionString);
    std::string token;

    while (ss >> token) {
      auto pos = token.find('=');
      if (pos == std::string::npos)
        continue;

      std::string key = token.substr(0, pos);
      std::string value = token.substr(pos + 1);

      // Remove quotes if present
      if (value.front() == '\'' && value.back() == '\'') {
        value = value.substr(1, value.length() - 2);
      }

      if (!key.empty() && !value.empty()) {
        result[key] = value;
      }
    }

    return result;
  }

private:
  static std::string trim(const std::string &str) {
    const char *whitespace = " \t\r\n";
    size_t start = str.find_first_not_of(whitespace);
    if (start == std::string::npos)
      return "";

    size_t end = str.find_last_not_of(whitespace);
    return str.substr(start, end - start + 1);
  }
};

// MariaDB connection string parser
struct MariaDBConnectionInfo {
  std::string host;
  std::string user;
  std::string password;
  std::string database;
  std::string port;
  unsigned int portNumber = 3306;

  static MariaDBConnectionInfo fromString(const std::string &connectionString) {
    auto params = ConnectionStringParser::parse(connectionString);

    MariaDBConnectionInfo info;
    info.host = params.count("host") ? params["host"] : "";
    info.user = params.count("user") ? params["user"] : "";
    info.password = params.count("password") ? params["password"] : "";
    info.database = params.count("db") ? params["db"] : "";
    info.port = params.count("port") ? params["port"] : "";

    // Parse port number
    if (!info.port.empty()) {
      try {
        info.portNumber = std::stoul(info.port);
      } catch (...) {
        info.portNumber = 3306;
      }
    }

    return info;
  }
};

// MSSQL connection string parser
struct MSSQLConnectionInfo {
  std::string server;
  std::string database;
  std::string uid;
  std::string pwd;
  std::string driver;
  std::string port;
  std::string trustedConnection;

  static MSSQLConnectionInfo fromString(const std::string &connectionString) {
    auto params = ConnectionStringParser::parse(connectionString);

    MSSQLConnectionInfo info;
    info.server = params.count("SERVER") ? params["SERVER"] : "";
    info.database = params.count("DATABASE") ? params["DATABASE"] : "";
    info.uid = params.count("UID") ? params["UID"] : "";
    info.pwd = params.count("PWD") ? params["PWD"] : "";
    info.driver = params.count("DRIVER") ? params["DRIVER"] : "";
    info.port = params.count("PORT") ? params["PORT"] : "";
    info.trustedConnection = params.count("TrustServerCertificate")
                                 ? params["TrustServerCertificate"]
                                 : "";

    return info;
  }
};

// PostgreSQL connection string parser
struct PostgresConnectionInfo {
  std::string host;
  std::string port;
  std::string dbname;
  std::string user;
  std::string password;
  std::string sslmode;

  static PostgresConnectionInfo
  fromString(const std::string &connectionString) {
    auto params = ConnectionStringParser::parseSpaceSeparated(connectionString);

    PostgresConnectionInfo info;
    info.host = params.count("host") ? params["host"] : "";
    info.port = params.count("port") ? params["port"] : "";
    info.dbname = params.count("dbname") ? params["dbname"] : "";
    info.user = params.count("user") ? params["user"] : "";
    info.password = params.count("password") ? params["password"] : "";
    info.sslmode = params.count("sslmode") ? params["sslmode"] : "";

    return info;
  }
};

} // namespace ConnectionParsing

#endif // CONNECTION_STRING_PARSER_H
