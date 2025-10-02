#include "DatabaseConnectionManager.h"
#include "logger.h"
#include <algorithm>
#include <sstream>

ConnectionInfo DatabaseConnectionManager::parseMariaDBConnection(const std::string &connectionString) {
  ConnectionInfo info;
  
  std::istringstream ss(connectionString);
  std::string token;
  while (std::getline(ss, token, ';')) {
    auto pos = token.find('=');
    if (pos == std::string::npos) continue;
    
    std::string key = trim(token.substr(0, pos));
    std::string value = trim(token.substr(pos + 1));
    
    if (key == "host") {
      info.host = value;
    } else if (key == "user") {
      info.username = value;
    } else if (key == "password") {
      info.password = value;
    } else if (key == "db") {
      info.database = value;
    } else if (key == "port") {
      info.port = value;
    }
  }
  
  return info;
}

ConnectionInfo DatabaseConnectionManager::parseMSSQLConnection(const std::string &connectionString) {
  ConnectionInfo info;
  
  std::istringstream ss(connectionString);
  std::string token;
  while (std::getline(ss, token, ';')) {
    auto pos = token.find('=');
    if (pos == std::string::npos) continue;
    
    std::string key = trim(token.substr(0, pos));
    std::string value = trim(token.substr(pos + 1));
    
    if (key == "SERVER" || key == "server") {
      info.host = value;
    } else if (key == "DATABASE" || key == "database") {
      info.database = value;
    } else if (key == "UID" || key == "username") {
      info.username = value;
    } else if (key == "PWD" || key == "password") {
      info.password = value;
    } else if (key == "PORT" || key == "port") {
      info.port = value;
    } else if (key == "DRIVER" || key == "driver") {
      info.driver = value;
    } else if (key == "Trusted_Connection" || key == "trusted_connection") {
      info.trusted_connection = (value == "yes" || value == "true");
    }
  }
  
  return info;
}

ConnectionInfo DatabaseConnectionManager::parsePostgreSQLConnection(const std::string &connectionString) {
  ConnectionInfo info;
  
  // PostgreSQL connection string format: postgresql://user:password@host:port/database
  if (connectionString.find("postgresql://") == 0) {
    std::string connStr = connectionString.substr(13); // Remove "postgresql://"
    
    auto atPos = connStr.find('@');
    if (atPos != std::string::npos) {
      std::string userPass = connStr.substr(0, atPos);
      std::string hostDb = connStr.substr(atPos + 1);
      
      auto colonPos = userPass.find(':');
      if (colonPos != std::string::npos) {
        info.username = userPass.substr(0, colonPos);
        info.password = userPass.substr(colonPos + 1);
      }
      
      auto slashPos = hostDb.find('/');
      if (slashPos != std::string::npos) {
        std::string hostPort = hostDb.substr(0, slashPos);
        info.database = hostDb.substr(slashPos + 1);
        
        auto portPos = hostPort.find(':');
        if (portPos != std::string::npos) {
          info.host = hostPort.substr(0, portPos);
          info.port = hostPort.substr(portPos + 1);
        } else {
          info.host = hostPort;
        }
      }
    }
  }
  
  return info;
}

MYSQL* DatabaseConnectionManager::establishMariaDBConnection(const ConnectionInfo &info) {
  if (!validateMariaDBConnection(info)) {
    return nullptr;
  }
  
  MYSQL *conn = mysql_init(nullptr);
  if (!conn) {
    logConnectionError("MariaDB", "mysql_init() failed");
    return nullptr;
  }
  
  // Set connection timeout
  const unsigned int timeout_seconds = 30;
  mysql_options(conn, MYSQL_OPT_CONNECT_TIMEOUT, (const char *)&timeout_seconds);
  mysql_options(conn, MYSQL_OPT_READ_TIMEOUT, (const char *)&timeout_seconds);
  mysql_options(conn, MYSQL_OPT_WRITE_TIMEOUT, (const char *)&timeout_seconds);
  
  unsigned int portNum = 3306;
  if (!info.port.empty()) {
    try {
      portNum = std::stoul(info.port);
      if (portNum == 0 || portNum > 65535) {
        logConnectionError("MariaDB", "Invalid port number: " + info.port);
        mysql_close(conn);
        return nullptr;
      }
    } catch (const std::exception &e) {
      logConnectionError("MariaDB", "Invalid port format: " + info.port + " - " + e.what());
      mysql_close(conn);
      return nullptr;
    }
  }
  
  if (mysql_real_connect(conn, info.host.c_str(), info.username.c_str(), 
                        info.password.c_str(), info.database.c_str(), 
                        portNum, nullptr, 0) == nullptr) {
    logConnectionError("MariaDB", "Connection failed: " + std::string(mysql_error(conn)));
    mysql_close(conn);
    return nullptr;
  }
  
  return conn;
}

SQLHDBC DatabaseConnectionManager::establishMSSQLConnection(const ConnectionInfo &info) {
  if (!validateMSSQLConnection(info)) {
    return nullptr;
  }
  
  SQLHENV env;
  SQLHDBC conn;
  SQLRETURN ret;
  
  ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
  if (ret != SQL_SUCCESS) {
    logConnectionError("MSSQL", "Failed to allocate ODBC environment");
    return nullptr;
  }
  
  ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
  if (ret != SQL_SUCCESS) {
    logConnectionError("MSSQL", "Failed to set ODBC version");
    SQLFreeHandle(SQL_HANDLE_ENV, env);
    return nullptr;
  }
  
  ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);
  if (ret != SQL_SUCCESS) {
    logConnectionError("MSSQL", "Failed to allocate ODBC connection");
    SQLFreeHandle(SQL_HANDLE_ENV, env);
    return nullptr;
  }
  
  // Set connection timeout
  SQLSetConnectAttr(conn, SQL_ATTR_CONNECTION_TIMEOUT, (SQLPOINTER)30, 0);
  SQLSetConnectAttr(conn, SQL_ATTR_LOGIN_TIMEOUT, (SQLPOINTER)30, 0);
  
  // Build connection string
  std::string connStr = "DRIVER=" + info.driver + ";";
  connStr += "SERVER=" + info.host + ";";
  connStr += "DATABASE=" + info.database + ";";
  if (!info.port.empty()) {
    connStr += "PORT=" + info.port + ";";
  }
  if (info.trusted_connection) {
    connStr += "Trusted_Connection=yes;";
  } else {
    connStr += "UID=" + info.username + ";";
    connStr += "PWD=" + info.password + ";";
  }
  connStr += "TrustServerCertificate=yes;";
  
  ret = SQLDriverConnect(conn, nullptr, (SQLCHAR *)connStr.c_str(),
                        SQL_NTS, nullptr, 0, nullptr, SQL_DRIVER_NOPROMPT);
  if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
    SQLCHAR sqlstate[SQL_SQLSTATE_SIZE + 1];
    SQLCHAR message[SQL_MAX_MESSAGE_LENGTH + 1];
    SQLSMALLINT i = 1;
    SQLINTEGER native_error;
    SQLSMALLINT msg_len;
    std::string error_details;
    
    while (SQLGetDiagRec(SQL_HANDLE_DBC, conn, i++, sqlstate, &native_error,
                        message, sizeof(message), &msg_len) == SQL_SUCCESS) {
      error_details += "SQLSTATE: " + std::string((char *)sqlstate) +
                      ", Native Error: " + std::to_string(native_error) +
                      ", Message: " + std::string((char *)message) + "\n";
    }
    
    logConnectionError("MSSQL", "Connection failed. Connection string: " + connStr + "\nDetailed errors:\n" + error_details);
    SQLFreeHandle(SQL_HANDLE_DBC, conn);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
    return nullptr;
  }
  
  SQLFreeHandle(SQL_HANDLE_ENV, env);
  return conn;
}

pqxx::connection* DatabaseConnectionManager::establishPostgreSQLConnection(const ConnectionInfo &info) {
  if (!validatePostgreSQLConnection(info)) {
    return nullptr;
  }
  
  try {
    std::string connStr = "postgresql://" + info.username + ":" + info.password + 
                         "@" + info.host;
    if (!info.port.empty()) {
      connStr += ":" + info.port;
    }
    connStr += "/" + info.database;
    
    return new pqxx::connection(connStr);
  } catch (const std::exception &e) {
    logConnectionError("PostgreSQL", "Connection failed: " + std::string(e.what()));
    return nullptr;
  }
}

bool DatabaseConnectionManager::validateMariaDBConnection(const ConnectionInfo &info) {
  if (info.host.empty() || info.username.empty() || info.database.empty()) {
    logConnectionError("MariaDB", "Missing required connection parameters (host, username, database)");
    return false;
  }
  return true;
}

bool DatabaseConnectionManager::validateMSSQLConnection(const ConnectionInfo &info) {
  if (info.host.empty() || info.database.empty()) {
    logConnectionError("MSSQL", "Missing required connection parameters (host, database)");
    return false;
  }
  if (!info.trusted_connection && (info.username.empty() || info.password.empty())) {
    logConnectionError("MSSQL", "Missing required connection parameters (username, password) for non-trusted connection");
    return false;
  }
  return true;
}

bool DatabaseConnectionManager::validatePostgreSQLConnection(const ConnectionInfo &info) {
  if (info.host.empty() || info.database.empty() || info.username.empty() || info.password.empty()) {
    logConnectionError("PostgreSQL", "Missing required connection parameters (host, database, username, password)");
    return false;
  }
  return true;
}

std::string DatabaseConnectionManager::trim(const std::string &str) {
  size_t first = str.find_first_not_of(' ');
  if (first == std::string::npos) return "";
  size_t last = str.find_last_not_of(' ');
  return str.substr(first, (last - first + 1));
}

void DatabaseConnectionManager::logConnectionError(const std::string &dbType, const std::string &error) {
  Logger::error(LogCategory::DDL_EXPORT, "DatabaseConnectionManager", 
                dbType + " connection error: " + error);
}
