#include "MSSQLConnectionManager.h"
#include <algorithm>
#include <sstream>

SQLHDBC MSSQLConnectionManager::getMSSQLConnection(
    const std::string &connectionString) {
  // Validate connection string
  if (connectionString.empty()) {
    Logger::getInstance().error(LogCategory::TRANSFER, "getMSSQLConnection",
                                "Empty connection string provided");
    return nullptr;
  }

  if (!validateConnectionString(connectionString)) {
    return nullptr;
  }

  // Parse connection string to validate required parameters
  std::string server, database, uid, pwd, port;
  std::istringstream ss(connectionString);
  std::string token;
  while (std::getline(ss, token, ';')) {
    auto pos = token.find('=');
    if (pos == std::string::npos)
      continue;
    std::string key = token.substr(0, pos);
    std::string value = token.substr(pos + 1);
    key.erase(0, key.find_first_not_of(" \t\r\n"));
    key.erase(key.find_last_not_of(" \t\r\n") + 1);
    value.erase(0, value.find_first_not_of(" \t\r\n"));
    value.erase(value.find_last_not_of(" \t\r\n") + 1);
    if (key == "SERVER")
      server = value;
    else if (key == "DATABASE")
      database = value;
    else if (key == "UID")
      uid = value;
    else if (key == "PWD")
      pwd = value;
    else if (key == "PORT")
      port = value;
  }

  // Validate required parameters
  if (server.empty() || database.empty() || uid.empty()) {
    Logger::getInstance().error(
        LogCategory::TRANSFER, "getMSSQLConnection",
        "Missing required connection parameters (SERVER, DATABASE, or UID)");
    return nullptr;
  }

  // Validate port number if provided
  if (!port.empty()) {
    try {
      int portNum = std::stoi(port);
      if (portNum <= 0 || portNum > 65535) {
        Logger::getInstance().warning(
            LogCategory::TRANSFER, "getMSSQLConnection",
            "Invalid port number " + port + ", using default 1433");
      }
    } catch (const std::exception &e) {
      Logger::getInstance().warning(LogCategory::TRANSFER, "getMSSQLConnection",
                                    "Could not parse port " + port + ": " +
                                        std::string(e.what()) +
                                        ", using default 1433");
    }
  }

  // Crear nueva conexión para cada consulta para evitar "Connection is busy"
  SQLHENV tempEnv = nullptr;
  SQLHDBC tempConn = nullptr;
  SQLRETURN ret;

  // Crear nueva conexión
  ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &tempEnv);
  if (!SQL_SUCCEEDED(ret)) {
    Logger::getInstance().error(LogCategory::TRANSFER,
                                "Failed to allocate ODBC environment handle");
    return nullptr;
  }

  ret = SQLSetEnvAttr(tempEnv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3,
                      0);
  if (!SQL_SUCCEEDED(ret)) {
    SQLFreeHandle(SQL_HANDLE_ENV, tempEnv);
    Logger::getInstance().error(LogCategory::TRANSFER,
                                "Failed to set ODBC version");
    return nullptr;
  }

  ret = SQLAllocHandle(SQL_HANDLE_DBC, tempEnv, &tempConn);
  if (!SQL_SUCCEEDED(ret)) {
    SQLFreeHandle(SQL_HANDLE_ENV, tempEnv);
    Logger::getInstance().error(LogCategory::TRANSFER,
                                "Failed to allocate ODBC connection handle");
    return nullptr;
  }

  // Set connection timeouts
  SQLSetConnectAttr(tempConn, SQL_ATTR_CONNECTION_TIMEOUT, (SQLPOINTER)30, 0);
  SQLSetConnectAttr(tempConn, SQL_ATTR_LOGIN_TIMEOUT, (SQLPOINTER)30, 0);

  SQLCHAR outConnStr[1024];
  SQLSMALLINT outConnStrLen;
  ret = SQLDriverConnect(tempConn, nullptr, (SQLCHAR *)connectionString.c_str(),
                         SQL_NTS, outConnStr, sizeof(outConnStr),
                         &outConnStrLen, SQL_DRIVER_NOPROMPT);
  if (!SQL_SUCCEEDED(ret)) {
    SQLCHAR sqlState[6], msg[SQL_MAX_MESSAGE_LENGTH];
    SQLINTEGER nativeError;
    SQLSMALLINT msgLen;
    SQLGetDiagRec(SQL_HANDLE_DBC, tempConn, 1, sqlState, &nativeError, msg,
                  sizeof(msg), &msgLen);
    SQLFreeHandle(SQL_HANDLE_DBC, tempConn);
    SQLFreeHandle(SQL_HANDLE_ENV, tempEnv);
    Logger::getInstance().error(
        LogCategory::TRANSFER,
        "Failed to connect to MSSQL: " + std::string((char *)msg) +
            " (server: " + server + ", database: " + database +
            ", uid: " + uid + ")");
    return nullptr;
  }

  // Test connection with a simple query
  if (!testConnection(tempConn)) {
    SQLFreeHandle(SQL_HANDLE_DBC, tempConn);
    SQLFreeHandle(SQL_HANDLE_ENV, tempEnv);
    return nullptr;
  }

  return tempConn;
}

void MSSQLConnectionManager::closeMSSQLConnection(SQLHDBC conn) {
  if (conn) {
    SQLDisconnect(conn);
    SQLFreeHandle(SQL_HANDLE_DBC, conn);
  }
}

std::vector<std::vector<std::string>>
MSSQLConnectionManager::executeQueryMSSQL(SQLHDBC conn,
                                          const std::string &query) {
  std::vector<std::vector<std::string>> results;
  if (!conn) {
    Logger::getInstance().error(LogCategory::TRANSFER,
                                "No valid MSSQL connection");
    return results;
  }

  SQLHSTMT stmt;
  SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, conn, &stmt);
  if (ret != SQL_SUCCESS) {
    Logger::getInstance().error(LogCategory::TRANSFER,
                                "SQLAllocHandle(STMT) failed");
    return results;
  }

  ret = SQLExecDirect(stmt, (SQLCHAR *)query.c_str(), SQL_NTS);
  if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
    SQLCHAR sqlState[6];
    SQLCHAR errorMsg[SQL_MAX_MESSAGE_LENGTH];
    SQLINTEGER nativeError;
    SQLSMALLINT msgLen;

    SQLGetDiagRec(SQL_HANDLE_STMT, stmt, 1, sqlState, &nativeError, errorMsg,
                  sizeof(errorMsg), &msgLen);

    Logger::getInstance().error(
        "SQLExecDirect failed - SQLState: " + std::string((char *)sqlState) +
        ", NativeError: " + std::to_string(nativeError) +
        ", Error: " + std::string((char *)errorMsg) + ", Query: " + query);
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    return results;
  }

  // Get number of columns
  SQLSMALLINT numCols;
  SQLNumResultCols(stmt, &numCols);

  // Fetch rows
  while (SQLFetch(stmt) == SQL_SUCCESS) {
    std::vector<std::string> row;
    for (SQLSMALLINT i = 1; i <= numCols; i++) {
      char buffer[1024];
      SQLLEN len;
      ret = SQLGetData(stmt, i, SQL_C_CHAR, buffer, sizeof(buffer), &len);
      if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
        if (len == SQL_NULL_DATA) {
          row.push_back("NULL");
        } else {
          row.push_back(std::string(buffer, len));
        }
      } else {
        row.push_back("NULL");
      }
    }
    results.push_back(row);
  }

  SQLFreeHandle(SQL_HANDLE_STMT, stmt);
  return results;
}

std::string MSSQLConnectionManager::extractDatabaseName(
    const std::string &connectionString) {
  std::istringstream ss(connectionString);
  std::string token;
  while (std::getline(ss, token, ';')) {
    auto pos = token.find('=');
    if (pos == std::string::npos)
      continue;
    std::string key = token.substr(0, pos);
    std::string value = token.substr(pos + 1);
    if (key == "DATABASE") {
      return value;
    }
  }
  return "master"; // fallback
}

std::string MSSQLConnectionManager::escapeSQL(const std::string &value) {
  std::string escaped = value;
  size_t pos = 0;
  while ((pos = escaped.find("'", pos)) != std::string::npos) {
    escaped.replace(pos, 1, "''");
    pos += 2;
  }
  return escaped;
}

bool MSSQLConnectionManager::validateConnectionString(
    const std::string &connectionString) {
  // Basic validation - check for required parameters
  return connectionString.find("SERVER=") != std::string::npos &&
         connectionString.find("DATABASE=") != std::string::npos &&
         connectionString.find("UID=") != std::string::npos;
}

bool MSSQLConnectionManager::testConnection(SQLHDBC conn) {
  SQLHSTMT testStmt;
  SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, conn, &testStmt);
  if (ret == SQL_SUCCESS) {
    ret = SQLExecDirect(testStmt, (SQLCHAR *)"SELECT 1", SQL_NTS);
    if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
      SQLFreeHandle(SQL_HANDLE_STMT, testStmt);
      return true;
    } else {
      SQLFreeHandle(SQL_HANDLE_STMT, testStmt);
      Logger::getInstance().error(LogCategory::TRANSFER, "testConnection",
                                  "Connection test failed");
      return false;
    }
  }
  return false;
}
