#include "utils/cluster_name_resolver.h"
#include "core/Config.h"
#include "core/logger.h"
#include "engines/mariadb_engine.h"
#include "engines/mssql_engine.h"
#include "utils/connection_utils.h"
#include <algorithm>
#include <pqxx/pqxx>

std::string ClusterNameResolver::resolve(const std::string &connectionString,
                                         const std::string &dbEngine) {
  std::string clusterName;

  if (dbEngine == "MariaDB")
    clusterName = resolveMariaDB(connectionString);
  else if (dbEngine == "MSSQL")
    clusterName = resolveMSSQL(connectionString);
  else if (dbEngine == "PostgreSQL")
    clusterName = resolvePostgreSQL(connectionString);

  if (clusterName.empty()) {
    std::string hostname = extractHostname(connectionString, dbEngine);
    clusterName = getClusterNameFromHostname(hostname);
  }

  return clusterName;
}

std::string
ClusterNameResolver::resolveMariaDB(const std::string &connectionString) {
  try {
    auto params = ConnectionStringParser::parse(connectionString);
    if (!params)
      return "";

    MySQLConnection conn(*params);
    if (!conn.isValid())
      return "";

    if (mysql_query(conn.get(), "SELECT @@hostname"))
      return "";

    MYSQL_RES *res = mysql_store_result(conn.get());
    if (!res)
      return "";

    MYSQL_ROW row = mysql_fetch_row(res);
    std::string name;
    if (row && row[0])
      name = row[0];

    mysql_free_result(res);

    std::transform(name.begin(), name.end(), name.begin(), ::toupper);
    return name;
  } catch (...) {
    return "";
  }
}

std::string
ClusterNameResolver::resolveMSSQL(const std::string &connectionString) {
  try {
    ODBCConnection conn(connectionString);
    if (!conn.isValid())
      return "";

    SQLHSTMT stmt;
    if (!SQL_SUCCEEDED(SQLAllocHandle(SQL_HANDLE_STMT, conn.getDbc(), &stmt)))
      return "";

    std::string name;
    if (SQL_SUCCEEDED(SQLExecDirect(
            stmt,
            (SQLCHAR *)"SELECT CAST(SERVERPROPERTY('MachineName') AS "
                       "VARCHAR(128)) AS name",
            SQL_NTS))) {
      if (SQLFetch(stmt) == SQL_SUCCESS) {
        char buffer[DatabaseDefaults::BUFFER_SIZE];
        SQLLEN len;
        if (SQL_SUCCEEDED(SQLGetData(stmt, 1, SQL_C_CHAR, buffer,
                                     sizeof(buffer), &len)) &&
            len != SQL_NULL_DATA) {
          name = std::string(buffer, len);
        }
      }
    }

    if (name.empty() || name == "NULL") {
      SQLFreeHandle(SQL_HANDLE_STMT, stmt);
      if (!SQL_SUCCEEDED(SQLAllocHandle(SQL_HANDLE_STMT, conn.getDbc(), &stmt)))
        return "";

      if (SQL_SUCCEEDED(SQLExecDirect(
              stmt,
              (SQLCHAR *)"SELECT CAST(@@SERVERNAME AS VARCHAR(128)) AS name",
              SQL_NTS))) {
        if (SQLFetch(stmt) == SQL_SUCCESS) {
          char buffer[DatabaseDefaults::BUFFER_SIZE];
          SQLLEN len;
          if (SQL_SUCCEEDED(SQLGetData(stmt, 1, SQL_C_CHAR, buffer,
                                       sizeof(buffer), &len)) &&
              len != SQL_NULL_DATA) {
            name = std::string(buffer, len);
          }
        }
      }
    }

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    std::transform(name.begin(), name.end(), name.begin(), ::toupper);
    return name;
  } catch (...) {
    return "";
  }
}

std::string
ClusterNameResolver::resolvePostgreSQL(const std::string &connectionString) {
  try {
    pqxx::connection conn(connectionString);
    if (!conn.is_open())
      return "";

    pqxx::work txn(conn);
    auto result = txn.exec("SELECT current_setting('cluster_name', true)");
    std::string name;

    if (!result.empty() && !result[0][0].is_null())
      name = result[0][0].as<std::string>();

    if (name.empty()) {
      auto result2 = txn.exec("SELECT inet_server_addr()::text");
      if (!result2.empty() && !result2[0][0].is_null())
        name = result2[0][0].as<std::string>();
    }

    txn.commit();
    std::transform(name.begin(), name.end(), name.begin(), ::toupper);
    return name;
  } catch (...) {
    return "";
  }
}

std::string
ClusterNameResolver::extractHostname(const std::string &connectionString,
                                     const std::string &dbEngine) {
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

    if ((dbEngine == "MariaDB" && key == "host") ||
        (dbEngine == "MSSQL" && key == "SERVER") ||
        (dbEngine == "PostgreSQL" && key == "host")) {
      return value;
    }
  }
  return "";
}

std::string
ClusterNameResolver::getClusterNameFromHostname(const std::string &hostname) {
  if (hostname.empty())
    return "";

  std::string lower = hostname;
  std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);

  if (lower.find("prod") != std::string::npos ||
      lower.find("production") != std::string::npos)
    return "PRODUCTION";
  if (lower.find("staging") != std::string::npos ||
      lower.find("stage") != std::string::npos)
    return "STAGING";
  if (lower.find("dev") != std::string::npos ||
      lower.find("development") != std::string::npos)
    return "DEVELOPMENT";
  if (lower.find("test") != std::string::npos ||
      lower.find("testing") != std::string::npos)
    return "TESTING";
  if (lower.find("local") != std::string::npos ||
      lower.find("localhost") != std::string::npos)
    return "LOCAL";
  if (lower.find("uat") != std::string::npos)
    return "UAT";
  if (lower.find("qa") != std::string::npos)
    return "QA";

  if (lower.find("cluster") != std::string::npos) {
    size_t pos = lower.find("cluster");
    std::string part = lower.substr(pos);
    std::transform(part.begin(), part.end(), part.begin(), ::toupper);
    return part;
  }

  if (lower.find("db-") != std::string::npos) {
    size_t pos = lower.find("db-");
    std::string part = lower.substr(pos);
    std::transform(part.begin(), part.end(), part.begin(), ::toupper);
    return part;
  }

  std::string upper = hostname;
  std::transform(upper.begin(), upper.end(), upper.begin(), ::toupper);
  return upper;
}
