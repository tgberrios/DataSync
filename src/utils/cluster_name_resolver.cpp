#include "utils/cluster_name_resolver.h"
#include "core/Config.h"
#include "core/logger.h"
#include "engines/mariadb_engine.h"
#include "engines/mssql_engine.h"
#include "utils/connection_utils.h"
#include "utils/string_utils.h"
#include <pqxx/pqxx>

// Main entry point for resolving cluster name from a connection string and
// database engine. Attempts to resolve the cluster name using engine-specific
// methods (resolveMariaDB, resolveMSSQL, resolvePostgreSQL). If those methods
// fail or return empty, falls back to extracting the hostname from the
// connection string and deriving the cluster name from the hostname using
// getClusterNameFromHostname. Returns the resolved cluster name, or an empty
// string if resolution fails. Used to identify which cluster/database instance
// a connection belongs to for catalog organization.
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
    std::string hostname = extractHostname(connectionString);
    clusterName = getClusterNameFromHostname(hostname);
  }

  return clusterName;
}

// Resolves cluster name for MariaDB by querying the database server for its
// hostname. Parses the connection string, creates a MySQL connection, and
// executes "SELECT @@hostname" to get the server hostname. Converts the
// result to uppercase and returns it. Returns an empty string if connection
// fails, query fails, or any exception occurs. This method provides accurate
// cluster identification by querying the database directly rather than
// parsing connection strings.
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

    return StringUtils::toUpper(name);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "ClusterNameResolver",
                  "Error resolving MariaDB cluster name: " +
                      std::string(e.what()));
    return "";
  } catch (...) {
    Logger::error(LogCategory::DATABASE, "ClusterNameResolver",
                  "Unknown error resolving MariaDB cluster name");
    return "";
  }
}

// Resolves cluster name for MSSQL by querying the database server for its
// machine name. Creates an ODBC connection and attempts two queries:
// "SELECT CAST(SERVERPROPERTY('MachineName') AS VARCHAR(128))" first, and if
// that fails or returns NULL, tries "SELECT CAST(@@SERVERNAME AS
// VARCHAR(128))". Converts the result to uppercase and returns it. Returns an
// empty string if connection fails, queries fail, or any exception occurs.
// Properly allocates and frees ODBC statement handles. This method provides
// accurate cluster identification by querying the database directly.
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
    return StringUtils::toUpper(name);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "ClusterNameResolver",
                  "Error resolving MSSQL cluster name: " +
                      std::string(e.what()));
    return "";
  } catch (...) {
    Logger::error(LogCategory::DATABASE, "ClusterNameResolver",
                  "Unknown error resolving MSSQL cluster name");
    return "";
  }
}

// Resolves cluster name for PostgreSQL by extracting the hostname from the
// connection string and deriving the cluster name from it. Parses the
// connection string to get the host parameter, then calls
// getClusterNameFromHostname to derive the cluster name. Returns an empty
// string if parsing fails. This is a simpler approach than querying the
// database directly, relying on hostname patterns to identify clusters.
std::string
ClusterNameResolver::resolvePostgreSQL(const std::string &connectionString) {
  auto params = ConnectionStringParser::parse(connectionString);
  if (!params)
    return "";
  return getClusterNameFromHostname(params->host);
}

// Extracts the hostname from a connection string by parsing it using
// ConnectionStringParser. Returns the host parameter from the parsed
// connection string, or an empty string if parsing fails. This is a helper
// function used as a fallback when engine-specific resolution methods fail.
std::string
ClusterNameResolver::extractHostname(const std::string &connectionString) {
  auto params = ConnectionStringParser::parse(connectionString);
  return params ? params->host : "";
}

// Derives a cluster name from a hostname by analyzing patterns in the
// hostname string. Converts hostname to lowercase for pattern matching.
// Recognizes common environment patterns: "prod"/"production" -> PRODUCTION,
// "staging"/"stage" -> STAGING, "dev"/"development" -> DEVELOPMENT,
// "test"/"testing" -> TESTING, "local"/"localhost" -> LOCAL, "uat" -> UAT,
// "qa" -> QA. Also recognizes cluster patterns: if hostname contains
// "cluster", returns the substring from "cluster" onwards in uppercase. If
// hostname contains "db-", returns the substring from "db-" onwards in
// uppercase. If no patterns match, returns the entire hostname in uppercase.
// Returns an empty string if hostname is empty. This heuristic-based
// approach works well for standardized hostname naming conventions.
std::string
ClusterNameResolver::getClusterNameFromHostname(const std::string &hostname) {
  if (hostname.empty())
    return "";

  std::string lower = StringUtils::toLower(hostname);

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
    return StringUtils::toUpper(lower.substr(pos));
  }

  if (lower.find("db-") != std::string::npos) {
    size_t pos = lower.find("db-");
    return StringUtils::toUpper(lower.substr(pos));
  }

  return StringUtils::toUpper(hostname);
}
