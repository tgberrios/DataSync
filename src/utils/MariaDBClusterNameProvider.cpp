#include "utils/MariaDBClusterNameProvider.h"
#include "core/logger.h"
#include "engines/mariadb_engine.h"
#include "utils/connection_utils.h"
#include "utils/string_utils.h"
#include <mysql/mysql.h>

std::string
MariaDBClusterNameProvider::resolve(const std::string &connectionString) {
  try {
    auto params = ConnectionStringParser::parse(connectionString);
    if (!params)
      return "";

    MySQLConnection conn(*params);
    if (!conn.isValid())
      return "";

    if (mysql_query(conn.get(), "SELECT @@hostname")) {
      Logger::error(LogCategory::DATABASE, "MariaDBClusterNameProvider",
                    "MySQL query failed: " +
                        std::string(mysql_error(conn.get())));
      return "";
    }

    MYSQL_RES *res = mysql_store_result(conn.get());
    if (!res) {
      if (mysql_field_count(conn.get()) > 0) {
        Logger::error(LogCategory::DATABASE, "MariaDBClusterNameProvider",
                      "Failed to store result: " +
                          std::string(mysql_error(conn.get())));
      }
      return "";
    }

    std::string name;
    try {
      MYSQL_ROW row = mysql_fetch_row(res);
      if (row && row[0])
        name = row[0];
    } catch (...) {
      mysql_free_result(res);
      throw;
    }
    mysql_free_result(res);

    return StringUtils::toUpper(name);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "MariaDBClusterNameProvider",
                  "Error resolving MariaDB cluster name: " +
                      std::string(e.what()));
    return "";
  } catch (...) {
    Logger::error(LogCategory::DATABASE, "MariaDBClusterNameProvider",
                  "Unknown error resolving MariaDB cluster name");
    return "";
  }
}
