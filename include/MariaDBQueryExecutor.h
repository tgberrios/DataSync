#ifndef MARIADBQUERYEXECUTOR_H
#define MARIADBQUERYEXECUTOR_H

#include "logger.h"
#include <mysql/mysql.h>
#include <string>
#include <vector>

class MariaDBQueryExecutor {
public:
  MariaDBQueryExecutor() = default;
  ~MariaDBQueryExecutor() = default;

  std::vector<std::vector<std::string>> executeQuery(MYSQL *conn,
                                                     const std::string &query);
  std::vector<std::string> getPrimaryKeyColumns(MYSQL *conn,
                                                const std::string &schema_name,
                                                const std::string &table_name);
  std::vector<std::vector<std::string>>
  getTableColumns(MYSQL *conn, const std::string &schema_name,
                  const std::string &table_name);
  std::vector<std::vector<std::string>>
  getTableIndexes(MYSQL *conn, const std::string &schema_name,
                  const std::string &table_name);
  size_t getTableRowCount(MYSQL *conn, const std::string &schema_name,
                          const std::string &table_name);

private:
  std::string escapeSQL(const std::string &value);
};

#endif // MARIADBQUERYEXECUTOR_H
