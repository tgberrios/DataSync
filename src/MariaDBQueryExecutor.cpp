#include "MariaDBQueryExecutor.h"
#include <algorithm>

std::vector<std::vector<std::string>>
MariaDBQueryExecutor::executeQuery(MYSQL *conn, const std::string &query) {
  std::vector<std::vector<std::string>> results;
  if (!conn) {
    Logger::getInstance().warning(LogCategory::TRANSFER, "MariaDBQueryExecutor",
                                  "No valid MariaDB connection");
    return results;
  }

  if (mysql_query(conn, query.c_str())) {
    Logger::getInstance().warning(
        LogCategory::TRANSFER, "MariaDBQueryExecutor",
        "Query execution failed: " + std::string(mysql_error(conn)) +
            " for query: " + query.substr(0, 100) + "...");
    return results;
  }

  MYSQL_RES *res = mysql_store_result(conn);
  if (!res) {
    if (mysql_field_count(conn) > 0) {
      Logger::getInstance().warning(
          LogCategory::TRANSFER, "MariaDBQueryExecutor",
          "mysql_store_result() failed: " + std::string(mysql_error(conn)));
    }
    return results;
  }

  unsigned int num_fields = mysql_num_fields(res);
  MYSQL_ROW row;
  while ((row = mysql_fetch_row(res))) {
    std::vector<std::string> rowData;
    rowData.reserve(num_fields);
    for (unsigned int i = 0; i < num_fields; ++i) {
      rowData.push_back(row[i] ? row[i] : "");
    }
    results.push_back(rowData);
  }
  mysql_free_result(res);
  return results;
}

std::vector<std::string>
MariaDBQueryExecutor::getPrimaryKeyColumns(MYSQL *conn,
                                           const std::string &schema_name,
                                           const std::string &table_name) {
  std::vector<std::string> pkColumns;

  if (!conn || schema_name.empty() || table_name.empty()) {
    Logger::getInstance().error(LogCategory::TRANSFER, "MariaDBQueryExecutor",
                                "Invalid parameters for getPrimaryKeyColumns");
    return pkColumns;
  }

  std::string query = "SELECT COLUMN_NAME "
                      "FROM information_schema.key_column_usage "
                      "WHERE table_schema = '" +
                      escapeSQL(schema_name) +
                      "' "
                      "AND table_name = '" +
                      escapeSQL(table_name) +
                      "' "
                      "AND constraint_name = 'PRIMARY' "
                      "ORDER BY ordinal_position;";

  std::vector<std::vector<std::string>> results = executeQuery(conn, query);

  for (const auto &row : results) {
    if (!row.empty()) {
      std::string colName = row[0];
      std::transform(colName.begin(), colName.end(), colName.begin(),
                     ::tolower);
      pkColumns.push_back(colName);
    }
  }

  return pkColumns;
}

std::vector<std::vector<std::string>>
MariaDBQueryExecutor::getTableColumns(MYSQL *conn,
                                      const std::string &schema_name,
                                      const std::string &table_name) {
  std::string query = "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, "
                      "COLUMN_KEY, EXTRA, CHARACTER_MAXIMUM_LENGTH "
                      "FROM information_schema.columns "
                      "WHERE table_schema = '" +
                      escapeSQL(schema_name) +
                      "' "
                      "AND table_name = '" +
                      escapeSQL(table_name) + "';";

  return executeQuery(conn, query);
}

std::vector<std::vector<std::string>>
MariaDBQueryExecutor::getTableIndexes(MYSQL *conn,
                                      const std::string &schema_name,
                                      const std::string &table_name) {
  std::string query = "SELECT INDEX_NAME, NON_UNIQUE, COLUMN_NAME "
                      "FROM information_schema.statistics "
                      "WHERE table_schema = '" +
                      escapeSQL(schema_name) +
                      "' "
                      "AND table_name = '" +
                      escapeSQL(table_name) +
                      "' "
                      "AND INDEX_NAME != 'PRIMARY' "
                      "ORDER BY INDEX_NAME, SEQ_IN_INDEX;";

  return executeQuery(conn, query);
}

size_t MariaDBQueryExecutor::getTableRowCount(MYSQL *conn,
                                              const std::string &schema_name,
                                              const std::string &table_name) {
  std::string query = "SELECT COUNT(*) FROM `" + escapeSQL(schema_name) +
                      "`.`" + escapeSQL(table_name) + "`;";

  std::vector<std::vector<std::string>> results = executeQuery(conn, query);

  if (results.empty() || results[0].empty() || results[0][0].empty()) {
    return 0;
  }

  try {
    std::string countStr = results[0][0];
    if (!countStr.empty() &&
        std::all_of(countStr.begin(), countStr.end(), ::isdigit)) {
      return std::stoul(countStr);
    }
  } catch (const std::exception &e) {
    Logger::getInstance().warning(LogCategory::TRANSFER, "MariaDBQueryExecutor",
                                  "Could not parse row count: " +
                                      std::string(e.what()));
  }

  return 0;
}

std::string MariaDBQueryExecutor::escapeSQL(const std::string &value) {
  if (value.empty())
    return "";

  std::string escaped = value;
  size_t pos = 0;
  while ((pos = escaped.find("'", pos)) != std::string::npos) {
    escaped.replace(pos, 1, "''");
    pos += 2;
  }
  return escaped;
}
