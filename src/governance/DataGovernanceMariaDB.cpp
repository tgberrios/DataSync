#include "governance/DataGovernanceMariaDB.h"
#include "core/logger.h"
#include "core/database_config.h"
#include "engines/mariadb_engine.h"
#include "utils/connection_utils.h"
#include <pqxx/pqxx>
#include <algorithm>
#include <sstream>
#include <iomanip>

DataGovernanceMariaDB::DataGovernanceMariaDB(const std::string &connectionString)
    : connectionString_(connectionString) {
}

DataGovernanceMariaDB::~DataGovernanceMariaDB() {
}

std::string DataGovernanceMariaDB::extractServerName(const std::string &connectionString) {
  auto params = ConnectionStringParser::parse(connectionString);
  if (params) {
    return params->host;
  }
  return "UNKNOWN";
}

std::string DataGovernanceMariaDB::extractDatabaseName(const std::string &connectionString) {
  auto params = ConnectionStringParser::parse(connectionString);
  if (params) {
    return params->db;
  }
  return "";
}

std::string DataGovernanceMariaDB::escapeSQL(MYSQL *conn, const std::string &str) {
  if (!conn || str.empty()) {
    return str;
  }
  char *escaped = new char[str.length() * 2 + 1];
  mysql_real_escape_string(conn, escaped, str.c_str(), str.length());
  std::string result(escaped);
  delete[] escaped;
  return result;
}

std::vector<std::vector<std::string>>
DataGovernanceMariaDB::executeQuery(MYSQL *conn, const std::string &query) {
  std::vector<std::vector<std::string>> results;
  if (!conn) {
    return results;
  }

  if (mysql_query(conn, query.c_str())) {
    Logger::error(LogCategory::GOVERNANCE, "DataGovernanceMariaDB",
                  "Query failed: " + std::string(mysql_error(conn)));
    return results;
  }

  MYSQL_RES *res = mysql_store_result(conn);
  if (!res) {
    if (mysql_field_count(conn) > 0) {
      Logger::error(LogCategory::GOVERNANCE, "DataGovernanceMariaDB",
                    "Result fetch failed: " + std::string(mysql_error(conn)));
    }
    return results;
  }

  unsigned int numFields = mysql_num_fields(res);
  MYSQL_ROW row;
  while ((row = mysql_fetch_row(res))) {
    std::vector<std::string> rowData;
    rowData.reserve(numFields);
    for (unsigned int i = 0; i < numFields; ++i) {
      rowData.push_back(row[i] ? row[i] : "");
    }
    results.push_back(std::move(rowData));
  }
  mysql_free_result(res);
  return results;
}

void DataGovernanceMariaDB::collectGovernanceData() {
  Logger::info(LogCategory::GOVERNANCE, "DataGovernanceMariaDB",
               "Starting governance data collection for MariaDB");

  governanceData_.clear();

  try {
    queryTableStats();
    queryServerConfig();
    queryIndexStats();
    queryUserInfo();
    calculateHealthScores();

    Logger::info(LogCategory::GOVERNANCE, "DataGovernanceMariaDB",
                 "Governance data collection completed. Collected " +
                     std::to_string(governanceData_.size()) + " records");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "DataGovernanceMariaDB",
                  "Error collecting governance data: " + std::string(e.what()));
  }
}

void DataGovernanceMariaDB::queryServerConfig() {
  try {
    auto params = ConnectionStringParser::parse(connectionString_);
    if (!params) {
      Logger::error(LogCategory::GOVERNANCE, "DataGovernanceMariaDB",
                    "Invalid connection string");
      return;
    }

    MySQLConnection conn(*params);
    if (!conn.isValid()) {
      Logger::error(LogCategory::GOVERNANCE, "DataGovernanceMariaDB",
                    "Failed to connect to MariaDB for server config");
      return;
    }

    MYSQL *mysqlConn = conn.get();
    std::string serverName = extractServerName(connectionString_);
    std::string databaseName = extractDatabaseName(connectionString_);

    std::string query = "SELECT VERSION(), "
                        "@@innodb_page_size, "
                        "@@innodb_file_per_table, "
                        "@@innodb_flush_log_at_trx_commit, "
                        "@@sync_binlog";

    auto results = executeQuery(mysqlConn, query);

    if (!results.empty() && results[0].size() >= 5) {
      std::string version = results[0][0];
      int innodbPageSize = 0;
      bool innodbFilePerTable = false;
      int innodbFlushLogAtTrxCommit = 0;
      int syncBinlog = 0;

      try {
        if (!results[0][1].empty()) innodbPageSize = std::stoi(results[0][1]);
        if (!results[0][2].empty()) innodbFilePerTable = (results[0][2] == "1" || results[0][2] == "ON");
        if (!results[0][3].empty()) innodbFlushLogAtTrxCommit = std::stoi(results[0][3]);
        if (!results[0][4].empty()) syncBinlog = std::stoi(results[0][4]);
      } catch (const std::exception &e) {
        Logger::warning(LogCategory::GOVERNANCE, "DataGovernanceMariaDB",
                        "Error parsing server config: " + std::string(e.what()));
      }

      std::string innodbVersionQuery = "SHOW VARIABLES LIKE 'innodb_version'";
      auto innodbVersionResults = executeQuery(mysqlConn, innodbVersionQuery);
      std::string innodbVersion = "";
      if (!innodbVersionResults.empty() && innodbVersionResults[0].size() >= 2) {
        innodbVersion = innodbVersionResults[0][1];
      }

      for (auto &data : governanceData_) {
        if (data.database_name == databaseName) {
          data.version = version;
          data.innodb_version = innodbVersion;
          data.innodb_page_size = innodbPageSize;
          data.innodb_file_per_table = innodbFilePerTable;
          data.innodb_flush_log_at_trx_commit = innodbFlushLogAtTrxCommit;
          data.sync_binlog = syncBinlog;
        }
      }
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "DataGovernanceMariaDB",
                  "Error querying server config: " + std::string(e.what()));
  }
}

void DataGovernanceMariaDB::queryTableStats() {
  try {
    auto params = ConnectionStringParser::parse(connectionString_);
    if (!params) {
      return;
    }

    MySQLConnection conn(*params);
    if (!conn.isValid()) {
      Logger::error(LogCategory::GOVERNANCE, "DataGovernanceMariaDB",
                    "Failed to connect to MariaDB for table stats");
      return;
    }

    MYSQL *mysqlConn = conn.get();
    std::string serverName = extractServerName(connectionString_);
    std::string databaseName = extractDatabaseName(connectionString_);

    std::string query = "SELECT "
                        "table_schema, "
                        "table_name, "
                        "table_rows, "
                        "ROUND((data_length + index_length) / 1024 / 1024, 2) AS total_size_mb, "
                        "ROUND(data_length / 1024 / 1024, 2) AS data_size_mb, "
                        "ROUND(index_length / 1024 / 1024, 2) AS index_size_mb, "
                        "ROUND(data_free / 1024 / 1024, 2) AS data_free_mb, "
                        "engine "
                        "FROM information_schema.tables "
                        "WHERE table_schema = '" + escapeSQL(mysqlConn, databaseName) + "' "
                        "AND table_type = 'BASE TABLE'";

    auto results = executeQuery(mysqlConn, query);

    for (const auto &row : results) {
      if (row.size() >= 9) {
        MariaDBGovernanceData data;
        data.server_name = serverName;
        data.database_name = databaseName;
        data.schema_name = row[0];
        data.table_name = row[1];

        try {
          if (!row[2].empty()) data.row_count = std::stoll(row[2]);
          if (!row[3].empty()) data.total_size_mb = std::stod(row[3]);
          if (!row[4].empty()) data.data_size_mb = std::stod(row[4]);
          if (!row[5].empty()) data.index_size_mb = std::stod(row[5]);
          if (!row[6].empty()) data.data_free_mb = std::stod(row[6]);
        } catch (const std::exception &e) {
          Logger::warning(LogCategory::GOVERNANCE, "DataGovernanceMariaDB",
                          "Error parsing table stats: " + std::string(e.what()));
        }

        data.engine = row[8];

        if (data.data_size_mb > 0) {
          data.fragmentation_pct = (data.data_free_mb / data.data_size_mb) * 100.0;
        }

        governanceData_.push_back(data);
      }
    }

    Logger::info(LogCategory::GOVERNANCE, "DataGovernanceMariaDB",
                 "Collected " + std::to_string(results.size()) +
                     " table stats records");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "DataGovernanceMariaDB",
                  "Error querying table stats: " + std::string(e.what()));
  }
}

void DataGovernanceMariaDB::queryIndexStats() {
  try {
    auto params = ConnectionStringParser::parse(connectionString_);
    if (!params) {
      return;
    }

    MySQLConnection conn(*params);
    if (!conn.isValid()) {
      return;
    }

    MYSQL *mysqlConn = conn.get();
    std::string serverName = extractServerName(connectionString_);
    std::string databaseName = extractDatabaseName(connectionString_);

    std::string query = "SELECT "
                        "table_schema, "
                        "table_name, "
                        "index_name, "
                        "GROUP_CONCAT(column_name ORDER BY seq_in_index SEPARATOR ',') AS index_columns, "
                        "non_unique, "
                        "index_type "
                        "FROM information_schema.statistics "
                        "WHERE table_schema = '" + escapeSQL(mysqlConn, databaseName) + "' "
                        "GROUP BY table_schema, table_name, index_name, non_unique, index_type";

    auto results = executeQuery(mysqlConn, query);

    for (const auto &row : results) {
      if (row.size() >= 6) {
        std::string schemaName = row[0];
        std::string tableName = row[1];
        std::string indexName = row[2];

        bool found = false;
        bool found = false;
        for (auto &data : governanceData_) {
          if (data.schema_name == schemaName && data.table_name == tableName && data.index_name.empty()) {
            data.index_name = indexName;
            data.index_columns = row[3];
            data.index_non_unique = (row[4] == "1");
            data.index_type = row[5];
            found = true;
            break;
          }
        }

        if (!found && !indexName.empty() && indexName != "PRIMARY") {
          MariaDBGovernanceData indexData;
          indexData.server_name = serverName;
          indexData.database_name = databaseName;
          indexData.schema_name = schemaName;
          indexData.table_name = tableName;
          indexData.index_name = indexName;
          indexData.index_columns = row[3];
          indexData.index_non_unique = (row[4] == "1");
          indexData.index_type = row[5];

          for (const auto &tableData : governanceData_) {
            if (tableData.schema_name == schemaName && tableData.table_name == tableName && tableData.index_name.empty()) {
              indexData.row_count = tableData.row_count;
              indexData.data_size_mb = tableData.data_size_mb;
              indexData.index_size_mb = tableData.index_size_mb;
              indexData.total_size_mb = tableData.total_size_mb;
              indexData.data_free_mb = tableData.data_free_mb;
              indexData.fragmentation_pct = tableData.fragmentation_pct;
              indexData.engine = tableData.engine;
              indexData.version = tableData.version;
              indexData.innodb_version = tableData.innodb_version;
              indexData.innodb_page_size = tableData.innodb_page_size;
              indexData.innodb_file_per_table = tableData.innodb_file_per_table;
              indexData.innodb_flush_log_at_trx_commit = tableData.innodb_flush_log_at_trx_commit;
              indexData.sync_binlog = tableData.sync_binlog;
              indexData.user_total = tableData.user_total;
              indexData.user_super_count = tableData.user_super_count;
              indexData.user_locked_count = tableData.user_locked_count;
              indexData.user_expired_count = tableData.user_expired_count;
              break;
            }
          }

          governanceData_.push_back(indexData);
        }
      }
    }

    Logger::info(LogCategory::GOVERNANCE, "DataGovernanceMariaDB",
                 "Updated " + std::to_string(results.size()) +
                     " index stats records");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "DataGovernanceMariaDB",
                  "Error querying index stats: " + std::string(e.what()));
  }
}

void DataGovernanceMariaDB::queryUserInfo() {
  try {
    auto params = ConnectionStringParser::parse(connectionString_);
    if (!params) {
      return;
    }

    MySQLConnection conn(*params);
    if (!conn.isValid()) {
      return;
    }

    MYSQL *mysqlConn = conn.get();

    std::string query = "SELECT "
                        "COUNT(*) AS user_total, "
                        "SUM(CASE WHEN Super_priv = 'Y' THEN 1 ELSE 0 END) AS user_super_count, "
                        "SUM(CASE WHEN password_expired = 'Y' THEN 1 ELSE 0 END) AS user_expired_count "
                        "FROM mysql.user";

    auto results = executeQuery(mysqlConn, query);

    if (!results.empty() && results[0].size() >= 3) {
      int userTotal = 0;
      int userSuperCount = 0;
      int userExpiredCount = 0;

      try {
        if (!results[0][0].empty()) userTotal = std::stoi(results[0][0]);
        if (!results[0][1].empty()) userSuperCount = std::stoi(results[0][1]);
        if (!results[0][2].empty()) userExpiredCount = std::stoi(results[0][2]);
      } catch (const std::exception &e) {
        Logger::warning(LogCategory::GOVERNANCE, "DataGovernanceMariaDB",
                        "Error parsing user info: " + std::string(e.what()));
      }

      std::string accountLockedQuery = "SELECT COUNT(*) FROM information_schema.COLUMNS "
                                       "WHERE TABLE_SCHEMA = 'mysql' AND TABLE_NAME = 'user' "
                                       "AND COLUMN_NAME = 'account_locked'";
      auto lockedCheckResults = executeQuery(mysqlConn, accountLockedQuery);
      int userLockedCount = 0;

      if (!lockedCheckResults.empty() && !lockedCheckResults[0][0].empty() && lockedCheckResults[0][0] != "0") {
        std::string lockedQuery = "SELECT SUM(CASE WHEN account_locked = 'Y' THEN 1 ELSE 0 END) "
                                  "FROM mysql.user";
        auto lockedResults = executeQuery(mysqlConn, lockedQuery);
        if (!lockedResults.empty() && !lockedResults[0][0].empty()) {
          try {
            userLockedCount = std::stoi(lockedResults[0][0]);
          } catch (...) {
          }
        }
      }

      for (auto &data : governanceData_) {
        data.user_total = userTotal;
        data.user_super_count = userSuperCount;
        data.user_locked_count = userLockedCount;
        data.user_expired_count = userExpiredCount;
      }
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "DataGovernanceMariaDB",
                  "Error querying user info: " + std::string(e.what()));
  }
}

void DataGovernanceMariaDB::calculateHealthScores() {
  for (auto &data : governanceData_) {
    double score = 100.0;

    if (data.fragmentation_pct > 30.0) {
      double penalty = (data.fragmentation_pct - 30.0) * 0.5;
      score -= std::min(penalty, 40.0);
    }

    if (data.data_free_mb > data.data_size_mb * 0.5) {
      score -= 15.0;
    }

    if (data.user_expired_count > 0) {
      score -= 10.0;
    }

    if (data.user_locked_count > data.user_total * 0.1) {
      score -= 10.0;
    }

    data.health_score = std::max(0.0, std::min(100.0, score));

    if (data.health_score >= 80.0) {
      data.health_status = "HEALTHY";
    } else if (data.health_score >= 60.0) {
      data.health_status = "WARNING";
    } else {
      data.health_status = "CRITICAL";
    }

    if (data.recommendation_summary.empty()) {
      if (data.fragmentation_pct > 30.0) {
        data.recommendation_summary = "Consider optimizing table due to high fragmentation (" +
                                     std::to_string(static_cast<int>(data.fragmentation_pct)) + "%)";
      } else if (data.data_free_mb > data.data_size_mb * 0.5) {
        data.recommendation_summary = "Consider optimizing table to reclaim free space";
      } else if (data.user_expired_count > 0) {
        data.recommendation_summary = "Review expired user accounts";
      }
    }
  }
}

void DataGovernanceMariaDB::storeGovernanceData() {
  if (governanceData_.empty()) {
    Logger::warning(LogCategory::GOVERNANCE, "DataGovernanceMariaDB",
                    "No governance data to store");
    return;
  }

  try {
    pqxx::connection conn(DatabaseConfig::getPostgresConnectionString());
    if (!conn.is_open()) {
      Logger::error(LogCategory::GOVERNANCE, "DataGovernanceMariaDB",
                    "Failed to connect to PostgreSQL");
      return;
    }

    int successCount = 0;
    int errorCount = 0;

    for (const auto &data : governanceData_) {
      if (data.server_name.empty() || data.database_name.empty() || data.schema_name.empty() || data.table_name.empty()) {
        Logger::warning(LogCategory::GOVERNANCE, "DataGovernanceMariaDB",
                        "Skipping record with missing required fields");
        errorCount++;
        continue;
      }

      try {
        pqxx::work txn(conn);

        std::ostringstream insertQuery;
        insertQuery << "INSERT INTO metadata.data_governance_catalog_mariadb ("
                    << "server_name, database_name, schema_name, table_name, "
                    << "index_name, index_columns, index_non_unique, index_type, "
                    << "row_count, data_size_mb, index_size_mb, total_size_mb, "
                    << "data_free_mb, fragmentation_pct, engine, version, "
                    << "innodb_version, innodb_page_size, innodb_file_per_table, "
                    << "innodb_flush_log_at_trx_commit, sync_binlog, "
                    << "table_reads, table_writes, index_reads, "
                    << "user_total, user_super_count, user_locked_count, user_expired_count, "
                    << "access_frequency, health_status, recommendation_summary, "
                    << "snapshot_date"
                    << ") VALUES ("
                    << txn.quote(data.server_name) << ", "
                    << txn.quote(data.database_name) << ", "
                    << txn.quote(data.schema_name) << ", "
                    << txn.quote(data.table_name) << ", "
                    << (data.index_name.empty() ? "NULL" : txn.quote(data.index_name)) << ", "
                    << (data.index_columns.empty() ? "NULL" : txn.quote(data.index_columns)) << ", "
                    << (data.index_non_unique ? "true" : "false") << ", "
                    << (data.index_type.empty() ? "NULL" : txn.quote(data.index_type)) << ", "
                    << (data.row_count == 0 ? "NULL" : std::to_string(data.row_count)) << ", "
                    << (data.data_size_mb == 0.0 ? "NULL" : std::to_string(data.data_size_mb)) << ", "
                    << (data.index_size_mb == 0.0 ? "NULL" : std::to_string(data.index_size_mb)) << ", "
                    << (data.total_size_mb == 0.0 ? "NULL" : std::to_string(data.total_size_mb)) << ", "
                    << (data.data_free_mb == 0.0 ? "NULL" : std::to_string(data.data_free_mb)) << ", "
                    << (data.fragmentation_pct == 0.0 ? "NULL" : std::to_string(data.fragmentation_pct)) << ", "
                    << (data.engine.empty() ? "NULL" : txn.quote(data.engine)) << ", "
                    << (data.version.empty() ? "NULL" : txn.quote(data.version)) << ", "
                    << (data.innodb_version.empty() ? "NULL" : txn.quote(data.innodb_version)) << ", "
                    << (data.innodb_page_size == 0 ? "NULL" : std::to_string(data.innodb_page_size)) << ", "
                    << (data.innodb_file_per_table ? "true" : "false") << ", "
                    << (data.innodb_flush_log_at_trx_commit == 0 ? "NULL" : std::to_string(data.innodb_flush_log_at_trx_commit)) << ", "
                    << (data.sync_binlog == 0 ? "NULL" : std::to_string(data.sync_binlog)) << ", "
                    << (data.table_reads == 0 ? "NULL" : std::to_string(data.table_reads)) << ", "
                    << (data.table_writes == 0 ? "NULL" : std::to_string(data.table_writes)) << ", "
                    << (data.index_reads == 0 ? "NULL" : std::to_string(data.index_reads)) << ", "
                    << (data.user_total == 0 ? "NULL" : std::to_string(data.user_total)) << ", "
                    << (data.user_super_count == 0 ? "NULL" : std::to_string(data.user_super_count)) << ", "
                    << (data.user_locked_count == 0 ? "NULL" : std::to_string(data.user_locked_count)) << ", "
                    << (data.user_expired_count == 0 ? "NULL" : std::to_string(data.user_expired_count)) << ", "
                    << (data.access_frequency.empty() ? "NULL" : txn.quote(data.access_frequency)) << ", "
                    << (data.health_status.empty() ? "NULL" : txn.quote(data.health_status)) << ", "
                    << (data.recommendation_summary.empty() ? "NULL" : txn.quote(data.recommendation_summary)) << ", "
                    << "NOW()"
                    << ") ON CONFLICT DO NOTHING;";

        txn.exec(insertQuery.str());
        txn.commit();
        successCount++;
      } catch (const std::exception &e) {
        Logger::error(LogCategory::GOVERNANCE, "DataGovernanceMariaDB",
                      "Error inserting record: " + std::string(e.what()));
        errorCount++;
        try {
          pqxx::work rollbackTxn(conn);
          rollbackTxn.abort();
        } catch (...) {
        }
      }
    }

    Logger::info(LogCategory::GOVERNANCE, "DataGovernanceMariaDB",
                 "Stored " + std::to_string(successCount) +
                     " governance records in PostgreSQL (errors: " +
                     std::to_string(errorCount) + ")");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "DataGovernanceMariaDB",
                  "Error storing governance data: " + std::string(e.what()));
  }
}

void DataGovernanceMariaDB::generateReport() {
  Logger::info(LogCategory::GOVERNANCE, "DataGovernanceMariaDB",
               "Generating governance report for " +
                   std::to_string(governanceData_.size()) + " records");

  int healthyCount = 0;
  int warningCount = 0;
  int criticalCount = 0;

  for (const auto &data : governanceData_) {
    if (data.health_status == "HEALTHY") {
      healthyCount++;
    } else if (data.health_status == "WARNING") {
      warningCount++;
    } else if (data.health_status == "CRITICAL") {
      criticalCount++;
    }
  }

  Logger::info(LogCategory::GOVERNANCE, "DataGovernanceMariaDB",
               "Report: Healthy=" + std::to_string(healthyCount) +
                   ", Warning=" + std::to_string(warningCount) +
                   ", Critical=" + std::to_string(criticalCount));
}
