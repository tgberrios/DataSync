#include "governance/DataGovernanceMSSQL.h"
#include "core/logger.h"
#include "core/database_config.h"
#include "core/database_defaults.h"
#include "engines/mssql_engine.h"
#include "utils/connection_utils.h"
#include <pqxx/pqxx>
#include <algorithm>
#include <sstream>
#include <iomanip>
#include <sql.h>
#include <sqlext.h>

DataGovernanceMSSQL::DataGovernanceMSSQL(const std::string &connectionString)
    : connectionString_(connectionString) {
}

DataGovernanceMSSQL::~DataGovernanceMSSQL() {
}

std::string DataGovernanceMSSQL::extractServerName(const std::string &connectionString) {
  auto params = ConnectionStringParser::parse(connectionString);
  if (params) {
    return params->host;
  }
  return "UNKNOWN";
}

std::string DataGovernanceMSSQL::extractDatabaseName(const std::string &connectionString) {
  auto params = ConnectionStringParser::parse(connectionString);
  if (params) {
    return params->db;
  }
  return "master";
}

std::string DataGovernanceMSSQL::escapeSQL(const std::string &str) {
  std::string escaped = str;
  size_t pos = 0;
  while ((pos = escaped.find('\'', pos)) != std::string::npos) {
    escaped.replace(pos, 1, "''");
    pos += 2;
  }
  return escaped;
}

std::vector<std::vector<std::string>>
DataGovernanceMSSQL::executeQueryMSSQL(SQLHDBC conn, const std::string &query) {
  std::vector<std::vector<std::string>> results;
  if (!conn) {
    return results;
  }

  SQLHSTMT stmt;
  SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, conn, &stmt);
  if (ret != SQL_SUCCESS) {
    Logger::error(LogCategory::GOVERNANCE, "DataGovernanceMSSQL",
                  "SQLAllocHandle(STMT) failed");
    return results;
  }

  ret = SQLExecDirect(stmt, (SQLCHAR *)query.c_str(), SQL_NTS);
  if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    Logger::error(LogCategory::GOVERNANCE, "DataGovernanceMSSQL",
                  "SQLExecDirect failed");
    return results;
  }

  SQLSMALLINT numCols;
  SQLNumResultCols(stmt, &numCols);

  while (SQLFetch(stmt) == SQL_SUCCESS) {
    std::vector<std::string> row;
    for (SQLSMALLINT i = 1; i <= numCols; i++) {
      char buffer[DatabaseDefaults::BUFFER_SIZE];
      SQLLEN len;
      ret = SQLGetData(stmt, i, SQL_C_CHAR, buffer, sizeof(buffer), &len);
      if (SQL_SUCCEEDED(ret)) {
        if (len == SQL_NULL_DATA)
          row.push_back("");
        else
          row.push_back(std::string(buffer, len));
      } else {
        row.push_back("");
      }
    }
    results.push_back(std::move(row));
  }

  SQLFreeHandle(SQL_HANDLE_STMT, stmt);
  return results;
}

void DataGovernanceMSSQL::collectGovernanceData() {
  Logger::info(LogCategory::GOVERNANCE, "DataGovernanceMSSQL",
               "Starting governance data collection for MSSQL");

  try {
    queryDatabaseConfig();
    queryIndexPhysicalStats();
    queryIndexUsageStats();
    queryMissingIndexes();
    queryBackupInfo();
    queryStoredProcedures();
    calculateHealthScores();

    Logger::info(LogCategory::GOVERNANCE, "DataGovernanceMSSQL",
                 "Governance data collection completed. Collected " +
                     std::to_string(governanceData_.size()) + " records");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "DataGovernanceMSSQL",
                  "Error collecting governance data: " + std::string(e.what()));
  }
}

void DataGovernanceMSSQL::queryDatabaseConfig() {
  try {
    ODBCConnection conn(connectionString_);
    if (!conn.isValid()) {
      Logger::error(LogCategory::GOVERNANCE, "DataGovernanceMSSQL",
                    "Failed to connect to MSSQL for database config");
      return;
    }

    std::string serverName = extractServerName(connectionString_);
    std::string databaseName = extractDatabaseName(connectionString_);

    std::string query = "SELECT "
                        "compatibility_level, "
                        "recovery_model_desc, "
                        "page_verify_option_desc, "
                        "is_auto_create_stats_on, "
                        "is_auto_update_stats_on, "
                        "is_auto_update_stats_async_on, "
                        "maxdop, "
                        "cost_threshold_for_parallelism "
                        "FROM sys.databases WHERE name = '" + escapeSQL(databaseName) + "';";

    auto results = executeQueryMSSQL(conn.getDbc(), query);

    for (const auto &row : results) {
      if (row.size() >= 8) {
        MSSQLGovernanceData data;
        data.server_name = serverName;
        data.database_name = databaseName;
        data.schema_name = "dbo";
        data.object_type = "DATABASE";
        data.object_name = databaseName;

        try {
          data.compatibility_level = std::stoi(row[0]);
        } catch (...) {
        }

        data.recovery_model = row[1];
        data.page_verify = row[2];

        data.auto_create_stats = (row[3] == "1" || row[3] == "true");
        data.auto_update_stats = (row[4] == "1" || row[4] == "true");
        data.auto_update_stats_async = (row[5] == "1" || row[5] == "true");

        try {
          data.maxdop = std::stoi(row[6]);
          data.cost_threshold = std::stoi(row[7]);
        } catch (...) {
        }

        governanceData_.push_back(data);
      }
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "DataGovernanceMSSQL",
                  "Error querying database config: " + std::string(e.what()));
  }
}

void DataGovernanceMSSQL::queryIndexPhysicalStats() {
  try {
    ODBCConnection conn(connectionString_);
    if (!conn.isValid()) {
      Logger::error(LogCategory::GOVERNANCE, "DataGovernanceMSSQL",
                    "Failed to connect to MSSQL for index physical stats");
      return;
    }

    std::string serverName = extractServerName(connectionString_);
    std::string databaseName = extractDatabaseName(connectionString_);

    std::string query = "SELECT "
                        "OBJECT_SCHEMA_NAME(ips.object_id) AS schema_name, "
                        "OBJECT_NAME(ips.object_id) AS table_name, "
                        "i.name AS index_name, "
                        "ips.index_id, "
                        "ips.record_count, "
                        "ips.avg_fragmentation_in_percent, "
                        "ips.page_count, "
                        "i.fill_factor "
                        "FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') ips "
                        "INNER JOIN sys.indexes i ON ips.object_id = i.object_id AND ips.index_id = i.index_id "
                        "WHERE ips.index_id > 0 "
                        "ORDER BY ips.avg_fragmentation_in_percent DESC;";

    auto results = executeQueryMSSQL(conn.getDbc(), query);

    for (const auto &row : results) {
      if (row.size() >= 8) {
        MSSQLGovernanceData data;
        data.server_name = serverName;
        data.database_name = databaseName;
        data.schema_name = row[0];
        data.table_name = row[1];
        data.index_name = row[2];
        data.object_type = "INDEX";

        try {
          data.index_id = std::stoi(row[3]);
          data.row_count = std::stoll(row[4]);
          data.fragmentation_pct = std::stod(row[5]);
          data.page_count = std::stoll(row[6]);
          data.fill_factor = std::stoi(row[7]);
        } catch (...) {
        }

        governanceData_.push_back(data);
      }
    }

    Logger::info(LogCategory::GOVERNANCE, "DataGovernanceMSSQL",
                 "Collected " + std::to_string(results.size()) +
                     " index physical stats records");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "DataGovernanceMSSQL",
                  "Error querying index physical stats: " +
                      std::string(e.what()));
  }
}

void DataGovernanceMSSQL::queryIndexUsageStats() {
  try {
    ODBCConnection conn(connectionString_);
    if (!conn.isValid()) {
      return;
    }

    std::string query = "SELECT "
                        "OBJECT_SCHEMA_NAME(i.object_id) AS schema_name, "
                        "OBJECT_NAME(i.object_id) AS table_name, "
                        "i.name AS index_name, "
                        "i.index_id, "
                        "ius.user_seeks, "
                        "ius.user_scans, "
                        "ius.user_lookups, "
                        "ius.user_updates, "
                        "ius.page_splits, "
                        "ius.leaf_insert_count "
                        "FROM sys.dm_db_index_usage_stats ius "
                        "INNER JOIN sys.indexes i ON ius.object_id = i.object_id AND ius.index_id = i.index_id "
                        "WHERE ius.database_id = DB_ID();";

    auto results = executeQueryMSSQL(conn.getDbc(), query);

    for (const auto &row : results) {
      if (row.size() >= 10) {
        std::string schemaName = row[0];
        std::string tableName = row[1];
        std::string indexName = row[2];

        for (auto &data : governanceData_) {
          if (data.schema_name == schemaName && data.table_name == tableName &&
              data.index_name == indexName) {
            try {
              data.user_seeks = std::stoll(row[4]);
              data.user_scans = std::stoll(row[5]);
              data.user_lookups = std::stoll(row[6]);
              data.user_updates = std::stoll(row[7]);
              data.page_splits = std::stoll(row[8]);
              data.leaf_inserts = std::stoll(row[9]);
            } catch (...) {
            }
            break;
          }
        }
      }
    }

    Logger::info(LogCategory::GOVERNANCE, "DataGovernanceMSSQL",
                 "Updated " + std::to_string(results.size()) +
                     " index usage stats records");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "DataGovernanceMSSQL",
                  "Error querying index usage stats: " +
                      std::string(e.what()));
  }
}

void DataGovernanceMSSQL::queryMissingIndexes() {
  try {
    ODBCConnection conn(connectionString_);
    if (!conn.isValid()) {
      return;
    }

    std::string query = "SELECT "
                        "OBJECT_SCHEMA_NAME(mid.object_id) AS schema_name, "
                        "OBJECT_NAME(mid.object_id) AS table_name, "
                        "migs.avg_user_impact, "
                        "migs.user_seeks, "
                        "migs.user_scans, "
                        "migs.avg_total_user_cost, "
                        "migs.unique_compiles, "
                        "mid.equality_columns, "
                        "mid.inequality_columns, "
                        "mid.included_columns "
                        "FROM sys.dm_db_missing_index_details mid "
                        "INNER JOIN sys.dm_db_missing_index_groups mig ON mid.index_handle = mig.index_handle "
                        "INNER JOIN sys.dm_db_missing_index_group_stats migs ON mig.index_group_handle = migs.group_handle "
                        "WHERE mid.database_id = DB_ID();";

    auto results = executeQueryMSSQL(conn.getDbc(), query);

    for (const auto &row : results) {
      if (row.size() >= 10) {
        MSSQLGovernanceData data;
        data.server_name = extractServerName(connectionString_);
        data.database_name = extractDatabaseName(connectionString_);
        data.schema_name = row[0];
        data.table_name = row[1];
        data.object_type = "MISSING_INDEX";
        data.has_missing_index = true;

        try {
          data.missing_index_avg_user_impact = std::stod(row[2]);
          data.missing_index_user_seeks = std::stoll(row[3]);
          data.missing_index_user_scans = std::stoll(row[4]);
          data.missing_index_avg_total_user_cost = std::stod(row[5]);
          data.missing_index_unique_compiles = std::stoll(row[6]);
        } catch (...) {
        }

        data.missing_index_equality_columns = row[7];
        data.missing_index_inequality_columns = row[8];
        data.missing_index_included_columns = row[9];

        governanceData_.push_back(data);
      }
    }

    Logger::info(LogCategory::GOVERNANCE, "DataGovernanceMSSQL",
                 "Collected " + std::to_string(results.size()) +
                     " missing index records");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "DataGovernanceMSSQL",
                  "Error querying missing indexes: " +
                      std::string(e.what()));
  }
}

void DataGovernanceMSSQL::queryBackupInfo() {
  try {
    ODBCConnection conn(connectionString_);
    if (!conn.isValid()) {
      return;
    }

    std::string databaseName = extractDatabaseName(connectionString_);

    std::string query = "SELECT TOP 1 "
                        "MAX(CASE WHEN type = 'D' THEN backup_finish_date END) AS last_full_backup, "
                        "MAX(CASE WHEN type = 'I' THEN backup_finish_date END) AS last_diff_backup, "
                        "MAX(CASE WHEN type = 'L' THEN backup_finish_date END) AS last_log_backup "
                        "FROM msdb.dbo.backupset "
                        "WHERE database_name = '" + escapeSQL(databaseName) + "';";

    auto results = executeQueryMSSQL(conn.getDbc(), query);

    if (!results.empty() && results[0].size() >= 3) {
      for (auto &data : governanceData_) {
        if (data.database_name == databaseName && data.object_type == "DATABASE") {
          data.last_full_backup = results[0][0];
          data.last_diff_backup = results[0][1];
          data.last_log_backup = results[0][2];
          break;
        }
      }
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "DataGovernanceMSSQL",
                  "Error querying backup info: " + std::string(e.what()));
  }
}

void DataGovernanceMSSQL::queryStoredProcedures() {
  try {
    ODBCConnection conn(connectionString_);
    if (!conn.isValid()) {
      return;
    }

    std::string serverName = extractServerName(connectionString_);
    std::string databaseName = extractDatabaseName(connectionString_);

    std::string query = "SELECT "
                        "OBJECT_SCHEMA_NAME(ps.object_id) AS schema_name, "
                        "OBJECT_NAME(ps.object_id) AS sp_name, "
                        "ps.execution_count, "
                        "ps.total_elapsed_time / 1000000.0 AS avg_execution_time_seconds, "
                        "ps.total_elapsed_time AS total_elapsed_time_ms, "
                        "ps.total_logical_reads / ps.execution_count AS avg_logical_reads, "
                        "ps.total_physical_reads / ps.execution_count AS avg_physical_reads "
                        "FROM sys.dm_exec_procedure_stats ps "
                        "WHERE ps.database_id = DB_ID() "
                        "ORDER BY ps.total_elapsed_time DESC;";

    auto results = executeQueryMSSQL(conn.getDbc(), query);

    for (const auto &row : results) {
      if (row.size() >= 7) {
        MSSQLGovernanceData data;
        data.server_name = serverName;
        data.database_name = databaseName;
        data.schema_name = row[0];
        data.object_name = row[1];
        data.object_type = "STORED_PROCEDURE";
        data.sp_name = row[1];

        try {
          data.execution_count = std::stoll(row[2]);
          data.avg_execution_time_seconds = std::stod(row[3]);
          data.total_elapsed_time_ms = std::stoll(row[4]);
          data.avg_logical_reads = std::stoll(row[5]);
          data.avg_physical_reads = std::stoll(row[6]);
        } catch (...) {
        }

        governanceData_.push_back(data);
      }
    }

    Logger::info(LogCategory::GOVERNANCE, "DataGovernanceMSSQL",
                 "Collected " + std::to_string(results.size()) +
                     " stored procedure records");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "DataGovernanceMSSQL",
                  "Error querying stored procedures: " +
                      std::string(e.what()));
  }
}

void DataGovernanceMSSQL::calculateHealthScores() {
  for (auto &data : governanceData_) {
    double score = 100.0;

    if (data.fragmentation_pct > 30.0) {
      score -= (data.fragmentation_pct - 30.0) * 0.5;
    }

    if (data.is_unused) {
      score -= 20.0;
    }

    if (data.has_missing_index && data.missing_index_avg_user_impact > 50.0) {
      score -= 15.0;
    }

    if (data.last_full_backup.empty()) {
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

    if (data.fragmentation_pct > 30.0) {
      data.recommendation_summary = "Consider rebuilding index due to high fragmentation";
    } else if (data.has_missing_index) {
      data.recommendation_summary = "Consider creating missing index for better performance";
    } else if (data.is_unused) {
      data.recommendation_summary = "Index appears unused, consider removing";
    }
  }
}

void DataGovernanceMSSQL::storeGovernanceData() {
  if (governanceData_.empty()) {
    Logger::warning(LogCategory::GOVERNANCE, "DataGovernanceMSSQL",
                    "No governance data to store");
    return;
  }

  try {
    pqxx::connection conn(DatabaseConfig::getPostgresConnectionString());
    pqxx::work txn(conn);

    for (const auto &data : governanceData_) {
      std::ostringstream insertQuery;
      insertQuery << "INSERT INTO metadata.data_governance_catalog_mssql ("
                  << "server_name, database_name, schema_name, table_name, "
                  << "object_name, object_type, index_name, index_id, "
                  << "row_count, table_size_mb, fragmentation_pct, page_count, "
                  << "fill_factor, user_seeks, user_scans, user_lookups, "
                  << "user_updates, page_splits, leaf_inserts, "
                  << "index_key_columns, index_include_columns, "
                  << "has_missing_index, missing_index_impact, is_unused, "
                  << "is_potential_duplicate, last_full_backup, last_diff_backup, "
                  << "last_log_backup, compatibility_level, recovery_model, "
                  << "page_verify, auto_create_stats, auto_update_stats, "
                  << "auto_update_stats_async, maxdop, cost_threshold, "
                  << "access_frequency, health_status, recommendation_summary, "
                  << "health_score, missing_index_equality_columns, "
                  << "missing_index_inequality_columns, missing_index_included_columns, "
                  << "missing_index_avg_user_impact, missing_index_user_seeks, "
                  << "missing_index_user_scans, missing_index_avg_total_user_cost, "
                  << "missing_index_unique_compiles, sp_name, "
                  << "avg_execution_time_seconds, total_elapsed_time_ms, "
                  << "avg_logical_reads, avg_physical_reads, execution_count, "
                  << "snapshot_date"
                  << ") VALUES ("
                  << txn.quote(data.server_name) << ", "
                  << txn.quote(data.database_name) << ", "
                  << txn.quote(data.schema_name) << ", "
                  << (data.table_name.empty() ? "NULL" : txn.quote(data.table_name)) << ", "
                  << (data.object_name.empty() ? "NULL" : txn.quote(data.object_name)) << ", "
                  << (data.object_type.empty() ? "NULL" : txn.quote(data.object_type)) << ", "
                  << (data.index_name.empty() ? "NULL" : txn.quote(data.index_name)) << ", "
                  << (data.index_id == 0 ? "NULL" : std::to_string(data.index_id)) << ", "
                  << (data.row_count == 0 ? "NULL" : std::to_string(data.row_count)) << ", "
                  << (data.table_size_mb == 0.0 ? "NULL" : std::to_string(data.table_size_mb)) << ", "
                  << (data.fragmentation_pct == 0.0 ? "NULL" : std::to_string(data.fragmentation_pct)) << ", "
                  << (data.page_count == 0 ? "NULL" : std::to_string(data.page_count)) << ", "
                  << (data.fill_factor == 0 ? "NULL" : std::to_string(data.fill_factor)) << ", "
                  << (data.user_seeks == 0 ? "NULL" : std::to_string(data.user_seeks)) << ", "
                  << (data.user_scans == 0 ? "NULL" : std::to_string(data.user_scans)) << ", "
                  << (data.user_lookups == 0 ? "NULL" : std::to_string(data.user_lookups)) << ", "
                  << (data.user_updates == 0 ? "NULL" : std::to_string(data.user_updates)) << ", "
                  << (data.page_splits == 0 ? "NULL" : std::to_string(data.page_splits)) << ", "
                  << (data.leaf_inserts == 0 ? "NULL" : std::to_string(data.leaf_inserts)) << ", "
                  << (data.index_key_columns.empty() ? "NULL" : txn.quote(data.index_key_columns)) << ", "
                  << (data.index_include_columns.empty() ? "NULL" : txn.quote(data.index_include_columns)) << ", "
                  << (data.has_missing_index ? "true" : "false") << ", "
                  << (data.missing_index_impact == 0.0 ? "NULL" : std::to_string(data.missing_index_impact)) << ", "
                  << (data.is_unused ? "true" : "false") << ", "
                  << (data.is_potential_duplicate ? "true" : "false") << ", "
                  << (data.last_full_backup.empty() ? "NULL" : txn.quote(data.last_full_backup)) << ", "
                  << (data.last_diff_backup.empty() ? "NULL" : txn.quote(data.last_diff_backup)) << ", "
                  << (data.last_log_backup.empty() ? "NULL" : txn.quote(data.last_log_backup)) << ", "
                  << (data.compatibility_level == 0 ? "NULL" : std::to_string(data.compatibility_level)) << ", "
                  << (data.recovery_model.empty() ? "NULL" : txn.quote(data.recovery_model)) << ", "
                  << (data.page_verify.empty() ? "NULL" : txn.quote(data.page_verify)) << ", "
                  << (data.auto_create_stats ? "true" : "false") << ", "
                  << (data.auto_update_stats ? "true" : "false") << ", "
                  << (data.auto_update_stats_async ? "true" : "false") << ", "
                  << (data.maxdop == 0 ? "NULL" : std::to_string(data.maxdop)) << ", "
                  << (data.cost_threshold == 0 ? "NULL" : std::to_string(data.cost_threshold)) << ", "
                  << (data.access_frequency.empty() ? "NULL" : txn.quote(data.access_frequency)) << ", "
                  << (data.health_status.empty() ? "NULL" : txn.quote(data.health_status)) << ", "
                  << (data.recommendation_summary.empty() ? "NULL" : txn.quote(data.recommendation_summary)) << ", "
                  << (data.health_score == 0.0 ? "NULL" : std::to_string(data.health_score)) << ", "
                  << (data.missing_index_equality_columns.empty() ? "NULL" : txn.quote(data.missing_index_equality_columns)) << ", "
                  << (data.missing_index_inequality_columns.empty() ? "NULL" : txn.quote(data.missing_index_inequality_columns)) << ", "
                  << (data.missing_index_included_columns.empty() ? "NULL" : txn.quote(data.missing_index_included_columns)) << ", "
                  << (data.missing_index_avg_user_impact == 0.0 ? "NULL" : std::to_string(data.missing_index_avg_user_impact)) << ", "
                  << (data.missing_index_user_seeks == 0 ? "NULL" : std::to_string(data.missing_index_user_seeks)) << ", "
                  << (data.missing_index_user_scans == 0 ? "NULL" : std::to_string(data.missing_index_user_scans)) << ", "
                  << (data.missing_index_avg_total_user_cost == 0.0 ? "NULL" : std::to_string(data.missing_index_avg_total_user_cost)) << ", "
                  << (data.missing_index_unique_compiles == 0 ? "NULL" : std::to_string(data.missing_index_unique_compiles)) << ", "
                  << (data.sp_name.empty() ? "NULL" : txn.quote(data.sp_name)) << ", "
                  << (data.avg_execution_time_seconds == 0.0 ? "NULL" : std::to_string(data.avg_execution_time_seconds)) << ", "
                  << (data.total_elapsed_time_ms == 0 ? "NULL" : std::to_string(data.total_elapsed_time_ms)) << ", "
                  << (data.avg_logical_reads == 0 ? "NULL" : std::to_string(data.avg_logical_reads)) << ", "
                  << (data.avg_physical_reads == 0 ? "NULL" : std::to_string(data.avg_physical_reads)) << ", "
                  << (data.execution_count == 0 ? "NULL" : std::to_string(data.execution_count)) << ", "
                  << "NOW()"
                  << ") ON CONFLICT DO NOTHING;";

      txn.exec(insertQuery.str());
    }

    txn.commit();

    Logger::info(LogCategory::GOVERNANCE, "DataGovernanceMSSQL",
                 "Stored " + std::to_string(governanceData_.size()) +
                     " governance records in PostgreSQL");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "DataGovernanceMSSQL",
                  "Error storing governance data: " + std::string(e.what()));
  }
}

void DataGovernanceMSSQL::generateReport() {
  Logger::info(LogCategory::GOVERNANCE, "DataGovernanceMSSQL",
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

  Logger::info(LogCategory::GOVERNANCE, "DataGovernanceMSSQL",
               "Report: Healthy=" + std::to_string(healthyCount) +
                   ", Warning=" + std::to_string(warningCount) +
                   ", Critical=" + std::to_string(criticalCount));
}
