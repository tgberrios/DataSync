#include "governance/MaintenanceManager.h"
#include "catalog/metadata_repository.h"
#include "core/database_config.h"
#include "core/database_defaults.h"
#include "core/logger.h"
#include "engines/mariadb_engine.h"
#include "engines/mssql_engine.h"
#include "utils/connection_utils.h"
#include "utils/time_utils.h"
#include <algorithm>
#include <cmath>
#include <ctime>
#include <iomanip>
#include <pqxx/pqxx>
#include <sql.h>
#include <sqlext.h>
#include <sstream>

MaintenanceManager::MaintenanceManager(
    const std::string &metadataConnectionString)
    : metadataConnectionString_(metadataConnectionString) {
  defaultThresholds_ = json::parse(R"({
    "postgresql": {
      "vacuum": {
        "dead_tuples_threshold": 1000,
        "dead_tuples_percentage": 10.0,
        "days_since_last_vacuum": 7
      },
      "analyze": {
        "days_since_last_analyze": 1
      },
      "reindex": {
        "fragmentation_threshold": 30.0
      }
    },
    "mariadb": {
      "optimize": {
        "fragmentation_threshold": 20.0,
        "free_space_threshold_mb": 100
      },
      "analyze": {
        "days_since_last_analyze": 1
      }
    },
    "mssql": {
      "rebuild_index": {
        "fragmentation_threshold": 30.0
      },
      "reorganize_index": {
        "fragmentation_min": 10.0,
        "fragmentation_max": 30.0
      },
      "update_statistics": {
        "days_since_last_update": 1
      }
    }
  })");
  loadThresholdsFromDatabase();
}

MaintenanceManager::~MaintenanceManager() {}

void MaintenanceManager::detectMaintenanceNeeds() {
  Logger::info(LogCategory::GOVERNANCE, "MaintenanceManager",
               "Starting maintenance needs detection");

  try {
    MetadataRepository repo(metadataConnectionString_);

    std::vector<std::string> pgConnections =
        repo.getConnectionStrings("PostgreSQL");
    if (pgConnections.empty()) {
      std::string dataLakeConnStr =
          DatabaseConfig::getPostgresConnectionString();
      if (!dataLakeConnStr.empty()) {
        Logger::info(LogCategory::GOVERNANCE, "MaintenanceManager",
                     "No PostgreSQL sources in catalog, detecting maintenance "
                     "for DataLake");
        try {
          detectPostgreSQLMaintenance(dataLakeConnStr);
        } catch (const std::exception &e) {
          Logger::error(LogCategory::GOVERNANCE, "MaintenanceManager",
                        "Error detecting DataLake PostgreSQL maintenance: " +
                            std::string(e.what()));
        }
      }
    } else {
      for (const auto &connStr : pgConnections) {
        if (!connStr.empty()) {
          try {
            detectPostgreSQLMaintenance(connStr);
          } catch (const std::exception &e) {
            Logger::error(LogCategory::GOVERNANCE, "MaintenanceManager",
                          "Error detecting PostgreSQL maintenance: " +
                              std::string(e.what()));
          }
        }
      }
    }

    std::vector<std::string> mariadbConnections =
        repo.getConnectionStrings("MariaDB");
    for (const auto &connStr : mariadbConnections) {
      if (!connStr.empty()) {
        try {
          detectMariaDBMaintenance(connStr);
        } catch (const std::exception &e) {
          Logger::error(LogCategory::GOVERNANCE, "MaintenanceManager",
                        "Error detecting MariaDB maintenance: " +
                            std::string(e.what()));
        }
      }
    }

    std::vector<std::string> mssqlConnections =
        repo.getConnectionStrings("MSSQL");
    for (const auto &connStr : mssqlConnections) {
      if (!connStr.empty()) {
        try {
          detectMSSQLMaintenance(connStr);
        } catch (const std::exception &e) {
          Logger::error(LogCategory::GOVERNANCE, "MaintenanceManager",
                        "Error detecting MSSQL maintenance: " +
                            std::string(e.what()));
        }
      }
    }

    Logger::info(LogCategory::GOVERNANCE, "MaintenanceManager",
                 "Maintenance needs detection completed");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "MaintenanceManager",
                  "Error in maintenance detection: " + std::string(e.what()));
  }
}

void MaintenanceManager::detectPostgreSQLMaintenance(
    const std::string &connStr) {
  try {
    pqxx::connection conn(connStr);
    if (!conn.is_open()) {
      Logger::warning(
          LogCategory::GOVERNANCE, "MaintenanceManager",
          "Failed to connect to PostgreSQL for maintenance detection");
      return;
    }

    detectVacuumNeeds(conn, connStr);
    detectAnalyzeNeeds(conn, connStr);
    detectReindexNeeds(conn, connStr);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "MaintenanceManager",
                  "Error in PostgreSQL maintenance detection: " +
                      std::string(e.what()));
  }
}

void MaintenanceManager::detectVacuumNeeds(pqxx::connection &conn,
                                           const std::string &connStr) {
  try {
    pqxx::work txn(conn);
    auto thresholds = getThresholds("postgresql", "vacuum");
    double deadTuplesPct = thresholds.value("dead_tuples_percentage", 10.0);
    long long deadTuplesThreshold =
        thresholds.value("dead_tuples_threshold", 1000LL);
    int daysSinceVacuum = thresholds.value("days_since_last_vacuum", 7);

    std::string query = R"(
      SELECT 
        schemaname,
        relname,
        n_dead_tup,
        n_live_tup,
        last_vacuum,
        last_autovacuum,
        pg_total_relation_size(schemaname||'.'||relname) as total_size
      FROM pg_stat_user_tables
      WHERE schemaname NOT IN ('information_schema', 'pg_catalog', 'metadata')
        AND (
          n_dead_tup > $1
          OR (n_live_tup > 0 AND (n_dead_tup::float / NULLIF(n_live_tup, 0)) * 100 > $2)
          OR (COALESCE(last_vacuum, last_autovacuum) IS NULL 
              OR COALESCE(last_vacuum, last_autovacuum) < NOW() - INTERVAL '1 day' * $3)
        )
    )";

    auto results = txn.exec_params(query, deadTuplesThreshold, deadTuplesPct,
                                   daysSinceVacuum);
    txn.commit();

    for (const auto &row : results) {
      MaintenanceTask task;
      task.maintenance_type = "VACUUM";
      task.db_engine = "PostgreSQL";
      task.connection_string = connStr;
      task.schema_name = row[0].as<std::string>();
      task.object_name = row[1].as<std::string>();
      task.object_type = "TABLE";
      task.auto_execute = true;
      task.enabled = true;

      auto params = ConnectionStringParser::parse(connStr);
      if (params) {
        task.server_name = params->host;
        task.database_name = params->db;
      }

      MaintenanceMetrics metrics;
      metrics.dead_tuples = row[2].as<long long>();
      metrics.live_tuples = row[3].as<long long>();
      metrics.table_size_mb = row[6].as<long long>() / (1024.0 * 1024.0);
      if (metrics.live_tuples > 0) {
        metrics.fragmentation_pct =
            (metrics.dead_tuples * 100.0) / metrics.live_tuples;
      }

      task.priority = calculatePriority(metrics, "VACUUM");
      task.status = "PENDING";
      task.next_maintenance_date = calculateNextMaintenanceDate("VACUUM");
      task.thresholds = thresholds;

      storeTask(task);
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "MaintenanceManager",
                  "Error detecting vacuum needs: " + std::string(e.what()));
  }
}

void MaintenanceManager::detectAnalyzeNeeds(pqxx::connection &conn,
                                            const std::string &connStr) {
  try {
    pqxx::work txn(conn);
    auto thresholds = getThresholds("postgresql", "analyze");
    int daysSinceAnalyze = thresholds.value("days_since_last_analyze", 1);

    std::string query = R"(
      SELECT 
        schemaname,
        relname
      FROM pg_stat_user_tables
      WHERE schemaname NOT IN ('information_schema', 'pg_catalog', 'metadata')
        AND (
          last_autoanalyze IS NULL 
          OR last_autoanalyze < NOW() - INTERVAL '1 day' * $1
        )
    )";

    auto results = txn.exec_params(query, daysSinceAnalyze);
    txn.commit();

    for (const auto &row : results) {
      MaintenanceTask task;
      task.maintenance_type = "ANALYZE";
      task.db_engine = "PostgreSQL";
      task.connection_string = connStr;
      task.schema_name = row[0].as<std::string>();
      task.object_name = row[1].as<std::string>();
      task.object_type = "TABLE";
      task.auto_execute = true;
      task.enabled = true;
      task.priority = 5;
      task.status = "PENDING";
      task.next_maintenance_date = calculateNextMaintenanceDate("ANALYZE");
      task.thresholds = thresholds;

      auto params = ConnectionStringParser::parse(connStr);
      if (params) {
        task.server_name = params->host;
        task.database_name = params->db;
      }

      storeTask(task);
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "MaintenanceManager",
                  "Error detecting analyze needs: " + std::string(e.what()));
  }
}

void MaintenanceManager::detectReindexNeeds(pqxx::connection &conn,
                                            const std::string &connStr) {
  try {
    pqxx::work txn(conn);
    auto thresholds = getThresholds("postgresql", "reindex");
    double fragmentationThreshold =
        thresholds.value("fragmentation_threshold", 30.0);

    std::string query = R"(
      SELECT 
        schemaname,
        tablename,
        indexname,
        pg_relation_size(schemaname||'.'||indexname) as index_size
      FROM pg_indexes
      WHERE schemaname NOT IN ('information_schema', 'pg_catalog', 'metadata')
    )";

    auto results = txn.exec(query);
    txn.commit();

    for (const auto &row : results) {
      std::string schema = row[0].as<std::string>();
      std::string table = row[1].as<std::string>();
      std::string index = row[2].as<std::string>();

      pqxx::work txn2(conn);
      std::string fragQuery = R"(
        SELECT 
          COALESCE(
            (SELECT 
              CASE 
                WHEN st.n_live_tup > 0 
                THEN (st.n_dead_tup::float / NULLIF(st.n_live_tup, 0)) * 100.0
                ELSE 0.0
              END
             FROM pg_stat_user_tables st
             JOIN pg_index i ON i.indrelid = st.relid
             JOIN pg_class idx ON i.indexrelid = idx.oid
             JOIN pg_namespace n ON idx.relnamespace = n.oid
             WHERE n.nspname = $1 AND idx.relname = $2
             LIMIT 1),
            0.0
          ) as fragmentation
      )";
      auto fragResult = txn2.exec_params(fragQuery, schema, index);
      txn2.commit();

      if (!fragResult.empty()) {
        double fragmentation = fragResult[0][0].as<double>();
        if (fragmentation > fragmentationThreshold) {
          MaintenanceTask task;
          task.maintenance_type = "REINDEX";
          task.db_engine = "PostgreSQL";
          task.connection_string = connStr;
          task.schema_name = schema;
          task.object_name = index;
          task.object_type = "INDEX";
          task.auto_execute = true;
          task.enabled = true;
          task.priority = fragmentation > 50 ? 8 : 6;
          task.status = "PENDING";
          task.next_maintenance_date = calculateNextMaintenanceDate("REINDEX");
          task.thresholds = thresholds;

          auto params = ConnectionStringParser::parse(connStr);
          if (params) {
            task.server_name = params->host;
            task.database_name = params->db;
          }

          storeTask(task);
        }
      }
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "MaintenanceManager",
                  "Error detecting reindex needs: " + std::string(e.what()));
  }
}

void MaintenanceManager::detectMariaDBMaintenance(const std::string &connStr) {
  try {
    auto params = ConnectionStringParser::parse(connStr);
    if (!params) {
      Logger::warning(LogCategory::GOVERNANCE, "MaintenanceManager",
                      "Invalid MariaDB connection string");
      return;
    }

    MySQLConnection conn(*params);
    if (!conn.isValid()) {
      Logger::warning(LogCategory::GOVERNANCE, "MaintenanceManager",
                      "Failed to connect to MariaDB for maintenance detection");
      return;
    }

    MYSQL *mysqlConn = conn.get();
    detectOptimizeNeeds(mysqlConn, connStr);
    detectAnalyzeTableNeeds(mysqlConn, connStr);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "MaintenanceManager",
                  "Error in MariaDB maintenance detection: " +
                      std::string(e.what()));
  }
}

void MaintenanceManager::detectOptimizeNeeds(MYSQL *conn,
                                             const std::string &connStr) {
  try {
    auto thresholds = getThresholds("mariadb", "optimize");
    double fragmentationThreshold =
        thresholds.value("fragmentation_threshold", 20.0);
    double freeSpaceThreshold =
        thresholds.value("free_space_threshold_mb", 100.0);

    std::string query = R"(
      SELECT 
        table_schema,
        table_name,
        data_free,
        data_length,
        index_length,
        (data_free / (1024 * 1024)) as free_space_mb,
        CASE 
          WHEN (data_length + index_length) > 0 
          THEN (data_free / (data_length + index_length)) * 100
          ELSE 0
        END as fragmentation_pct
      FROM information_schema.tables
      WHERE table_schema NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
        AND table_type = 'BASE TABLE'
        AND (
          (data_free / (1024 * 1024)) > )" +
                        std::to_string(freeSpaceThreshold) + R"(
          OR (
            (data_length + index_length) > 0 
            AND (data_free / (data_length + index_length)) * 100 > )" +
                        std::to_string(fragmentationThreshold) + R"(
          )
        )
    )";

    if (mysql_query(conn, query.c_str())) {
      Logger::error(LogCategory::GOVERNANCE, "MaintenanceManager",
                    "Query failed: " + std::string(mysql_error(conn)));
      return;
    }

    MYSQL_RES *res = mysql_store_result(conn);
    if (!res)
      return;

    MYSQL_ROW row;
    while ((row = mysql_fetch_row(res))) {
      MaintenanceTask task;
      task.maintenance_type = "OPTIMIZE TABLE";
      task.db_engine = "MariaDB";
      task.connection_string = connStr;
      task.schema_name = row[0] ? row[0] : "";
      task.object_name = row[1] ? row[1] : "";
      task.object_type = "TABLE";
      task.auto_execute = true;
      task.enabled = true;

      double fragmentation = row[6] ? std::stod(row[6]) : 0.0;
      task.priority = fragmentation > 30 ? 7 : 5;
      task.status = "PENDING";
      task.next_maintenance_date =
          calculateNextMaintenanceDate("OPTIMIZE TABLE");
      task.thresholds = thresholds;

      auto params = ConnectionStringParser::parse(connStr);
      if (params) {
        task.server_name = params->host;
        task.database_name = params->db;
      }

      storeTask(task);
    }
    mysql_free_result(res);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "MaintenanceManager",
                  "Error detecting optimize needs: " + std::string(e.what()));
  }
}

void MaintenanceManager::detectAnalyzeTableNeeds(MYSQL *conn,
                                                 const std::string &connStr) {
  try {
    auto thresholds = getThresholds("mariadb", "analyze");
    int daysSinceAnalyze = thresholds.value("days_since_last_analyze", 1);

    std::string query = R"(
      SELECT 
        table_schema,
        table_name,
        update_time
      FROM information_schema.tables
      WHERE table_schema NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
        AND table_type = 'BASE TABLE'
        AND (
          update_time IS NULL 
          OR update_time < DATE_SUB(NOW(), INTERVAL )" +
                        std::to_string(daysSinceAnalyze) + R"( DAY)
        )
    )";

    if (mysql_query(conn, query.c_str())) {
      Logger::error(LogCategory::GOVERNANCE, "MaintenanceManager",
                    "Query failed: " + std::string(mysql_error(conn)));
      return;
    }

    MYSQL_RES *res = mysql_store_result(conn);
    if (!res)
      return;

    MYSQL_ROW row;
    while ((row = mysql_fetch_row(res))) {
      MaintenanceTask task;
      task.maintenance_type = "ANALYZE TABLE";
      task.db_engine = "MariaDB";
      task.connection_string = connStr;
      task.schema_name = row[0] ? row[0] : "";
      task.object_name = row[1] ? row[1] : "";
      task.object_type = "TABLE";
      task.auto_execute = true;
      task.enabled = true;
      task.priority = 4;
      task.status = "PENDING";
      task.next_maintenance_date =
          calculateNextMaintenanceDate("ANALYZE TABLE");
      task.thresholds = thresholds;

      auto params = ConnectionStringParser::parse(connStr);
      if (params) {
        task.server_name = params->host;
        task.database_name = params->db;
      }

      storeTask(task);
    }
    mysql_free_result(res);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "MaintenanceManager",
                  "Error detecting analyze table needs: " +
                      std::string(e.what()));
  }
}

void MaintenanceManager::detectMSSQLMaintenance(const std::string &connStr) {
  try {
    ODBCConnection conn(connStr);
    if (!conn.isValid()) {
      Logger::warning(LogCategory::GOVERNANCE, "MaintenanceManager",
                      "Failed to connect to MSSQL for maintenance detection");
      return;
    }

    SQLHDBC hdbc = conn.getDbc();
    detectUpdateStatisticsNeeds(hdbc, connStr);
    detectRebuildIndexNeeds(hdbc, connStr);
    detectReorganizeIndexNeeds(hdbc, connStr);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "MaintenanceManager",
                  "Error in MSSQL maintenance detection: " +
                      std::string(e.what()));
  }
}

void MaintenanceManager::detectUpdateStatisticsNeeds(
    SQLHDBC conn, const std::string &connStr) {
  try {
    auto thresholds = getThresholds("mssql", "update_statistics");
    int daysSinceUpdate = thresholds.value("days_since_last_update", 1);

    std::string query = R"(
      SELECT 
        OBJECT_SCHEMA_NAME(s.object_id) AS schema_name,
        OBJECT_NAME(s.object_id) AS table_name,
        s.name AS stats_name
      FROM sys.stats s
      WHERE OBJECTPROPERTY(s.object_id, 'IsUserTable') = 1
        AND (
          STATS_DATE(s.object_id, s.stats_id) IS NULL
          OR STATS_DATE(s.object_id, s.stats_id) < DATEADD(day, -)" +
                        std::to_string(daysSinceUpdate) + R"(, GETDATE())
        )
    )";

    std::vector<std::vector<std::string>> results;
    SQLHSTMT stmt;
    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, conn, &stmt);
    if (ret != SQL_SUCCESS)
      return;

    ret = SQLExecDirect(stmt, (SQLCHAR *)query.c_str(), SQL_NTS);
    if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
      SQLSMALLINT numCols;
      SQLNumResultCols(stmt, &numCols);
      char buffer[1024];
      SQLLEN len;

      while (SQLFetch(stmt) == SQL_SUCCESS) {
        std::vector<std::string> row;
        for (SQLSMALLINT i = 1; i <= numCols; i++) {
          if (SQLGetData(stmt, i, SQL_C_CHAR, buffer, sizeof(buffer), &len) ==
              SQL_SUCCESS) {
            row.push_back((len > 0 && len < 1024) ? std::string(buffer, len)
                                                  : "");
          } else {
            row.push_back("");
          }
        }
        results.push_back(row);
      }
    }
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);

    auto params = ConnectionStringParser::parse(connStr);
    std::string serverName = params ? params->host : "UNKNOWN";
    std::string databaseName = params ? params->db : "master";

    for (const auto &row : results) {
      if (row.size() >= 3) {
        MaintenanceTask task;
        task.maintenance_type = "UPDATE STATISTICS";
        task.db_engine = "MSSQL";
        task.connection_string = connStr;
        task.schema_name = row[0];
        task.object_name = row[1];
        task.object_type = "TABLE";
        task.auto_execute = true;
        task.enabled = true;
        task.priority = 5;
        task.status = "PENDING";
        task.next_maintenance_date =
            calculateNextMaintenanceDate("UPDATE STATISTICS");
        task.thresholds = thresholds;
        task.server_name = serverName;
        task.database_name = databaseName;

        storeTask(task);
      }
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "MaintenanceManager",
                  "Error detecting UPDATE STATISTICS needs: " +
                      std::string(e.what()));
  }
}

void MaintenanceManager::detectRebuildIndexNeeds(SQLHDBC conn,
                                                 const std::string &connStr) {
  try {
    auto thresholds = getThresholds("mssql", "rebuild_index");
    double fragmentationThreshold =
        thresholds.value("fragmentation_threshold", 30.0);

    std::string query = R"(
      SELECT 
        OBJECT_SCHEMA_NAME(ips.object_id) AS schema_name,
        OBJECT_NAME(ips.object_id) AS table_name,
        i.name AS index_name,
        ips.avg_fragmentation_in_percent,
        ips.page_count
      FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') ips
      INNER JOIN sys.indexes i ON ips.object_id = i.object_id AND ips.index_id = i.index_id
      WHERE ips.index_id > 0
        AND ips.avg_fragmentation_in_percent > )" +
                        std::to_string(fragmentationThreshold) + R"(
        AND ips.page_count > 100
    )";

    std::vector<std::vector<std::string>> results;
    SQLHSTMT stmt;
    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, conn, &stmt);
    if (ret != SQL_SUCCESS)
      return;

    ret = SQLExecDirect(stmt, (SQLCHAR *)query.c_str(), SQL_NTS);
    if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
      SQLSMALLINT numCols;
      SQLNumResultCols(stmt, &numCols);
      char buffer[1024];
      SQLLEN len;

      while (SQLFetch(stmt) == SQL_SUCCESS) {
        std::vector<std::string> row;
        for (SQLSMALLINT i = 1; i <= numCols; i++) {
          if (SQLGetData(stmt, i, SQL_C_CHAR, buffer, sizeof(buffer), &len) ==
              SQL_SUCCESS) {
            row.push_back((len > 0 && len < 1024) ? std::string(buffer, len)
                                                  : "");
          } else {
            row.push_back("");
          }
        }
        results.push_back(row);
      }
    }
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);

    auto params = ConnectionStringParser::parse(connStr);
    std::string serverName = params ? params->host : "UNKNOWN";
    std::string databaseName = params ? params->db : "master";

    for (const auto &row : results) {
      if (row.size() >= 5) {
        double fragmentation = 0.0;
        try {
          if (!row[3].empty())
            fragmentation = std::stod(row[3]);
        } catch (...) {
        }

        MaintenanceTask task;
        task.maintenance_type = "REBUILD INDEX";
        task.db_engine = "MSSQL";
        task.connection_string = connStr;
        task.schema_name = row[0];
        task.object_name = row[2];
        task.object_type = "INDEX";
        task.auto_execute = true;
        task.enabled = true;
        task.priority = fragmentation > 50 ? 8 : 6;
        task.status = "PENDING";
        task.next_maintenance_date =
            calculateNextMaintenanceDate("REBUILD INDEX");
        task.thresholds = thresholds;
        task.server_name = serverName;
        task.database_name = databaseName;

        storeTask(task);
      }
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "MaintenanceManager",
                  "Error detecting REBUILD INDEX needs: " +
                      std::string(e.what()));
  }
}

void MaintenanceManager::detectReorganizeIndexNeeds(
    SQLHDBC conn, const std::string &connStr) {
  try {
    auto thresholds = getThresholds("mssql", "reorganize_index");
    double fragmentationMin = thresholds.value("fragmentation_min", 10.0);
    double fragmentationMax = thresholds.value("fragmentation_max", 30.0);

    std::string query = R"(
      SELECT 
        OBJECT_SCHEMA_NAME(ips.object_id) AS schema_name,
        OBJECT_NAME(ips.object_id) AS table_name,
        i.name AS index_name,
        ips.avg_fragmentation_in_percent,
        ips.page_count
      FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') ips
      INNER JOIN sys.indexes i ON ips.object_id = i.object_id AND ips.index_id = i.index_id
      WHERE ips.index_id > 0
        AND ips.avg_fragmentation_in_percent >= )" +
                        std::to_string(fragmentationMin) + R"(
        AND ips.avg_fragmentation_in_percent < )" +
                        std::to_string(fragmentationMax) + R"(
        AND ips.page_count > 100
    )";

    std::vector<std::vector<std::string>> results;
    SQLHSTMT stmt;
    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, conn, &stmt);
    if (ret != SQL_SUCCESS)
      return;

    ret = SQLExecDirect(stmt, (SQLCHAR *)query.c_str(), SQL_NTS);
    if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
      SQLSMALLINT numCols;
      SQLNumResultCols(stmt, &numCols);
      char buffer[1024];
      SQLLEN len;

      while (SQLFetch(stmt) == SQL_SUCCESS) {
        std::vector<std::string> row;
        for (SQLSMALLINT i = 1; i <= numCols; i++) {
          if (SQLGetData(stmt, i, SQL_C_CHAR, buffer, sizeof(buffer), &len) ==
              SQL_SUCCESS) {
            row.push_back((len > 0 && len < 1024) ? std::string(buffer, len)
                                                  : "");
          } else {
            row.push_back("");
          }
        }
        results.push_back(row);
      }
    }
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);

    auto params = ConnectionStringParser::parse(connStr);
    std::string serverName = params ? params->host : "UNKNOWN";
    std::string databaseName = params ? params->db : "master";

    for (const auto &row : results) {
      if (row.size() >= 5) {
        MaintenanceTask task;
        task.maintenance_type = "REORGANIZE INDEX";
        task.db_engine = "MSSQL";
        task.connection_string = connStr;
        task.schema_name = row[0];
        task.object_name = row[2];
        task.object_type = "INDEX";
        task.auto_execute = true;
        task.enabled = true;
        task.priority = 5;
        task.status = "PENDING";
        task.next_maintenance_date =
            calculateNextMaintenanceDate("REORGANIZE INDEX");
        task.thresholds = thresholds;
        task.server_name = serverName;
        task.database_name = databaseName;

        storeTask(task);
      }
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "MaintenanceManager",
                  "Error detecting REORGANIZE INDEX needs: " +
                      std::string(e.what()));
  }
}

void MaintenanceManager::executeMaintenance() {
  Logger::info(LogCategory::GOVERNANCE, "MaintenanceManager",
               "Starting maintenance execution");

  try {
    auto tasks = getPendingTasks();
    int executed = 0;

    for (const auto &task : tasks) {
      if (!task.auto_execute || !task.enabled) {
        continue;
      }

      try {
        updateTaskStatus(task.id, "RUNNING");
        auto startTime = std::chrono::high_resolution_clock::now();

        MaintenanceMetrics before = collectMetricsBefore(task);

        if (task.db_engine == "PostgreSQL") {
          executePostgreSQLMaintenance(task);
        } else if (task.db_engine == "MariaDB") {
          executeMariaDBMaintenance(task);
        } else if (task.db_engine == "MSSQL") {
          executeMSSQLMaintenance(task);
        }

        auto endTime = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::seconds>(
            endTime - startTime);

        MaintenanceMetrics after = collectMetricsAfter(task);
        MaintenanceTask updatedTask = task;
        calculateImpact(updatedTask, before, after);

        std::string resultMsg = "Maintenance completed successfully. ";
        if (updatedTask.space_reclaimed_mb > 0) {
          resultMsg += "Space reclaimed: " +
                       std::to_string(updatedTask.space_reclaimed_mb) + " MB. ";
        }
        if (updatedTask.performance_improvement_pct > 0) {
          resultMsg += "Performance improvement: " +
                       std::to_string(updatedTask.performance_improvement_pct) +
                       "%. ";
        }

        updateTaskStatus(task.id, "COMPLETED", resultMsg);
        storeExecutionMetrics(updatedTask, before, after);
        executed++;
      } catch (const std::exception &e) {
        updateTaskStatus(task.id, "FAILED", "", std::string(e.what()));
        Logger::error(LogCategory::GOVERNANCE, "MaintenanceManager",
                      "Error executing maintenance task " +
                          std::to_string(task.id) + ": " +
                          std::string(e.what()));
      }
    }

    Logger::info(LogCategory::GOVERNANCE, "MaintenanceManager",
                 "Maintenance execution completed. Executed " +
                     std::to_string(executed) + " tasks");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "MaintenanceManager",
                  "Error in maintenance execution: " + std::string(e.what()));
  }
}

void MaintenanceManager::executePostgreSQLMaintenance(
    const MaintenanceTask &task) {
  try {
    pqxx::connection conn(task.connection_string);
    if (!conn.is_open()) {
      throw std::runtime_error("Failed to connect to PostgreSQL");
    }

    pqxx::work txn(conn);
    std::string query;

    if (task.maintenance_type == "VACUUM") {
      query = "VACUUM ANALYZE " + txn.quote_name(task.schema_name) + "." +
              txn.quote_name(task.object_name);
    } else if (task.maintenance_type == "ANALYZE") {
      query = "ANALYZE " + txn.quote_name(task.schema_name) + "." +
              txn.quote_name(task.object_name);
    } else if (task.maintenance_type == "REINDEX") {
      if (task.object_type == "INDEX") {
        query = "REINDEX INDEX " + txn.quote_name(task.schema_name) + "." +
                txn.quote_name(task.object_name);
      } else {
        query = "REINDEX TABLE " + txn.quote_name(task.schema_name) + "." +
                txn.quote_name(task.object_name);
      }
    }

    if (!query.empty()) {
      txn.exec(query);
      txn.commit();
      Logger::info(LogCategory::GOVERNANCE, "MaintenanceManager",
                   "Executed " + task.maintenance_type + " on " +
                       task.schema_name + "." + task.object_name);
    }
  } catch (const std::exception &e) {
    throw std::runtime_error("PostgreSQL maintenance failed: " +
                             std::string(e.what()));
  }
}

void MaintenanceManager::executeMariaDBMaintenance(
    const MaintenanceTask &task) {
  try {
    auto params = ConnectionStringParser::parse(task.connection_string);
    if (!params) {
      throw std::runtime_error("Invalid connection string");
    }

    MySQLConnection conn(*params);
    if (!conn.isValid()) {
      throw std::runtime_error("Failed to connect to MariaDB");
    }

    MYSQL *mysqlConn = conn.get();
    std::string query;

    if (task.maintenance_type == "OPTIMIZE TABLE") {
      query = "OPTIMIZE TABLE " + escapeSQL(mysqlConn, task.schema_name) + "." +
              escapeSQL(mysqlConn, task.object_name);
    } else if (task.maintenance_type == "ANALYZE TABLE") {
      query = "ANALYZE TABLE " + escapeSQL(mysqlConn, task.schema_name) + "." +
              escapeSQL(mysqlConn, task.object_name);
    }

    if (!query.empty()) {
      if (mysql_query(mysqlConn, query.c_str())) {
        throw std::runtime_error("Query failed: " +
                                 std::string(mysql_error(mysqlConn)));
      }
      Logger::info(LogCategory::GOVERNANCE, "MaintenanceManager",
                   "Executed " + task.maintenance_type + " on " +
                       task.schema_name + "." + task.object_name);
    }
  } catch (const std::exception &e) {
    throw std::runtime_error("MariaDB maintenance failed: " +
                             std::string(e.what()));
  }
}

void MaintenanceManager::executeMSSQLMaintenance(const MaintenanceTask &task) {
  try {
    ODBCConnection conn(task.connection_string);
    if (!conn.isValid()) {
      throw std::runtime_error("Failed to connect to MSSQL");
    }

    SQLHDBC hdbc = conn.getDbc();
    SQLHSTMT stmt;
    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &stmt);
    if (ret != SQL_SUCCESS) {
      throw std::runtime_error("SQLAllocHandle(STMT) failed");
    }

    std::string query;
    std::string escapedSchema = escapeSQLMSSQL(task.schema_name);
    std::string escapedObject = escapeSQLMSSQL(task.object_name);

    if (task.maintenance_type == "UPDATE STATISTICS") {
      query =
          "UPDATE STATISTICS [" + escapedSchema + "].[" + escapedObject + "]";
    } else if (task.maintenance_type == "REBUILD INDEX") {
      if (task.object_type == "INDEX") {
        std::string tableName = task.object_name;
        query = "ALTER INDEX [" + escapedObject + "] ON [" + escapedSchema +
                "].[" + escapeSQLMSSQL(tableName) + "] REBUILD";
      } else {
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);
        throw std::runtime_error(
            "REBUILD INDEX requires object_type to be INDEX");
      }
    } else if (task.maintenance_type == "REORGANIZE INDEX") {
      if (task.object_type == "INDEX") {
        std::string tableName = task.object_name;
        query = "ALTER INDEX [" + escapedObject + "] ON [" + escapedSchema +
                "].[" + escapeSQLMSSQL(tableName) + "] REORGANIZE";
      } else {
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);
        throw std::runtime_error(
            "REORGANIZE INDEX requires object_type to be INDEX");
      }
    } else {
      SQLFreeHandle(SQL_HANDLE_STMT, stmt);
      throw std::runtime_error("Unknown maintenance type: " +
                               task.maintenance_type);
    }

    if (!query.empty()) {
      ret = SQLExecDirect(stmt, (SQLCHAR *)query.c_str(), SQL_NTS);
      if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        SQLCHAR sqlState[6];
        SQLCHAR errorMsg[SQL_MAX_MESSAGE_LENGTH];
        SQLINTEGER nativeError;
        SQLSMALLINT msgLen;
        std::string errorStr = "Query failed";
        if (SQLGetDiagRec(SQL_HANDLE_STMT, stmt, 1, sqlState, &nativeError,
                          errorMsg, sizeof(errorMsg), &msgLen) == SQL_SUCCESS) {
          errorStr = std::string((char *)errorMsg);
        }
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);
        throw std::runtime_error(errorStr);
      }
      SQLFreeHandle(SQL_HANDLE_STMT, stmt);
      Logger::info(LogCategory::GOVERNANCE, "MaintenanceManager",
                   "Executed " + task.maintenance_type + " on " +
                       task.schema_name + "." + task.object_name);
    }
  } catch (const std::exception &e) {
    throw std::runtime_error("MSSQL maintenance failed: " +
                             std::string(e.what()));
  }
}

MaintenanceMetrics
MaintenanceManager::collectMetricsBefore(const MaintenanceTask &task) {
  MaintenanceMetrics metrics;

  try {
    if (task.db_engine == "PostgreSQL") {
      pqxx::connection conn(task.connection_string);
      if (conn.is_open()) {
        pqxx::work txn(conn);
        std::string query = R"(
          SELECT 
            n_dead_tup,
            n_live_tup,
            pg_total_relation_size($1::regclass) as total_size,
            pg_relation_size($1::regclass, 'main') as table_size,
            (SELECT SUM(pg_relation_size(indexrelid)) 
             FROM pg_index WHERE indrelid = $1::regclass) as index_size
          FROM pg_stat_user_tables
          WHERE schemaname = $2 AND relname = $3
        )";
        std::string fullName = task.schema_name + "." + task.object_name;
        auto results = txn.exec_params(query, fullName, task.schema_name,
                                       task.object_name);
        txn.commit();

        if (!results.empty()) {
          const auto &row = results[0];
          metrics.dead_tuples = row[0].as<long long>();
          metrics.live_tuples = row[1].as<long long>();
          metrics.table_size_mb = row[3].as<long long>() / (1024.0 * 1024.0);
          metrics.index_size_mb =
              (row[4].is_null() ? 0 : row[4].as<long long>()) /
              (1024.0 * 1024.0);
          if (metrics.live_tuples > 0) {
            metrics.fragmentation_pct =
                (metrics.dead_tuples * 100.0) / metrics.live_tuples;
          }
        }
      }
    } else if (task.db_engine == "MariaDB") {
      auto params = ConnectionStringParser::parse(task.connection_string);
      if (params) {
        MySQLConnection conn(*params);
        if (conn.isValid()) {
          MYSQL *mysqlConn = conn.get();
          std::string query = "SELECT data_free, data_length, index_length "
                              "FROM information_schema.tables "
                              "WHERE table_schema = '" +
                              escapeSQL(mysqlConn, task.schema_name) +
                              "' "
                              "AND table_name = '" +
                              escapeSQL(mysqlConn, task.object_name) + "'";
          if (mysql_query(mysqlConn, query.c_str()) == 0) {
            MYSQL_RES *res = mysql_store_result(mysqlConn);
            if (res) {
              MYSQL_ROW row = mysql_fetch_row(res);
              if (row) {
                metrics.free_space_mb =
                    (row[0] ? std::stod(row[0]) : 0) / (1024.0 * 1024.0);
                metrics.table_size_mb =
                    (row[1] ? std::stod(row[1]) : 0) / (1024.0 * 1024.0);
                metrics.index_size_mb =
                    (row[2] ? std::stod(row[2]) : 0) / (1024.0 * 1024.0);
                long long totalSize = (row[1] ? std::stoll(row[1]) : 0) +
                                      (row[2] ? std::stoll(row[2]) : 0);
                if (totalSize > 0) {
                  metrics.fragmentation_pct =
                      ((row[0] ? std::stoll(row[0]) : 0) * 100.0) / totalSize;
                }
              }
              mysql_free_result(res);
            }
          }
        }
      }
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "MaintenanceManager",
                  "Error collecting metrics before: " + std::string(e.what()));
  }

  return metrics;
}

MaintenanceMetrics
MaintenanceManager::collectMetricsAfter(const MaintenanceTask &task) {
  return collectMetricsBefore(task);
}

void MaintenanceManager::calculateImpact(MaintenanceTask &task,
                                         const MaintenanceMetrics &before,
                                         const MaintenanceMetrics &after) {
  if (before.table_size_mb > 0 && after.table_size_mb > 0) {
    task.space_reclaimed_mb = before.table_size_mb - after.table_size_mb;
  }
  if (before.fragmentation_pct > 0 && after.fragmentation_pct > 0) {
    task.fragmentation_before = before.fragmentation_pct;
    task.fragmentation_after = after.fragmentation_pct;
    task.performance_improvement_pct =
        before.fragmentation_pct - after.fragmentation_pct;
  }
}

int MaintenanceManager::calculatePriority(const MaintenanceMetrics &metrics,
                                          const std::string &maintenanceType) {
  int priority = 5;

  if (maintenanceType == "VACUUM") {
    if (metrics.fragmentation_pct > 50)
      priority = 9;
    else if (metrics.fragmentation_pct > 30)
      priority = 7;
    else if (metrics.fragmentation_pct > 10)
      priority = 5;
  } else if (maintenanceType == "REINDEX" ||
             maintenanceType == "REBUILD INDEX") {
    if (metrics.fragmentation_pct > 50)
      priority = 8;
    else if (metrics.fragmentation_pct > 30)
      priority = 6;
  }

  return priority;
}

void MaintenanceManager::storeTask(const MaintenanceTask &task) {
  try {
    pqxx::connection conn(metadataConnectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      INSERT INTO metadata.maintenance_control (
        maintenance_type, db_engine, connection_string, schema_name, object_name,
        object_type, auto_execute, enabled, priority, status, next_maintenance_date, thresholds,
        server_name, database_name
      ) VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
      )
      ON CONFLICT (db_engine, maintenance_type, schema_name, object_name, object_type)
      DO UPDATE SET
        priority = EXCLUDED.priority,
        next_maintenance_date = EXCLUDED.next_maintenance_date,
        last_checked_date = NOW(),
        updated_at = NOW()
    )";

    auto nextDate =
        std::chrono::system_clock::to_time_t(task.next_maintenance_date);
    std::stringstream ss;
    ss << std::put_time(std::gmtime(&nextDate), "%Y-%m-%d %H:%M:%S");
    std::string nextDateStr = ss.str();

    txn.exec_params(
        query, task.maintenance_type, task.db_engine, task.connection_string,
        task.schema_name, task.object_name, task.object_type, task.auto_execute,
        task.enabled, task.priority, task.status, nextDateStr,
        task.thresholds.dump(),
        task.server_name.empty() ? nullptr : task.server_name.c_str(),
        task.database_name.empty() ? nullptr : task.database_name.c_str());
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "MaintenanceManager",
                  "Error storing task: " + std::string(e.what()));
  }
}

void MaintenanceManager::updateTaskStatus(int taskId, const std::string &status,
                                          const std::string &resultMessage,
                                          const std::string &errorDetails) {
  try {
    pqxx::connection conn(metadataConnectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      UPDATE metadata.maintenance_control
      SET status = $1,
          result_message = $2,
          error_details = $3,
          updated_at = NOW()
      WHERE id = $4
    )";

    txn.exec_params(query, status, resultMessage, errorDetails, taskId);
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "MaintenanceManager",
                  "Error updating task status: " + std::string(e.what()));
  }
}

void MaintenanceManager::storeExecutionMetrics(
    const MaintenanceTask &task, const MaintenanceMetrics &before,
    const MaintenanceMetrics &after) {
  try {
    pqxx::connection conn(metadataConnectionString_);
    pqxx::work txn(conn);

    json metricsBeforeJson;
    metricsBeforeJson["fragmentation_pct"] = before.fragmentation_pct;
    metricsBeforeJson["dead_tuples"] = before.dead_tuples;
    metricsBeforeJson["live_tuples"] = before.live_tuples;
    metricsBeforeJson["table_size_mb"] = before.table_size_mb;
    metricsBeforeJson["index_size_mb"] = before.index_size_mb;

    json metricsAfterJson;
    metricsAfterJson["fragmentation_pct"] = after.fragmentation_pct;
    metricsAfterJson["dead_tuples"] = after.dead_tuples;
    metricsAfterJson["live_tuples"] = after.live_tuples;
    metricsAfterJson["table_size_mb"] = after.table_size_mb;
    metricsAfterJson["index_size_mb"] = after.index_size_mb;

    std::string query = R"(
      UPDATE metadata.maintenance_control
      SET metrics_before = $1,
          metrics_after = $2,
          space_reclaimed_mb = $3,
          performance_improvement_pct = $4,
          fragmentation_before = $5,
          fragmentation_after = $6,
          dead_tuples_before = $7,
          dead_tuples_after = $8,
          table_size_before_mb = $9,
          table_size_after_mb = $10,
          last_maintenance_date = NOW(),
          maintenance_count = maintenance_count + 1,
          next_maintenance_date = $11,
          updated_at = NOW()
      WHERE id = $12
    )";

    auto nextDate =
        std::chrono::system_clock::to_time_t(task.next_maintenance_date);
    std::stringstream ss;
    ss << std::put_time(std::gmtime(&nextDate), "%Y-%m-%d %H:%M:%S");
    std::string nextDateStr = ss.str();

    txn.exec_params(query, metricsBeforeJson.dump(), metricsAfterJson.dump(),
                    task.space_reclaimed_mb, task.performance_improvement_pct,
                    task.fragmentation_before, task.fragmentation_after,
                    before.dead_tuples, after.dead_tuples, before.table_size_mb,
                    after.table_size_mb, nextDateStr, task.id);
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "MaintenanceManager",
                  "Error storing execution metrics: " + std::string(e.what()));
  }
}

std::vector<MaintenanceTask> MaintenanceManager::getPendingTasks() {
  std::vector<MaintenanceTask> tasks;
  try {
    pqxx::connection conn(metadataConnectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      SELECT id, maintenance_type, db_engine, connection_string, schema_name,
             object_name, object_type, auto_execute, enabled, priority, status,
             next_maintenance_date, thresholds
      FROM metadata.maintenance_control
      WHERE status = 'PENDING'
        AND auto_execute = true
        AND enabled = true
        AND next_maintenance_date <= NOW()
      ORDER BY priority DESC, next_maintenance_date ASC
      LIMIT 10
    )";

    auto results = txn.exec(query);
    txn.commit();

    for (const auto &row : results) {
      MaintenanceTask task;
      task.id = row[0].as<int>();
      task.maintenance_type = row[1].as<std::string>();
      task.db_engine = row[2].as<std::string>();
      task.connection_string = row[3].as<std::string>();
      task.schema_name = row[4].as<std::string>();
      task.object_name = row[5].as<std::string>();
      task.object_type = row[6].as<std::string>();
      task.auto_execute = row[7].as<bool>();
      task.enabled = row[8].as<bool>();
      task.priority = row[9].as<int>();
      task.status = row[10].as<std::string>();

      if (!row[11].is_null()) {
        std::string dateStr = row[11].as<std::string>();
        std::tm tm = {};
        std::istringstream ss(dateStr);
        ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
        task.next_maintenance_date =
            std::chrono::system_clock::from_time_t(std::mktime(&tm));
      }

      if (!row[12].is_null()) {
        task.thresholds = json::parse(row[12].as<std::string>());
      }

      tasks.push_back(task);
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "MaintenanceManager",
                  "Error getting pending tasks: " + std::string(e.what()));
  }
  return tasks;
}

std::chrono::system_clock::time_point
MaintenanceManager::calculateNextMaintenanceDate(
    const std::string &maintenanceType) {
  auto now = std::chrono::system_clock::now();

  if (maintenanceType == "VACUUM" || maintenanceType == "OPTIMIZE TABLE") {
    return now + std::chrono::hours(24 * 7);
  } else if (maintenanceType == "ANALYZE" ||
             maintenanceType == "ANALYZE TABLE" ||
             maintenanceType == "UPDATE STATISTICS") {
    return now + std::chrono::hours(24);
  } else if (maintenanceType == "REINDEX" ||
             maintenanceType == "REBUILD INDEX") {
    return now + std::chrono::hours(24 * 30);
  }

  return now + std::chrono::hours(24);
}

void MaintenanceManager::loadThresholdsFromDatabase() {
  try {
    pqxx::connection conn(metadataConnectionString_);
    if (!conn.is_open()) {
      Logger::warning(LogCategory::GOVERNANCE, "MaintenanceManager",
                      "Failed to connect to metadata database for loading "
                      "thresholds, using defaults");
      return;
    }

    pqxx::work txn(conn);
    auto results = txn.exec("SELECT value FROM metadata.config WHERE key = "
                            "'maintenance_thresholds'");
    txn.commit();

    if (!results.empty() && !results[0][0].is_null()) {
      std::string valueStr = results[0][0].as<std::string>();
      if (!valueStr.empty()) {
        try {
          json loadedThresholds = json::parse(valueStr);
          if (!loadedThresholds.empty() && loadedThresholds.is_object()) {
            defaultThresholds_ = loadedThresholds;
            Logger::info(LogCategory::GOVERNANCE, "MaintenanceManager",
                         "Maintenance thresholds loaded from database");
            return;
          }
        } catch (const json::parse_error &e) {
          Logger::error(LogCategory::GOVERNANCE, "MaintenanceManager",
                        "Failed to parse maintenance_thresholds JSON: " +
                            std::string(e.what()) + ", using defaults");
        }
      }
    }

    Logger::info(LogCategory::GOVERNANCE, "MaintenanceManager",
                 "No maintenance_thresholds found in database, using defaults");
  } catch (const pqxx::sql_error &e) {
    Logger::error(LogCategory::GOVERNANCE, "MaintenanceManager",
                  "SQL error loading thresholds: " + std::string(e.what()) +
                      ", using defaults");
  } catch (const pqxx::broken_connection &e) {
    Logger::error(LogCategory::GOVERNANCE, "MaintenanceManager",
                  "Connection error loading thresholds: " +
                      std::string(e.what()) + ", using defaults");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "MaintenanceManager",
                  "Error loading thresholds from database: " +
                      std::string(e.what()) + ", using defaults");
  }
}

json MaintenanceManager::getThresholds(const std::string &dbEngine,
                                       const std::string &maintenanceType) {
  try {
    return defaultThresholds_[dbEngine][maintenanceType];
  } catch (...) {
    return json::object();
  }
}

std::string MaintenanceManager::escapeSQL(pqxx::connection &conn,
                                          const std::string &str) {
  return conn.quote(str);
}

std::string MaintenanceManager::escapeSQL(MYSQL *conn, const std::string &str) {
  if (!conn || str.empty()) {
    return str;
  }
  std::vector<char> escaped(str.length() * 2 + 1);
  unsigned long len =
      mysql_real_escape_string(conn, escaped.data(), str.c_str(), str.length());
  if (len == (unsigned long)-1) {
    return str;
  }
  return std::string(escaped.data(), len);
}

std::string MaintenanceManager::escapeSQLMSSQL(const std::string &str) {
  std::string escaped = str;
  size_t pos = 0;
  while ((pos = escaped.find(']', pos)) != std::string::npos) {
    escaped.replace(pos, 1, "]]");
    pos += 2;
  }
  return escaped;
}

void MaintenanceManager::executeManual(int maintenanceId) {
  Logger::info(LogCategory::GOVERNANCE, "MaintenanceManager",
               "Executing manual maintenance task: " +
                   std::to_string(maintenanceId));

  try {
    pqxx::connection conn(metadataConnectionString_);
    pqxx::work txn(conn);

    std::string query = R"(
      SELECT id, maintenance_type, db_engine, connection_string, schema_name,
             object_name, object_type, auto_execute, enabled, priority, status,
             next_maintenance_date, thresholds
      FROM metadata.maintenance_control
      WHERE id = $1
    )";

    auto results = txn.exec_params(query, maintenanceId);
    txn.commit();

    if (results.empty()) {
      Logger::error(LogCategory::GOVERNANCE, "MaintenanceManager",
                    "Maintenance task not found: " +
                        std::to_string(maintenanceId));
      return;
    }

    const auto &row = results[0];
    MaintenanceTask task;
    task.id = row[0].as<int>();
    task.maintenance_type = row[1].as<std::string>();
    task.db_engine = row[2].as<std::string>();
    task.connection_string = row[3].as<std::string>();
    task.schema_name = row[4].as<std::string>();
    task.object_name = row[5].as<std::string>();
    task.object_type = row[6].as<std::string>();
    task.auto_execute = row[7].as<bool>();
    task.enabled = row[8].as<bool>();
    task.priority = row[9].as<int>();
    task.status = row[10].as<std::string>();

    if (!row[11].is_null()) {
      std::string dateStr = row[11].as<std::string>();
      std::tm tm = {};
      std::istringstream ss(dateStr);
      ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
      task.next_maintenance_date =
          std::chrono::system_clock::from_time_t(std::mktime(&tm));
    }

    if (!row[12].is_null()) {
      task.thresholds = json::parse(row[12].as<std::string>());
    }

    updateTaskStatus(task.id, "RUNNING");
    auto startTime = std::chrono::high_resolution_clock::now();

    MaintenanceMetrics before = collectMetricsBefore(task);

    if (task.db_engine == "PostgreSQL") {
      executePostgreSQLMaintenance(task);
    } else if (task.db_engine == "MariaDB") {
      executeMariaDBMaintenance(task);
    } else if (task.db_engine == "MSSQL") {
      executeMSSQLMaintenance(task);
    }

    auto endTime = std::chrono::high_resolution_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::seconds>(endTime - startTime);

    MaintenanceMetrics after = collectMetricsAfter(task);
    MaintenanceTask updatedTask = task;
    calculateImpact(updatedTask, before, after);

    std::string resultMsg = "Manual maintenance completed successfully. ";
    if (updatedTask.space_reclaimed_mb > 0) {
      resultMsg +=
          "Space reclaimed: " + std::to_string(updatedTask.space_reclaimed_mb) +
          " MB. ";
    }
    if (updatedTask.performance_improvement_pct > 0) {
      resultMsg += "Performance improvement: " +
                   std::to_string(updatedTask.performance_improvement_pct) +
                   "%. ";
    }

    updateTaskStatus(task.id, "COMPLETED", resultMsg);
    storeExecutionMetrics(updatedTask, before, after);

    Logger::info(LogCategory::GOVERNANCE, "MaintenanceManager",
                 "Manual maintenance task " + std::to_string(maintenanceId) +
                     " completed in " + std::to_string(duration.count()) +
                     " seconds");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "MaintenanceManager",
                  "Error executing manual maintenance: " +
                      std::string(e.what()));
    try {
      updateTaskStatus(maintenanceId, "FAILED", "", std::string(e.what()));
    } catch (...) {
    }
  }
}

void MaintenanceManager::generateReport() {
  Logger::info(LogCategory::GOVERNANCE, "MaintenanceManager",
               "Generating maintenance report");

  try {
    pqxx::connection conn(metadataConnectionString_);
    pqxx::work txn(conn);

    std::string reportQuery = R"(
      SELECT 
        COUNT(*) as total_tasks,
        COUNT(*) FILTER (WHERE status = 'PENDING') as pending_tasks,
        COUNT(*) FILTER (WHERE status = 'RUNNING') as running_tasks,
        COUNT(*) FILTER (WHERE status = 'COMPLETED') as completed_tasks,
        COUNT(*) FILTER (WHERE status = 'FAILED') as failed_tasks,
        SUM(space_reclaimed_mb) as total_space_reclaimed_mb,
        AVG(performance_improvement_pct) as avg_performance_improvement,
        COUNT(*) FILTER (WHERE db_engine = 'PostgreSQL') as postgresql_tasks,
        COUNT(*) FILTER (WHERE db_engine = 'MariaDB') as mariadb_tasks,
        COUNT(*) FILTER (WHERE db_engine = 'MSSQL') as mssql_tasks
      FROM metadata.maintenance_control
    )";

    auto result = txn.exec(reportQuery);
    txn.commit();

    if (!result.empty()) {
      const auto &row = result[0];
      int totalTasks = row[0].as<int>();
      int pendingTasks = row[1].as<int>();
      int runningTasks = row[2].as<int>();
      int completedTasks = row[3].as<int>();
      int failedTasks = row[4].as<int>();
      double totalSpaceReclaimed = row[5].is_null() ? 0.0 : row[5].as<double>();
      double avgPerformanceImprovement =
          row[6].is_null() ? 0.0 : row[6].as<double>();
      int postgresqlTasks = row[7].as<int>();
      int mariadbTasks = row[8].as<int>();
      int mssqlTasks = row[9].as<int>();

      Logger::info(LogCategory::GOVERNANCE, "MaintenanceManager",
                   "=== Maintenance Report ===");
      Logger::info(LogCategory::GOVERNANCE, "MaintenanceManager",
                   "Total tasks: " + std::to_string(totalTasks));
      Logger::info(LogCategory::GOVERNANCE, "MaintenanceManager",
                   "Pending: " + std::to_string(pendingTasks) +
                       ", Running: " + std::to_string(runningTasks) +
                       ", Completed: " + std::to_string(completedTasks) +
                       ", Failed: " + std::to_string(failedTasks));
      Logger::info(LogCategory::GOVERNANCE, "MaintenanceManager",
                   "Total space reclaimed: " +
                       std::to_string(totalSpaceReclaimed) + " MB");
      Logger::info(LogCategory::GOVERNANCE, "MaintenanceManager",
                   "Average performance improvement: " +
                       std::to_string(avgPerformanceImprovement) + "%");
      Logger::info(
          LogCategory::GOVERNANCE, "MaintenanceManager",
          "By engine - PostgreSQL: " + std::to_string(postgresqlTasks) +
              ", MariaDB: " + std::to_string(mariadbTasks) +
              ", MSSQL: " + std::to_string(mssqlTasks));
    }

    std::string recentQuery = R"(
      SELECT maintenance_type, db_engine, schema_name, object_name, status,
             last_maintenance_date, space_reclaimed_mb, performance_improvement_pct
      FROM metadata.maintenance_control
      WHERE last_maintenance_date IS NOT NULL
      ORDER BY last_maintenance_date DESC
      LIMIT 10
    )";

    pqxx::work txn2(conn);
    auto recentResults = txn2.exec(recentQuery);
    txn2.commit();

    if (!recentResults.empty()) {
      Logger::info(LogCategory::GOVERNANCE, "MaintenanceManager",
                   "=== Recent Maintenance Activities ===");
      for (const auto &row : recentResults) {
        std::string maintType = row[0].as<std::string>();
        std::string engine = row[1].as<std::string>();
        std::string schema = row[2].as<std::string>();
        std::string object = row[3].as<std::string>();
        std::string status = row[4].as<std::string>();
        std::string lastDate =
            row[5].is_null() ? "N/A" : row[5].as<std::string>();
        double spaceReclaimed = row[6].is_null() ? 0.0 : row[6].as<double>();

        Logger::info(
            LogCategory::GOVERNANCE, "MaintenanceManager",
            engine + " - " + maintType + " on " + schema + "." + object +
                " - Status: " + status +
                (spaceReclaimed > 0
                     ? " - Space: " + std::to_string(spaceReclaimed) + " MB"
                     : "") +
                " - Date: " + lastDate);
      }
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "MaintenanceManager",
                  "Error generating report: " + std::string(e.what()));
  }
}
