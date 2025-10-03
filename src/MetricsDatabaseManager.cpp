#include "MetricsDatabaseManager.h"
#include <algorithm>

pqxx::connection MetricsDatabaseManager::getConnection() const {
  pqxx::connection conn(DatabaseConfig::getPostgresConnectionString());
  conn.set_session_var("statement_timeout", "30000");
  conn.set_session_var("lock_timeout", "10000");
  return conn;
}

std::string MetricsDatabaseManager::escapeSQL(const std::string &value) const {
  std::string escaped = value;
  size_t pos = 0;
  while ((pos = escaped.find("'", pos)) != std::string::npos) {
    escaped.replace(pos, 1, "''");
    pos += 2;
  }
  return escaped;
}

void MetricsDatabaseManager::executeQuery(const std::string &query) {
  auto conn = getConnection();
  if (!conn.is_open()) {
    Logger::getInstance().error(LogCategory::METRICS,
                                "Failed to connect to database");
    return;
  }

  pqxx::work txn(conn);
  txn.exec(query);
  txn.commit();
}

pqxx::result
MetricsDatabaseManager::executeSelectQuery(const std::string &query) const {
  auto conn = getConnection();
  if (!conn.is_open()) {
    Logger::getInstance().error(LogCategory::METRICS,
                                "Failed to connect to database");
    return pqxx::result();
  }

  pqxx::work txn(conn);
  auto result = txn.exec(query);
  txn.commit();
  return result;
}

void MetricsDatabaseManager::createMetricsTable() {
  try {
    std::string createTableSQL =
        "CREATE TABLE IF NOT EXISTS metadata.transfer_metrics ("
        "id SERIAL PRIMARY KEY,"
        "schema_name VARCHAR(100) NOT NULL,"
        "table_name VARCHAR(100) NOT NULL,"
        "db_engine VARCHAR(50) NOT NULL,"
        "records_transferred BIGINT DEFAULT 0,"
        "bytes_transferred BIGINT DEFAULT 0,"
        "memory_used_mb DECIMAL(15,2) DEFAULT 0.0,"
        "io_operations_per_second INTEGER DEFAULT 0,"
        "transfer_type VARCHAR(20) DEFAULT 'UNKNOWN',"
        "status VARCHAR(20) DEFAULT 'PENDING',"
        "error_message TEXT,"
        "started_at TIMESTAMP,"
        "completed_at TIMESTAMP,"
        "created_at TIMESTAMP DEFAULT NOW(),"
        "created_date DATE GENERATED ALWAYS AS (created_at::DATE) STORED,"
        "CONSTRAINT unique_table_metrics UNIQUE (schema_name, table_name, "
        "db_engine, created_date)"
        ");";

    executeQuery(createTableSQL);

    std::string createIndexesSQL =
        "CREATE INDEX IF NOT EXISTS idx_transfer_metrics_schema_table "
        "ON metadata.transfer_metrics (schema_name, table_name);"
        "CREATE INDEX IF NOT EXISTS idx_transfer_metrics_db_engine "
        "ON metadata.transfer_metrics (db_engine);"
        "CREATE INDEX IF NOT EXISTS idx_transfer_metrics_status "
        "ON metadata.transfer_metrics (status);";

    executeQuery(createIndexesSQL);

    Logger::getInstance().info(LogCategory::METRICS,
                               "Transfer metrics table created successfully");
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::METRICS,
                                "Error creating metrics table: " +
                                    std::string(e.what()));
  }
}

bool MetricsDatabaseManager::tableExists() const {
  try {
    std::string checkTableSQL =
        "SELECT EXISTS (SELECT FROM information_schema.tables "
        "WHERE table_schema = 'metadata' AND table_name = 'transfer_metrics');";

    auto result = executeSelectQuery(checkTableSQL);
    return !result.empty() && result[0][0].as<bool>();
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::METRICS,
                                "Error checking table existence: " +
                                    std::string(e.what()));
    return false;
  }
}

void MetricsDatabaseManager::saveMetrics(const TransferMetricsData &data) {
  try {
    auto conn = getConnection();
    if (!conn.is_open()) {
      Logger::getInstance().error(LogCategory::METRICS,
                                  "Failed to connect to database");
      return;
    }

    pqxx::work txn(conn);

    std::string insertQuery =
        "INSERT INTO metadata.transfer_metrics ("
        "schema_name, table_name, db_engine,"
        "records_transferred, bytes_transferred, memory_used_mb, "
        "io_operations_per_second,"
        "transfer_type, status, error_message,"
        "started_at, completed_at"
        ") VALUES ("
        "$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12"
        ") ON CONFLICT (schema_name, table_name, db_engine, "
        "created_date) DO UPDATE SET "
        "records_transferred = EXCLUDED.records_transferred,"
        "bytes_transferred = EXCLUDED.bytes_transferred,"
        "memory_used_mb = EXCLUDED.memory_used_mb,"
        "io_operations_per_second = EXCLUDED.io_operations_per_second,"
        "transfer_type = EXCLUDED.transfer_type,"
        "status = EXCLUDED.status,"
        "error_message = EXCLUDED.error_message,"
        "started_at = EXCLUDED.started_at,"
        "completed_at = EXCLUDED.completed_at;";

    for (const auto &metric : data.getMetrics()) {
      txn.exec_params(
          insertQuery, metric.schema_name, metric.table_name, metric.db_engine,
          metric.records_transferred, metric.bytes_transferred,
          metric.memory_used_mb, metric.io_operations_per_second,
          metric.transfer_type, metric.status,
          metric.error_message.empty() ? nullptr : metric.error_message.c_str(),
          metric.started_at.empty() ? nullptr : metric.started_at.c_str(),
          metric.completed_at.empty() ? nullptr : metric.completed_at.c_str());
    }

    txn.commit();
    Logger::getInstance().info(LogCategory::METRICS,
                               "Saved " + std::to_string(data.size()) +
                                   " metrics to database");
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::METRICS,
                                "Error saving metrics to database: " +
                                    std::string(e.what()));
  }
}

void MetricsDatabaseManager::loadMetrics(TransferMetricsData &data) {
  try {
    std::string selectQuery =
        "SELECT schema_name, table_name, db_engine, records_transferred, "
        "bytes_transferred, memory_used_mb, io_operations_per_second, "
        "transfer_type, status, error_message, started_at, completed_at "
        "FROM metadata.transfer_metrics "
        "WHERE created_at >= CURRENT_DATE "
        "ORDER BY created_at DESC;";

    auto result = executeSelectQuery(selectQuery);
    data.clear();

    for (const auto &row : result) {
      TransferMetrics metric;
      metric.schema_name = row[0].as<std::string>();
      metric.table_name = row[1].as<std::string>();
      metric.db_engine = row[2].as<std::string>();
      metric.records_transferred = row[3].as<long long>();
      metric.bytes_transferred = row[4].as<long long>();
      metric.memory_used_mb = row[5].as<double>();
      metric.io_operations_per_second = row[6].as<int>();
      metric.transfer_type = row[7].as<std::string>();
      metric.status = row[8].as<std::string>();
      metric.error_message = row[9].is_null() ? "" : row[9].as<std::string>();
      metric.started_at = row[10].is_null() ? "" : row[10].as<std::string>();
      metric.completed_at = row[11].is_null() ? "" : row[11].as<std::string>();

      data.addMetric(metric);
    }

    Logger::getInstance().info(LogCategory::METRICS,
                               "Loaded " + std::to_string(data.size()) +
                                   " metrics from database");
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::METRICS,
                                "Error loading metrics from database: " +
                                    std::string(e.what()));
  }
}

void MetricsDatabaseManager::clearOldMetrics(int daysToKeep) {
  try {
    std::string deleteQuery = "DELETE FROM metadata.transfer_metrics "
                              "WHERE created_at < NOW() - INTERVAL '" +
                              std::to_string(daysToKeep) + " days';";

    executeQuery(deleteQuery);
    Logger::getInstance().info(LogCategory::METRICS,
                               "Cleared metrics older than " +
                                   std::to_string(daysToKeep) + " days");
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::METRICS,
                                "Error clearing old metrics: " +
                                    std::string(e.what()));
  }
}

std::vector<TransferMetrics> MetricsDatabaseManager::getMetricsByDateRange(
    const std::string &startDate, const std::string &endDate) const {
  std::vector<TransferMetrics> result;
  try {
    std::string query =
        "SELECT schema_name, table_name, db_engine, records_transferred, "
        "bytes_transferred, memory_used_mb, io_operations_per_second, "
        "transfer_type, status, error_message, started_at, completed_at "
        "FROM metadata.transfer_metrics "
        "WHERE created_at BETWEEN '" +
        escapeSQL(startDate) + "' AND '" + escapeSQL(endDate) +
        "' "
        "ORDER BY created_at DESC;";

    auto dbResult = executeSelectQuery(query);
    for (const auto &row : dbResult) {
      TransferMetrics metric;
      metric.schema_name = row[0].as<std::string>();
      metric.table_name = row[1].as<std::string>();
      metric.db_engine = row[2].as<std::string>();
      metric.records_transferred = row[3].as<long long>();
      metric.bytes_transferred = row[4].as<long long>();
      metric.memory_used_mb = row[5].as<double>();
      metric.io_operations_per_second = row[6].as<int>();
      metric.transfer_type = row[7].as<std::string>();
      metric.status = row[8].as<std::string>();
      metric.error_message = row[9].is_null() ? "" : row[9].as<std::string>();
      metric.started_at = row[10].is_null() ? "" : row[10].as<std::string>();
      metric.completed_at = row[11].is_null() ? "" : row[11].as<std::string>();

      result.push_back(metric);
    }
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::METRICS,
                                "Error getting metrics by date range: " +
                                    std::string(e.what()));
  }
  return result;
}

std::vector<TransferMetrics>
MetricsDatabaseManager::getMetricsByStatus(const std::string &status) const {
  std::vector<TransferMetrics> result;
  try {
    std::string query =
        "SELECT schema_name, table_name, db_engine, records_transferred, "
        "bytes_transferred, memory_used_mb, io_operations_per_second, "
        "transfer_type, status, error_message, started_at, completed_at "
        "FROM metadata.transfer_metrics "
        "WHERE status = '" +
        escapeSQL(status) +
        "' "
        "ORDER BY created_at DESC;";

    auto dbResult = executeSelectQuery(query);
    for (const auto &row : dbResult) {
      TransferMetrics metric;
      metric.schema_name = row[0].as<std::string>();
      metric.table_name = row[1].as<std::string>();
      metric.db_engine = row[2].as<std::string>();
      metric.records_transferred = row[3].as<long long>();
      metric.bytes_transferred = row[4].as<long long>();
      metric.memory_used_mb = row[5].as<double>();
      metric.io_operations_per_second = row[6].as<int>();
      metric.transfer_type = row[7].as<std::string>();
      metric.status = row[8].as<std::string>();
      metric.error_message = row[9].is_null() ? "" : row[9].as<std::string>();
      metric.started_at = row[10].is_null() ? "" : row[10].as<std::string>();
      metric.completed_at = row[11].is_null() ? "" : row[11].as<std::string>();

      result.push_back(metric);
    }
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::METRICS,
                                "Error getting metrics by status: " +
                                    std::string(e.what()));
  }
  return result;
}

std::vector<TransferMetrics>
MetricsDatabaseManager::getMetricsByEngine(const std::string &engine) const {
  std::vector<TransferMetrics> result;
  try {
    std::string query =
        "SELECT schema_name, table_name, db_engine, records_transferred, "
        "bytes_transferred, memory_used_mb, io_operations_per_second, "
        "transfer_type, status, error_message, started_at, completed_at "
        "FROM metadata.transfer_metrics "
        "WHERE db_engine = '" +
        escapeSQL(engine) +
        "' "
        "ORDER BY created_at DESC;";

    auto dbResult = executeSelectQuery(query);
    for (const auto &row : dbResult) {
      TransferMetrics metric;
      metric.schema_name = row[0].as<std::string>();
      metric.table_name = row[1].as<std::string>();
      metric.db_engine = row[2].as<std::string>();
      metric.records_transferred = row[3].as<long long>();
      metric.bytes_transferred = row[4].as<long long>();
      metric.memory_used_mb = row[5].as<double>();
      metric.io_operations_per_second = row[6].as<int>();
      metric.transfer_type = row[7].as<std::string>();
      metric.status = row[8].as<std::string>();
      metric.error_message = row[9].is_null() ? "" : row[9].as<std::string>();
      metric.started_at = row[10].is_null() ? "" : row[10].as<std::string>();
      metric.completed_at = row[11].is_null() ? "" : row[11].as<std::string>();

      result.push_back(metric);
    }
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::METRICS,
                                "Error getting metrics by engine: " +
                                    std::string(e.what()));
  }
  return result;
}

long long MetricsDatabaseManager::getTotalRecordsTransferred() const {
  try {
    std::string query =
        "SELECT COALESCE(SUM(records_transferred), 0) FROM "
        "metadata.transfer_metrics WHERE created_at >= CURRENT_DATE;";
    auto result = executeSelectQuery(query);
    return result.empty() ? 0 : result[0][0].as<long long>();
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::METRICS,
                                "Error getting total records: " +
                                    std::string(e.what()));
    return 0;
  }
}

long long MetricsDatabaseManager::getTotalBytesTransferred() const {
  try {
    std::string query =
        "SELECT COALESCE(SUM(bytes_transferred), 0) FROM "
        "metadata.transfer_metrics WHERE created_at >= CURRENT_DATE;";
    auto result = executeSelectQuery(query);
    return result.empty() ? 0 : result[0][0].as<long long>();
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::METRICS,
                                "Error getting total bytes: " +
                                    std::string(e.what()));
    return 0;
  }
}

double MetricsDatabaseManager::getAverageMemoryUsage() const {
  try {
    std::string query =
        "SELECT COALESCE(AVG(memory_used_mb), 0) FROM "
        "metadata.transfer_metrics WHERE created_at >= CURRENT_DATE;";
    auto result = executeSelectQuery(query);
    return result.empty() ? 0.0 : result[0][0].as<double>();
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::METRICS,
                                "Error getting average memory usage: " +
                                    std::string(e.what()));
    return 0.0;
  }
}

int MetricsDatabaseManager::getSuccessCount() const {
  try {
    std::string query = "SELECT COUNT(*) FROM metadata.transfer_metrics WHERE "
                        "status = 'SUCCESS' AND created_at >= CURRENT_DATE;";
    auto result = executeSelectQuery(query);
    return result.empty() ? 0 : result[0][0].as<int>();
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::METRICS,
                                "Error getting success count: " +
                                    std::string(e.what()));
    return 0;
  }
}

int MetricsDatabaseManager::getFailedCount() const {
  try {
    std::string query = "SELECT COUNT(*) FROM metadata.transfer_metrics WHERE "
                        "status = 'FAILED' AND created_at >= CURRENT_DATE;";
    auto result = executeSelectQuery(query);
    return result.empty() ? 0 : result[0][0].as<int>();
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::METRICS,
                                "Error getting failed count: " +
                                    std::string(e.what()));
    return 0;
  }
}

int MetricsDatabaseManager::getPendingCount() const {
  try {
    std::string query = "SELECT COUNT(*) FROM metadata.transfer_metrics WHERE "
                        "status = 'PENDING' AND created_at >= CURRENT_DATE;";
    auto result = executeSelectQuery(query);
    return result.empty() ? 0 : result[0][0].as<int>();
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::METRICS,
                                "Error getting pending count: " +
                                    std::string(e.what()));
    return 0;
  }
}

double MetricsDatabaseManager::getSuccessRate() const {
  try {
    std::string query =
        "SELECT CASE WHEN COUNT(*) > 0 THEN "
        "(COUNT(*) FILTER (WHERE status = 'SUCCESS')::float / COUNT(*) * 100) "
        "ELSE 0 END FROM metadata.transfer_metrics WHERE created_at >= "
        "CURRENT_DATE;";
    auto result = executeSelectQuery(query);
    return result.empty() ? 0.0 : result[0][0].as<double>();
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::METRICS,
                                "Error getting success rate: " +
                                    std::string(e.what()));
    return 0.0;
  }
}
