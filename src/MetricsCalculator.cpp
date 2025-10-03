#include "MetricsCalculator.h"
#include <algorithm>
#include <iomanip>
#include <sstream>

pqxx::connection MetricsCalculator::getConnection() const {
  pqxx::connection conn(DatabaseConfig::getPostgresConnectionString());
  conn.set_session_var("statement_timeout", "30000");
  conn.set_session_var("lock_timeout", "10000");
  return conn;
}

std::string MetricsCalculator::escapeSQL(const std::string &value) const {
  std::string escaped = value;
  size_t pos = 0;
  while ((pos = escaped.find("'", pos)) != std::string::npos) {
    escaped.replace(pos, 1, "''");
    pos += 2;
  }
  return escaped;
}

bool MetricsCalculator::isValidRow(const pqxx::row &row) const {
  return !row[0].is_null() && !row[1].is_null() && !row[2].is_null() &&
         !row[0].as<std::string>().empty() &&
         !row[1].as<std::string>().empty() && !row[2].as<std::string>().empty();
}

void MetricsCalculator::validateMetric(TransferMetrics &metric) const {
  if (metric.schema_name.empty())
    metric.schema_name = "unknown";
  if (metric.table_name.empty())
    metric.table_name = "unknown";
  if (metric.db_engine.empty())
    metric.db_engine = "unknown";
  if (metric.records_transferred < 0)
    metric.records_transferred = 0;
  if (metric.bytes_transferred < 0)
    metric.bytes_transferred = 0;
  if (metric.memory_used_mb < 0.0)
    metric.memory_used_mb = 0.0;
  if (metric.io_operations_per_second < 0)
    metric.io_operations_per_second = 0;
}

void MetricsCalculator::collectTransferMetrics(TransferMetricsData &data) {
  try {
    auto conn = getConnection();
    if (!conn.is_open()) {
      Logger::getInstance().error(LogCategory::METRICS,
                                  "Failed to connect to database");
      return;
    }

    pqxx::work txn(conn);

    std::string transferQuery =
        "SELECT "
        "c.schema_name,"
        "c.table_name,"
        "c.db_engine,"
        "c.status,"
        "c.last_sync_time,"
        "c.last_sync_column,"
        "COALESCE(pg.n_live_tup, 0) as current_records,"
        "COALESCE(pg_total_relation_size(pc.oid), 0) as table_size_bytes "
        "FROM metadata.catalog c "
        "LEFT JOIN pg_stat_user_tables pg ON c.schema_name = pg.schemaname AND "
        "c.table_name = pg.relname "
        "LEFT JOIN pg_class pc ON pg.relname = pc.relname AND pg.schemaname "
        "= pc.relnamespace::regnamespace::text "
        "WHERE c.db_engine IS NOT NULL AND c.active = true;";

    auto result = txn.exec(transferQuery);
    txn.commit();

    data.clear();
    data.getMetrics().reserve(result.size());

    for (const auto &row : result) {
      if (!isValidRow(row)) {
        Logger::getInstance().warning(LogCategory::METRICS,
                                      "Skipping invalid row");
        continue;
      }

      TransferMetrics metric;
      metric.schema_name = row[0].as<std::string>();
      metric.table_name = row[1].as<std::string>();
      metric.db_engine = row[2].as<std::string>();

      std::string status = row[3].is_null() ? "" : row[3].as<std::string>();
      long long currentRecords = row[6].is_null() ? 0 : row[6].as<long long>();
      long long tableSizeBytes = row[7].is_null() ? 0 : row[7].as<long long>();

      if (currentRecords < 0)
        currentRecords = 0;
      if (tableSizeBytes < 0)
        tableSizeBytes = 0;

      if (currentRecords <= 0 && tableSizeBytes <= 0) {
        continue;
      }

      metric.records_transferred = currentRecords;
      metric.bytes_transferred = tableSizeBytes;
      metric.memory_used_mb = calculateMemoryUsage(tableSizeBytes);
      metric.io_operations_per_second = 0;
      metric.transfer_type = mapTransferType(status);
      metric.status = mapStatus(status, true);
      metric.error_message = mapErrorMessage(status);

      if (!row[4].is_null()) {
        metric.completed_at = row[4].as<std::string>();
        metric.started_at = getEstimatedStartTime(metric.completed_at);
      } else {
        metric.started_at = getCurrentTimestamp();
        metric.completed_at = "";
      }

      validateMetric(metric);
      data.addMetric(metric);
    }

    Logger::getInstance().info(LogCategory::METRICS,
                               "Collected transfer metrics for " +
                                   std::to_string(data.size()) + " tables");
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::METRICS,
                                "Error collecting transfer metrics: " +
                                    std::string(e.what()));
  }
}

void MetricsCalculator::collectPerformanceMetrics(TransferMetricsData &data) {
  try {
    auto conn = getConnection();
    if (!conn.is_open()) {
      Logger::getInstance().error(LogCategory::METRICS,
                                  "Failed to connect to database");
      return;
    }

    pqxx::work txn(conn);

    std::string performanceQuery =
        "SELECT "
        "pst.schemaname,"
        "pst.relname,"
        "pst.n_tup_ins as inserts,"
        "pst.n_tup_upd as updates,"
        "pst.n_tup_del as deletes,"
        "pst.n_live_tup as live_tuples,"
        "pst.n_dead_tup as dead_tuples,"
        "pst.last_autoanalyze,"
        "pst.last_autovacuum,"
        "COALESCE(pg_total_relation_size(pc.oid), 0) as table_size_bytes "
        "FROM pg_stat_user_tables pst "
        "LEFT JOIN pg_class pc ON pst.relname = pc.relname "
        "AND pst.schemaname = pc.relnamespace::regnamespace::text "
        "WHERE pst.schemaname IN (SELECT DISTINCT schema_name FROM "
        "metadata.catalog);";

    auto result = txn.exec(performanceQuery);
    txn.commit();

    for (auto &metric : data.getMetrics()) {
      for (const auto &row : result) {
        if (row[0].as<std::string>() == metric.schema_name &&
            row[1].as<std::string>() == metric.table_name) {

          long long total_operations = row[2].as<long long>() +
                                       row[3].as<long long>() +
                                       row[4].as<long long>();

          metric.io_operations_per_second = static_cast<int>(total_operations);
          metric.memory_used_mb = calculateMemoryUsage(row[9].as<long long>());
          break;
        }
      }
    }

    Logger::getInstance().info(LogCategory::METRICS,
                               "Collected performance metrics");
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::METRICS,
                                "Error collecting performance metrics: " +
                                    std::string(e.what()));
  }
}

void MetricsCalculator::collectMetadataMetrics(TransferMetricsData &data) {
  try {
    auto conn = getConnection();
    if (!conn.is_open()) {
      Logger::getInstance().error(LogCategory::METRICS,
                                  "Failed to connect to database");
      return;
    }

    pqxx::work txn(conn);

    std::string metadataQuery =
        "SELECT schema_name, table_name, db_engine, status, active, "
        "last_sync_time, last_sync_column "
        "FROM metadata.catalog WHERE db_engine IS NOT NULL;";

    auto result = txn.exec(metadataQuery);
    txn.commit();

    for (auto &metric : data.getMetrics()) {
      for (const auto &row : result) {
        if (row[0].as<std::string>() == metric.schema_name &&
            row[1].as<std::string>() == metric.table_name &&
            row[2].as<std::string>() == metric.db_engine) {

          std::string status = row[3].as<std::string>();
          bool active = row[4].as<bool>();

          metric.transfer_type = mapTransferType(status);
          metric.status = mapStatus(status, active);
          metric.error_message = mapErrorMessage(status);

          if (!active) {
            metric.status = "FAILED";
            metric.error_message = "Table marked as inactive";
          } else if (row[5].is_null()) {
            metric.status = "PENDING";
          } else {
            metric.status = "SUCCESS";
          }

          break;
        }
      }
    }

    Logger::getInstance().info(LogCategory::METRICS,
                               "Collected metadata metrics");
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::METRICS,
                                "Error collecting metadata metrics: " +
                                    std::string(e.what()));
  }
}

void MetricsCalculator::collectTimestampMetrics(TransferMetricsData &data) {
  try {
    auto conn = getConnection();
    if (!conn.is_open()) {
      Logger::getInstance().error(LogCategory::METRICS,
                                  "Failed to connect to database");
      return;
    }

    pqxx::work txn(conn);

    std::string timestampQuery =
        "SELECT schema_name, table_name, db_engine, last_sync_time "
        "FROM metadata.catalog "
        "WHERE db_engine IS NOT NULL AND last_sync_time IS NOT NULL;";

    auto result = txn.exec(timestampQuery);
    txn.commit();

    for (auto &metric : data.getMetrics()) {
      for (const auto &row : result) {
        if (row[0].as<std::string>() == metric.schema_name &&
            row[1].as<std::string>() == metric.table_name &&
            row[2].as<std::string>() == metric.db_engine) {

          metric.completed_at = row[3].as<std::string>();
          metric.started_at = metric.completed_at;
          break;
        }
      }
    }

    Logger::getInstance().info(LogCategory::METRICS,
                               "Collected timestamp metrics");
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::METRICS,
                                "Error collecting timestamp metrics: " +
                                    std::string(e.what()));
  }
}

double MetricsCalculator::calculateTransferRate(long long records,
                                                int duration_ms) const {
  if (duration_ms <= 0)
    return 0.0;
  return static_cast<double>(records) / (duration_ms / 1000.0);
}

long long MetricsCalculator::calculateBytesTransferred(
    const std::string &schema_name, const std::string &table_name) const {
  try {
    auto conn = getConnection();
    if (!conn.is_open()) {
      Logger::getInstance().error(LogCategory::METRICS,
                                  "Failed to connect to database");
      return 0;
    }

    pqxx::work txn(conn);

    std::string sizeQuery =
        "SELECT COALESCE(pg_total_relation_size(to_regclass('\"" +
        escapeSQL(schema_name) + "\".\"" + escapeSQL(table_name) +
        "\"')), 0) as size_bytes;";

    auto result = txn.exec(sizeQuery);
    txn.commit();

    if (!result.empty()) {
      return result[0][0].as<long long>();
    }
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::METRICS,
                                "Error calculating bytes transferred: " +
                                    std::string(e.what()));
  }

  return 0;
}

double MetricsCalculator::calculateMemoryUsage(long long bytes) const {
  return bytes / (1024.0 * 1024.0);
}

int MetricsCalculator::calculateIOOperations(
    const std::string &schema_name, const std::string &table_name) const {
  try {
    auto conn = getConnection();
    if (!conn.is_open()) {
      Logger::getInstance().error(LogCategory::METRICS,
                                  "Failed to connect to database");
      return 0;
    }

    pqxx::work txn(conn);

    std::string ioQuery =
        "SELECT COALESCE(n_tup_ins + n_tup_upd + n_tup_del, 0) as total_ops "
        "FROM pg_stat_user_tables "
        "WHERE schemaname = '" +
        escapeSQL(schema_name) + "' AND relname = '" + escapeSQL(table_name) +
        "';";

    auto result = txn.exec(ioQuery);
    txn.commit();

    if (!result.empty()) {
      return static_cast<int>(result[0][0].as<long long>());
    }
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::METRICS,
                                "Error calculating IO operations: " +
                                    std::string(e.what()));
  }

  return 0;
}

std::string
MetricsCalculator::mapTransferType(const std::string &status) const {
  if (status == "full_load" || status == "FULL_LOAD") {
    return "FULL_LOAD";
  } else if (status == "incremental" || status == "INCREMENTAL") {
    return "INCREMENTAL";
  } else if (status == "sync" || status == "SYNC") {
    return "SYNC";
  } else {
    return "UNKNOWN";
  }
}

std::string MetricsCalculator::mapStatus(const std::string &catalogStatus,
                                         bool active) const {
  if (!active) {
    return "FAILED";
  } else if (catalogStatus == "ERROR" || catalogStatus == "FAILED") {
    return "FAILED";
  } else if (catalogStatus == "NO_DATA" || catalogStatus == "EMPTY") {
    return "SUCCESS";
  } else if (catalogStatus == "LISTENING_CHANGES" ||
             catalogStatus == "ACTIVE") {
    return "SUCCESS";
  } else if (catalogStatus == "PENDING" || catalogStatus == "WAITING") {
    return "PENDING";
  } else {
    return "UNKNOWN";
  }
}

std::string
MetricsCalculator::mapErrorMessage(const std::string &status) const {
  if (status == "ERROR" || status == "FAILED") {
    return "Transfer failed";
  } else if (status == "NO_DATA" || status == "EMPTY") {
    return "No data to transfer";
  } else if (status == "LISTENING_CHANGES" || status == "ACTIVE") {
    return "";
  } else if (status == "PENDING" || status == "WAITING") {
    return "Waiting for sync";
  } else {
    return "Unknown status: " + status;
  }
}

std::string MetricsCalculator::getCurrentTimestamp() const {
  auto now = std::chrono::system_clock::now();
  auto time_t = std::chrono::system_clock::to_time_t(now);
  auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                now.time_since_epoch()) %
            1000;

  std::stringstream ss;
  ss << std::put_time(std::localtime(&time_t), "%Y-%m-%d %H:%M:%S");
  ss << "." << std::setfill('0') << std::setw(3) << ms.count();
  return ss.str();
}

std::string
MetricsCalculator::getEstimatedStartTime(const std::string &completedAt) const {
  try {
    std::tm tm = {};
    std::istringstream ss(completedAt);
    ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");

    if (ss.fail()) {
      return getCurrentTimestamp();
    }

    auto time_point = std::chrono::system_clock::from_time_t(std::mktime(&tm));
    auto estimated_start = time_point - std::chrono::hours(1);
    auto time_t_start = std::chrono::system_clock::to_time_t(estimated_start);

    std::stringstream result;
    result << std::put_time(std::localtime(&time_t_start), "%Y-%m-%d %H:%M:%S");
    result << ".000";

    return result.str();
  } catch (const std::exception &e) {
    Logger::getInstance().warning(LogCategory::METRICS,
                                  "Error estimating start time: " +
                                      std::string(e.what()));
    return getCurrentTimestamp();
  }
}
