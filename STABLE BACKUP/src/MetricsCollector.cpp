#include "MetricsCollector.h"
#include <algorithm>
#include <chrono>
#include <iomanip>
#include <numeric>
#include <sstream>

void MetricsCollector::collectAllMetrics() {

  try {
    createMetricsTable();
    collectTransferMetrics();
    collectPerformanceMetrics();
    collectMetadataMetrics();
    collectTimestampMetrics();
    collectLatencyMetrics();
    saveMetricsToDatabase();
    generateMetricsReport();

    Logger::info(LogCategory::METRICS, "Metrics collection completed");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::METRICS, "Error in metrics collection: " + std::string(e.what()));
  }
}

void MetricsCollector::createMetricsTable() {
  try {
    pqxx::connection conn(DatabaseConfig::getPostgresConnectionString());
    pqxx::work txn(conn);

    std::string createTableSQL =
        "CREATE TABLE IF NOT EXISTS metadata.transfer_metrics ("
        "id SERIAL PRIMARY KEY,"
        "schema_name VARCHAR(100) NOT NULL,"
        "table_name VARCHAR(100) NOT NULL,"
        "db_engine VARCHAR(50) NOT NULL,"
        "records_transferred BIGINT,"
        "bytes_transferred BIGINT,"
        "memory_used_mb DECIMAL(15,2),"
        "io_operations_per_second INTEGER,"
        "avg_latency_ms DECIMAL(10,2),"
        "min_latency_ms DECIMAL(10,2),"
        "max_latency_ms DECIMAL(10,2),"
        "p95_latency_ms DECIMAL(10,2),"
        "p99_latency_ms DECIMAL(10,2),"
        "latency_samples INTEGER,"
        "transfer_type VARCHAR(20),"
        "status VARCHAR(20),"
        "error_message TEXT,"
        "started_at TIMESTAMP,"
        "completed_at TIMESTAMP,"
        "created_at TIMESTAMP DEFAULT NOW(),"
        "created_date DATE GENERATED ALWAYS AS (created_at::DATE) STORED,"
        "CONSTRAINT unique_table_metrics UNIQUE (schema_name, table_name, "
        "db_engine, created_date)"
        ");";

    txn.exec(createTableSQL);

    std::string createIndexesSQL =
        "CREATE INDEX IF NOT EXISTS idx_transfer_metrics_schema_table "
        "ON metadata.transfer_metrics (schema_name, table_name);"
        "CREATE INDEX IF NOT EXISTS idx_transfer_metrics_db_engine "
        "ON metadata.transfer_metrics (db_engine);"
        "CREATE INDEX IF NOT EXISTS idx_transfer_metrics_status "
        "ON metadata.transfer_metrics (status);";

    txn.exec(createIndexesSQL);
    txn.commit();

    Logger::info(LogCategory::METRICS, "Transfer metrics table created successfully");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::METRICS, "Error creating metrics table: " + std::string(e.what()));
  }
}

void MetricsCollector::collectTransferMetrics() {
  try {
    pqxx::connection conn(DatabaseConfig::getPostgresConnectionString());
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

    metrics.clear();
    for (const auto &row : result) {
      TransferMetrics metric;
      metric.schema_name = row[0].as<std::string>();
      metric.table_name = row[1].as<std::string>();
      metric.db_engine = row[2].as<std::string>();
      std::string status = row[3].as<std::string>();
      long long currentRecords = row[6].is_null() ? 0 : row[6].as<long long>();
      long long tableSizeBytes = row[7].is_null() ? 0 : row[7].as<long long>();

      if (currentRecords <= 0 && tableSizeBytes <= 0) {
        continue;
      }

      metric.records_transferred = currentRecords;
      metric.bytes_transferred = tableSizeBytes;
      metric.memory_used_mb = tableSizeBytes / (1024.0 * 1024.0);
      metric.io_operations_per_second = 0;
      metric.avg_latency_ms = 0.0;
      metric.min_latency_ms = 0.0;
      metric.max_latency_ms = 0.0;
      metric.p95_latency_ms = 0.0;
      metric.p99_latency_ms = 0.0;
      metric.latency_samples = 0;

      if (status == "full_load") {
        metric.transfer_type = "FULL_LOAD";
      } else if (status == "incremental") {
        metric.transfer_type = "INCREMENTAL";
      } else {
        metric.transfer_type = "SYNC";
      }

      if (status == "ERROR") {
        metric.status = "FAILED";
        metric.error_message = "Transfer failed";
      } else if (status == "NO_DATA") {
        metric.status = "SUCCESS";
        metric.error_message = "No data to transfer";
      } else {
        metric.status = "SUCCESS";
        metric.error_message = "";
      }

      if (!row[4].is_null()) {
        metric.completed_at = row[4].as<std::string>();
        metric.started_at = metric.completed_at;
      } else {
        metric.started_at = getCurrentTimestamp();
        metric.completed_at = getCurrentTimestamp();
      }

      metrics.push_back(metric);
    }

    Logger::info(LogCategory::METRICS, "Collected transfer metrics for " +
                 std::to_string(metrics.size()) + " tables");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::METRICS, "Error collecting transfer metrics: " +
                  std::string(e.what()));
  }
}

void MetricsCollector::collectPerformanceMetrics() {
  try {
    pqxx::connection conn(DatabaseConfig::getPostgresConnectionString());
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
        "AND pst.schemaname = "
        "pc.relnamespace::regnamespace::text "
        "WHERE pst.schemaname IN (SELECT DISTINCT schema_name FROM "
        "metadata.catalog);";

    auto result = txn.exec(performanceQuery);
    txn.commit();

    for (auto &metric : metrics) {
      for (const auto &row : result) {
        if (row[0].as<std::string>() == metric.schema_name &&
            row[1].as<std::string>() == metric.table_name) {

          long long total_operations = row[2].as<long long>() +
                                       row[3].as<long long>() +
                                       row[4].as<long long>();

          metric.io_operations_per_second = static_cast<int>(total_operations);
          metric.memory_used_mb = row[9].as<long long>() / (1024.0 * 1024.0);

          break;
        }
      }
    }

    Logger::info(LogCategory::METRICS, "Collected performance metrics");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::METRICS, "Error collecting performance metrics: " +
                  std::string(e.what()));
  }
}

void MetricsCollector::collectMetadataMetrics() {
  try {
    pqxx::connection conn(DatabaseConfig::getPostgresConnectionString());
    pqxx::work txn(conn);

    // Query para obtener metadatos
    std::string metadataQuery = "SELECT "
                                "schema_name,"
                                "table_name,"
                                "db_engine,"
                                "status,"
                                "active,"
                                "last_sync_time,"
                                "last_sync_column "
                                "FROM metadata.catalog "
                                "WHERE db_engine IS NOT NULL;";

    auto result = txn.exec(metadataQuery);
    txn.commit();

    // Mapear metadatos a nuestras métricas
    for (auto &metric : metrics) {
      for (const auto &row : result) {
        if (row[0].as<std::string>() == metric.schema_name &&
            row[1].as<std::string>() == metric.table_name &&
            row[2].as<std::string>() == metric.db_engine) {

          // Determinar tipo de transferencia
          std::string status = row[3].as<std::string>();
          if (status == "full_load") {
            metric.transfer_type = "FULL_LOAD";
          } else if (status == "incremental") {
            metric.transfer_type = "INCREMENTAL";
          } else {
            metric.transfer_type = "SYNC";
          }

          // Determinar status
          bool active = row[4].as<bool>();
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

    Logger::info(LogCategory::METRICS, "Collected metadata metrics");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::METRICS, "Error collecting metadata metrics: " +
                  std::string(e.what()));
  }
}

void MetricsCollector::collectTimestampMetrics() {
  try {
    pqxx::connection conn(DatabaseConfig::getPostgresConnectionString());
    pqxx::work txn(conn);

    std::string timestampQuery =
        "SELECT "
        "schema_name,"
        "table_name,"
        "db_engine,"
        "last_sync_time "
        "FROM metadata.catalog "
        "WHERE db_engine IS NOT NULL AND last_sync_time IS NOT NULL;";

    auto result = txn.exec(timestampQuery);
    txn.commit();

    for (auto &metric : metrics) {
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

    Logger::info(LogCategory::METRICS, "Collected timestamp metrics");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::METRICS, "Error collecting timestamp metrics: " +
                  std::string(e.what()));
  }
}

void MetricsCollector::collectLatencyMetrics() {
  try {
    pqxx::connection conn(DatabaseConfig::getPostgresConnectionString());
    pqxx::work txn(conn);

    for (auto &metric : metrics) {
      std::string latencyQuery = "SELECT "
                                 "EXTRACT(EPOCH FROM (completed_at - "
                                 "started_at)) * 1000 as latency_ms "
                                 "FROM metadata.transfer_metrics "
                                 "WHERE schema_name = $1 "
                                 "AND table_name = $2 "
                                 "AND db_engine = $3 "
                                 "AND completed_at IS NOT NULL "
                                 "AND started_at IS NOT NULL "
                                 "ORDER BY created_at DESC LIMIT 100;";

      auto result = txn.exec_params(latencyQuery, metric.schema_name,
                                    metric.table_name, metric.db_engine);

      std::vector<double> latencySamples;
      for (const auto &row : result) {
        if (!row[0].is_null()) {
          double latency = row[0].as<double>();
          if (latency > 0) {
            latencySamples.push_back(latency);
          }
        }
      }

      if (!latencySamples.empty()) {
        std::sort(latencySamples.begin(), latencySamples.end());

        metric.latency_samples = latencySamples.size();
        metric.avg_latency_ms =
            std::accumulate(latencySamples.begin(), latencySamples.end(), 0.0) /
            latencySamples.size();
        metric.min_latency_ms = latencySamples.front();
        metric.max_latency_ms = latencySamples.back();
        metric.p95_latency_ms = calculatePercentile(latencySamples, 95.0);
        metric.p99_latency_ms = calculatePercentile(latencySamples, 99.0);
      } else {
        metric.latency_samples = 0;
        metric.avg_latency_ms = 0.0;
        metric.min_latency_ms = 0.0;
        metric.max_latency_ms = 0.0;
        metric.p95_latency_ms = 0.0;
        metric.p99_latency_ms = 0.0;
      }
    }

    txn.commit();
    Logger::info(LogCategory::METRICS, "Collected latency metrics");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::METRICS, "Error collecting latency metrics: " + std::string(e.what()));
  }
}

void MetricsCollector::saveMetricsToDatabase() {
  try {
    pqxx::connection conn(DatabaseConfig::getPostgresConnectionString());
    pqxx::work txn(conn);

    std::string insertQuery =
        "INSERT INTO metadata.transfer_metrics ("
        "schema_name, table_name, db_engine,"
        "records_transferred, bytes_transferred, memory_used_mb, "
        "io_operations_per_second,"
        "avg_latency_ms, min_latency_ms, max_latency_ms, "
        "p95_latency_ms, p99_latency_ms, latency_samples,"
        "transfer_type, status, error_message,"
        "started_at, completed_at"
        ") VALUES ("
        "$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, "
        "$16, $17, $18"
        ") ON CONFLICT (schema_name, table_name, db_engine, "
        "created_date) DO UPDATE SET "
        "records_transferred = EXCLUDED.records_transferred,"
        "bytes_transferred = EXCLUDED.bytes_transferred,"
        "memory_used_mb = EXCLUDED.memory_used_mb,"
        "io_operations_per_second = EXCLUDED.io_operations_per_second,"
        "avg_latency_ms = EXCLUDED.avg_latency_ms,"
        "min_latency_ms = EXCLUDED.min_latency_ms,"
        "max_latency_ms = EXCLUDED.max_latency_ms,"
        "p95_latency_ms = EXCLUDED.p95_latency_ms,"
        "p99_latency_ms = EXCLUDED.p99_latency_ms,"
        "latency_samples = EXCLUDED.latency_samples,"
        "transfer_type = EXCLUDED.transfer_type,"
        "status = EXCLUDED.status,"
        "error_message = EXCLUDED.error_message,"
        "started_at = EXCLUDED.started_at,"
        "completed_at = EXCLUDED.completed_at;";

    for (const auto &metric : metrics) {
      txn.exec_params(
          insertQuery, metric.schema_name, metric.table_name, metric.db_engine,
          metric.records_transferred, metric.bytes_transferred,
          metric.memory_used_mb, metric.io_operations_per_second,
          metric.avg_latency_ms, metric.min_latency_ms, metric.max_latency_ms,
          metric.p95_latency_ms, metric.p99_latency_ms, metric.latency_samples,
          metric.transfer_type, metric.status,
          metric.error_message.empty() ? nullptr : metric.error_message.c_str(),
          metric.started_at.empty() ? nullptr : metric.started_at.c_str(),
          metric.completed_at.empty() ? nullptr : metric.completed_at.c_str());
    }

    txn.commit();
    Logger::info(LogCategory::METRICS, "Saved " + std::to_string(metrics.size()) +
                 " metrics to database");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::METRICS, "Error saving metrics to database: " + std::string(e.what()));
  }
}

void MetricsCollector::generateMetricsReport() {
  try {
    pqxx::connection conn(DatabaseConfig::getPostgresConnectionString());
    pqxx::work txn(conn);

    std::string reportQuery =
        "SELECT "
        "COUNT(*) as total_tables,"
        "COUNT(*) FILTER (WHERE status = 'SUCCESS') as successful_transfers,"
        "COUNT(*) FILTER (WHERE status = 'FAILED') as failed_transfers,"
        "COUNT(*) FILTER (WHERE status = 'PENDING') as pending_transfers,"
        "SUM(records_transferred) as total_records_transferred,"
        "SUM(bytes_transferred) as total_bytes_transferred,"
        "AVG(memory_used_mb) as avg_memory_used_mb,"
        "SUM(io_operations_per_second) as total_io_operations,"
        "AVG(avg_latency_ms) as avg_latency_ms,"
        "MIN(min_latency_ms) as min_latency_ms,"
        "MAX(max_latency_ms) as max_latency_ms,"
        "AVG(p95_latency_ms) as avg_p95_latency_ms,"
        "AVG(p99_latency_ms) as avg_p99_latency_ms "
        "FROM metadata.transfer_metrics "
        "WHERE created_at >= CURRENT_DATE;";

    auto result = txn.exec(reportQuery);
    txn.commit();

    if (!result.empty()) {
      auto row = result[0];
      int totalTables = row[0].as<int>();
      int successfulTransfers = row[1].as<int>();
      int failedTransfers = row[2].as<int>();
      int pendingTransfers = row[3].as<int>();
      long long totalRecords = row[4].is_null() ? 0 : row[4].as<long long>();
      long long totalBytes = row[5].is_null() ? 0 : row[5].as<long long>();
      double avgMemoryUsed = row[6].is_null() ? 0.0 : row[6].as<double>();
      long long totalIOOperations =
          row[7].is_null() ? 0 : row[7].as<long long>();
      double avgLatency = row[8].is_null() ? 0.0 : row[8].as<double>();
      double minLatency = row[9].is_null() ? 0.0 : row[9].as<double>();
      double maxLatency = row[10].is_null() ? 0.0 : row[10].as<double>();
      double avgP95Latency = row[11].is_null() ? 0.0 : row[11].as<double>();
      double avgP99Latency = row[12].is_null() ? 0.0 : row[12].as<double>();
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::METRICS, "Error generating metrics report: " + std::string(e.what()));
  }
}

std::string MetricsCollector::escapeSQL(const std::string &value) {
  std::string escaped = value;
  size_t pos = 0;
  while ((pos = escaped.find("'", pos)) != std::string::npos) {
    escaped.replace(pos, 1, "''");
    pos += 2;
  }
  return escaped;
}

std::string MetricsCollector::getCurrentTimestamp() {
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

double MetricsCollector::calculateTransferRate(long long records,
                                               int duration_ms) {
  if (duration_ms <= 0)
    return 0.0;
  return static_cast<double>(records) / (duration_ms / 1000.0);
}

long long
MetricsCollector::calculateBytesTransferred(const std::string &schema_name,
                                            const std::string &table_name) {
  try {
    pqxx::connection conn(DatabaseConfig::getPostgresConnectionString());
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
    Logger::error(LogCategory::METRICS, "Error calculating bytes transferred: " +
                  std::string(e.what()));
  }

  return 0;
}

double MetricsCollector::calculatePercentile(const std::vector<double> &values,
                                             double percentile) {
  if (values.empty())
    return 0.0;

  size_t index =
      static_cast<size_t>((percentile / 100.0) * (values.size() - 1));
  return values[index];
}

void MetricsCollector::measureQueryLatency(const std::string &query,
                                           double &latency_ms) {
  try {
    pqxx::connection conn(DatabaseConfig::getPostgresConnectionString());
    pqxx::work txn(conn);

    auto start = std::chrono::high_resolution_clock::now();
    txn.exec(query);
    auto end = std::chrono::high_resolution_clock::now();

    auto duration =
        std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    latency_ms = duration.count() / 1000.0;

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::METRICS, "Error measuring query latency: " + std::string(e.what()));
    latency_ms = 0.0;
  }
}
