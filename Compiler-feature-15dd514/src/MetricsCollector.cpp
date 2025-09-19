#include "MetricsCollector.h"
#include <algorithm>
#include <chrono>
#include <iomanip>
#include <numeric>
#include <sstream>

void MetricsCollector::collectAllMetrics() {
  // Logger::info("MetricsCollector", "Starting comprehensive metrics
  // collection");

  try {
    createMetricsTable();
    collectTransferMetrics();
    collectPerformanceMetrics();
    collectMetadataMetrics();
    collectTimestampMetrics();
    collectLatencyMetrics();
    saveMetricsToDatabase();
    generateMetricsReport();

    Logger::info("MetricsCollector",
                 "Metrics collection completed successfully");
  } catch (const std::exception &e) {
    Logger::error("MetricsCollector",
                  "Error in metrics collection: " + std::string(e.what()));
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
        "transfer_duration_ms INTEGER,"
        "transfer_rate_per_second DECIMAL(20,2),"
        "chunk_size INTEGER,"
        "memory_used_mb DECIMAL(15,2),"
        "cpu_usage_percent DECIMAL(5,2),"
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

    Logger::info("MetricsCollector",
                 "Transfer metrics table created successfully");
  } catch (const std::exception &e) {
    Logger::error("MetricsCollector",
                  "Error creating metrics table: " + std::string(e.what()));
  }
}

void MetricsCollector::collectTransferMetrics() {
  try {
    pqxx::connection conn(DatabaseConfig::getPostgresConnectionString());
    pqxx::work txn(conn);

    // Query para obtener métricas reales de transferencia - solo tablas con datos
    std::string transferQuery =
        "SELECT "
        "c.schema_name,"
        "c.table_name,"
        "c.db_engine,"
        "c.status,"
        "c.last_offset,"
        "c.last_sync_time,"
        "COALESCE(pg.n_live_tup, 0) as current_records,"
        "COALESCE(pg_total_relation_size(pg_class.oid), 0) as table_size_bytes,"
        "COALESCE(pg.n_tup_ins, 0) as total_inserts,"
        "COALESCE(pg.n_tup_upd, 0) as total_updates,"
        "COALESCE(pg.n_tup_del, 0) as total_deletes "
        "FROM metadata.catalog c "
        "INNER JOIN pg_stat_user_tables pg ON c.schema_name = pg.schemaname AND c.table_name = pg.relname "
        "INNER JOIN pg_class ON pg.relname = pg_class.relname AND pg.schemaname = pg_class.relnamespace::regnamespace::text "
        "WHERE c.db_engine IS NOT NULL AND c.active = true "
        "AND pg.n_live_tup > 0 "
        "AND c.status IN ('PERFECT_MATCH', 'LISTENING_CHANGES', 'full_load', 'incremental', 'sync');";

    auto result = txn.exec(transferQuery);
    txn.commit();

    metrics.clear();
    for (const auto &row : result) {
      TransferMetrics metric;
      metric.schema_name = row[0].as<std::string>();
      metric.table_name = row[1].as<std::string>();
      metric.db_engine = row[2].as<std::string>();
      std::string status = row[3].as<std::string>();
      long long lastOffset = row[4].is_null() ? 0 : row[4].as<long long>();
      long long currentRecords = row[6].is_null() ? 0 : row[6].as<long long>();
      long long tableSizeBytes = row[7].is_null() ? 0 : row[7].as<long long>();
      long long totalInserts = row[8].is_null() ? 0 : row[8].as<long long>();
      long long totalUpdates = row[9].is_null() ? 0 : row[9].as<long long>();
      long long totalDeletes = row[10].is_null() ? 0 : row[10].as<long long>();

      // Skip tables with no data or invalid data
      if (currentRecords <= 0 && tableSizeBytes <= 0) {
        continue;
      }

      // Calcular registros transferidos basado en el estado
      if (status == "PERFECT_MATCH" || status == "LISTENING_CHANGES") {
        metric.records_transferred = std::max(0LL, currentRecords);
      } else if (status == "NO_DATA") {
        metric.records_transferred = 0;
      } else {
        // Usar el mayor entre last_offset y current_records, pero nunca negativo
        metric.records_transferred = std::max(0LL, std::max(lastOffset, currentRecords));
      }

      // Calcular duración de transferencia
      if (!row[5].is_null()) {
        std::string lastSyncTime = row[5].as<std::string>();
        // Parse timestamp y calcular duración
        metric.transfer_duration_ms = std::max(0, calculateTransferDuration(lastSyncTime));
      } else {
        metric.transfer_duration_ms = 0;
      }

      // Calcular tasa de transferencia
      if (metric.transfer_duration_ms > 0) {
        metric.transfer_rate_per_second = calculateTransferRate(
            metric.records_transferred, metric.transfer_duration_ms);
      } else {
        metric.transfer_rate_per_second = 0.0;
      }

      // Usar tamaño real de la tabla
      metric.bytes_transferred = tableSizeBytes;

      // Calcular métricas de rendimiento basadas en estadísticas reales
      long long totalOperations = totalInserts + totalUpdates + totalDeletes;
      metric.io_operations_per_second = static_cast<int>(
          totalOperations / std::max(1.0, metric.transfer_duration_ms / 1000.0));

      // Estimar uso de memoria basado en tamaño de tabla
      metric.memory_used_mb = tableSizeBytes / (1024.0 * 1024.0);

      // Estimar CPU basado en operaciones
      metric.cpu_usage_percent = std::min(100.0, (totalOperations / 1000.0) * 5.0);

      // Configurar chunk size basado en el motor de BD
      if (metric.db_engine == "MariaDB") {
        metric.chunk_size = 1000;
      } else if (metric.db_engine == "MSSQL") {
        metric.chunk_size = 500;
      } else if (metric.db_engine == "MongoDB") {
        metric.chunk_size = 200;
      } else {
        metric.chunk_size = 1000;
      }

      // Configurar latencia basada en el estado
      if (status == "PERFECT_MATCH") {
        metric.avg_latency_ms = 0.0; // Transferencia completada
        metric.min_latency_ms = 0.0;
        metric.max_latency_ms = 0.0;
        metric.p95_latency_ms = 0.0;
        metric.p99_latency_ms = 0.0;
        metric.latency_samples = 0;
      } else {
        // Estimar latencia basada en operaciones
        metric.avg_latency_ms = std::max(1.0, totalOperations / 1000.0);
        metric.min_latency_ms = 0.5;
        metric.max_latency_ms = metric.avg_latency_ms * 2.0;
        metric.p95_latency_ms = metric.avg_latency_ms * 1.5;
        metric.p99_latency_ms = metric.avg_latency_ms * 1.8;
        metric.latency_samples = static_cast<int>(totalOperations);
      }

      // Configurar timestamps
      if (!row[5].is_null()) {
        metric.completed_at = row[5].as<std::string>();
        // Estimar started_at basado en duración
        if (metric.transfer_duration_ms > 0) {
          metric.started_at = calculateStartTime(metric.completed_at, metric.transfer_duration_ms);
        } else {
          metric.started_at = metric.completed_at;
        }
      } else {
        metric.started_at = getCurrentTimestamp();
        metric.completed_at = getCurrentTimestamp();
      }

      // Configurar tipo de transferencia y estado
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

      metrics.push_back(metric);
    }

    Logger::info("MetricsCollector", "Collected real transfer metrics for " +
                                         std::to_string(metrics.size()) +
                                         " tables");
  } catch (const std::exception &e) {
    Logger::error("MetricsCollector", "Error collecting transfer metrics: " +
                                          std::string(e.what()));
  }
}

void MetricsCollector::collectPerformanceMetrics() {
  try {
    pqxx::connection conn(DatabaseConfig::getPostgresConnectionString());
    pqxx::work txn(conn);

    // Query para obtener métricas de rendimiento
    std::string performanceQuery = "SELECT "
                                   "schemaname,"
                                   "relname,"
                                   "n_tup_ins as inserts,"
                                   "n_tup_upd as updates,"
                                   "n_tup_del as deletes,"
                                   "n_live_tup as live_tuples,"
                                   "n_dead_tup as dead_tuples,"
                                   "last_autoanalyze,"
                                   "last_autovacuum "
                                   "FROM pg_stat_user_tables "
                                   "WHERE schemaname IN (SELECT DISTINCT "
                                   "schema_name FROM metadata.catalog);";

    auto result = txn.exec(performanceQuery);
    txn.commit();

    // Mapear métricas de rendimiento a nuestras métricas
    for (auto &metric : metrics) {
      for (const auto &row : result) {
        if (row[0].as<std::string>() == metric.schema_name &&
            row[1].as<std::string>() == metric.table_name) {

          // Calcular métricas de rendimiento basadas en estadísticas
          long long total_operations = row[2].as<long long>() +
                                       row[3].as<long long>() +
                                       row[4].as<long long>();
          metric.io_operations_per_second = static_cast<int>(
              total_operations /
              std::max(1.0, metric.transfer_duration_ms / 1000.0));

          // Estimar uso de memoria basado en tamaño de tabla
          metric.memory_used_mb = metric.bytes_transferred / (1024.0 * 1024.0);

          // Estimar CPU basado en operaciones
          metric.cpu_usage_percent =
              std::min(100.0, (total_operations / 1000.0) * 10.0);

          break;
        }
      }
    }

    Logger::info("MetricsCollector", "Collected performance metrics");
  } catch (const std::exception &e) {
    Logger::error("MetricsCollector", "Error collecting performance metrics: " +
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

    Logger::info("MetricsCollector", "Collected metadata metrics");
  } catch (const std::exception &e) {
    Logger::error("MetricsCollector", "Error collecting metadata metrics: " +
                                          std::string(e.what()));
  }
}

void MetricsCollector::collectTimestampMetrics() {
  try {
    pqxx::connection conn(DatabaseConfig::getPostgresConnectionString());
    pqxx::work txn(conn);

    // Query para obtener timestamps
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

    // Mapear timestamps a nuestras métricas
    for (auto &metric : metrics) {
      for (const auto &row : result) {
        if (row[0].as<std::string>() == metric.schema_name &&
            row[1].as<std::string>() == metric.table_name &&
            row[2].as<std::string>() == metric.db_engine) {

          metric.completed_at = row[3].as<std::string>();

          // Estimar started_at basado en duración
          if (metric.transfer_duration_ms > 0) {
            auto completed = std::chrono::system_clock::from_time_t(
                std::chrono::duration_cast<std::chrono::seconds>(
                    std::chrono::milliseconds(metric.transfer_duration_ms))
                    .count());
            auto started = completed - std::chrono::milliseconds(
                                           metric.transfer_duration_ms);
            auto time_t = std::chrono::system_clock::to_time_t(started);
            std::stringstream ss;
            ss << std::put_time(std::gmtime(&time_t), "%Y-%m-%d %H:%M:%S");
            metric.started_at = ss.str();
          }

          break;
        }
      }
    }

    Logger::info("MetricsCollector", "Collected timestamp metrics");
  } catch (const std::exception &e) {
    Logger::error("MetricsCollector", "Error collecting timestamp metrics: " +
                                          std::string(e.what()));
  }
}

void MetricsCollector::collectLatencyMetrics() {
  try {
    pqxx::connection conn(DatabaseConfig::getPostgresConnectionString());
    pqxx::work txn(conn);

    for (auto &metric : metrics) {
      std::vector<double> latencySamples;

      std::string latencyQuery = "SELECT "
                                 "EXTRACT(EPOCH FROM (completed_at - "
                                 "started_at)) * 1000 as latency_ms "
                                 "FROM metadata.transfer_metrics "
                                 "WHERE schema_name = '" +
                                 escapeSQL(metric.schema_name) +
                                 "' "
                                 "AND table_name = '" +
                                 escapeSQL(metric.table_name) +
                                 "' "
                                 "AND db_engine = '" +
                                 escapeSQL(metric.db_engine) +
                                 "' "
                                 "AND completed_at IS NOT NULL "
                                 "AND started_at IS NOT NULL "
                                 "ORDER BY created_at DESC LIMIT 100;";

      auto result = txn.exec(latencyQuery);

      for (const auto &row : result) {
        if (!row[0].is_null()) {
          double latency = row[0].as<double>();
          latencySamples.push_back(latency);
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
    Logger::info("MetricsCollector", "Collected latency metrics");
  } catch (const std::exception &e) {
    Logger::error("MetricsCollector",
                  "Error collecting latency metrics: " + std::string(e.what()));
  }
}

void MetricsCollector::saveMetricsToDatabase() {
  try {
    pqxx::connection conn(DatabaseConfig::getPostgresConnectionString());
    pqxx::work txn(conn);

    for (const auto &metric : metrics) {
      std::string insertQuery =
          "INSERT INTO metadata.transfer_metrics ("
          "schema_name, table_name, db_engine,"
          "records_transferred, bytes_transferred, transfer_duration_ms, "
          "transfer_rate_per_second,"
          "chunk_size, memory_used_mb, cpu_usage_percent, "
          "io_operations_per_second,"
          "avg_latency_ms, min_latency_ms, max_latency_ms, "
          "p95_latency_ms, p99_latency_ms, latency_samples,"
          "transfer_type, status, error_message,"
          "started_at, completed_at"
          ") VALUES ("
          "'" +
          escapeSQL(metric.schema_name) + "', '" +
          escapeSQL(metric.table_name) + "', '" + escapeSQL(metric.db_engine) +
          "'," + std::to_string(metric.records_transferred) + ", " +
          std::to_string(metric.bytes_transferred) + ", " +
          std::to_string(metric.transfer_duration_ms) + ", " +
          std::to_string(metric.transfer_rate_per_second) + "," +
          std::to_string(metric.chunk_size) + ", " +
          std::to_string(metric.memory_used_mb) + ", " +
          std::to_string(metric.cpu_usage_percent) + ", " +
          std::to_string(metric.io_operations_per_second) + "," +
          std::to_string(metric.avg_latency_ms) + ", " +
          std::to_string(metric.min_latency_ms) + ", " +
          std::to_string(metric.max_latency_ms) + ", " +
          std::to_string(metric.p95_latency_ms) + ", " +
          std::to_string(metric.p99_latency_ms) + ", " +
          std::to_string(metric.latency_samples) +
          ","
          "'" +
          escapeSQL(metric.transfer_type) + "', '" + escapeSQL(metric.status) +
          "', " +
          (metric.error_message.empty()
               ? "NULL"
               : "'" + escapeSQL(metric.error_message) + "'") +
          "," +
          (metric.started_at.empty()
               ? "NULL"
               : "'" + escapeSQL(metric.started_at) + "'") +
          ", " +
          (metric.completed_at.empty()
               ? "NULL"
               : "'" + escapeSQL(metric.completed_at) + "'") +
          ") ON CONFLICT (schema_name, table_name, db_engine, "
          "created_date) DO UPDATE SET "
          "records_transferred = EXCLUDED.records_transferred,"
          "bytes_transferred = EXCLUDED.bytes_transferred,"
          "transfer_duration_ms = EXCLUDED.transfer_duration_ms,"
          "transfer_rate_per_second = EXCLUDED.transfer_rate_per_second,"
          "chunk_size = EXCLUDED.chunk_size,"
          "memory_used_mb = EXCLUDED.memory_used_mb,"
          "cpu_usage_percent = EXCLUDED.cpu_usage_percent,"
          "io_operations_per_second = EXCLUDED.io_operations_per_second,"
          "transfer_type = EXCLUDED.transfer_type,"
          "status = EXCLUDED.status,"
          "error_message = EXCLUDED.error_message,"
          "started_at = EXCLUDED.started_at,"
          "completed_at = EXCLUDED.completed_at;";

      txn.exec(insertQuery);
    }

    txn.commit();
    Logger::info("MetricsCollector", "Saved " + std::to_string(metrics.size()) +
                                         " metrics to database");
  } catch (const std::exception &e) {
    Logger::error("MetricsCollector",
                  "Error saving metrics to database: " + std::string(e.what()));
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
        "AVG(transfer_rate_per_second) as avg_transfer_rate,"
        "SUM(records_transferred) as total_records_transferred,"
        "SUM(bytes_transferred) as total_bytes_transferred,"
        "AVG(transfer_duration_ms) as avg_transfer_duration_ms,"
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
      double avgTransferRate = row[4].is_null() ? 0.0 : row[4].as<double>();
      long long totalRecords = row[5].is_null() ? 0 : row[5].as<long long>();
      long long totalBytes = row[6].is_null() ? 0 : row[6].as<long long>();
      double avgDuration = row[7].is_null() ? 0.0 : row[7].as<double>();
      double avgLatency = row[8].is_null() ? 0.0 : row[8].as<double>();
      double minLatency = row[9].is_null() ? 0.0 : row[9].as<double>();
      double maxLatency = row[10].is_null() ? 0.0 : row[10].as<double>();
      double avgP95Latency = row[11].is_null() ? 0.0 : row[11].as<double>();
      double avgP99Latency = row[12].is_null() ? 0.0 : row[12].as<double>();

      Logger::info("MetricsCollector", "=== TRANSFER METRICS REPORT ===");
      Logger::info("MetricsCollector",
                   "Total Tables: " + std::to_string(totalTables));
      Logger::info("MetricsCollector", "Successful Transfers: " +
                                           std::to_string(successfulTransfers));
      Logger::info("MetricsCollector",
                   "Failed Transfers: " + std::to_string(failedTransfers));
      Logger::info("MetricsCollector",
                   "Pending Transfers: " + std::to_string(pendingTransfers));
      Logger::info("MetricsCollector",
                   "Average Transfer Rate: " + std::to_string(avgTransferRate) +
                       " records/sec");
      Logger::info("MetricsCollector", "Total Records Transferred: " +
                                           std::to_string(totalRecords));
      Logger::info("MetricsCollector",
                   "Total Bytes Transferred: " + std::to_string(totalBytes) +
                       " bytes");
      Logger::info("MetricsCollector", "Average Transfer Duration: " +
                                           std::to_string(avgDuration) + " ms");
      Logger::info("MetricsCollector", "=== LATENCY METRICS ===");
      Logger::info("MetricsCollector",
                   "Average Latency: " + std::to_string(avgLatency) + " ms");
      Logger::info("MetricsCollector",
                   "Min Latency: " + std::to_string(minLatency) + " ms");
      Logger::info("MetricsCollector",
                   "Max Latency: " + std::to_string(maxLatency) + " ms");
      Logger::info("MetricsCollector",
                   "P95 Latency: " + std::to_string(avgP95Latency) + " ms");
      Logger::info("MetricsCollector",
                   "P99 Latency: " + std::to_string(avgP99Latency) + " ms");
      Logger::info("MetricsCollector", "===============================");
    }
  } catch (const std::exception &e) {
    Logger::error("MetricsCollector",
                  "Error generating metrics report: " + std::string(e.what()));
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
    Logger::error("MetricsCollector", "Error calculating bytes transferred: " +
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
    Logger::error("MetricsCollector",
                  "Error measuring query latency: " + std::string(e.what()));
    latency_ms = 0.0;
  }
}

int MetricsCollector::calculateTransferDuration(const std::string &lastSyncTime) {
  try {
    // Parse timestamp string to time_t
    std::tm tm = {};
    std::istringstream ss(lastSyncTime);
    ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
    
    if (ss.fail()) {
      return 0;
    }
    
    auto syncTime = std::chrono::system_clock::from_time_t(std::mktime(&tm));
    auto now = std::chrono::system_clock::now();
    
    // Solo calcular duración si lastSyncTime es en el pasado
    if (syncTime <= now) {
      auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(now - syncTime);
      return static_cast<int>(duration.count());
    } else {
      // Si lastSyncTime es en el futuro, usar 0
      return 0;
    }
  } catch (const std::exception &e) {
    Logger::error("MetricsCollector", "Error calculating transfer duration: " + std::string(e.what()));
    return 0;
  }
}

std::string MetricsCollector::calculateStartTime(const std::string &completedAt, int durationMs) {
  try {
    // Parse completed timestamp
    std::tm tm = {};
    std::istringstream ss(completedAt);
    ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
    
    if (ss.fail()) {
      return completedAt;
    }
    
    auto completed = std::chrono::system_clock::from_time_t(std::mktime(&tm));
    auto started = completed - std::chrono::milliseconds(durationMs);
    auto time_t = std::chrono::system_clock::to_time_t(started);
    
    std::stringstream result;
    result << std::put_time(std::gmtime(&time_t), "%Y-%m-%d %H:%M:%S");
    return result.str();
  } catch (const std::exception &e) {
    Logger::error("MetricsCollector", "Error calculating start time: " + std::string(e.what()));
    return completedAt;
  }
}
