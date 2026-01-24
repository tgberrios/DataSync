#include "monitoring/APMManager.h"
#include "core/database_config.h"
#include <algorithm>
#include <cmath>
#include <pqxx/pqxx>
#include <sys/statvfs.h>
#include <iomanip>
#include <sstream>
#include <ctime>

APMManager::APMManager(const std::string& connectionString) : connectionString_(connectionString) {
  // Crear tablas si no existen
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    txn.exec(
        "CREATE TABLE IF NOT EXISTS metadata.apm_metrics ("
        "id SERIAL PRIMARY KEY,"
        "operation_name VARCHAR(255) NOT NULL,"
        "service_name VARCHAR(100) NOT NULL,"
        "request_count BIGINT DEFAULT 0,"
        "error_count BIGINT DEFAULT 0,"
        "latency_p50 DECIMAL(10,2),"
        "latency_p95 DECIMAL(10,2),"
        "latency_p99 DECIMAL(10,2),"
        "throughput DECIMAL(10,2),"
        "error_rate DECIMAL(5,2),"
        "time_window VARCHAR(20) NOT NULL,"
        "timestamp TIMESTAMP DEFAULT NOW()"
        ")");

    txn.exec(
        "CREATE TABLE IF NOT EXISTS metadata.apm_baselines ("
        "id SERIAL PRIMARY KEY,"
        "operation_name VARCHAR(255) NOT NULL,"
        "service_name VARCHAR(100) NOT NULL,"
        "latency_p50 DECIMAL(10,2),"
        "latency_p95 DECIMAL(10,2),"
        "latency_p99 DECIMAL(10,2),"
        "throughput DECIMAL(10,2),"
        "error_rate DECIMAL(5,2),"
        "sample_count INTEGER,"
        "calculated_at TIMESTAMP DEFAULT NOW(),"
        "UNIQUE(operation_name, service_name)"
        ")");

    txn.exec(
        "CREATE TABLE IF NOT EXISTS metadata.apm_health_checks ("
        "id SERIAL PRIMARY KEY,"
        "check_name VARCHAR(255) NOT NULL,"
        "component VARCHAR(100) NOT NULL,"
        "status VARCHAR(20) NOT NULL,"
        "message TEXT,"
        "metadata JSONB DEFAULT '{}'::jsonb,"
        "timestamp TIMESTAMP DEFAULT NOW()"
        ")");

    txn.commit();
  } catch (const std::exception& e) {
    Logger::error(LogCategory::MONITORING, "APMManager",
                  "Error creating tables: " + std::string(e.what()));
  }
}

void APMManager::recordRequest(const std::string& operationName, const std::string& serviceName,
                                int64_t latencyMs, bool /* isError */) {
  std::string key = operationName + "::" + serviceName;
  latencySamples_[key].push_back(latencyMs);

  // Agregar cada 100 requests o cada minuto
  if (latencySamples_[key].size() >= 100) {
    aggregateMetrics();
  }
}

void APMManager::aggregateMetrics() {
  for (auto& [key, latencies] : latencySamples_) {
    if (latencies.empty()) continue;

    size_t pos = key.find("::");
    std::string operationName = key.substr(0, pos);
    std::string serviceName = key.substr(pos + 2);

    std::sort(latencies.begin(), latencies.end());

    APMMetric metric;
    metric.operationName = operationName;
    metric.serviceName = serviceName;
    metric.requestCount = latencies.size();
    metric.errorCount = 0; // TODO: track errors separately
    metric.latencyP50 = latencies[latencies.size() * 0.5];
    metric.latencyP95 = latencies[latencies.size() * 0.95];
    metric.latencyP99 = latencies[latencies.size() * 0.99];
    metric.throughput = latencies.size() / 60.0; // requests per second (assuming 1min window)
    metric.errorRate = 0.0;
    metric.timestamp = std::chrono::system_clock::now();
    metric.timeWindow = "1min";

    saveMetricToDatabase(metric);
    latencies.clear();
  }
}

bool APMManager::saveMetricToDatabase(const APMMetric& metric) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    txn.exec_params(
        "INSERT INTO metadata.apm_metrics "
        "(operation_name, service_name, request_count, error_count, latency_p50, latency_p95, "
        "latency_p99, throughput, error_rate, time_window) "
        "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
        metric.operationName, metric.serviceName, metric.requestCount, metric.errorCount,
        metric.latencyP50, metric.latencyP95, metric.latencyP99, metric.throughput,
        metric.errorRate, metric.timeWindow);

    txn.commit();
    return true;
  } catch (const std::exception& e) {
    Logger::error(LogCategory::MONITORING, "APMManager",
                  "Error saving metric: " + std::string(e.what()));
    return false;
  }
}

std::vector<APMManager::APMMetric> APMManager::getMetrics(const std::string& operationName,
                                                            const std::string& timeWindow) {
  std::vector<APMMetric> metrics;

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query =
        "SELECT * FROM metadata.apm_metrics WHERE time_window = $1";
    if (!operationName.empty()) {
      query += " AND operation_name = $2 ORDER BY timestamp DESC LIMIT 100";
    } else {
      query += " ORDER BY timestamp DESC LIMIT 100";
    }

    pqxx::result result;
    if (!operationName.empty()) {
      result = txn.exec_params(query, timeWindow, operationName);
    } else {
      result = txn.exec_params(query, timeWindow);
    }

    for (const auto& row : result) {
      APMMetric metric;
      metric.operationName = row["operation_name"].as<std::string>();
      metric.serviceName = row["service_name"].as<std::string>();
      metric.requestCount = row["request_count"].as<int64_t>();
      metric.errorCount = row["error_count"].as<int64_t>();
      metric.latencyP50 = row["latency_p50"].as<double>();
      metric.latencyP95 = row["latency_p95"].as<double>();
      metric.latencyP99 = row["latency_p99"].as<double>();
      metric.throughput = row["throughput"].as<double>();
      metric.errorRate = row["error_rate"].as<double>();
      metric.timeWindow = row["time_window"].as<std::string>();
      if (!row["timestamp"].is_null()) {
        auto timestampStr = row["timestamp"].as<std::string>();
        std::tm tm = {};
        std::istringstream ss(timestampStr);
        ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
        metric.timestamp = std::chrono::system_clock::from_time_t(std::mktime(&tm));
      }

      metrics.push_back(metric);
    }
  } catch (const std::exception& e) {
    Logger::error(LogCategory::MONITORING, "APMManager",
                  "Error loading metrics: " + std::string(e.what()));
  }

  return metrics;
}

std::unique_ptr<APMManager::Baseline> APMManager::calculateBaseline(
    const std::string& operationName, const std::string& serviceName, int days) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    auto result = txn.exec_params(
        "SELECT AVG(latency_p50) as avg_p50, AVG(latency_p95) as avg_p95, "
        "AVG(latency_p99) as avg_p99, AVG(throughput) as avg_throughput, "
        "AVG(error_rate) as avg_error_rate, COUNT(*) as sample_count "
        "FROM metadata.apm_metrics "
        "WHERE operation_name = $1 AND service_name = $2 "
        "AND timestamp >= NOW() - INTERVAL '" + std::to_string(days) + " days'",
        operationName, serviceName);

    if (result.empty() || result[0]["sample_count"].as<int>() == 0) {
      return nullptr;
    }

    auto row = result[0];
    auto baseline = std::make_unique<Baseline>();
    baseline->operationName = operationName;
    baseline->serviceName = serviceName;
    baseline->latencyP50 = row["avg_p50"].as<double>();
    baseline->latencyP95 = row["avg_p95"].as<double>();
    baseline->latencyP99 = row["avg_p99"].as<double>();
    baseline->throughput = row["avg_throughput"].as<double>();
    baseline->errorRate = row["avg_error_rate"].as<double>();
    baseline->sampleCount = row["sample_count"].as<int>();
    baseline->calculatedAt = std::chrono::system_clock::now();

    saveBaselineToDatabase(*baseline);
    return baseline;
  } catch (const std::exception& e) {
    Logger::error(LogCategory::MONITORING, "APMManager",
                  "Error calculating baseline: " + std::string(e.what()));
    return nullptr;
  }
}

bool APMManager::saveBaselineToDatabase(const Baseline& baseline) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    txn.exec_params(
        "INSERT INTO metadata.apm_baselines "
        "(operation_name, service_name, latency_p50, latency_p95, latency_p99, throughput, "
        "error_rate, sample_count) "
        "VALUES ($1, $2, $3, $4, $5, $6, $7, $8) "
        "ON CONFLICT (operation_name, service_name) DO UPDATE SET "
        "latency_p50 = EXCLUDED.latency_p50, latency_p95 = EXCLUDED.latency_p95, "
        "latency_p99 = EXCLUDED.latency_p99, throughput = EXCLUDED.throughput, "
        "error_rate = EXCLUDED.error_rate, sample_count = EXCLUDED.sample_count, "
        "calculated_at = NOW()",
        baseline.operationName, baseline.serviceName, baseline.latencyP50, baseline.latencyP95,
        baseline.latencyP99, baseline.throughput, baseline.errorRate, baseline.sampleCount);

    txn.commit();
    return true;
  } catch (const std::exception& e) {
    Logger::error(LogCategory::MONITORING, "APMManager",
                  "Error saving baseline: " + std::string(e.what()));
    return false;
  }
}

std::unique_ptr<APMManager::Baseline> APMManager::getBaseline(const std::string& operationName,
                                                               const std::string& serviceName) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    auto result = txn.exec_params(
        "SELECT * FROM metadata.apm_baselines WHERE operation_name = $1 AND service_name = $2",
        operationName, serviceName);

    if (result.empty()) {
      return nullptr;
    }

    auto row = result[0];
    auto baseline = std::make_unique<Baseline>();
    baseline->operationName = row["operation_name"].as<std::string>();
    baseline->serviceName = row["service_name"].as<std::string>();
    baseline->latencyP50 = row["latency_p50"].as<double>();
    baseline->latencyP95 = row["latency_p95"].as<double>();
    baseline->latencyP99 = row["latency_p99"].as<double>();
    baseline->throughput = row["throughput"].as<double>();
    baseline->errorRate = row["error_rate"].as<double>();
    baseline->sampleCount = row["sample_count"].as<int>();
    baseline->calculatedAt = std::chrono::system_clock::from_time_t(
        std::chrono::seconds(row["calculated_at"].as<int64_t>()).count());

    return baseline;
  } catch (const std::exception& e) {
    Logger::error(LogCategory::MONITORING, "APMManager",
                  "Error loading baseline: " + std::string(e.what()));
    return nullptr;
  }
}

std::vector<APMManager::Baseline> APMManager::listBaselines() {
  std::vector<Baseline> baselines;

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    auto result = txn.exec("SELECT * FROM metadata.apm_baselines ORDER BY calculated_at DESC");

    for (const auto& row : result) {
      Baseline baseline;
      baseline.operationName = row["operation_name"].as<std::string>();
      baseline.serviceName = row["service_name"].as<std::string>();
      baseline.latencyP50 = row["latency_p50"].as<double>();
      baseline.latencyP95 = row["latency_p95"].as<double>();
      baseline.latencyP99 = row["latency_p99"].as<double>();
      baseline.throughput = row["throughput"].as<double>();
      baseline.errorRate = row["error_rate"].as<double>();
      baseline.sampleCount = row["sample_count"].as<int>();
      if (!row["calculated_at"].is_null()) {
        auto calculatedAtStr = row["calculated_at"].as<std::string>();
        std::tm tm = {};
        std::istringstream ss(calculatedAtStr);
        ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
        baseline.calculatedAt = std::chrono::system_clock::from_time_t(std::mktime(&tm));
      }

      baselines.push_back(baseline);
    }
  } catch (const std::exception& e) {
    Logger::error(LogCategory::MONITORING, "APMManager",
                  "Error listing baselines: " + std::string(e.what()));
  }

  return baselines;
}

APMManager::HealthCheck APMManager::performHealthCheck(const std::string& checkName,
                                                        const std::string& component) {
  HealthCheck healthCheck;
  healthCheck.checkName = checkName;
  healthCheck.component = component;
  healthCheck.timestamp = std::chrono::system_clock::now();

  if (component == "database") {
    healthCheck = checkDatabaseHealth();
  } else if (component == "disk_space") {
    healthCheck = checkDiskSpaceHealth();
  } else if (component == "external_service") {
    healthCheck = checkExternalServiceHealth(checkName);
  } else {
    healthCheck.status = "unknown";
    healthCheck.message = "Unknown component type";
  }

  healthCheck.checkName = checkName;
  saveHealthCheckToDatabase(healthCheck);
  return healthCheck;
}

APMManager::HealthCheck APMManager::checkDatabaseHealth() {
  HealthCheck healthCheck;
  healthCheck.component = "database";
  healthCheck.timestamp = std::chrono::system_clock::now();

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);
    txn.exec("SELECT 1");
    healthCheck.status = "healthy";
    healthCheck.message = "Database connection successful";
  } catch (const std::exception& e) {
    healthCheck.status = "unhealthy";
    healthCheck.message = "Database connection failed: " + std::string(e.what());
  }

  return healthCheck;
}

APMManager::HealthCheck APMManager::checkDiskSpaceHealth() {
  HealthCheck healthCheck;
  healthCheck.component = "disk_space";
  healthCheck.timestamp = std::chrono::system_clock::now();

  struct statvfs stat;
  if (statvfs("/", &stat) == 0) {
    unsigned long total = stat.f_blocks * stat.f_frsize;
    unsigned long available = stat.f_bavail * stat.f_frsize;
    double percentUsed = (1.0 - (double)available / total) * 100.0;

    healthCheck.metadata = json{{"total_bytes", total},
                                {"available_bytes", available},
                                {"percent_used", percentUsed}};

    if (percentUsed > 90.0) {
      healthCheck.status = "unhealthy";
      healthCheck.message = "Disk space critically low: " + std::to_string(percentUsed) + "% used";
    } else if (percentUsed > 80.0) {
      healthCheck.status = "degraded";
      healthCheck.message = "Disk space low: " + std::to_string(percentUsed) + "% used";
    } else {
      healthCheck.status = "healthy";
      healthCheck.message = "Disk space adequate: " + std::to_string(percentUsed) + "% used";
    }
  } else {
    healthCheck.status = "unknown";
    healthCheck.message = "Could not check disk space";
  }

  return healthCheck;
}

APMManager::HealthCheck APMManager::checkExternalServiceHealth(const std::string& serviceName) {
  HealthCheck healthCheck;
  healthCheck.component = "external_service";
  healthCheck.checkName = serviceName;
  healthCheck.timestamp = std::chrono::system_clock::now();
  healthCheck.status = "unknown";
  healthCheck.message = "External service health check not implemented";
  return healthCheck;
}

std::vector<APMManager::HealthCheck> APMManager::getHealthChecks() {
  std::vector<HealthCheck> healthChecks;

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    auto result = txn.exec(
        "SELECT * FROM metadata.apm_health_checks ORDER BY timestamp DESC LIMIT 100");

    for (const auto& row : result) {
      HealthCheck healthCheck;
      healthCheck.checkName = row["check_name"].as<std::string>();
      healthCheck.component = row["component"].as<std::string>();
      healthCheck.status = row["status"].as<std::string>();
      healthCheck.message = row["message"].is_null() ? "" : row["message"].as<std::string>();
      healthCheck.timestamp = std::chrono::system_clock::from_time_t(
          std::chrono::seconds(row["timestamp"].as<int64_t>()).count());

      if (!row["metadata"].is_null()) {
        healthCheck.metadata = json::parse(row["metadata"].as<std::string>());
      }

      healthChecks.push_back(healthCheck);
    }
  } catch (const std::exception& e) {
    Logger::error(LogCategory::MONITORING, "APMManager",
                  "Error loading health checks: " + std::string(e.what()));
  }

  return healthChecks;
}

bool APMManager::saveHealthCheckToDatabase(const HealthCheck& healthCheck) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    txn.exec_params(
        "INSERT INTO metadata.apm_health_checks (check_name, component, status, message, metadata) "
        "VALUES ($1, $2, $3, $4, $5)",
        healthCheck.checkName, healthCheck.component, healthCheck.status, healthCheck.message,
        healthCheck.metadata.dump());

    txn.commit();
    return true;
  } catch (const std::exception& e) {
    Logger::error(LogCategory::MONITORING, "APMManager",
                  "Error saving health check: " + std::string(e.what()));
    return false;
  }
}

bool APMManager::exceedsBaseline(const APMMetric& metric, const Baseline& baseline,
                                  double threshold) {
  return metric.latencyP95 > baseline.latencyP95 * threshold ||
         metric.errorRate > baseline.errorRate * threshold ||
         metric.throughput < baseline.throughput / threshold;
}
