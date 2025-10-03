#include "MetricsReporter.h"
#include <fstream>
#include <iomanip>
#include <sstream>

MetricsReporter::MetricsReporter(MetricsDatabaseManager &dbManager)
    : dbManager(dbManager) {}

void MetricsReporter::generateSummaryReport() const {
  try {
    TransferMetricsData data;
    dbManager.loadMetrics(data);

    Logger::getInstance().info(LogCategory::METRICS,
                               "=== METRICS SUMMARY REPORT ===");

    int totalTables = data.size();
    int successfulTransfers = data.getSuccessCount();
    int failedTransfers = data.getFailedCount();
    int pendingTransfers = data.getPendingCount();
    long long totalRecords = data.getTotalRecords();
    long long totalBytes = data.getTotalBytes();
    double avgMemory = data.getAverageMemoryUsage();
    double successRate = data.getSuccessRate();

    Logger::getInstance().info(
        LogCategory::METRICS,
        "Total Tables: " + std::to_string(totalTables) +
            " | Success: " + std::to_string(successfulTransfers) +
            " | Failed: " + std::to_string(failedTransfers) +
            " | Pending: " + std::to_string(pendingTransfers));

    Logger::getInstance().info(
        LogCategory::METRICS,
        "Success Rate: " + std::to_string(static_cast<int>(successRate)) + "%");

    Logger::getInstance().info(
        LogCategory::METRICS, "Total Records: " + std::to_string(totalRecords) +
                                  " | Total Data: " + formatBytes(totalBytes) +
                                  " | Avg Memory: " + formatMemory(avgMemory));

    Logger::getInstance().info(LogCategory::METRICS,
                               "=== END SUMMARY REPORT ===");
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::METRICS,
                                "Error generating summary report: " +
                                    std::string(e.what()));
  }
}

void MetricsReporter::generateDetailedReport() const {
  try {
    TransferMetricsData data;
    dbManager.loadMetrics(data);

    Logger::getInstance().info(LogCategory::METRICS,
                               "=== DETAILED METRICS REPORT ===");

    const auto &metrics = data.getMetrics();
    for (const auto &metric : metrics) {
      Logger::getInstance().info(
          LogCategory::METRICS,
          "Table: " + metric.schema_name + "." + metric.table_name +
              " | Engine: " + metric.db_engine + " | Status: " + metric.status +
              " | Records: " + std::to_string(metric.records_transferred) +
              " | Size: " + formatBytes(metric.bytes_transferred) +
              " | Memory: " + formatMemory(metric.memory_used_mb) +
              " | IO Ops: " + std::to_string(metric.io_operations_per_second));

      if (!metric.error_message.empty()) {
        Logger::getInstance().warning(LogCategory::METRICS,
                                      "Error: " + metric.error_message);
      }
    }

    Logger::getInstance().info(LogCategory::METRICS,
                               "=== END DETAILED REPORT ===");
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::METRICS,
                                "Error generating detailed report: " +
                                    std::string(e.what()));
  }
}

void MetricsReporter::generateStatusReport() const {
  try {
    Logger::getInstance().info(LogCategory::METRICS, "=== STATUS REPORT ===");

    auto successMetrics = dbManager.getMetricsByStatus("SUCCESS");
    auto failedMetrics = dbManager.getMetricsByStatus("FAILED");
    auto pendingMetrics = dbManager.getMetricsByStatus("PENDING");

    Logger::getInstance().info(
        LogCategory::METRICS,
        "SUCCESS (" + std::to_string(successMetrics.size()) + " tables):");
    for (const auto &metric : successMetrics) {
      Logger::getInstance().info(LogCategory::METRICS,
                                 "  " + metric.schema_name + "." +
                                     metric.table_name + " (" +
                                     metric.db_engine + ")");
    }

    if (!failedMetrics.empty()) {
      Logger::getInstance().warning(
          LogCategory::METRICS,
          "FAILED (" + std::to_string(failedMetrics.size()) + " tables):");
      for (const auto &metric : failedMetrics) {
        Logger::getInstance().warning(
            LogCategory::METRICS,
            "  " + metric.schema_name + "." + metric.table_name + " (" +
                metric.db_engine + ") - " + metric.error_message);
      }
    }

    if (!pendingMetrics.empty()) {
      Logger::getInstance().info(
          LogCategory::METRICS,
          "PENDING (" + std::to_string(pendingMetrics.size()) + " tables):");
      for (const auto &metric : pendingMetrics) {
        Logger::getInstance().info(LogCategory::METRICS,
                                   "  " + metric.schema_name + "." +
                                       metric.table_name + " (" +
                                       metric.db_engine + ")");
      }
    }

    Logger::getInstance().info(LogCategory::METRICS,
                               "=== END STATUS REPORT ===");
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::METRICS,
                                "Error generating status report: " +
                                    std::string(e.what()));
  }
}

void MetricsReporter::generateEngineReport() const {
  try {
    Logger::getInstance().info(LogCategory::METRICS, "=== ENGINE REPORT ===");

    auto mariadbMetrics = dbManager.getMetricsByEngine("mariadb");
    auto mssqlMetrics = dbManager.getMetricsByEngine("mssql");
    auto postgresMetrics = dbManager.getMetricsByEngine("postgres");

    Logger::getInstance().info(
        LogCategory::METRICS,
        "MariaDB: " + std::to_string(mariadbMetrics.size()) + " tables");
    Logger::getInstance().info(LogCategory::METRICS,
                               "MSSQL: " + std::to_string(mssqlMetrics.size()) +
                                   " tables");
    Logger::getInstance().info(
        LogCategory::METRICS,
        "PostgreSQL: " + std::to_string(postgresMetrics.size()) + " tables");

    Logger::getInstance().info(LogCategory::METRICS,
                               "=== END ENGINE REPORT ===");
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::METRICS,
                                "Error generating engine report: " +
                                    std::string(e.what()));
  }
}

void MetricsReporter::generateReportByDateRange(
    const std::string &startDate, const std::string &endDate) const {
  try {
    auto metrics = dbManager.getMetricsByDateRange(startDate, endDate);

    Logger::getInstance().info(LogCategory::METRICS, "=== REPORT FOR " +
                                                         startDate + " TO " +
                                                         endDate + " ===");
    Logger::getInstance().info(LogCategory::METRICS,
                               "Found " + std::to_string(metrics.size()) +
                                   " metrics in date range");

    for (const auto &metric : metrics) {
      Logger::getInstance().info(
          LogCategory::METRICS,
          "Table: " + metric.schema_name + "." + metric.table_name +
              " | Status: " + metric.status +
              " | Records: " + std::to_string(metric.records_transferred) +
              " | Size: " + formatBytes(metric.bytes_transferred));
    }

    Logger::getInstance().info(LogCategory::METRICS,
                               "=== END DATE RANGE REPORT ===");
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::METRICS,
                                "Error generating date range report: " +
                                    std::string(e.what()));
  }
}

void MetricsReporter::generateReportByStatus(const std::string &status) const {
  try {
    auto metrics = dbManager.getMetricsByStatus(status);

    Logger::getInstance().info(LogCategory::METRICS,
                               "=== REPORT FOR STATUS: " + status + " ===");
    Logger::getInstance().info(LogCategory::METRICS,
                               "Found " + std::to_string(metrics.size()) +
                                   " metrics with status " + status);

    for (const auto &metric : metrics) {
      Logger::getInstance().info(
          LogCategory::METRICS,
          "Table: " + metric.schema_name + "." + metric.table_name +
              " | Engine: " + metric.db_engine +
              " | Records: " + std::to_string(metric.records_transferred) +
              " | Size: " + formatBytes(metric.bytes_transferred));
    }

    Logger::getInstance().info(LogCategory::METRICS,
                               "=== END STATUS REPORT ===");
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::METRICS,
                                "Error generating status report: " +
                                    std::string(e.what()));
  }
}

void MetricsReporter::generateReportByEngine(const std::string &engine) const {
  try {
    auto metrics = dbManager.getMetricsByEngine(engine);

    Logger::getInstance().info(LogCategory::METRICS,
                               "=== REPORT FOR ENGINE: " + engine + " ===");
    Logger::getInstance().info(LogCategory::METRICS,
                               "Found " + std::to_string(metrics.size()) +
                                   " metrics for engine " + engine);

    for (const auto &metric : metrics) {
      Logger::getInstance().info(
          LogCategory::METRICS,
          "Table: " + metric.schema_name + "." + metric.table_name +
              " | Status: " + metric.status +
              " | Records: " + std::to_string(metric.records_transferred) +
              " | Size: " + formatBytes(metric.bytes_transferred));
    }

    Logger::getInstance().info(LogCategory::METRICS,
                               "=== END ENGINE REPORT ===");
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::METRICS,
                                "Error generating engine report: " +
                                    std::string(e.what()));
  }
}

void MetricsReporter::exportToCSV(const std::string &filename) const {
  try {
    TransferMetricsData data;
    dbManager.loadMetrics(data);

    std::ofstream file(filename);
    if (!file.is_open()) {
      Logger::getInstance().error(LogCategory::METRICS,
                                  "Failed to open file for CSV export: " +
                                      filename);
      return;
    }

    // Write header
    file << "schema_name,table_name,db_engine,records_transferred,bytes_"
            "transferred,"
         << "memory_used_mb,io_operations_per_second,transfer_type,status,"
            "error_message,"
         << "started_at,completed_at\n";

    // Write data
    for (const auto &metric : data.getMetrics()) {
      file << metric.schema_name << "," << metric.table_name << ","
           << metric.db_engine << "," << metric.records_transferred << ","
           << metric.bytes_transferred << "," << std::fixed
           << std::setprecision(2) << metric.memory_used_mb << ","
           << metric.io_operations_per_second << "," << metric.transfer_type
           << "," << metric.status << ",\"" << metric.error_message << "\","
           << metric.started_at << "," << metric.completed_at << "\n";
    }

    file.close();
    Logger::getInstance().info(LogCategory::METRICS,
                               "Exported metrics to CSV: " + filename);
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::METRICS,
                                "Error exporting to CSV: " +
                                    std::string(e.what()));
  }
}

void MetricsReporter::exportToJSON(const std::string &filename) const {
  try {
    TransferMetricsData data;
    dbManager.loadMetrics(data);

    std::ofstream file(filename);
    if (!file.is_open()) {
      Logger::getInstance().error(LogCategory::METRICS,
                                  "Failed to open file for JSON export: " +
                                      filename);
      return;
    }

    file << "{\n  \"metrics\": [\n";

    const auto &metrics = data.getMetrics();
    for (size_t i = 0; i < metrics.size(); ++i) {
      const auto &metric = metrics[i];
      file << "    {\n"
           << "      \"schema_name\": \"" << metric.schema_name << "\",\n"
           << "      \"table_name\": \"" << metric.table_name << "\",\n"
           << "      \"db_engine\": \"" << metric.db_engine << "\",\n"
           << "      \"records_transferred\": " << metric.records_transferred
           << ",\n"
           << "      \"bytes_transferred\": " << metric.bytes_transferred
           << ",\n"
           << "      \"memory_used_mb\": " << std::fixed << std::setprecision(2)
           << metric.memory_used_mb << ",\n"
           << "      \"io_operations_per_second\": "
           << metric.io_operations_per_second << ",\n"
           << "      \"transfer_type\": \"" << metric.transfer_type << "\",\n"
           << "      \"status\": \"" << metric.status << "\",\n"
           << "      \"error_message\": \"" << metric.error_message << "\",\n"
           << "      \"started_at\": \"" << metric.started_at << "\",\n"
           << "      \"completed_at\": \"" << metric.completed_at << "\"\n"
           << "    }";

      if (i < metrics.size() - 1) {
        file << ",";
      }
      file << "\n";
    }

    file << "  ]\n}\n";
    file.close();
    Logger::getInstance().info(LogCategory::METRICS,
                               "Exported metrics to JSON: " + filename);
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::METRICS,
                                "Error exporting to JSON: " +
                                    std::string(e.what()));
  }
}

std::string MetricsReporter::formatBytes(long long bytes) const {
  if (bytes < 1024)
    return std::to_string(bytes) + " B";
  if (bytes < 1024 * 1024)
    return std::to_string(bytes / 1024) + " KB";
  if (bytes < 1024 * 1024 * 1024)
    return std::to_string(bytes / (1024 * 1024)) + " MB";
  return std::to_string(bytes / (1024 * 1024 * 1024)) + " GB";
}

std::string MetricsReporter::formatMemory(double mb) const {
  std::stringstream ss;
  ss << std::fixed << std::setprecision(2) << mb << " MB";
  return ss.str();
}

std::string
MetricsReporter::formatTimestamp(const std::string &timestamp) const {
  if (timestamp.empty())
    return "N/A";
  return timestamp;
}
