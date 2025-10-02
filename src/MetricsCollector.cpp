#include "MetricsCollector.h"

MetricsCollector::MetricsCollector() { initializeComponents(); }

void MetricsCollector::initializeComponents() {
  metricsData = std::make_unique<TransferMetricsData>();
  dbManager = std::make_unique<MetricsDatabaseManager>();
  calculator = std::make_unique<MetricsCalculator>();
  reporter = std::make_unique<MetricsReporter>(*dbManager);
}

void MetricsCollector::collectAllMetrics() {
  try {
    Logger::getInstance().info(LogCategory::METRICS,
                               "Starting metrics collection");

    // Ensure database table exists
    dbManager->createMetricsTable();

    // Collect all types of metrics
    collectTransferMetrics();
    collectPerformanceMetrics();
    collectMetadataMetrics();
    collectTimestampMetrics();

    // Validate collected data
    validateData();

    // Save to database
    dbManager->saveMetrics(*metricsData);

    // Generate reports
    generateSummaryReport();

    Logger::getInstance().info(LogCategory::METRICS,
                               "Metrics collection completed successfully");
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::METRICS,
                                "Error in metrics collection: " +
                                    std::string(e.what()));
  }
}

void MetricsCollector::collectTransferMetrics() {
  try {
    Logger::getInstance().info(LogCategory::METRICS,
                               "Collecting transfer metrics");
    calculator->collectTransferMetrics(*metricsData);
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::METRICS,
                                "Error collecting transfer metrics: " +
                                    std::string(e.what()));
  }
}

void MetricsCollector::collectPerformanceMetrics() {
  try {
    Logger::getInstance().info(LogCategory::METRICS,
                               "Collecting performance metrics");
    calculator->collectPerformanceMetrics(*metricsData);
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::METRICS,
                                "Error collecting performance metrics: " +
                                    std::string(e.what()));
  }
}

void MetricsCollector::collectMetadataMetrics() {
  try {
    Logger::getInstance().info(LogCategory::METRICS,
                               "Collecting metadata metrics");
    calculator->collectMetadataMetrics(*metricsData);
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::METRICS,
                                "Error collecting metadata metrics: " +
                                    std::string(e.what()));
  }
}

void MetricsCollector::collectTimestampMetrics() {
  try {
    Logger::getInstance().info(LogCategory::METRICS,
                               "Collecting timestamp metrics");
    calculator->collectTimestampMetrics(*metricsData);
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::METRICS,
                                "Error collecting timestamp metrics: " +
                                    std::string(e.what()));
  }
}

void MetricsCollector::generateSummaryReport() {
  try {
    Logger::getInstance().info(LogCategory::METRICS,
                               "Generating summary report");
    reporter->generateSummaryReport();
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::METRICS,
                                "Error generating summary report: " +
                                    std::string(e.what()));
  }
}

void MetricsCollector::generateDetailedReport() {
  try {
    Logger::getInstance().info(LogCategory::METRICS,
                               "Generating detailed report");
    reporter->generateDetailedReport();
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::METRICS,
                                "Error generating detailed report: " +
                                    std::string(e.what()));
  }
}

void MetricsCollector::generateStatusReport() {
  try {
    Logger::getInstance().info(LogCategory::METRICS,
                               "Generating status report");
    reporter->generateStatusReport();
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::METRICS,
                                "Error generating status report: " +
                                    std::string(e.what()));
  }
}

void MetricsCollector::generateEngineReport() {
  try {
    Logger::getInstance().info(LogCategory::METRICS,
                               "Generating engine report");
    reporter->generateEngineReport();
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::METRICS,
                                "Error generating engine report: " +
                                    std::string(e.what()));
  }
}

void MetricsCollector::exportToCSV(const std::string &filename) {
  try {
    Logger::getInstance().info(LogCategory::METRICS,
                               "Exporting metrics to CSV: " + filename);
    reporter->exportToCSV(filename);
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::METRICS,
                                "Error exporting to CSV: " +
                                    std::string(e.what()));
  }
}

void MetricsCollector::exportToJSON(const std::string &filename) {
  try {
    Logger::getInstance().info(LogCategory::METRICS,
                               "Exporting metrics to JSON: " + filename);
    reporter->exportToJSON(filename);
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::METRICS,
                                "Error exporting to JSON: " +
                                    std::string(e.what()));
  }
}

void MetricsCollector::clearOldMetrics(int daysToKeep) {
  try {
    Logger::getInstance().info(LogCategory::METRICS,
                               "Clearing old metrics (keeping " +
                                   std::to_string(daysToKeep) + " days)");
    dbManager->clearOldMetrics(daysToKeep);
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::METRICS,
                                "Error clearing old metrics: " +
                                    std::string(e.what()));
  }
}

void MetricsCollector::validateData() {
  try {
    Logger::getInstance().info(LogCategory::METRICS,
                               "Validating collected data");

    const auto &metrics = metricsData->getMetrics();
    int validCount = 0;
    int invalidCount = 0;

    for (const auto &metric : metrics) {
      if (metric.isValid()) {
        validCount++;
      } else {
        invalidCount++;
        Logger::getInstance().warning(LogCategory::METRICS,
                                      "Invalid metric: " + metric.schema_name +
                                          "." + metric.table_name);
      }
    }

    Logger::getInstance().info(
        LogCategory::METRICS,
        "Data validation complete: " + std::to_string(validCount) + " valid, " +
            std::to_string(invalidCount) + " invalid metrics");

    if (invalidCount > 0) {
      Logger::getInstance().warning(
          LogCategory::METRICS, "Found " + std::to_string(invalidCount) +
                                    " invalid metrics that will be skipped");
    }
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::METRICS,
                                "Error validating data: " +
                                    std::string(e.what()));
  }
}