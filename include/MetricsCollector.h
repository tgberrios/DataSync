#ifndef METRICSCOLLECTOR_H
#define METRICSCOLLECTOR_H

#include "MetricsCalculator.h"
#include "MetricsDatabaseManager.h"
#include "MetricsReporter.h"
#include "TransferMetricsData.h"
#include "logger.h"
#include <memory>

class MetricsCollector {
public:
  MetricsCollector();
  ~MetricsCollector() = default;

  // Main collection method
  void collectAllMetrics();

  // Individual collection methods
  void collectTransferMetrics();
  void collectPerformanceMetrics();
  void collectMetadataMetrics();
  void collectTimestampMetrics();

  // Reporting methods
  void generateSummaryReport();
  void generateDetailedReport();
  void generateStatusReport();
  void generateEngineReport();

  // Export methods
  void exportToCSV(const std::string &filename);
  void exportToJSON(const std::string &filename);

  // Maintenance methods
  void clearOldMetrics(int daysToKeep = 30);

private:
  // Core components
  std::unique_ptr<TransferMetricsData> metricsData;
  std::unique_ptr<MetricsDatabaseManager> dbManager;
  std::unique_ptr<MetricsCalculator> calculator;
  std::unique_ptr<MetricsReporter> reporter;

  // Helper methods
  void initializeComponents();
  void validateData();
};

#endif // METRICSCOLLECTOR_H