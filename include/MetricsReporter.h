#ifndef METRICSREPORTER_H
#define METRICSREPORTER_H

#include "MetricsDatabaseManager.h"
#include "TransferMetricsData.h"
#include "logger.h"
#include <string>

class MetricsReporter {
public:
  MetricsReporter(MetricsDatabaseManager &dbManager);
  ~MetricsReporter() = default;

  // Report generation
  void generateSummaryReport() const;
  void generateDetailedReport() const;
  void generateStatusReport() const;
  void generateEngineReport() const;

  // Custom reports
  void generateReportByDateRange(const std::string &startDate,
                                 const std::string &endDate) const;
  void generateReportByStatus(const std::string &status) const;
  void generateReportByEngine(const std::string &engine) const;

  // Export reports
  void exportToCSV(const std::string &filename) const;
  void exportToJSON(const std::string &filename) const;

private:
  MetricsDatabaseManager &dbManager;

  // Helper methods
  void logSummary(const TransferMetricsData &data) const;
  void logDetailedMetrics(const std::vector<TransferMetrics> &metrics) const;
  std::string formatBytes(long long bytes) const;
  std::string formatMemory(double mb) const;
  std::string formatTimestamp(const std::string &timestamp) const;
};

#endif // METRICSREPORTER_H
