#ifndef METRICSCALCULATOR_H
#define METRICSCALCULATOR_H

#include "Config.h"
#include "TransferMetricsData.h"
#include "logger.h"
#include <chrono>
#include <pqxx/pqxx>
#include <string>

class MetricsCalculator {
public:
  MetricsCalculator() = default;
  ~MetricsCalculator() = default;

  // Data collection from database
  void collectTransferMetrics(TransferMetricsData &data);
  void collectPerformanceMetrics(TransferMetricsData &data);
  void collectMetadataMetrics(TransferMetricsData &data);
  void collectTimestampMetrics(TransferMetricsData &data);

  // Calculations
  double calculateTransferRate(long long records, int duration_ms) const;
  long long calculateBytesTransferred(const std::string &schema_name,
                                      const std::string &table_name) const;
  double calculateMemoryUsage(long long bytes) const;
  int calculateIOOperations(const std::string &schema_name,
                            const std::string &table_name) const;

  // Status mapping
  std::string mapTransferType(const std::string &status) const;
  std::string mapStatus(const std::string &catalogStatus, bool active) const;
  std::string mapErrorMessage(const std::string &status) const;

  // Time utilities
  std::string getCurrentTimestamp() const;
  std::string getEstimatedStartTime(const std::string &completedAt) const;

private:
  pqxx::connection getConnection() const;
  std::string escapeSQL(const std::string &value) const;
  void validateMetric(TransferMetrics &metric) const;
  bool isValidRow(const pqxx::row &row) const;
};

#endif // METRICSCALCULATOR_H
