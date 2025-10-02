#ifndef METRICSDATABASEMANAGER_H
#define METRICSDATABASEMANAGER_H

#include "Config.h"
#include "TransferMetricsData.h"
#include "logger.h"
#include <pqxx/pqxx>
#include <string>

class MetricsDatabaseManager {
public:
  MetricsDatabaseManager() = default;
  ~MetricsDatabaseManager() = default;

  // Table management
  void createMetricsTable();
  bool tableExists() const;

  // Data operations
  void saveMetrics(const TransferMetricsData &data);
  void loadMetrics(TransferMetricsData &data);
  void clearOldMetrics(int daysToKeep = 30);

  // Queries
  std::vector<TransferMetrics>
  getMetricsByDateRange(const std::string &startDate,
                        const std::string &endDate) const;
  std::vector<TransferMetrics>
  getMetricsByStatus(const std::string &status) const;
  std::vector<TransferMetrics>
  getMetricsByEngine(const std::string &engine) const;

  // Statistics queries
  long long getTotalRecordsTransferred() const;
  long long getTotalBytesTransferred() const;
  double getAverageMemoryUsage() const;
  int getSuccessCount() const;
  int getFailedCount() const;
  int getPendingCount() const;
  double getSuccessRate() const;

private:
  pqxx::connection getConnection() const;
  std::string escapeSQL(const std::string &value) const;
  void executeQuery(const std::string &query);
  pqxx::result executeSelectQuery(const std::string &query) const;
};

#endif // METRICSDATABASEMANAGER_H
