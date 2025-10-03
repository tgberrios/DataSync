#ifndef TRANSFERMETRICSDATA_H
#define TRANSFERMETRICSDATA_H

#include <string>
#include <vector>

struct TransferMetrics {
  std::string schema_name;
  std::string table_name;
  std::string db_engine;

  // Database Metrics
  long long records_transferred = 0;
  long long bytes_transferred = 0;
  double memory_used_mb = 0.0;
  int io_operations_per_second = 0;

  // Metadata
  std::string transfer_type;
  std::string status;
  std::string error_message;

  // Timestamps
  std::string started_at;
  std::string completed_at;

  // Validation methods
  bool isValid() const;
  void validate();
  void setDefaults();
};

class TransferMetricsData {
public:
  TransferMetricsData() = default;
  ~TransferMetricsData() = default;

  // Data management
  void addMetric(const TransferMetrics &metric);
  void clear();
  size_t size() const;
  std::vector<TransferMetrics> &getMetrics();
  const std::vector<TransferMetrics> &getMetrics() const;

  // Filtering and searching
  std::vector<TransferMetrics> getByStatus(const std::string &status) const;
  std::vector<TransferMetrics> getByEngine(const std::string &engine) const;
  TransferMetrics *findMetric(const std::string &schema,
                              const std::string &table,
                              const std::string &engine);

  // Statistics
  long long getTotalRecords() const;
  long long getTotalBytes() const;
  double getAverageMemoryUsage() const;
  int getSuccessCount() const;
  int getFailedCount() const;
  int getPendingCount() const;
  double getSuccessRate() const;

private:
  std::vector<TransferMetrics> metrics;
};

#endif // TRANSFERMETRICSDATA_H
