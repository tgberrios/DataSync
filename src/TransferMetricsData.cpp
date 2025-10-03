#include "TransferMetricsData.h"
#include <algorithm>
#include <numeric>

bool TransferMetrics::isValid() const {
  return !schema_name.empty() && !table_name.empty() && !db_engine.empty() &&
         records_transferred >= 0 && bytes_transferred >= 0 &&
         memory_used_mb >= 0.0;
}

void TransferMetrics::validate() {
  if (schema_name.empty())
    schema_name = "unknown";
  if (table_name.empty())
    table_name = "unknown";
  if (db_engine.empty())
    db_engine = "unknown";
  if (records_transferred < 0)
    records_transferred = 0;
  if (bytes_transferred < 0)
    bytes_transferred = 0;
  if (memory_used_mb < 0.0)
    memory_used_mb = 0.0;
  if (io_operations_per_second < 0)
    io_operations_per_second = 0;
}

void TransferMetrics::setDefaults() {
  schema_name = "";
  table_name = "";
  db_engine = "";
  records_transferred = 0;
  bytes_transferred = 0;
  memory_used_mb = 0.0;
  io_operations_per_second = 0;
  transfer_type = "UNKNOWN";
  status = "PENDING";
  error_message = "";
  started_at = "";
  completed_at = "";
}

void TransferMetricsData::addMetric(const TransferMetrics &metric) {
  metrics.push_back(metric);
}

void TransferMetricsData::clear() { metrics.clear(); }

size_t TransferMetricsData::size() const { return metrics.size(); }

std::vector<TransferMetrics> &TransferMetricsData::getMetrics() {
  return metrics;
}

const std::vector<TransferMetrics> &TransferMetricsData::getMetrics() const {
  return metrics;
}

std::vector<TransferMetrics>
TransferMetricsData::getByStatus(const std::string &status) const {
  std::vector<TransferMetrics> result;
  std::copy_if(
      metrics.begin(), metrics.end(), std::back_inserter(result),
      [&status](const TransferMetrics &m) { return m.status == status; });
  return result;
}

std::vector<TransferMetrics>
TransferMetricsData::getByEngine(const std::string &engine) const {
  std::vector<TransferMetrics> result;
  std::copy_if(
      metrics.begin(), metrics.end(), std::back_inserter(result),
      [&engine](const TransferMetrics &m) { return m.db_engine == engine; });
  return result;
}

TransferMetrics *TransferMetricsData::findMetric(const std::string &schema,
                                                 const std::string &table,
                                                 const std::string &engine) {
  auto it = std::find_if(metrics.begin(), metrics.end(),
                         [&schema, &table, &engine](const TransferMetrics &m) {
                           return m.schema_name == schema &&
                                  m.table_name == table &&
                                  m.db_engine == engine;
                         });
  return (it != metrics.end()) ? &(*it) : nullptr;
}

long long TransferMetricsData::getTotalRecords() const {
  return std::accumulate(metrics.begin(), metrics.end(), 0LL,
                         [](long long sum, const TransferMetrics &m) {
                           return sum + m.records_transferred;
                         });
}

long long TransferMetricsData::getTotalBytes() const {
  return std::accumulate(metrics.begin(), metrics.end(), 0LL,
                         [](long long sum, const TransferMetrics &m) {
                           return sum + m.bytes_transferred;
                         });
}

double TransferMetricsData::getAverageMemoryUsage() const {
  if (metrics.empty())
    return 0.0;

  double total = std::accumulate(metrics.begin(), metrics.end(), 0.0,
                                 [](double sum, const TransferMetrics &m) {
                                   return sum + m.memory_used_mb;
                                 });
  return total / metrics.size();
}

int TransferMetricsData::getSuccessCount() const {
  return std::count_if(
      metrics.begin(), metrics.end(),
      [](const TransferMetrics &m) { return m.status == "SUCCESS"; });
}

int TransferMetricsData::getFailedCount() const {
  return std::count_if(
      metrics.begin(), metrics.end(),
      [](const TransferMetrics &m) { return m.status == "FAILED"; });
}

int TransferMetricsData::getPendingCount() const {
  return std::count_if(
      metrics.begin(), metrics.end(),
      [](const TransferMetrics &m) { return m.status == "PENDING"; });
}

double TransferMetricsData::getSuccessRate() const {
  if (metrics.empty())
    return 0.0;
  return (static_cast<double>(getSuccessCount()) / metrics.size()) * 100.0;
}
