#ifndef METRICSCOLLECTOR_H
#define METRICSCOLLECTOR_H

#include "core/Config.h"
#include "core/logger.h"
#include <pqxx/pqxx>
#include <string>
#include <vector>

struct TransferMetrics {
  std::string schema_name;
  std::string table_name;
  std::string db_engine;

  // Métricas Reales de la Base de Datos
  long long records_transferred = 0;
  long long bytes_transferred = 0;
  double memory_used_mb = 0.0;
  int io_operations_per_second = 0;

  // Metadatos
  std::string transfer_type;
  std::string status;
  std::string error_message;

  // Timestamps
  std::string started_at;
  std::string completed_at;
};

class MetricsCollector {
public:
  MetricsCollector() = default;
  ~MetricsCollector() = default;

  void collectAllMetrics();
  void createMetricsTable();

private:
  void collectTransferMetrics();
  void collectPerformanceMetrics();
  void collectMetadataMetrics();
  void collectTimestampMetrics();
  void saveMetricsToDatabase();
  void generateMetricsReport();

  std::string getEstimatedStartTime(const std::string &completedAt);
  [[deprecated("This function is not used in the codebase and may be removed "
               "in a future version")]]
  double calculateTransferRate(long long records, int duration_ms);
  [[deprecated("This function is not used in the codebase and may be removed "
               "in a future version")]]
  long long calculateBytesTransferred(const std::string &schema_name,
                                      const std::string &table_name);

  std::vector<TransferMetrics> metrics;
};

#endif // METRICSCOLLECTOR_H
