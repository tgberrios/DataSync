#ifndef DATAGOVERNANCE_H
#define DATAGOVERNANCE_H

#include "Config.h"
#include "TableMetadata.h"
#include "DataClassifier.h"
#include "DataQualityCalculator.h"
#include "logger.h"
#include <pqxx/pqxx>
#include <string>
#include <vector>

class DataGovernance {
public:
  DataGovernance() = default;
  ~DataGovernance() = default;

  void initialize();
  void runDiscovery();
  void generateReport();

private:
  // Database operations
  void createGovernanceTable();
  std::vector<TableMetadata> discoverTables();
  TableMetadata extractTableMetadata(const std::string &schema_name,
                                     const std::string &table_name);
  void storeMetadata(const TableMetadata &metadata);
  void updateExistingMetadata(const TableMetadata &metadata);

  // Analysis operations
  void analyzeTableStructure(pqxx::connection &conn,
                             const std::string &schema_name,
                             const std::string &table_name,
                             TableMetadata &metadata);
  void analyzeDataQuality(pqxx::connection &conn,
                          const std::string &schema_name,
                          const std::string &table_name,
                          TableMetadata &metadata);
  void analyzeUsageStatistics(pqxx::connection &conn,
                              const std::string &schema_name,
                              const std::string &table_name,
                              TableMetadata &metadata);
  void analyzeHealthStatus(pqxx::connection &conn,
                           const std::string &schema_name,
                           const std::string &table_name,
                           TableMetadata &metadata);
  void inferSourceEngine(TableMetadata &metadata);

  // Utility functions
  std::string escapeSQL(const std::string &value);
  std::string getCurrentTimestamp();
  std::string determineAccessFrequency(int query_count);
  std::string determineHealthStatus(const TableMetadata &metadata);

  // Helper classes
  DataClassifier classifier;
  DataQualityCalculator qualityCalculator;
};

#endif // DATAGOVERNANCE_H
