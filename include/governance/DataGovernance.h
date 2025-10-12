#ifndef DATAGOVERNANCE_H
#define DATAGOVERNANCE_H

#include "core/Config.h"
#include "core/logger.h"
#include "governance/data_classifier.h"
#include <map>
#include <memory>
#include <pqxx/pqxx>
#include <string>
#include <vector>

struct TableMetadata {
  std::string schema_name;
  std::string table_name;
  int total_columns = 0;
  long long total_rows = 0;
  double table_size_mb = 0.0;

  std::string primary_key_columns;
  int index_count = 0;
  int constraint_count = 0;

  // Enhanced data quality attributes
  double data_quality_score = 0.0;
  double null_percentage = 0.0;
  double duplicate_percentage = 0.0;
  double completeness_score = 0.0;
  double accuracy_score = 0.0;
  double consistency_score = 0.0;
  double validity_score = 0.0;
  double timeliness_score = 0.0;
  double uniqueness_score = 0.0;
  double integrity_score = 0.0;

  std::string inferred_source_engine;
  std::string last_analyzed;

  std::string last_accessed;
  std::string access_frequency;
  int query_count_daily = 0;

  // Expanded categorization
  std::string data_category;
  std::string business_domain;
  std::string sensitivity_level;
  std::string data_classification;
  std::string retention_policy;
  std::string backup_frequency;

  std::string health_status;
  std::string last_vacuum;
  double fragmentation_percentage = 0.0;

  // Additional metadata
  std::string table_owner;
  std::string creation_date;
  std::string last_modified;
  std::string table_comment;
  std::string data_lineage;
  std::string compliance_requirements;
};

class DataGovernance {
public:
  DataGovernance();
  ~DataGovernance() = default;

  void initialize();
  void runDiscovery();
  void generateReport();

private:
  std::unique_ptr<DataClassifier> classifier_;
  void createGovernanceTable();
  std::vector<TableMetadata> discoverTables();
  TableMetadata extractTableMetadata(const std::string &schema_name,
                                     const std::string &table_name);
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
  void classifyTable(TableMetadata &metadata);
  void inferSourceEngine(TableMetadata &metadata);
  void storeMetadata(const TableMetadata &metadata);
  void updateExistingMetadata(const TableMetadata &metadata);

  double calculateDataQualityScore(const TableMetadata &metadata);
  std::string determineAccessFrequency(int query_count);
  std::string determineHealthStatus(const TableMetadata &metadata);
  std::string determineDataCategory(const std::string &table_name,
                                    const std::string &schema_name);
  std::string determineBusinessDomain(const std::string &table_name,
                                      const std::string &schema_name);
  std::string determineSensitivityLevel(const std::string &table_name,
                                        const std::string &schema_name);
  std::string determineDataClassification(const std::string &table_name,
                                          const std::string &schema_name);
  std::string determineRetentionPolicy(const std::string &data_category,
                                       const std::string &sensitivity_level);
  std::string determineBackupFrequency(const std::string &data_category,
                                       const std::string &access_frequency);
  std::string
  determineComplianceRequirements(const std::string &sensitivity_level,
                                  const std::string &business_domain);

  // Enhanced quality analysis functions
  double calculateCompletenessScore(const TableMetadata &metadata);
  double calculateAccuracyScore(const TableMetadata &metadata);
  double calculateConsistencyScore(const TableMetadata &metadata);
  double calculateValidityScore(const TableMetadata &metadata);
  double calculateTimelinessScore(const TableMetadata &metadata);
  double calculateUniquenessScore(const TableMetadata &metadata);
  double calculateIntegrityScore(const TableMetadata &metadata);
};

#endif // DATAGOVERNANCE_H
