#ifndef COLUMN_CATALOG_COLLECTOR_H
#define COLUMN_CATALOG_COLLECTOR_H

#include "third_party/json.hpp"
#include <string>
#include <vector>

using json = nlohmann::json;

struct ColumnMetadata {
  std::string schema_name;
  std::string table_name;
  std::string column_name;
  std::string db_engine;
  std::string connection_string;

  int ordinal_position = 0;
  std::string data_type;
  int character_maximum_length = 0;
  int numeric_precision = 0;
  int numeric_scale = 0;
  bool is_nullable = true;
  std::string column_default;

  bool is_primary_key = false;
  bool is_foreign_key = false;
  bool is_unique = false;
  bool is_indexed = false;
  bool is_auto_increment = false;
  bool is_generated = false;

  long long null_count = 0;
  double null_percentage = 0.0;
  long long distinct_count = 0;
  double distinct_percentage = 0.0;
  std::string min_value;
  std::string max_value;
  double avg_value = 0.0;

  std::string data_category;
  std::string sensitivity_level;
  bool contains_pii = false;
  bool contains_phi = false;
  std::string pii_detection_method;
  double pii_confidence_score = 0.0;
  std::string pii_category;
  std::string phi_detection_method;
  double phi_confidence_score = 0.0;
  bool masking_applied = false;
  bool encryption_applied = false;
  bool tokenization_applied = false;

  json column_metadata_json;

  double median_value = 0.0;
  double std_deviation = 0.0;
  std::string mode_value;
  double mode_frequency = 0.0;

  double percentile_25 = 0.0;
  double percentile_75 = 0.0;
  double percentile_90 = 0.0;
  double percentile_95 = 0.0;
  double percentile_99 = 0.0;

  json value_distribution;
  json top_values;
  long long outlier_count = 0;
  double outlier_percentage = 0.0;

  std::string detected_pattern;
  double pattern_confidence = 0.0;
  json pattern_examples;

  json anomalies;
  bool has_anomalies = false;

  double profiling_quality_score = 0.0;
};

class ColumnCatalogCollector {
private:
  std::string metadataConnectionString_;
  std::vector<ColumnMetadata> columnData_;

  void collectPostgreSQLColumns(const std::string &connectionString);
  void collectMariaDBColumns(const std::string &connectionString);
  void collectMSSQLColumns(const std::string &connectionString);

  void analyzeColumnStatistics(ColumnMetadata &column,
                               const std::string &connectionString);
  void classifyColumn(ColumnMetadata &column);
  void detectPIIAdvanced(ColumnMetadata &column,
                         const std::string &connectionString);
  bool detectPIIByContent(const std::string &sampleValue,
                          const std::string &dataType);
  bool detectPHIByContent(const std::string &sampleValue,
                          const std::string &dataType);
  std::string detectPIICategory(const std::string &sampleValue,
                                const std::string &dataType);
  double calculatePIIConfidence(const std::string &columnName,
                                const std::string &dataType,
                                const std::string &sampleValue,
                                bool contentMatch);
  json buildColumnMetadataJSON(const ColumnMetadata &column,
                               const std::string &engine);
  std::string escapeSQL(const std::string &str);

  void calculateAdvancedStatistics(ColumnMetadata &column,
                                   const std::string &connectionString);
  void detectPatterns(ColumnMetadata &column,
                      const std::string &connectionString);
  void analyzeDistribution(ColumnMetadata &column,
                           const std::string &connectionString);
  void detectOutliers(ColumnMetadata &column,
                      const std::string &connectionString);
  void calculateProfilingQualityScore(ColumnMetadata &column);

  std::vector<double> getNumericValues(const std::string &connectionString,
                                       const ColumnMetadata &column,
                                       int maxSamples = 10000);
  std::vector<std::string> getSampleValues(const std::string &connectionString,
                                           const ColumnMetadata &column,
                                           int sampleSize = 1000);
  bool matchesPattern(const std::string &value, const std::string &pattern);
  double calculatePercentile(const std::vector<double> &values,
                             double percentile);
  double calculateMedian(const std::vector<double> &values);
  double calculateStdDeviation(const std::vector<double> &values, double mean);

public:
  explicit ColumnCatalogCollector(const std::string &metadataConnectionString);
  ~ColumnCatalogCollector();

  void collectAllColumns();
  void storeColumnMetadata();
  void generateReport();
};

#endif
