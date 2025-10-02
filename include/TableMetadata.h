#ifndef TABLEMETADATA_H
#define TABLEMETADATA_H

#include <string>

struct TableMetadata {
  // Basic table information
  std::string schema_name;
  std::string table_name;
  int total_columns = 0;
  long long total_rows = 0;
  double table_size_mb = 0.0;

  // Structure information
  std::string primary_key_columns;
  int index_count = 0;
  int constraint_count = 0;

  // Data quality metrics
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

  // Source and analysis information
  std::string inferred_source_engine;
  std::string last_analyzed;
  std::string last_accessed;
  std::string access_frequency;
  int query_count_daily = 0;

  // Data classification
  std::string data_category;
  std::string business_domain;
  std::string sensitivity_level;
  std::string data_classification;
  std::string retention_policy;
  std::string backup_frequency;

  // Health and maintenance
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

#endif // TABLEMETADATA_H
