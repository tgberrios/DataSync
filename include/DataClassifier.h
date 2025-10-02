#ifndef DATACLASSIFIER_H
#define DATACLASSIFIER_H

#include "TableMetadata.h"
#include <string>
#include <vector>

class DataClassifier {
public:
  DataClassifier() = default;
  ~DataClassifier() = default;

  // Main classification method
  void classifyTable(TableMetadata &metadata);

private:
  // Data category classification
  std::string determineDataCategory(const std::string &table_name, const std::string &schema_name);
  std::string determineBusinessDomain(const std::string &table_name, const std::string &schema_name);
  std::string determineSensitivityLevel(const std::string &table_name, const std::string &schema_name);
  std::string determineDataClassification(const std::string &table_name, const std::string &schema_name);
  
  // Policy determination
  std::string determineRetentionPolicy(const std::string &data_category, const std::string &sensitivity_level);
  std::string determineBackupFrequency(const std::string &data_category, const std::string &access_frequency);
  std::string determineComplianceRequirements(const std::string &sensitivity_level, const std::string &business_domain);

  // Helper methods for pattern matching
  bool containsAnyPattern(const std::string &text, const std::vector<std::string> &patterns);
  std::string toLowerCase(const std::string &text);
};

#endif // DATACLASSIFIER_H
