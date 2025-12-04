#include "governance/data_classifier.h"
#include "core/logger.h"
#include "utils/string_utils.h"
#include <fstream>

DataClassifier::DataClassifier() : loaded_(false) {
  loadRules("/rules/governance_rules.json");
}

DataClassifier::DataClassifier(const std::string &rulesPath) : loaded_(false) {
  loadRules(rulesPath);
}

bool DataClassifier::loadRules(const std::string &rulesPath) {
  try {
    std::ifstream file(rulesPath);
    if (!file.is_open()) {
      Logger::warning(LogCategory::GOVERNANCE, "DataClassifier",
                      "Could not open rules file: " + rulesPath);
      return false;
    }

    file >> rules_;
    loaded_ = true;
    Logger::info(LogCategory::GOVERNANCE, "DataClassifier",
                 "Loaded governance rules from: " + rulesPath);
    return true;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "DataClassifier",
                  "Error loading rules: " + std::string(e.what()));
    loaded_ = false;
    return false;
  }
}

bool DataClassifier::matchesAny(const std::string &text,
                                const std::vector<std::string> &patterns) {
  std::string lowerText = StringUtils::toLower(text);
  for (const auto &pattern : patterns) {
    if (lowerText.find(StringUtils::toLower(pattern)) != std::string::npos) {
      return true;
    }
  }
  return false;
}

std::string
DataClassifier::classifyDataCategory(const std::string &tableName,
                                     const std::string &schemaName) {
  if (!loaded_) {
    return "TRANSACTIONAL";
  }

  try {
    auto &categories = rules_["data_categories"];

    if (categories.contains("schema_patterns")) {
      for (const auto &rule : categories["schema_patterns"]) {
        std::vector<std::string> patterns =
            rule["patterns"].get<std::vector<std::string>>();
        if (matchesAny(schemaName, patterns)) {
          return rule["category"].get<std::string>();
        }
      }
    }

    if (categories.contains("table_patterns")) {
      for (const auto &rule : categories["table_patterns"]) {
        std::vector<std::string> patterns =
            rule["patterns"].get<std::vector<std::string>>();
        if (matchesAny(tableName, patterns)) {
          return rule["category"].get<std::string>();
        }
      }
    }

    return categories.value("default", "TRANSACTIONAL");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "classifyDataCategory",
                  "Error classifying: " + std::string(e.what()));
    return "TRANSACTIONAL";
  }
}

std::string
DataClassifier::classifyBusinessDomain(const std::string &tableName,
                                       const std::string &schemaName) {
  if (!loaded_) {
    return "GENERAL";
  }

  try {
    auto &domains = rules_["business_domains"];

    if (domains.contains("patterns")) {
      for (const auto &rule : domains["patterns"]) {
        std::vector<std::string> keywords =
            rule["keywords"].get<std::vector<std::string>>();
        if (matchesAny(tableName, keywords) ||
            matchesAny(schemaName, keywords)) {
          return rule["domain"].get<std::string>();
        }
      }
    }

    return domains.value("default", "GENERAL");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "classifyBusinessDomain",
                  "Error classifying: " + std::string(e.what()));
    return "GENERAL";
  }
}

std::string
DataClassifier::classifySensitivityLevel(const std::string &tableName,
                                         const std::string &schemaName) {
  if (!loaded_) {
    return "PUBLIC";
  }

  try {
    auto &levels = rules_["sensitivity_levels"];

    if (levels.contains("patterns")) {
      for (const auto &rule : levels["patterns"]) {
        std::vector<std::string> keywords =
            rule["keywords"].get<std::vector<std::string>>();
        if (matchesAny(tableName, keywords) ||
            matchesAny(schemaName, keywords)) {
          return rule["level"].get<std::string>();
        }
      }
    }

    return levels.value("default", "PUBLIC");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "classifySensitivityLevel",
                  "Error classifying: " + std::string(e.what()));
    return "PUBLIC";
  }
}

std::string
DataClassifier::classifyDataClassification(const std::string &tableName,
                                           const std::string &schemaName) {
  if (!loaded_) {
    return "PUBLIC";
  }

  try {
    auto &classifications = rules_["data_classifications"];

    if (classifications.contains("patterns")) {
      for (const auto &rule : classifications["patterns"]) {
        std::vector<std::string> keywords =
            rule["keywords"].get<std::vector<std::string>>();
        if (matchesAny(tableName, keywords) ||
            matchesAny(schemaName, keywords)) {
          return rule["classification"].get<std::string>();
        }
      }
    }

    return classifications.value("default", "PUBLIC");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "classifyDataClassification",
                  "Error classifying: " + std::string(e.what()));
    return "PUBLIC";
  }
}
