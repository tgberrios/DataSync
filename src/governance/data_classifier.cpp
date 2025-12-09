#include "governance/data_classifier.h"
#include "core/logger.h"
#include "utils/string_utils.h"
#include <fstream>

// Default constructor for DataClassifier. Initializes the classifier with
// loaded_ set to false. The classifier will return default values for all
// classification methods (TRANSACTIONAL, GENERAL, PUBLIC).
DataClassifier::DataClassifier() : loaded_(false) {
}

// Constructor for DataClassifier with custom rules path. Initializes the
// classifier with loaded_ set to false and attempts to load rules from the
// specified path. If loading fails, the classifier will still be functional
// but will return default values for all classification methods.
DataClassifier::DataClassifier(const std::string &rulesPath) : loaded_(false) {
  loadRules(rulesPath);
}

// Loads governance rules from a JSON file. Parses the JSON file and stores it
// in the rules_ member. Sets loaded_ to true on success, false on failure.
// Logs warnings if the file cannot be opened, and errors if JSON parsing
// fails. Returns true if rules were successfully loaded, false otherwise.
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

// Checks if the given text matches any of the patterns in the provided vector.
// Performs case-insensitive substring matching by converting both the text and
// each pattern to lowercase before comparison. Returns true if any pattern is
// found as a substring in the text, false otherwise. This is used internally
// for pattern matching in classification methods.
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

// Classifies a table into a data category (e.g., TRANSACTIONAL, ANALYTICAL)
// based on schema and table name patterns defined in the governance rules.
// First checks schema_patterns, then table_patterns. If no match is found,
// returns the default category from rules or "TRANSACTIONAL" if rules are not
// loaded. Returns "TRANSACTIONAL" on error or if rules are not loaded.
std::string
DataClassifier::classifyDataCategory(const std::string &tableName,
                                     const std::string &schemaName) {
  if (tableName.empty() || schemaName.empty()) {
    Logger::error(LogCategory::GOVERNANCE, "DataClassifier",
                  "classifyDataCategory: tableName and schemaName must not be empty");
    return "TRANSACTIONAL";
  }

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

// Classifies a table into a business domain (e.g., FINANCE, HEALTHCARE, SPORTS)
// based on keywords found in the table or schema name. Searches through
// business_domains patterns in the governance rules. If no match is found,
// returns the default domain from rules or "GENERAL" if rules are not loaded.
// Returns "GENERAL" on error or if rules are not loaded.
std::string
DataClassifier::classifyBusinessDomain(const std::string &tableName,
                                       const std::string &schemaName) {
  if (tableName.empty() || schemaName.empty()) {
    Logger::error(LogCategory::GOVERNANCE, "DataClassifier",
                  "classifyBusinessDomain: tableName and schemaName must not be empty");
    return "GENERAL";
  }

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

// Classifies a table's sensitivity level (e.g., PUBLIC, PRIVATE, CRITICAL)
// based on keywords found in the table or schema name. Searches through
// sensitivity_levels patterns in the governance rules. If no match is found,
// returns the default level from rules or "PUBLIC" if rules are not loaded.
// Returns "PUBLIC" on error or if rules are not loaded.
std::string
DataClassifier::classifySensitivityLevel(const std::string &tableName,
                                         const std::string &schemaName) {
  if (tableName.empty() || schemaName.empty()) {
    Logger::error(LogCategory::GOVERNANCE, "DataClassifier",
                  "classifySensitivityLevel: tableName and schemaName must not be empty");
    return "PUBLIC";
  }

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

// Classifies a table's data classification (e.g., PUBLIC, CONFIDENTIAL)
// based on keywords found in the table or schema name. Searches through
// data_classifications patterns in the governance rules. If no match is found,
// returns the default classification from rules or "PUBLIC" if rules are not
// loaded. Returns "PUBLIC" on error or if rules are not loaded.
std::string
DataClassifier::classifyDataClassification(const std::string &tableName,
                                           const std::string &schemaName) {
  if (tableName.empty() || schemaName.empty()) {
    Logger::error(LogCategory::GOVERNANCE, "DataClassifier",
                  "classifyDataClassification: tableName and schemaName must not be empty");
    return "PUBLIC";
  }

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
