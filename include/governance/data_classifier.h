#ifndef DATA_CLASSIFIER_H
#define DATA_CLASSIFIER_H

#include "third_party/json.hpp"
#include <string>
#include <vector>

using json = nlohmann::json;

class DataClassifier {
private:
  json rules_;
  bool loaded_;

  bool matchesAny(const std::string &text,
                  const std::vector<std::string> &patterns);

public:
  DataClassifier();
  explicit DataClassifier(const std::string &rulesPath);

  bool loadRules(const std::string &rulesPath);
  bool isLoaded() const { return loaded_; }

  std::string classifyDataCategory(const std::string &tableName,
                                   const std::string &schemaName);
  std::string classifyBusinessDomain(const std::string &tableName,
                                     const std::string &schemaName);
  std::string classifySensitivityLevel(const std::string &tableName,
                                       const std::string &schemaName);
  std::string classifyDataClassification(const std::string &tableName,
                                         const std::string &schemaName);
};

#endif // DATA_CLASSIFIER_H
