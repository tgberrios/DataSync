#ifndef JSON_PARSER_TRANSFORMATION_H
#define JSON_PARSER_TRANSFORMATION_H

#include "transformation_engine.h"
#include "third_party/json.hpp"
#include <string>
#include <vector>

using json = nlohmann::json;

// JSON/XML Parser transformation: Parse nested structures
class JsonParserTransformation : public Transformation {
public:
  JsonParserTransformation();
  ~JsonParserTransformation() override = default;
  
  std::vector<json> execute(
    const std::vector<json>& inputData,
    const json& config
  ) override;
  
  std::string getType() const override { return "json_parser"; }
  
  bool validateConfig(const json& config) const override;

private:
  // Parse JSON column and extract fields
  json parseJsonColumn(
    const json& row,
    const std::string& sourceColumn,
    const std::vector<std::string>& fieldsToExtract
  );
  
  // Extract nested field from JSON path (e.g., "user.address.city")
  json extractNestedField(const json& jsonData, const std::string& path);
  
  // Parse XML column (basic implementation)
  json parseXmlColumn(
    const json& row,
    const std::string& sourceColumn,
    const std::vector<std::string>& fieldsToExtract
  );
};

#endif // JSON_PARSER_TRANSFORMATION_H
