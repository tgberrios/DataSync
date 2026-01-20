#ifndef DATA_CLEANSING_TRANSFORMATION_H
#define DATA_CLEANSING_TRANSFORMATION_H

#include "transformation_engine.h"
#include <string>
#include <vector>

// Data Cleansing transformation: Clean and standardize data
class DataCleansingTransformation : public Transformation {
public:
  struct CleansingRule {
    std::string column;
    std::vector<std::string> operations; // trim, uppercase, lowercase, remove_special, etc.
  };
  
  DataCleansingTransformation();
  ~DataCleansingTransformation() override = default;
  
  std::vector<json> execute(
    const std::vector<json>& inputData,
    const json& config
  ) override;
  
  std::string getType() const override { return "data_cleansing"; }
  
  bool validateConfig(const json& config) const override;

private:
  // Apply cleansing operations to a value
  json applyCleansing(const json& value, const std::vector<std::string>& operations);
  
  // Individual cleansing operations
  std::string trim(const std::string& str);
  std::string upperCase(const std::string& str);
  std::string lowerCase(const std::string& str);
  std::string removeSpecialChars(const std::string& str, const std::string& keepChars = "");
  std::string removeWhitespace(const std::string& str);
  std::string removeLeadingZeros(const std::string& str);
  std::string normalizeWhitespace(const std::string& str);
};

#endif // DATA_CLEANSING_TRANSFORMATION_H
