#ifndef DATA_VALIDATION_TRANSFORMATION_H
#define DATA_VALIDATION_TRANSFORMATION_H

#include "transformation_engine.h"
#include <string>
#include <vector>
#include <regex>

// Data Validation transformation: Address, Phone, Email validation
class DataValidationTransformation : public Transformation {
public:
  DataValidationTransformation();
  ~DataValidationTransformation() override = default;
  
  std::vector<json> execute(
    const std::vector<json>& inputData,
    const json& config
  ) override;
  
  std::string getType() const override { return "data_validation"; }
  
  bool validateConfig(const json& config) const override;

private:
  // Validate and normalize address
  json validateAddress(const json& addressData);
  
  // Validate and format phone number
  json validatePhone(const std::string& phone);
  
  // Validate and normalize email
  json validateEmail(const std::string& email);
  
  // Standardize address format
  std::string standardizeAddress(const std::string& address);
  
  // Format phone number
  std::string formatPhone(const std::string& phone);
  
  // Normalize email
  std::string normalizeEmail(const std::string& email);
  
  // Check if email is valid
  bool isValidEmail(const std::string& email);
  
  // Check if phone is valid
  bool isValidPhone(const std::string& phone);
};

#endif // DATA_VALIDATION_TRANSFORMATION_H
