#include "transformations/data_validation_transformation.h"
#include "core/logger.h"
#include <algorithm>
#include <sstream>
#include <iomanip>

DataValidationTransformation::DataValidationTransformation() = default;

bool DataValidationTransformation::validateConfig(const json& config) const {
  if (!config.contains("validation_type") || !config["validation_type"].is_string()) {
    Logger::error(LogCategory::TRANSFER, "DataValidationTransformation",
                  "Missing or invalid validation_type in config");
    return false;
  }
  
  std::string validationType = config["validation_type"];
  std::vector<std::string> validTypes = {
    "address", "phone", "email"
  };
  
  if (std::find(validTypes.begin(), validTypes.end(), validationType) == 
      validTypes.end()) {
    Logger::error(LogCategory::TRANSFER, "DataValidationTransformation",
                  "Invalid validation_type: " + validationType);
    return false;
  }
  
  if (!config.contains("source_column") || !config["source_column"].is_string()) {
    Logger::error(LogCategory::TRANSFER, "DataValidationTransformation",
                  "Missing or invalid source_column in config");
    return false;
  }
  
  return true;
}

std::vector<json> DataValidationTransformation::execute(
  const std::vector<json>& inputData,
  const json& config
) {
  if (inputData.empty()) {
    return inputData;
  }
  
  if (!validateConfig(config)) {
    Logger::error(LogCategory::TRANSFER, "DataValidationTransformation",
                  "Invalid config, returning input data unchanged");
    return inputData;
  }
  
  std::string validationType = config["validation_type"];
  std::string sourceColumn = config["source_column"];
  std::string targetColumn = config.value("target_column", sourceColumn + "_validated");
  std::string isValidColumn = config.value("is_valid_column", sourceColumn + "_is_valid");
  
  std::vector<json> result;
  result.reserve(inputData.size());
  
  for (const auto& row : inputData) {
    json outputRow = row;
    
    if (!row.contains(sourceColumn)) {
      outputRow[targetColumn] = json(nullptr);
      outputRow[isValidColumn] = false;
      result.push_back(outputRow);
      continue;
    }
    
    json sourceValue = row[sourceColumn];
    json validatedValue;
    bool isValid = false;
    
    if (validationType == "address") {
      validatedValue = validateAddress(sourceValue);
      isValid = !validatedValue.is_null() && validatedValue.contains("formatted");
    } else if (validationType == "phone") {
      std::string phoneStr = sourceValue.is_string() ? 
                             sourceValue.get<std::string>() : 
                             sourceValue.dump();
      validatedValue = validatePhone(phoneStr);
      isValid = validatedValue.value("is_valid", false);
    } else if (validationType == "email") {
      std::string emailStr = sourceValue.is_string() ? 
                              sourceValue.get<std::string>() : 
                              sourceValue.dump();
      validatedValue = validateEmail(emailStr);
      isValid = validatedValue.value("is_valid", false);
    }
    
    outputRow[targetColumn] = validatedValue;
    outputRow[isValidColumn] = isValid;
    result.push_back(outputRow);
  }
  
  Logger::info(LogCategory::TRANSFER, "DataValidationTransformation",
               "Validated " + validationType + " for " + 
               std::to_string(inputData.size()) + " rows");
  
  return result;
}

json DataValidationTransformation::validateAddress(const json& addressData) {
  json result;
  
  if (addressData.is_string()) {
    std::string address = addressData.get<std::string>();
    std::string standardized = standardizeAddress(address);
    result["original"] = address;
    result["formatted"] = standardized;
    result["is_valid"] = !standardized.empty();
  } else if (addressData.is_object()) {
    result = addressData;
    if (addressData.contains("street") || addressData.contains("address")) {
      std::ostringstream formatted;
      if (addressData.contains("street")) {
        formatted << addressData["street"].get<std::string>();
      } else if (addressData.contains("address")) {
        formatted << addressData["address"].get<std::string>();
      }
      
      if (addressData.contains("city")) {
        if (formatted.str().length() > 0) formatted << ", ";
        formatted << addressData["city"].get<std::string>();
      }
      
      if (addressData.contains("state")) {
        if (formatted.str().length() > 0) formatted << ", ";
        formatted << addressData["state"].get<std::string>();
      }
      
      if (addressData.contains("zip") || addressData.contains("postal_code")) {
        if (formatted.str().length() > 0) formatted << " ";
        std::string zip = addressData.contains("zip") ? 
                          addressData["zip"].get<std::string>() : 
                          addressData["postal_code"].get<std::string>();
        formatted << zip;
      }
      
      result["formatted"] = formatted.str();
      result["is_valid"] = true;
    }
  }
  
  return result;
}

json DataValidationTransformation::validatePhone(const std::string& phone) {
  json result;
  
  std::string formatted = formatPhone(phone);
  bool valid = isValidPhone(phone);
  
  result["original"] = phone;
  result["formatted"] = formatted;
  result["is_valid"] = valid;
  
  return result;
}

json DataValidationTransformation::validateEmail(const std::string& email) {
  json result;
  
  std::string normalized = normalizeEmail(email);
  bool valid = isValidEmail(email);
  
  result["original"] = email;
  result["normalized"] = normalized;
  result["is_valid"] = valid;
  
  return result;
}

std::string DataValidationTransformation::standardizeAddress(const std::string& address) {
  std::string result = address;
  
  // Remove extra whitespace
  std::regex whitespaceRegex(R"(\s+)");
  result = std::regex_replace(result, whitespaceRegex, " ");
  
  // Trim
  result.erase(0, result.find_first_not_of(" \t\n\r"));
  result.erase(result.find_last_not_of(" \t\n\r") + 1);
  
  // Standardize common abbreviations
  std::regex stRegex(R"(\bSt\b)", std::regex_constants::icase);
  result = std::regex_replace(result, stRegex, "Street");
  
  std::regex aveRegex(R"(\bAve\b)", std::regex_constants::icase);
  result = std::regex_replace(result, aveRegex, "Avenue");
  
  std::regex blvdRegex(R"(\bBlvd\b)", std::regex_constants::icase);
  result = std::regex_replace(result, blvdRegex, "Boulevard");
  
  return result;
}

std::string DataValidationTransformation::formatPhone(const std::string& phone) {
  std::string digits;
  for (char c : phone) {
    if (std::isdigit(c)) {
      digits += c;
    }
  }
  
  if (digits.length() == 10) {
    return "(" + digits.substr(0, 3) + ") " + 
           digits.substr(3, 3) + "-" + 
           digits.substr(6);
  } else if (digits.length() == 11 && digits[0] == '1') {
    return "+1 (" + digits.substr(1, 3) + ") " + 
           digits.substr(4, 3) + "-" + 
           digits.substr(7);
  }
  
  return phone;
}

std::string DataValidationTransformation::normalizeEmail(const std::string& email) {
  std::string result = email;
  
  // Convert to lowercase
  std::transform(result.begin(), result.end(), result.begin(), ::tolower);
  
  // Remove whitespace
  result.erase(std::remove_if(result.begin(), result.end(), ::isspace), result.end());
  
  return result;
}

bool DataValidationTransformation::isValidEmail(const std::string& email) {
  std::regex emailRegex(R"([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})");
  return std::regex_match(email, emailRegex);
}

bool DataValidationTransformation::isValidPhone(const std::string& phone) {
  std::string digits;
  for (char c : phone) {
    if (std::isdigit(c)) {
      digits += c;
    }
  }
  
  return digits.length() == 10 || (digits.length() == 11 && digits[0] == '1');
}
