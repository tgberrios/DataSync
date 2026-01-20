#include "transformations/json_parser_transformation.h"
#include "core/logger.h"
#include <sstream>

JsonParserTransformation::JsonParserTransformation() = default;

bool JsonParserTransformation::validateConfig(const json& config) const {
  if (!config.contains("source_column") || !config["source_column"].is_string()) {
    Logger::error(LogCategory::TRANSFER, "JsonParserTransformation",
                  "Missing or invalid source_column in config");
    return false;
  }
  
  if (!config.contains("format") || !config["format"].is_string()) {
    Logger::error(LogCategory::TRANSFER, "JsonParserTransformation",
                  "Missing or invalid format in config");
    return false;
  }
  
  std::string format = config["format"];
  if (format != "json" && format != "xml") {
    Logger::error(LogCategory::TRANSFER, "JsonParserTransformation",
                  "Invalid format, must be 'json' or 'xml'");
    return false;
  }
  
  if (!config.contains("fields_to_extract") || !config["fields_to_extract"].is_array()) {
    Logger::error(LogCategory::TRANSFER, "JsonParserTransformation",
                  "Missing or invalid fields_to_extract in config");
    return false;
  }
  
  return true;
}

std::vector<json> JsonParserTransformation::execute(
  const std::vector<json>& inputData,
  const json& config
) {
  if (inputData.empty()) {
    return inputData;
  }
  
  if (!validateConfig(config)) {
    Logger::error(LogCategory::TRANSFER, "JsonParserTransformation",
                  "Invalid config, returning input data unchanged");
    return inputData;
  }
  
  std::string sourceColumn = config["source_column"];
  std::string format = config["format"];
  std::vector<std::string> fieldsToExtract;
  for (const auto& field : config["fields_to_extract"]) {
    fieldsToExtract.push_back(field.get<std::string>());
  }
  
  std::vector<json> result;
  result.reserve(inputData.size());
  
  for (const auto& row : inputData) {
    json outputRow = row;
    
    if (format == "json") {
      json parsedData = parseJsonColumn(row, sourceColumn, fieldsToExtract);
      for (const auto& [key, value] : parsedData.items()) {
        outputRow[key] = value;
      }
    } else if (format == "xml") {
      json parsedData = parseXmlColumn(row, sourceColumn, fieldsToExtract);
      for (const auto& [key, value] : parsedData.items()) {
        outputRow[key] = value;
      }
    }
    
    result.push_back(outputRow);
  }
  
  Logger::info(LogCategory::TRANSFER, "JsonParserTransformation",
               "Parsed " + format + " from " + std::to_string(inputData.size()) + " rows");
  
  return result;
}

json JsonParserTransformation::parseJsonColumn(
  const json& row,
  const std::string& sourceColumn,
  const std::vector<std::string>& fieldsToExtract
) {
  json result;
  
  if (!row.contains(sourceColumn)) {
    return result;
  }
  
  json sourceValue = row[sourceColumn];
  
  if (sourceValue.is_string()) {
    try {
      sourceValue = json::parse(sourceValue.get<std::string>());
    } catch (...) {
      Logger::warning(LogCategory::TRANSFER, "JsonParserTransformation",
                      "Failed to parse JSON string in column: " + sourceColumn);
      return result;
    }
  }
  
  if (!sourceValue.is_object() && !sourceValue.is_array()) {
    Logger::warning(LogCategory::TRANSFER, "JsonParserTransformation",
                    "Source column is not a JSON object or array");
    return result;
  }
  
  if (fieldsToExtract.empty()) {
    // Extract all fields
    if (sourceValue.is_object()) {
      return sourceValue;
    }
  } else {
    // Extract specific fields
    for (const auto& fieldPath : fieldsToExtract) {
      json fieldValue = extractNestedField(sourceValue, fieldPath);
      if (!fieldValue.is_null()) {
        size_t lastDot = fieldPath.find_last_of('.');
        std::string fieldName = (lastDot != std::string::npos) ? 
                                fieldPath.substr(lastDot + 1) : fieldPath;
        result[fieldName] = fieldValue;
      }
    }
  }
  
  return result;
}

json JsonParserTransformation::extractNestedField(
  const json& jsonData,
  const std::string& path
) {
  json current = jsonData;
  std::istringstream pathStream(path);
  std::string segment;
  
  while (std::getline(pathStream, segment, '.')) {
    if (current.is_object() && current.contains(segment)) {
      current = current[segment];
    } else if (current.is_array()) {
      try {
        size_t index = std::stoul(segment);
        if (index < current.size()) {
          current = current[index];
        } else {
          return json(nullptr);
        }
      } catch (...) {
        return json(nullptr);
      }
    } else {
      return json(nullptr);
    }
  }
  
  return current;
}

json JsonParserTransformation::parseXmlColumn(
  const json& row,
  const std::string& sourceColumn,
  const std::vector<std::string>& fieldsToExtract
) {
  json result;
  
  if (!row.contains(sourceColumn)) {
    return result;
  }
  
  std::string xmlContent = row[sourceColumn].is_string() ? 
                           row[sourceColumn].get<std::string>() : 
                           row[sourceColumn].dump();
  
  // Basic XML parsing - extract simple tag values
  // For production, use a proper XML parser library
  for (const auto& field : fieldsToExtract) {
    std::string tagStart = "<" + field + ">";
    std::string tagEnd = "</" + field + ">";
    
    size_t startPos = xmlContent.find(tagStart);
    if (startPos != std::string::npos) {
      startPos += tagStart.length();
      size_t endPos = xmlContent.find(tagEnd, startPos);
      if (endPos != std::string::npos) {
        std::string value = xmlContent.substr(startPos, endPos - startPos);
        result[field] = value;
      }
    }
  }
  
  return result;
}
