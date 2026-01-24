#include "engines/fixed_width_engine.h"
#include "core/logger.h"
#include <algorithm>
#include <cctype>
#include <fstream>
#include <sstream>

FixedWidthEngine::FixedWidthEngine(const std::string &source, const FixedWidthConfig &config)
    : source_(source), config_(config) {}

std::string FixedWidthEngine::readFromFile(const std::string &filePath) {
  std::ifstream file(filePath, std::ios::binary);
  if (!file.is_open()) {
    throw std::runtime_error("Failed to open fixed-width file: " + filePath);
  }

  std::stringstream buffer;
  buffer << file.rdbuf();
  file.close();

  return buffer.str();
}

std::string FixedWidthEngine::trim(const std::string &str) {
  size_t first = str.find_first_not_of(" \t\r\n");
  if (first == std::string::npos)
    return "";
  size_t last = str.find_last_not_of(" \t\r\n");
  return str.substr(first, (last - first + 1));
}

std::vector<std::string> FixedWidthEngine::splitFixedWidthLine(const std::string &line) {
  std::vector<std::string> fields;
  
  if (config_.columnWidths.empty()) {
    Logger::warning(LogCategory::DATABASE, "FixedWidthEngine",
                    "No column widths specified, using default width of 10");
    size_t pos = 0;
    const size_t defaultWidth = 10;
    while (pos < line.length()) {
      std::string field = line.substr(pos, defaultWidth);
      fields.push_back(trim(field));
      pos += defaultWidth;
    }
    return fields;
  }

  size_t pos = 0;
  for (int width : config_.columnWidths) {
    if (pos >= line.length()) {
      fields.push_back("");
      continue;
    }
    std::string field = line.substr(pos, width);
    fields.push_back(trim(field));
    pos += width;
  }

  return fields;
}

std::vector<json> FixedWidthEngine::parseFixedWidth() {
  std::string content = readFromFile(source_);
  std::istringstream stream(content);
  std::string line;
  std::vector<json> records;
  int lineNumber = 0;

  while (std::getline(stream, line)) {
    if (lineNumber < config_.skip_rows) {
      lineNumber++;
      continue;
    }

    if (config_.skip_empty_rows && trim(line).empty()) {
      continue;
    }

    std::vector<std::string> fields = splitFixedWidthLine(line);
    
    if (lineNumber == config_.skip_rows && config_.has_header) {
      lineNumber++;
      continue;
    }

    json record = json::object();
    for (size_t i = 0; i < fields.size(); ++i) {
      record["column_" + std::to_string(i)] = fields[i];
    }
    records.push_back(record);
    lineNumber++;
  }

  return records;
}

std::vector<std::string> FixedWidthEngine::detectColumns(const std::vector<json> &data) {
  if (data.empty()) {
    return {};
  }

  std::vector<std::string> columns;
  for (const auto &item : data[0].items()) {
    columns.push_back(item.key());
  }
  return columns;
}

void FixedWidthEngine::setConfig(const FixedWidthConfig &config) {
  config_ = config;
}
