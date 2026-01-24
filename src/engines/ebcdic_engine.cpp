#include "engines/ebcdic_engine.h"
#include "core/logger.h"
#include <algorithm>
#include <cctype>
#include <fstream>
#include <sstream>
#include <codecvt>
#include <locale>

EBCDICEngine::EBCDICEngine(const std::string &source, const EBCDICConfig &config)
    : source_(source), config_(config) {}

std::string EBCDICEngine::readFromFile(const std::string &filePath) {
  std::ifstream file(filePath, std::ios::binary);
  if (!file.is_open()) {
    throw std::runtime_error("Failed to open EBCDIC file: " + filePath);
  }

  std::vector<uint8_t> buffer((std::istreambuf_iterator<char>(file)),
                               std::istreambuf_iterator<char>());
  file.close();

  return convertEBCDICToUTF8(buffer);
}

std::string EBCDICEngine::convertEBCDICToUTF8(const std::vector<uint8_t> &ebcdicData) {
  static const char ebcdicToAscii[256] = {
    0, 1, 2, 3, 0, 9, 0, 127, 0, 0, 0, 11, 12, 13, 14, 15,
    16, 17, 18, 19, 0, 0, 8, 0, 24, 25, 0, 0, 28, 29, 30, 31,
    0, 0, 0, 0, 0, 10, 23, 27, 0, 0, 0, 0, 0, 5, 6, 7,
    0, 0, 22, 0, 0, 0, 0, 4, 0, 0, 0, 0, 20, 21, 0, 26,
    32, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 46, 60, 40, 43, 124,
    38, 0, 0, 0, 0, 0, 0, 0, 0, 0, 33, 36, 42, 41, 59, 94,
    45, 47, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 44, 37, 95, 62,
    63, 0, 0, 0, 0, 0, 0, 0, 0, 0, 96, 58, 35, 64, 39, 61,
    34, 97, 98, 99, 100, 101, 102, 103, 104, 105, 0, 0, 0, 0, 0, 0,
    0, 106, 107, 108, 109, 110, 111, 112, 113, 114, 0, 0, 0, 0, 0, 0,
    0, 126, 115, 116, 117, 118, 119, 120, 121, 122, 0, 0, 0, 91, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 93, 0, 0,
    123, 65, 66, 67, 68, 69, 70, 71, 72, 73, 0, 0, 0, 0, 0, 0,
    125, 74, 75, 76, 77, 78, 79, 80, 81, 82, 0, 0, 0, 0, 0, 0,
    92, 0, 83, 84, 85, 86, 87, 88, 89, 90, 0, 0, 0, 0, 0, 0,
    48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 0, 0, 0, 0, 0, 0
  };

  std::string result;
  result.reserve(ebcdicData.size());
  
  for (uint8_t byte : ebcdicData) {
    char ascii = ebcdicToAscii[byte];
    if (ascii != 0 || byte == 0) {
      result += ascii;
    } else {
      result += '?';
    }
  }
  
  return result;
}

std::string EBCDICEngine::trim(const std::string &str) {
  size_t first = str.find_first_not_of(" \t\r\n");
  if (first == std::string::npos)
    return "";
  size_t last = str.find_last_not_of(" \t\r\n");
  return str.substr(first, (last - first + 1));
}

std::vector<std::string> EBCDICEngine::splitLine(const std::string &line) {
  std::vector<std::string> fields;
  std::stringstream ss(line);
  std::string field;

  while (std::getline(ss, field, config_.delimiter[0])) {
    fields.push_back(trim(field));
  }

  return fields;
}

std::vector<json> EBCDICEngine::parseEBCDIC() {
  std::string content = readFromFile(source_);
  std::istringstream stream(content);
  std::string line;
  std::vector<json> records;
  int lineNumber = 0;
  std::vector<std::string> headers;

  while (std::getline(stream, line)) {
    if (lineNumber < config_.skip_rows) {
      lineNumber++;
      continue;
    }

    if (config_.skip_empty_rows && trim(line).empty()) {
      continue;
    }

    std::vector<std::string> fields = splitLine(line);

    if (lineNumber == config_.skip_rows && config_.has_header) {
      headers = fields;
      lineNumber++;
      continue;
    }

    json record = json::object();
    if (!headers.empty()) {
      for (size_t i = 0; i < fields.size() && i < headers.size(); ++i) {
        record[headers[i]] = fields[i];
      }
    } else {
      for (size_t i = 0; i < fields.size(); ++i) {
        record["column_" + std::to_string(i)] = fields[i];
      }
    }
    records.push_back(record);
    lineNumber++;
  }

  return records;
}

std::vector<std::string> EBCDICEngine::detectColumns(const std::vector<json> &data) {
  if (data.empty()) {
    return {};
  }

  std::vector<std::string> columns;
  for (const auto &item : data[0].items()) {
    columns.push_back(item.key());
  }
  return columns;
}

void EBCDICEngine::setConfig(const EBCDICConfig &config) {
  config_ = config;
}
