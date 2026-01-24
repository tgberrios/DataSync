#include "engines/orc_engine.h"
#include "core/logger.h"
#include <fstream>

ORCEngine::ORCEngine(const std::string &source, const ORCConfig &config)
    : source_(source), config_(config) {}

std::vector<uint8_t> ORCEngine::readFromFile(const std::string &filePath) {
  std::ifstream file(filePath, std::ios::binary);
  if (!file.is_open()) {
    throw std::runtime_error("Failed to open ORC file: " + filePath);
  }

  std::vector<uint8_t> buffer((std::istreambuf_iterator<char>(file)),
                               std::istreambuf_iterator<char>());
  file.close();

  return buffer;
}

std::vector<std::string> ORCEngine::getColumnNames(const std::vector<uint8_t> &fileData) {
  Logger::warning(LogCategory::DATABASE, "ORCEngine",
                  "Column name extraction requires ORC library - returning empty list");
  return {};
}

std::vector<json> ORCEngine::parseORCFile(const std::vector<uint8_t> &fileData) {
  Logger::warning(LogCategory::DATABASE, "ORCEngine",
                  "ORC parsing requires ORC library - returning empty records");
  return {};
}

std::vector<json> ORCEngine::parseORC() {
  std::vector<uint8_t> fileData = readFromFile(source_);
  return parseORCFile(fileData);
}

std::vector<std::string> ORCEngine::detectColumns(const std::vector<json> &data) {
  if (data.empty()) {
    return {};
  }

  std::vector<std::string> columns;
  for (const auto &item : data[0].items()) {
    columns.push_back(item.key());
  }
  return columns;
}

void ORCEngine::setConfig(const ORCConfig &config) {
  config_ = config;
}
