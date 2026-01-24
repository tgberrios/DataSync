#include "engines/parquet_engine.h"
#include "core/logger.h"
#include <fstream>

ParquetEngine::ParquetEngine(const std::string &source, const ParquetConfig &config)
    : source_(source), config_(config) {}

std::vector<uint8_t> ParquetEngine::readFromFile(const std::string &filePath) {
  std::ifstream file(filePath, std::ios::binary);
  if (!file.is_open()) {
    throw std::runtime_error("Failed to open Parquet file: " + filePath);
  }

  std::vector<uint8_t> buffer((std::istreambuf_iterator<char>(file)),
                               std::istreambuf_iterator<char>());
  file.close();

  return buffer;
}

std::vector<std::string> ParquetEngine::getColumnNames(const std::vector<uint8_t> &fileData) {
  Logger::warning(LogCategory::DATABASE, "ParquetEngine",
                  "Column name extraction requires parquet-cpp library - returning empty list");
  return {};
}

std::vector<json> ParquetEngine::parseParquetFile(const std::vector<uint8_t> &fileData) {
  Logger::warning(LogCategory::DATABASE, "ParquetEngine",
                  "Parquet parsing requires parquet-cpp library - returning empty records");
  return {};
}

std::vector<json> ParquetEngine::parseParquet() {
  std::vector<uint8_t> fileData = readFromFile(source_);
  return parseParquetFile(fileData);
}

std::vector<std::string> ParquetEngine::detectColumns(const std::vector<json> &data) {
  if (data.empty()) {
    return {};
  }

  std::vector<std::string> columns;
  for (const auto &item : data[0].items()) {
    columns.push_back(item.key());
  }
  return columns;
}

void ParquetEngine::setConfig(const ParquetConfig &config) {
  config_ = config;
}
