#include "engines/compressed_file_engine.h"
#include "core/logger.h"
#include <algorithm>
#include <fstream>
#include <sstream>

CompressedFileEngine::CompressedFileEngine(const std::string &source, const CompressedFileConfig &config)
    : source_(source), config_(config) {}

CompressionType CompressedFileEngine::detectCompressionType(const std::string &filePath) {
  if (filePath.length() < 4) {
    return CompressionType::UNKNOWN;
  }

  std::string ext = filePath.substr(filePath.length() - 4);
  std::transform(ext.begin(), ext.end(), ext.begin(), ::tolower);

  if (ext == ".gz" || ext == "gzip") {
    return CompressionType::GZIP;
  } else if (ext == ".bz2" || ext == "bzip") {
    return CompressionType::BZIP2;
  } else if (ext == ".lz4") {
    return CompressionType::LZ4;
  }

  std::ifstream file(filePath, std::ios::binary);
  if (!file.is_open()) {
    return CompressionType::UNKNOWN;
  }

  uint8_t header[4];
  file.read(reinterpret_cast<char*>(header), 4);
  file.close();

  if (header[0] == 0x1f && header[1] == 0x8b) {
    return CompressionType::GZIP;
  } else if (header[0] == 'B' && header[1] == 'Z' && header[2] == 'h') {
    return CompressionType::BZIP2;
  } else if (header[0] == 0x04 && header[1] == 0x22 && header[2] == 0x4d && header[3] == 0x18) {
    return CompressionType::LZ4;
  }

  return CompressionType::UNKNOWN;
}

std::vector<uint8_t> CompressedFileEngine::readCompressedFile(const std::string &filePath) {
  std::ifstream file(filePath, std::ios::binary);
  if (!file.is_open()) {
    throw std::runtime_error("Failed to open compressed file: " + filePath);
  }

  std::vector<uint8_t> buffer((std::istreambuf_iterator<char>(file)),
                               std::istreambuf_iterator<char>());
  file.close();

  return buffer;
}

std::vector<uint8_t> CompressedFileEngine::decompressGZIP(const std::vector<uint8_t> &compressedData) {
  Logger::warning(LogCategory::DATABASE, "CompressedFileEngine",
                  "GZIP decompression requires zlib library - returning empty data");
  return {};
}

std::vector<uint8_t> CompressedFileEngine::decompressBZIP2(const std::vector<uint8_t> &compressedData) {
  Logger::warning(LogCategory::DATABASE, "CompressedFileEngine",
                  "BZIP2 decompression requires bzip2 library - returning empty data");
  return {};
}

std::vector<uint8_t> CompressedFileEngine::decompressLZ4(const std::vector<uint8_t> &compressedData) {
  Logger::warning(LogCategory::DATABASE, "CompressedFileEngine",
                  "LZ4 decompression requires lz4 library - returning empty data");
  return {};
}

std::string CompressedFileEngine::decompressToString(const std::vector<uint8_t> &decompressedData) {
  return std::string(decompressedData.begin(), decompressedData.end());
}

std::vector<json> CompressedFileEngine::parseCompressedFile() {
  CompressionType type = config_.compression_type;
  if (type == CompressionType::UNKNOWN) {
    type = detectCompressionType(source_);
  }

  if (type == CompressionType::UNKNOWN) {
    throw std::runtime_error("Unknown compression type for file: " + source_);
  }

  std::vector<uint8_t> compressedData = readCompressedFile(source_);
  std::vector<uint8_t> decompressedData;

  switch (type) {
    case CompressionType::GZIP:
      decompressedData = decompressGZIP(compressedData);
      break;
    case CompressionType::BZIP2:
      decompressedData = decompressBZIP2(compressedData);
      break;
    case CompressionType::LZ4:
      decompressedData = decompressLZ4(compressedData);
      break;
    default:
      throw std::runtime_error("Unsupported compression type");
  }

  if (decompressedData.empty()) {
    throw std::runtime_error("Failed to decompress file: " + source_);
  }

  std::string content = decompressToString(decompressedData);

  if (config_.inner_format == "CSV") {
    std::istringstream stream(content);
    std::string line;
    std::vector<json> records;
    int lineNumber = 0;

    while (std::getline(stream, line)) {
      if (lineNumber == 0 && config_.has_header) {
        lineNumber++;
        continue;
      }

      std::istringstream lineStream(line);
      std::string field;
      json record = json::object();
      int colIndex = 0;

      while (std::getline(lineStream, field, config_.delimiter[0])) {
        record["column_" + std::to_string(colIndex++)] = field;
      }
      records.push_back(record);
      lineNumber++;
    }

    return records;
  }

  return {};
}

std::vector<std::string> CompressedFileEngine::detectColumns(const std::vector<json> &data) {
  if (data.empty()) {
    return {};
  }

  std::vector<std::string> columns;
  for (const auto &item : data[0].items()) {
    columns.push_back(item.key());
  }
  return columns;
}

void CompressedFileEngine::setConfig(const CompressedFileConfig &config) {
  config_ = config;
}
