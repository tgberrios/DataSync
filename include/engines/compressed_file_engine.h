#ifndef COMPRESSED_FILE_ENGINE_H
#define COMPRESSED_FILE_ENGINE_H

#include "third_party/json.hpp"
#include <string>
#include <vector>
#include <fstream>
#include <memory>

using json = nlohmann::json;

enum class CompressionType {
  GZIP,
  BZIP2,
  LZ4,
  UNKNOWN
};

struct CompressedFileConfig {
  CompressionType compression_type = CompressionType::UNKNOWN;
  std::string inner_format = "CSV";
  std::string delimiter = ",";
  bool has_header = true;
  std::string encoding = "UTF-8";
};

class CompressedFileEngine {
  std::string source_;
  CompressedFileConfig config_;

  CompressionType detectCompressionType(const std::string &filePath);
  std::vector<uint8_t> readCompressedFile(const std::string &filePath);
  std::vector<uint8_t> decompressGZIP(const std::vector<uint8_t> &compressedData);
  std::vector<uint8_t> decompressBZIP2(const std::vector<uint8_t> &compressedData);
  std::vector<uint8_t> decompressLZ4(const std::vector<uint8_t> &compressedData);
  std::string decompressToString(const std::vector<uint8_t> &decompressedData);

public:
  explicit CompressedFileEngine(const std::string &source, 
                               const CompressedFileConfig &config = CompressedFileConfig());
  ~CompressedFileEngine() = default;

  CompressedFileEngine(const CompressedFileEngine &) = delete;
  CompressedFileEngine &operator=(const CompressedFileEngine &) = delete;

  std::vector<json> parseCompressedFile();
  std::vector<std::string> detectColumns(const std::vector<json> &data);
  void setConfig(const CompressedFileConfig &config);
};

#endif
