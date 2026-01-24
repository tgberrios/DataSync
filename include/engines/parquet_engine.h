#ifndef PARQUET_ENGINE_H
#define PARQUET_ENGINE_H

#include "third_party/json.hpp"
#include <string>
#include <vector>
#include <fstream>
#include <memory>

using json = nlohmann::json;

struct ParquetConfig {
  int max_rows = 0;
  std::vector<std::string> columns = {};
  bool use_column_indices = false;
};

class ParquetEngine {
  std::string source_;
  ParquetConfig config_;

  std::vector<uint8_t> readFromFile(const std::string &filePath);
  std::vector<std::string> getColumnNames(const std::vector<uint8_t> &fileData);
  std::vector<json> parseParquetFile(const std::vector<uint8_t> &fileData);

public:
  explicit ParquetEngine(const std::string &source, 
                        const ParquetConfig &config = ParquetConfig());
  ~ParquetEngine() = default;

  ParquetEngine(const ParquetEngine &) = delete;
  ParquetEngine &operator=(const ParquetEngine &) = delete;

  std::vector<json> parseParquet();
  std::vector<std::string> detectColumns(const std::vector<json> &data);
  void setConfig(const ParquetConfig &config);
};

#endif
