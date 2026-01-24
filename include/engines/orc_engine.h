#ifndef ORC_ENGINE_H
#define ORC_ENGINE_H

#include "third_party/json.hpp"
#include <string>
#include <vector>
#include <fstream>
#include <memory>

using json = nlohmann::json;

struct ORCConfig {
  int max_rows = 0;
  std::vector<std::string> columns = {};
  bool use_column_indices = false;
};

class ORCEngine {
  std::string source_;
  ORCConfig config_;

  std::vector<uint8_t> readFromFile(const std::string &filePath);
  std::vector<std::string> getColumnNames(const std::vector<uint8_t> &fileData);
  std::vector<json> parseORCFile(const std::vector<uint8_t> &fileData);

public:
  explicit ORCEngine(const std::string &source, 
                    const ORCConfig &config = ORCConfig());
  ~ORCEngine() = default;

  ORCEngine(const ORCEngine &) = delete;
  ORCEngine &operator=(const ORCEngine &) = delete;

  std::vector<json> parseORC();
  std::vector<std::string> detectColumns(const std::vector<json> &data);
  void setConfig(const ORCConfig &config);
};

#endif
