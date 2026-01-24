#ifndef FIXED_WIDTH_ENGINE_H
#define FIXED_WIDTH_ENGINE_H

#include "third_party/json.hpp"
#include <string>
#include <vector>
#include <fstream>

using json = nlohmann::json;

struct FixedWidthConfig {
  std::vector<int> columnWidths;
  bool has_header = true;
  std::string encoding = "UTF-8";
  bool skip_empty_rows = true;
  int skip_rows = 0;
  char padding_char = ' ';
};

class FixedWidthEngine {
  std::string source_;
  FixedWidthConfig config_;

  std::string readFromFile(const std::string &filePath);
  std::vector<std::string> splitFixedWidthLine(const std::string &line);
  std::string trim(const std::string &str);

public:
  explicit FixedWidthEngine(const std::string &source, 
                           const FixedWidthConfig &config = FixedWidthConfig());
  ~FixedWidthEngine() = default;

  FixedWidthEngine(const FixedWidthEngine &) = delete;
  FixedWidthEngine &operator=(const FixedWidthEngine &) = delete;

  std::vector<json> parseFixedWidth();
  std::vector<std::string> detectColumns(const std::vector<json> &data);
  void setConfig(const FixedWidthConfig &config);
};

#endif
