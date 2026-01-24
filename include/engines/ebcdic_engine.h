#ifndef EBCDIC_ENGINE_H
#define EBCDIC_ENGINE_H

#include "third_party/json.hpp"
#include <string>
#include <vector>
#include <fstream>

using json = nlohmann::json;

struct EBCDICConfig {
  std::string delimiter = ",";
  bool has_header = true;
  bool skip_empty_rows = true;
  int skip_rows = 0;
  std::string target_encoding = "UTF-8";
};

class EBCDICEngine {
  std::string source_;
  EBCDICConfig config_;

  std::string readFromFile(const std::string &filePath);
  std::string convertEBCDICToUTF8(const std::vector<uint8_t> &ebcdicData);
  std::vector<std::string> splitLine(const std::string &line);
  std::string trim(const std::string &str);

public:
  explicit EBCDICEngine(const std::string &source, 
                       const EBCDICConfig &config = EBCDICConfig());
  ~EBCDICEngine() = default;

  EBCDICEngine(const EBCDICEngine &) = delete;
  EBCDICEngine &operator=(const EBCDICEngine &) = delete;

  std::vector<json> parseEBCDIC();
  std::vector<std::string> detectColumns(const std::vector<json> &data);
  void setConfig(const EBCDICConfig &config);
};

#endif
