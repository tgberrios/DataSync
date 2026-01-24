#ifndef EXCEL_ENGINE_H
#define EXCEL_ENGINE_H

#include "third_party/json.hpp"
#include <string>
#include <vector>
#include <memory>

using json = nlohmann::json;

struct ExcelConfig {
  int sheet_index = 0;
  std::string sheet_name = "";
  bool has_header = true;
  int skip_rows = 0;
  int max_rows = 0;
  std::string encoding = "UTF-8";
};

class ExcelEngine {
  std::string source_;
  ExcelConfig config_;

  std::vector<uint8_t> readFromFile(const std::string &filePath);
  std::vector<std::string> getSheetNames(const std::vector<uint8_t> &fileData);
  std::vector<json> parseXLSX(const std::vector<uint8_t> &fileData);
  std::vector<json> parseXLS(const std::vector<uint8_t> &fileData);
  bool isXLSX(const std::string &filePath);
  bool isXLS(const std::string &filePath);

public:
  explicit ExcelEngine(const std::string &source, 
                      const ExcelConfig &config = ExcelConfig());
  ~ExcelEngine() = default;

  ExcelEngine(const ExcelEngine &) = delete;
  ExcelEngine &operator=(const ExcelEngine &) = delete;

  std::vector<json> parseExcel();
  std::vector<std::string> detectColumns(const std::vector<json> &data);
  std::vector<std::string> getAvailableSheets();
  void setConfig(const ExcelConfig &config);
};

#endif
