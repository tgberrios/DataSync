#include "engines/excel_engine.h"
#include "core/logger.h"
#include <algorithm>
#include <fstream>

ExcelEngine::ExcelEngine(const std::string &source, const ExcelConfig &config)
    : source_(source), config_(config) {}

std::vector<uint8_t> ExcelEngine::readFromFile(const std::string &filePath) {
  std::ifstream file(filePath, std::ios::binary);
  if (!file.is_open()) {
    throw std::runtime_error("Failed to open Excel file: " + filePath);
  }

  std::vector<uint8_t> buffer((std::istreambuf_iterator<char>(file)),
                               std::istreambuf_iterator<char>());
  file.close();

  return buffer;
}

bool ExcelEngine::isXLSX(const std::string &filePath) {
  std::string ext = filePath.substr(filePath.length() - 5);
  std::transform(ext.begin(), ext.end(), ext.begin(), ::tolower);
  return ext == ".xlsx";
}

bool ExcelEngine::isXLS(const std::string &filePath) {
  std::string ext = filePath.substr(filePath.length() - 4);
  std::transform(ext.begin(), ext.end(), ext.begin(), ::tolower);
  return ext == ".xls";
}

std::vector<std::string> ExcelEngine::getSheetNames(const std::vector<uint8_t> &fileData) {
  // libxlsxwriter only writes, not reads
  // For reading, would need OpenXLSX, libxl, or similar library
  // For now, return default sheet name
  Logger::warning(LogCategory::DATABASE, "ExcelEngine",
                  "getSheetNames requires Excel reading library (libxlsxwriter only writes) - returning default sheet");
  return {"Sheet1"};
}

std::vector<json> ExcelEngine::parseXLSX(const std::vector<uint8_t> &fileData) {
  // XLSX is a ZIP file containing XML
  // libxlsxwriter only writes, not reads
  // For production, use OpenXLSX, libxl, or similar library
  Logger::warning(LogCategory::DATABASE, "ExcelEngine",
                  "XLSX parsing requires Excel reading library (libxlsxwriter only writes) - returning empty data. "
                  "Consider installing OpenXLSX or libxl for reading support.");
  return {};
}

std::vector<json> ExcelEngine::parseXLS(const std::vector<uint8_t> &fileData) {
  // XLS is a binary format requiring specialized library
  Logger::warning(LogCategory::DATABASE, "ExcelEngine",
                  "XLS parsing requires Excel reading library (e.g., libxl) - returning empty data");
  return {};
}

std::vector<json> ExcelEngine::parseExcel() {
  std::vector<uint8_t> fileData = readFromFile(source_);

  if (isXLSX(source_)) {
    return parseXLSX(fileData);
  } else if (isXLS(source_)) {
    return parseXLS(fileData);
  } else {
    throw std::runtime_error("Unknown Excel file format: " + source_);
  }
}

std::vector<std::string> ExcelEngine::detectColumns(const std::vector<json> &data) {
  if (data.empty()) {
    return {};
  }

  std::vector<std::string> columns;
  for (const auto &item : data[0].items()) {
    columns.push_back(item.key());
  }
  return columns;
}

std::vector<std::string> ExcelEngine::getAvailableSheets() {
  std::vector<uint8_t> fileData = readFromFile(source_);
  return getSheetNames(fileData);
}

void ExcelEngine::setConfig(const ExcelConfig &config) {
  config_ = config;
}
