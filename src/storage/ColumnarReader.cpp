#include "storage/ColumnarReader.h"
#include <filesystem>
#include <sstream>

namespace fs = std::filesystem;

ColumnarReader::ColumnarReader(const std::string& filePath)
    : filePath_(filePath) {
}

ColumnarReader::~ColumnarReader() {
  close();
}

bool ColumnarReader::open() {
  if (isOpen_) {
    return true;
  }

  if (!loadMetadata()) {
    return false;
  }

  std::string dataPath = filePath_ + ".data";
  dataFile_.open(dataPath, std::ios::binary);
  
  if (!dataFile_.is_open()) {
    Logger::error(LogCategory::SYSTEM, "ColumnarReader",
                  "Failed to open data file: " + dataPath);
    return false;
  }

  isOpen_ = true;
  return true;
}

void ColumnarReader::close() {
  if (dataFile_.is_open()) {
    dataFile_.close();
  }
  isOpen_ = false;
}

std::vector<json> ColumnarReader::readAll() {
  if (!isOpen_) {
    return {};
  }

  std::vector<json> rows;
  rows.reserve(metadata_.rowCount);

  for (size_t i = 0; i < metadata_.rowCount; ++i) {
    json row = readRow(i, {});
    if (!row.is_null()) {
      rows.push_back(row);
    }
  }

  return rows;
}

std::vector<json> ColumnarReader::readColumns(const std::vector<std::string>& columnNames) {
  if (!isOpen_) {
    return {};
  }

  std::vector<json> rows;
  rows.reserve(metadata_.rowCount);

  for (size_t i = 0; i < metadata_.rowCount; ++i) {
    json row = readRow(i, columnNames);
    if (!row.is_null()) {
      rows.push_back(row);
    }
  }

  return rows;
}

std::vector<json> ColumnarReader::readRange(size_t startRow, size_t endRow,
                                             const std::vector<std::string>& columnNames) {
  if (!isOpen_) {
    return {};
  }

  if (startRow >= metadata_.rowCount) {
    return {};
  }

  if (endRow > metadata_.rowCount) {
    endRow = metadata_.rowCount;
  }

  std::vector<json> rows;
  rows.reserve(endRow - startRow);

  for (size_t i = startRow; i < endRow; ++i) {
    json row = readRow(i, columnNames);
    if (!row.is_null()) {
      rows.push_back(row);
    }
  }

  return rows;
}

ColumnarStorage::StorageMetadata ColumnarReader::getMetadata() const {
  return metadata_;
}

bool ColumnarReader::loadMetadata() {
  std::string metaPath = filePath_ + ".meta";
  
  if (!fs::exists(metaPath)) {
    Logger::error(LogCategory::SYSTEM, "ColumnarReader",
                  "Metadata file not found: " + metaPath);
    return false;
  }

  std::ifstream file(metaPath);
  if (!file.is_open()) {
    Logger::error(LogCategory::SYSTEM, "ColumnarReader",
                  "Failed to open metadata file: " + metaPath);
    return false;
  }

  json meta = json::parse(file);
  file.close();

  metadata_.rowCount = meta.value("row_count", 0);
  metadata_.columnCount = meta.value("column_count", 0);
  metadata_.compressionAlgorithm = meta.value("compression_algorithm", "");
  metadata_.uncompressedSize = meta.value("uncompressed_size", 0);
  metadata_.compressedSize = meta.value("compressed_size", 0);

  if (meta.contains("columns")) {
    metadata_.columns.clear();
    for (const auto& colJson : meta["columns"]) {
      ColumnarStorage::ColumnMetadata col;
      col.name = colJson.value("name", "");
      col.type = colJson.value("type", "");
      col.offset = colJson.value("offset", 0);
      col.length = colJson.value("length", 0);
      col.nullCount = colJson.value("null_count", 0);
      if (colJson.contains("min_value")) {
        col.minValue = colJson["min_value"];
      }
      if (colJson.contains("max_value")) {
        col.maxValue = colJson["max_value"];
      }
      metadata_.columns.push_back(col);
    }
  }

  return true;
}

json ColumnarReader::readRow(size_t rowIndex, const std::vector<std::string>& columnNames) {
  json row;

  std::vector<std::string> colsToRead = columnNames.empty() ? 
    [this]() {
      std::vector<std::string> all;
      for (const auto& col : metadata_.columns) {
        all.push_back(col.name);
      }
      return all;
    }() : columnNames;

  for (const auto& colName : colsToRead) {
    // Buscar metadata de columna
    auto it = std::find_if(metadata_.columns.begin(), metadata_.columns.end(),
                          [&colName](const ColumnarStorage::ColumnMetadata& col) {
                            return col.name == colName;
                          });

    if (it == metadata_.columns.end()) {
      continue;
    }

    // Leer datos de columna (simplificado - en implementación real,
    // leería desde el archivo de columna específico)
    // Por ahora, placeholder
    row[colName] = json(nullptr);
  }

  return row;
}

std::vector<uint8_t> ColumnarReader::readColumnData(const std::string& columnName,
                                                      size_t startRow,
                                                      size_t endRow) {
  // Placeholder - en implementación real, leería datos comprimidos
  // y los descomprimiría
  return {};
}
