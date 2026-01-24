#include "storage/ColumnarStorage.h"
#include "storage/ColumnarWriter.h"
#include "storage/ColumnarReader.h"
#include <fstream>
#include <filesystem>

namespace fs = std::filesystem;

ColumnarStorage::ColumnarStorage(const std::string& filePath)
    : filePath_(filePath) {
}

bool ColumnarStorage::write(const std::vector<json>& rows,
                             const std::vector<std::string>& columnNames) {
  ColumnarWriter::WriteConfig config;
  ColumnarWriter writer(filePath_, config);
  
  if (!writer.open(columnNames)) {
    return false;
  }

  if (!writer.writeRows(rows)) {
    writer.close();
    return false;
  }

  if (!writer.finalize()) {
    writer.close();
    return false;
  }

  writer.close();
  
  // Cargar metadata
  deserializeMetadata();
  
  return true;
}

std::vector<json> ColumnarStorage::readAll() {
  ColumnarReader reader(filePath_);
  if (!reader.open()) {
    return {};
  }
  return reader.readAll();
}

std::vector<json> ColumnarStorage::readColumns(const std::vector<std::string>& columnNames) {
  ColumnarReader reader(filePath_);
  if (!reader.open()) {
    return {};
  }
  return reader.readColumns(columnNames);
}

ColumnarStorage::StorageMetadata ColumnarStorage::getMetadata() const {
  return metadata_;
}

bool ColumnarStorage::exists() const {
  return fs::exists(filePath_ + ".meta");
}

void ColumnarStorage::serializeMetadata() {
  std::string metaPath = getMetadataPath();
  std::ofstream file(metaPath, std::ios::binary);
  
  if (!file.is_open()) {
    Logger::error(LogCategory::SYSTEM, "ColumnarStorage",
                  "Failed to open metadata file: " + metaPath);
    return;
  }

  json meta;
  meta["row_count"] = metadata_.rowCount;
  meta["column_count"] = metadata_.columnCount;
  meta["compression_algorithm"] = metadata_.compressionAlgorithm;
  meta["uncompressed_size"] = metadata_.uncompressedSize;
  meta["compressed_size"] = metadata_.compressedSize;

  json columnsJson = json::array();
  for (const auto& col : metadata_.columns) {
    json colJson;
    colJson["name"] = col.name;
    colJson["type"] = col.type;
    colJson["offset"] = col.offset;
    colJson["length"] = col.length;
    colJson["null_count"] = col.nullCount;
    if (!col.minValue.is_null()) {
      colJson["min_value"] = col.minValue;
    }
    if (!col.maxValue.is_null()) {
      colJson["max_value"] = col.maxValue;
    }
    columnsJson.push_back(colJson);
  }
  meta["columns"] = columnsJson;

  file << meta.dump();
  file.close();
}

void ColumnarStorage::deserializeMetadata() {
  std::string metaPath = getMetadataPath();
  
  if (!fs::exists(metaPath)) {
    return;
  }

  std::ifstream file(metaPath);
  if (!file.is_open()) {
    return;
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
      ColumnMetadata col;
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
}

std::string ColumnarStorage::getMetadataPath() const {
  return filePath_ + ".meta";
}

std::string ColumnarStorage::getDataPath() const {
  return filePath_ + ".data";
}
