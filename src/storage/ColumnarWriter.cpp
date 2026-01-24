#include "storage/ColumnarWriter.h"
#include "utils/DataCompressor.h"
#include <filesystem>
#include <algorithm>
#include <sstream>

ColumnarWriter::ColumnarWriter(const std::string& filePath, const WriteConfig& config)
    : filePath_(filePath), config_(config) {
}

ColumnarWriter::~ColumnarWriter() {
  if (isOpen_) {
    close();
  }
}

bool ColumnarWriter::open(const std::vector<std::string>& columnNames) {
  if (isOpen_) {
    return true;
  }

  columnNames_ = columnNames;
  metadata_.columnCount = columnNames.size();

    // Crear archivos para cada columna
    for (const auto& colName : columnNames) {
      std::string colPath = getColumnFilePath(colName);
      std::filesystem::path colPathObj(colPath);
      std::filesystem::create_directories(colPathObj.parent_path());
    
    columnFiles_[colName].open(colPath, std::ios::binary);
    if (!columnFiles_[colName].is_open()) {
      Logger::error(LogCategory::SYSTEM, "ColumnarWriter",
                    "Failed to open column file: " + colPath);
      close();
      return false;
    }

    // Inicializar metadata de columna
    ColumnarStorage::ColumnMetadata colMeta;
    colMeta.name = colName;
    colMeta.type = "unknown";  // Se detectará al escribir
    metadata_.columns.push_back(colMeta);
  }

  isOpen_ = true;
  return true;
}

void ColumnarWriter::close() {
  if (!isOpen_) {
    return;
  }

  flushBatch();

  // Cerrar todos los archivos de columna
  for (auto& [colName, file] : columnFiles_) {
    if (file.is_open()) {
      file.close();
    }
  }

  columnFiles_.clear();
  isOpen_ = false;
}

bool ColumnarWriter::writeRows(const std::vector<json>& rows) {
  if (!isOpen_) {
    return false;
  }

  for (const auto& row : rows) {
    batchBuffer_.push_back(row);

    if (batchBuffer_.size() >= config_.batchSize) {
      flushBatch();
    }
  }

  metadata_.rowCount += rows.size();
  return true;
}

bool ColumnarWriter::finalize() {
  if (!isOpen_) {
    return false;
  }

  flushBatch();

  // Actualizar metadata de todas las columnas
  for (const auto& colName : columnNames_) {
    // En implementación real, se calcularían estadísticas reales
    updateColumnMetadata(colName, batchBuffer_);
  }

  serializeMetadata();
  return true;
}

void ColumnarWriter::flushBatch() {
  if (batchBuffer_.empty()) {
    return;
  }

  // Escribir datos por columna
  for (const auto& colName : columnNames_) {
    if (!writeColumnData(colName, batchBuffer_)) {
      Logger::error(LogCategory::SYSTEM, "ColumnarWriter",
                    "Failed to write column data: " + colName);
    }
  }

  batchBuffer_.clear();
}

std::string ColumnarWriter::getColumnFilePath(const std::string& columnName) const {
  return filePath_ + "_" + columnName + ".col";
}

bool ColumnarWriter::writeColumnData(const std::string& columnName,
                                      const std::vector<json>& rows) {
  auto it = columnFiles_.find(columnName);
  if (it == columnFiles_.end() || !it->second.is_open()) {
    return false;
  }

  // Extraer valores de la columna
  std::vector<uint8_t> columnData;
  for (const auto& row : rows) {
    if (row.contains(columnName)) {
      std::string value = row[columnName].dump();
      columnData.insert(columnData.end(), value.begin(), value.end());
      columnData.push_back('\n');  // Separador
    } else {
      // NULL value
      columnData.push_back(0);
    }
  }

  // Comprimir si está configurado
  if (config_.compressPerColumn && config_.compression != DataCompressor::CompressionAlgorithm::NONE) {
    auto compressResult = DataCompressor::compress(
        columnData.data(),
        columnData.size(),
        config_.compression
    );

    if (compressResult.success) {
      it->second.write(reinterpret_cast<const char*>(compressResult.compressedData.data()),
                       compressResult.compressedData.size());
      
      // Actualizar metadata de compresión
      metadata_.compressedSize += compressResult.compressedSize;
      metadata_.uncompressedSize += compressResult.originalSize;
      metadata_.compressionAlgorithm = 
          config_.compression == DataCompressor::CompressionAlgorithm::GZIP ? "GZIP" :
          config_.compression == DataCompressor::CompressionAlgorithm::LZ4 ? "LZ4" :
          config_.compression == DataCompressor::CompressionAlgorithm::SNAPPY ? "SNAPPY" : "NONE";
    } else {
      // Fallback: escribir sin comprimir
      it->second.write(reinterpret_cast<const char*>(columnData.data()), columnData.size());
      metadata_.uncompressedSize += columnData.size();
    }
  } else {
    it->second.write(reinterpret_cast<const char*>(columnData.data()), columnData.size());
    metadata_.uncompressedSize += columnData.size();
  }

  return true;
}

void ColumnarWriter::updateColumnMetadata(const std::string& columnName,
                                           const std::vector<json>& rows) {
  auto it = std::find_if(metadata_.columns.begin(), metadata_.columns.end(),
                        [&columnName](const ColumnarStorage::ColumnMetadata& col) {
                          return col.name == columnName;
                        });

  if (it == metadata_.columns.end()) {
    return;
  }

  // Calcular estadísticas básicas
  size_t nullCount = 0;
  json minValue = json(nullptr);
  json maxValue = json(nullptr);

  for (const auto& row : rows) {
    if (!row.contains(columnName) || row[columnName].is_null()) {
      nullCount++;
      continue;
    }

    json value = row[columnName];
    
    if (minValue.is_null() || value < minValue) {
      minValue = value;
    }
    if (maxValue.is_null() || value > maxValue) {
      maxValue = value;
    }
  }

  it->nullCount += nullCount;
  if (!minValue.is_null()) {
    if (it->minValue.is_null() || minValue < it->minValue) {
      it->minValue = minValue;
    }
  }
  if (!maxValue.is_null()) {
    if (it->maxValue.is_null() || maxValue > it->maxValue) {
      it->maxValue = maxValue;
    }
  }
}

void ColumnarWriter::serializeMetadata() {
  std::string metaPath = filePath_ + ".meta";
  std::ofstream file(metaPath, std::ios::binary);
  
  if (!file.is_open()) {
    Logger::error(LogCategory::SYSTEM, "ColumnarWriter",
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
