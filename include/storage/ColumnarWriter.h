#ifndef COLUMNAR_WRITER_H
#define COLUMNAR_WRITER_H

#include "storage/ColumnarStorage.h"
#include "utils/DataCompressor.h"
#include "third_party/json.hpp"
#include <string>
#include <vector>
#include <memory>
#include <fstream>
#include <map>

using json = nlohmann::json;

// ColumnarWriter: Escritor eficiente de datos columnares
class ColumnarWriter {
public:
  struct WriteConfig {
    DataCompressor::CompressionAlgorithm compression;
    bool compressPerColumn;
    size_t batchSize;
    
    WriteConfig() : compression(DataCompressor::CompressionAlgorithm::GZIP), 
                    compressPerColumn(true), 
                    batchSize(10000) {}
  };

  explicit ColumnarWriter(const std::string& filePath, const WriteConfig& config = WriteConfig());
  ~ColumnarWriter();

  // Abrir archivo para escritura
  bool open(const std::vector<std::string>& columnNames);

  // Cerrar y finalizar escritura
  void close();

  // Escribir filas
  bool writeRows(const std::vector<json>& rows);

  // Finalizar escritura y escribir metadata
  bool finalize();

  // Verificar si está abierto
  bool isOpen() const { return isOpen_; }

private:
  std::string filePath_;
  WriteConfig config_;
  bool isOpen_{false};
  std::vector<std::string> columnNames_;
  ColumnarStorage::StorageMetadata metadata_;
  std::map<std::string, std::ofstream> columnFiles_;
  std::vector<json> batchBuffer_;

  // Helper methods
  void flushBatch();
  std::string getColumnFilePath(const std::string& columnName) const;
  bool writeColumnData(const std::string& columnName, const std::vector<json>& rows);
  void updateColumnMetadata(const std::string& columnName, const std::vector<json>& rows);
  void serializeMetadata();
};

#endif // COLUMNAR_WRITER_H
