#ifndef COLUMNAR_READER_H
#define COLUMNAR_READER_H

#include "storage/ColumnarStorage.h"
#include "third_party/json.hpp"
#include <string>
#include <vector>
#include <memory>
#include <fstream>

using json = nlohmann::json;

// ColumnarReader: Lector eficiente de datos columnares
class ColumnarReader {
public:
  explicit ColumnarReader(const std::string& filePath);
  ~ColumnarReader();

  // Abrir archivo columnar
  bool open();

  // Cerrar archivo
  void close();

  // Leer todas las filas
  std::vector<json> readAll();

  // Leer solo columnas específicas (más eficiente)
  std::vector<json> readColumns(const std::vector<std::string>& columnNames);

  // Leer rango de filas
  std::vector<json> readRange(size_t startRow, size_t endRow,
                               const std::vector<std::string>& columnNames = {});

  // Obtener metadata
  ColumnarStorage::StorageMetadata getMetadata() const;

  // Verificar si está abierto
  bool isOpen() const { return isOpen_; }

private:
  std::string filePath_;
  bool isOpen_{false};
  ColumnarStorage::StorageMetadata metadata_;
  std::ifstream dataFile_;

  // Helper methods
  bool loadMetadata();
  json readRow(size_t rowIndex, const std::vector<std::string>& columnNames);
  std::vector<uint8_t> readColumnData(const std::string& columnName, size_t startRow, size_t endRow);
};

#endif // COLUMNAR_READER_H
