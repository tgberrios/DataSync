#ifndef COLUMNAR_STORAGE_H
#define COLUMNAR_STORAGE_H

#include "third_party/json.hpp"
#include "core/logger.h"
#include <string>
#include <vector>
#include <memory>
#include <map>
#include <cstddef>

using json = nlohmann::json;

// ColumnarStorage: Almacenamiento de datos en formato columnar
class ColumnarStorage {
public:
  struct ColumnMetadata {
    std::string name;
    std::string type;
    size_t offset{0};
    size_t length{0};
    size_t nullCount{0};
    json minValue;
    json maxValue;
  };

  struct StorageMetadata {
    size_t rowCount{0};
    size_t columnCount{0};
    std::vector<ColumnMetadata> columns;
    std::string compressionAlgorithm;
    size_t uncompressedSize{0};
    size_t compressedSize{0};
  };

  explicit ColumnarStorage(const std::string& filePath);
  ~ColumnarStorage() = default;

  // Escribir datos en formato columnar
  bool write(const std::vector<json>& rows, const std::vector<std::string>& columnNames);

  // Leer datos completos
  std::vector<json> readAll();

  // Leer solo columnas específicas
  std::vector<json> readColumns(const std::vector<std::string>& columnNames);

  // Obtener metadata
  StorageMetadata getMetadata() const;

  // Verificar si el archivo existe
  bool exists() const;

private:
  std::string filePath_;
  StorageMetadata metadata_;

  // Helper methods
  void serializeMetadata();
  void deserializeMetadata();
  std::string getMetadataPath() const;
  std::string getDataPath() const;
};

#endif // COLUMNAR_STORAGE_H
