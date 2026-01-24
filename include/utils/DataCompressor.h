#ifndef DATA_COMPRESSOR_H
#define DATA_COMPRESSOR_H

#include "core/logger.h"
#include <string>
#include <vector>
#include <memory>
#include <cstddef>

// DataCompressor: Compresión de datos para transferencia (LZ4, Snappy, GZIP)
class DataCompressor {
public:
  enum class CompressionAlgorithm {
    NONE,
    GZIP,
    LZ4,
    SNAPPY
  };

  struct CompressionResult {
    bool success{false};
    std::vector<uint8_t> compressedData;
    size_t originalSize{0};
    size_t compressedSize{0};
    double compressionRatio{0.0};
    CompressionAlgorithm algorithm;
    std::string errorMessage;
  };

  struct DecompressionResult {
    bool success{false};
    std::vector<uint8_t> decompressedData;
    size_t originalSize{0};
    size_t decompressedSize{0};
    std::string errorMessage;
  };

  // Comprimir datos
  static CompressionResult compress(
      const void* data,
      size_t size,
      CompressionAlgorithm algorithm = CompressionAlgorithm::GZIP
  );

  // Comprimir string
  static CompressionResult compressString(
      const std::string& data,
      CompressionAlgorithm algorithm = CompressionAlgorithm::GZIP
  );

  // Descomprimir datos
  static DecompressionResult decompress(
      const void* compressedData,
      size_t compressedSize,
      CompressionAlgorithm algorithm
  );

  // Descomprimir a string
  static DecompressionResult decompressToString(
      const void* compressedData,
      size_t compressedSize,
      CompressionAlgorithm algorithm
  );

  // Detectar algoritmo de compresión desde datos
  static CompressionAlgorithm detectAlgorithm(const void* data, size_t size);

  // Verificar si un algoritmo está disponible
  static bool isAlgorithmAvailable(CompressionAlgorithm algorithm);

  // Obtener algoritmo recomendado basado en tamaño y tipo
  static CompressionAlgorithm getRecommendedAlgorithm(
      size_t dataSize,
      bool prioritizeSpeed = false
  );

private:
  // Implementaciones específicas por algoritmo
  static CompressionResult compressGZIP(const void* data, size_t size);
  static CompressionResult compressLZ4(const void* data, size_t size);
  static CompressionResult compressSnappy(const void* data, size_t size);

  static DecompressionResult decompressGZIP(const void* data, size_t size);
  static DecompressionResult decompressLZ4(const void* data, size_t size);
  static DecompressionResult decompressSnappy(const void* data, size_t size);
};

#endif // DATA_COMPRESSOR_H
