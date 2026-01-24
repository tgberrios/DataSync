#include "utils/DataCompressor.h"
#include <zlib.h>
#include <cstring>
#include <algorithm>

// LZ4 y Snappy pueden no estar disponibles, usar stubs si no están
#ifdef HAVE_LZ4
#include <lz4.h>
#include <lz4hc.h>
#endif

#ifdef HAVE_SNAPPY
#include <snappy.h>
#endif

DataCompressor::CompressionResult DataCompressor::compress(
    const void* data,
    size_t size,
    CompressionAlgorithm algorithm) {
  
  switch (algorithm) {
    case CompressionAlgorithm::GZIP:
      return compressGZIP(data, size);
    case CompressionAlgorithm::LZ4:
      return compressLZ4(data, size);
    case CompressionAlgorithm::SNAPPY:
      return compressSnappy(data, size);
    case CompressionAlgorithm::NONE: {
      CompressionResult result;
      result.success = true;
      result.algorithm = CompressionAlgorithm::NONE;
      result.originalSize = size;
      result.compressedSize = size;
      result.compressionRatio = 1.0;
      result.compressedData.assign(static_cast<const uint8_t*>(data),
                                    static_cast<const uint8_t*>(data) + size);
      return result;
    }
  }

  CompressionResult result;
  result.success = false;
  result.errorMessage = "Unknown compression algorithm";
  return result;
}

DataCompressor::CompressionResult DataCompressor::compressString(
    const std::string& data,
    CompressionAlgorithm algorithm) {
  
  return compress(data.data(), data.size(), algorithm);
}

DataCompressor::DecompressionResult DataCompressor::decompress(
    const void* compressedData,
    size_t compressedSize,
    CompressionAlgorithm algorithm) {
  
  switch (algorithm) {
    case CompressionAlgorithm::GZIP:
      return decompressGZIP(compressedData, compressedSize);
    case CompressionAlgorithm::LZ4:
      return decompressLZ4(compressedData, compressedSize);
    case CompressionAlgorithm::SNAPPY:
      return decompressSnappy(compressedData, compressedSize);
    case CompressionAlgorithm::NONE: {
      DecompressionResult result;
      result.success = true;
      result.originalSize = compressedSize;
      result.decompressedSize = compressedSize;
      result.decompressedData.assign(static_cast<const uint8_t*>(compressedData),
                                      static_cast<const uint8_t*>(compressedData) + compressedSize);
      return result;
    }
  }

  DecompressionResult result;
  result.success = false;
  result.errorMessage = "Unknown compression algorithm";
  return result;
}

DataCompressor::DecompressionResult DataCompressor::decompressToString(
    const void* compressedData,
    size_t compressedSize,
    CompressionAlgorithm algorithm) {
  
  return decompress(compressedData, compressedSize, algorithm);
}

DataCompressor::CompressionAlgorithm DataCompressor::detectAlgorithm(
    const void* data,
    size_t size) {
  
  if (size < 2) {
    return CompressionAlgorithm::NONE;
  }

  const uint8_t* bytes = static_cast<const uint8_t*>(data);

  // GZIP magic number: 1F 8B
  if (size >= 2 && bytes[0] == 0x1F && bytes[1] == 0x8B) {
    return CompressionAlgorithm::GZIP;
  }

  // LZ4 magic number: 04 22 4D 18
  if (size >= 4 && bytes[0] == 0x04 && bytes[1] == 0x22 &&
      bytes[2] == 0x4D && bytes[3] == 0x18) {
    return CompressionAlgorithm::LZ4;
  }

  // Snappy: formato más complejo, verificar header
  // (simplificado - en implementación real, verificar formato completo)

  return CompressionAlgorithm::NONE;
}

bool DataCompressor::isAlgorithmAvailable(CompressionAlgorithm algorithm) {
  switch (algorithm) {
    case CompressionAlgorithm::GZIP:
      return true;  // zlib siempre disponible
    case CompressionAlgorithm::LZ4:
#ifdef HAVE_LZ4
      return true;
#else
      return false;
#endif
    case CompressionAlgorithm::SNAPPY:
#ifdef HAVE_SNAPPY
      return true;
#else
      return false;
#endif
    case CompressionAlgorithm::NONE:
      return true;
  }
  return false;
}

DataCompressor::CompressionAlgorithm DataCompressor::getRecommendedAlgorithm(
    size_t dataSize,
    bool prioritizeSpeed) {
  
  if (dataSize < 1024) {
    // Datos muy pequeños, no comprimir
    return CompressionAlgorithm::NONE;
  }

  if (prioritizeSpeed) {
    // Priorizar velocidad: LZ4 o Snappy
    if (isAlgorithmAvailable(CompressionAlgorithm::LZ4)) {
      return CompressionAlgorithm::LZ4;
    }
    if (isAlgorithmAvailable(CompressionAlgorithm::SNAPPY)) {
      return CompressionAlgorithm::SNAPPY;
    }
  }

  // Priorizar ratio: GZIP
  return CompressionAlgorithm::GZIP;
}

DataCompressor::CompressionResult DataCompressor::compressGZIP(
    const void* data,
    size_t size) {
  
  CompressionResult result;
  result.algorithm = CompressionAlgorithm::GZIP;
  result.originalSize = size;

  // Estimar tamaño comprimido (típicamente 50-70% del original)
  uLongf compressedSize = compressBound(static_cast<uLong>(size));
  result.compressedData.resize(compressedSize);

  int ret = compress2(
      result.compressedData.data(),
      &compressedSize,
      static_cast<const Bytef*>(data),
      static_cast<uLong>(size),
      Z_DEFAULT_COMPRESSION
  );

  if (ret == Z_OK) {
    result.compressedData.resize(compressedSize);
    result.compressedSize = compressedSize;
    result.compressionRatio = static_cast<double>(compressedSize) / static_cast<double>(size);
    result.success = true;
  } else {
    result.success = false;
    result.errorMessage = "GZIP compression failed: " + std::to_string(ret);
  }

  return result;
}

DataCompressor::CompressionResult DataCompressor::compressLZ4(
    const void* data,
    size_t size) {
  
  CompressionResult result;
  result.algorithm = CompressionAlgorithm::LZ4;
  result.originalSize = size;

#ifdef HAVE_LZ4
  int maxCompressedSize = LZ4_compressBound(static_cast<int>(size));
  result.compressedData.resize(maxCompressedSize);

  int compressedSize = LZ4_compress_default(
      static_cast<const char*>(data),
      reinterpret_cast<char*>(result.compressedData.data()),
      static_cast<int>(size),
      maxCompressedSize
  );

  if (compressedSize > 0) {
    result.compressedData.resize(compressedSize);
    result.compressedSize = compressedSize;
    result.compressionRatio = static_cast<double>(compressedSize) / static_cast<double>(size);
    result.success = true;
  } else {
    result.success = false;
    result.errorMessage = "LZ4 compression failed";
  }
#else
  result.success = false;
  result.errorMessage = "LZ4 not available";
#endif

  return result;
}

DataCompressor::CompressionResult DataCompressor::compressSnappy(
    const void* data,
    size_t size) {
  
  CompressionResult result;
  result.algorithm = CompressionAlgorithm::SNAPPY;
  result.originalSize = size;

#ifdef HAVE_SNAPPY
  size_t maxCompressedSize = snappy::MaxCompressedLength(size);
  result.compressedData.resize(maxCompressedSize);

  size_t compressedSize;
  snappy::RawCompress(
      static_cast<const char*>(data),
      size,
      reinterpret_cast<char*>(result.compressedData.data()),
      &compressedSize
  );

  result.compressedData.resize(compressedSize);
  result.compressedSize = compressedSize;
  result.compressionRatio = static_cast<double>(compressedSize) / static_cast<double>(size);
  result.success = true;
#else
  result.success = false;
  result.errorMessage = "Snappy not available";
#endif

  return result;
}

DataCompressor::DecompressionResult DataCompressor::decompressGZIP(
    const void* data,
    size_t size) {
  
  DecompressionResult result;
  result.originalSize = size;

  // Estimar tamaño descomprimido (empezar con 4x el tamaño comprimido)
  uLongf decompressedSize = size * 4;
  result.decompressedData.resize(decompressedSize);

  int ret = uncompress(
      result.decompressedData.data(),
      &decompressedSize,
      static_cast<const Bytef*>(data),
      static_cast<uLong>(size)
  );

  if (ret == Z_OK) {
    result.decompressedData.resize(decompressedSize);
    result.decompressedSize = decompressedSize;
    result.success = true;
  } else if (ret == Z_BUF_ERROR) {
    // Buffer muy pequeño, intentar con tamaño mayor
    decompressedSize = size * 10;
    result.decompressedData.resize(decompressedSize);
    ret = uncompress(
        result.decompressedData.data(),
        &decompressedSize,
        static_cast<const Bytef*>(data),
        static_cast<uLong>(size)
    );
    if (ret == Z_OK) {
      result.decompressedData.resize(decompressedSize);
      result.decompressedSize = decompressedSize;
      result.success = true;
    } else {
      result.success = false;
      result.errorMessage = "GZIP decompression failed: " + std::to_string(ret);
    }
  } else {
    result.success = false;
    result.errorMessage = "GZIP decompression failed: " + std::to_string(ret);
  }

  return result;
}

DataCompressor::DecompressionResult DataCompressor::decompressLZ4(
    const void* data,
    size_t size) {
  
  DecompressionResult result;
  result.originalSize = size;

#ifdef HAVE_LZ4
  // LZ4 requiere conocer el tamaño descomprimido
  // En implementación real, esto debería estar en el header
  // Por ahora, usar un tamaño estimado
  size_t decompressedSize = size * 4;  // Estimación
  result.decompressedData.resize(decompressedSize);

  int actualSize = LZ4_decompress_safe(
      static_cast<const char*>(data),
      reinterpret_cast<char*>(result.decompressedData.data()),
      static_cast<int>(size),
      static_cast<int>(decompressedSize)
  );

  if (actualSize > 0) {
    result.decompressedData.resize(actualSize);
    result.decompressedSize = actualSize;
    result.success = true;
  } else {
    result.success = false;
    result.errorMessage = "LZ4 decompression failed";
  }
#else
  result.success = false;
  result.errorMessage = "LZ4 not available";
#endif

  return result;
}

DataCompressor::DecompressionResult DataCompressor::decompressSnappy(
    const void* data,
    size_t size) {
  
  DecompressionResult result;
  result.originalSize = size;

#ifdef HAVE_SNAPPY
  size_t decompressedSize;
  if (!snappy::GetUncompressedLength(static_cast<const char*>(data), size, &decompressedSize)) {
    result.success = false;
    result.errorMessage = "Snappy: Failed to get uncompressed length";
    return result;
  }

  result.decompressedData.resize(decompressedSize);

  if (!snappy::RawUncompress(static_cast<const char*>(data), size,
                              reinterpret_cast<char*>(result.decompressedData.data()))) {
    result.success = false;
    result.errorMessage = "Snappy decompression failed";
    return result;
  }

  result.decompressedSize = decompressedSize;
  result.success = true;
#else
  result.success = false;
  result.errorMessage = "Snappy not available";
#endif

  return result;
}
