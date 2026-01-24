#ifndef CACHE_MANAGER_H
#define CACHE_MANAGER_H

#include "third_party/json.hpp"
#include "core/logger.h"
#include <string>
#include <unordered_map>
#include <list>
#include <mutex>
#include <chrono>
#include <memory>
#include <functional>
#include <optional>
#include <vector>

using json = nlohmann::json;

// CacheManager: Sistema centralizado de cache con LRU y TTL
class CacheManager {
public:
  struct CacheEntry {
    std::string key;
    json value;
    std::chrono::system_clock::time_point createdAt;
    std::chrono::system_clock::time_point expiresAt;
    size_t accessCount{0};
    std::chrono::system_clock::time_point lastAccessed;
  };

  struct CacheStats {
    size_t hits{0};
    size_t misses{0};
    size_t evictions{0};
    size_t currentSize{0};
    size_t maxSize{0};
    double hitRate{0.0};
  };

  explicit CacheManager(size_t maxSize = 1000, 
                       std::chrono::seconds defaultTTL = std::chrono::seconds(3600));
  ~CacheManager() = default;

  // Obtener valor del cache
  std::optional<json> get(const std::string& key);

  // Guardar valor en cache
  void put(const std::string& key, const json& value, 
           std::optional<std::chrono::seconds> ttl = std::nullopt);

  // Verificar si existe una key
  bool exists(const std::string& key);

  // Eliminar una key del cache
  bool remove(const std::string& key);

  // Limpiar todo el cache
  void clear();

  // Obtener estadísticas
  CacheStats getStats() const;

  // Configurar tamaño máximo
  void setMaxSize(size_t maxSize);

  // Configurar TTL por defecto
  void setDefaultTTL(std::chrono::seconds ttl);

  // Limpiar entradas expiradas
  size_t cleanupExpired();

  // Obtener todas las keys
  std::vector<std::string> getKeys() const;

  // Obtener tamaño actual del cache
  size_t size() const;

private:
  mutable std::mutex mutex_;
  size_t maxSize_;
  std::chrono::seconds defaultTTL_;

  // LRU: lista doblemente enlazada para orden de acceso
  std::list<std::string> accessOrder_;
  
  // Mapa de keys a entradas y posición en LRU
  struct EntryNode {
    CacheEntry entry;
    std::list<std::string>::iterator lruIterator;
  };
  std::unordered_map<std::string, EntryNode> cache_;

  // Estadísticas
  mutable CacheStats stats_;

  // Helper methods
  void touch(const std::string& key);
  void evictLRU();
  bool isExpired(const CacheEntry& entry) const;
  void updateStats(bool hit);
};

#endif // CACHE_MANAGER_H
