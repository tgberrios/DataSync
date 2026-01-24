#ifndef RESULT_CACHE_H
#define RESULT_CACHE_H

#include "utils/CacheManager.h"
#include "third_party/json.hpp"
#include <string>
#include <memory>
#include <chrono>
#include <functional>

using json = nlohmann::json;

// ResultCache: Cache específico para resultados de queries con invalidación inteligente
class ResultCache {
public:
  struct QueryKey {
    std::string connectionString;
    std::string dbEngine;
    std::string schema;
    std::string table;
    std::string query;
    std::string queryHash;  // Hash del query para comparación rápida

    bool operator==(const QueryKey& other) const {
      return queryHash == other.queryHash &&
             connectionString == other.connectionString &&
             dbEngine == other.dbEngine &&
             schema == other.schema &&
             table == other.table;
    }
  };

  struct CacheConfig {
    size_t maxSize;
    std::chrono::seconds defaultTTL;
    bool enableInvalidation;
    std::chrono::seconds invalidationCheckInterval;
    
    CacheConfig() : maxSize(500), defaultTTL(1800), enableInvalidation(true), invalidationCheckInterval(60) {}
  };

  explicit ResultCache(const CacheConfig& config = CacheConfig());
  ~ResultCache() = default;

  // Obtener resultado cacheado
  std::optional<json> get(const QueryKey& key);

  // Guardar resultado en cache
  void put(const QueryKey& key, const json& result, 
           std::optional<std::chrono::seconds> ttl = std::nullopt);

  // Invalidar cache para una tabla específica
  void invalidateTable(const std::string& connectionString,
                       const std::string& dbEngine,
                       const std::string& schema,
                       const std::string& table);

  // Invalidar todo el cache
  void clear();

  // Obtener estadísticas
  CacheManager::CacheStats getStats() const;

  // Generar hash de query para usar como key
  static std::string generateQueryHash(const std::string& query);

  // Crear QueryKey desde parámetros
  static QueryKey createKey(const std::string& connectionString,
                           const std::string& dbEngine,
                           const std::string& schema,
                           const std::string& table,
                           const std::string& query);

private:
  std::unique_ptr<CacheManager> cacheManager_;
  CacheConfig config_;

  // Mapa de tablas a keys de cache (para invalidación)
  std::unordered_map<std::string, std::vector<std::string>> tableToKeys_;

  // Generar key string desde QueryKey
  std::string keyToString(const QueryKey& key) const;

  // Extraer información de tabla desde key string
  std::string extractTableKey(const QueryKey& key) const;
};

// Hash function para QueryKey (para usar en unordered_map)
namespace std {
  template<>
  struct hash<ResultCache::QueryKey> {
    size_t operator()(const ResultCache::QueryKey& key) const {
      return std::hash<std::string>()(key.queryHash + key.connectionString + 
                                      key.dbEngine + key.schema + key.table);
    }
  };
}

#endif // RESULT_CACHE_H
