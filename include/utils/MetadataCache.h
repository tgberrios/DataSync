#ifndef METADATA_CACHE_H
#define METADATA_CACHE_H

#include "utils/CacheManager.h"
#include "third_party/json.hpp"
#include <string>
#include <memory>
#include <chrono>
#include <vector>
#include <map>

using json = nlohmann::json;

// MetadataCache: Cache para metadata de bases de datos (schemas, columnas, tipos, constraints)
class MetadataCache {
public:
  struct SchemaInfo {
    std::string schemaName;
    std::vector<std::string> tables;
    std::chrono::system_clock::time_point lastRefreshed;
  };

  struct TableInfo {
    std::string schemaName;
    std::string tableName;
    std::vector<std::string> columnNames;
    std::vector<std::string> columnTypes;
    std::map<std::string, std::string> columnConstraints;
    std::vector<std::string> primaryKeys;
    std::vector<std::string> foreignKeys;
    std::chrono::system_clock::time_point lastRefreshed;
  };

  struct CacheConfig {
    size_t maxSize;
    std::chrono::seconds defaultTTL;
    std::chrono::seconds autoRefreshInterval;
    bool enableAutoRefresh;
    
    CacheConfig() : maxSize(1000), defaultTTL(3600), autoRefreshInterval(1800), enableAutoRefresh(true) {}
  };

  explicit MetadataCache(const CacheConfig& config = CacheConfig());
  ~MetadataCache() = default;

  // Obtener lista de schemas
  std::optional<std::vector<std::string>> getSchemas(
      const std::string& connectionString,
      const std::string& dbEngine);

  // Guardar lista de schemas
  void putSchemas(const std::string& connectionString,
                  const std::string& dbEngine,
                  const std::vector<std::string>& schemas);

  // Obtener lista de tablas en un schema
  std::optional<std::vector<std::string>> getTables(
      const std::string& connectionString,
      const std::string& dbEngine,
      const std::string& schemaName);

  // Guardar lista de tablas
  void putTables(const std::string& connectionString,
                 const std::string& dbEngine,
                 const std::string& schemaName,
                 const std::vector<std::string>& tables);

  // Obtener información de tabla
  std::optional<TableInfo> getTableInfo(
      const std::string& connectionString,
      const std::string& dbEngine,
      const std::string& schemaName,
      const std::string& tableName);

  // Guardar información de tabla
  void putTableInfo(const std::string& connectionString,
                    const std::string& dbEngine,
                    const std::string& schemaName,
                    const std::string& tableName,
                    const TableInfo& tableInfo);

  // Invalidar cache para una tabla específica
  void invalidateTable(const std::string& connectionString,
                      const std::string& dbEngine,
                      const std::string& schemaName,
                      const std::string& tableName);

  // Invalidar cache para un schema completo
  void invalidateSchema(const std::string& connectionString,
                       const std::string& dbEngine,
                       const std::string& schemaName);

  // Limpiar todo el cache
  void clear();

  // Obtener estadísticas
  CacheManager::CacheStats getStats() const;

  // Refrescar metadata automáticamente (si está habilitado)
  void refreshIfNeeded(const std::string& connectionString,
                       const std::string& dbEngine,
                       std::function<void()> refreshFunction);

private:
  std::unique_ptr<CacheManager> cacheManager_;
  CacheConfig config_;

  // Generar keys para diferentes tipos de metadata
  std::string getSchemasKey(const std::string& connectionString,
                            const std::string& dbEngine) const;
  
  std::string getTablesKey(const std::string& connectionString,
                           const std::string& dbEngine,
                           const std::string& schemaName) const;
  
  std::string getTableInfoKey(const std::string& connectionString,
                              const std::string& dbEngine,
                              const std::string& schemaName,
                              const std::string& tableName) const;
};

#endif // METADATA_CACHE_H
