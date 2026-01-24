#ifndef CDC_CLEANUP_MANAGER_H
#define CDC_CLEANUP_MANAGER_H

#include "core/logger.h"
#include "third_party/json.hpp"
#include <chrono>
#include <string>
#include <vector>

using json = nlohmann::json;

// CDCCleanupManager: Limpieza de tablas ds_change_log antiguas
class CDCCleanupManager {
public:
  struct CleanupPolicy {
    int policyId;
    std::string connectionString;
    std::string dbEngine;
    int retentionDays;
    int batchSize;
    bool enabled;
    std::chrono::system_clock::time_point lastCleanupAt;
  };

  struct CleanupResult {
    int cleanupId;
    int policyId;
    std::string connectionString;
    std::string dbEngine;
    int64_t rowsDeleted;
    int tablesCleaned;
    double spaceFreedMB;
    std::chrono::system_clock::time_point startedAt;
    std::chrono::system_clock::time_point completedAt;
    std::string status; // 'running', 'completed', 'failed'
    std::string errorMessage;
  };

  explicit CDCCleanupManager(const std::string& connectionString);
  ~CDCCleanupManager() = default;

  // Crear o actualizar política de limpieza
  int createOrUpdatePolicy(const CleanupPolicy& policy);

  // Obtener política
  std::unique_ptr<CleanupPolicy> getPolicy(const std::string& connectionString,
                                          const std::string& dbEngine);

  // Listar todas las políticas
  std::vector<CleanupPolicy> listPolicies(bool enabledOnly = false);

  // Ejecutar limpieza para una política específica
  CleanupResult executeCleanup(int policyId);

  // Ejecutar limpieza para una conexión específica
  CleanupResult executeCleanup(const std::string& connectionString,
                              const std::string& dbEngine);

  // Ejecutar limpieza para todas las políticas habilitadas
  std::vector<CleanupResult> executeCleanupAll();

  // Obtener historial de limpiezas
  std::vector<CleanupResult> getCleanupHistory(const std::string& connectionString = "",
                                              int limit = 100);

  // Obtener estadísticas de limpieza
  json getCleanupStats();

private:
  std::string connectionString_;

  bool savePolicyToDatabase(const CleanupPolicy& policy);
  std::unique_ptr<CleanupPolicy> loadPolicyFromDatabase(const std::string& connectionString,
                                                        const std::string& dbEngine);
  bool saveCleanupResultToDatabase(const CleanupResult& result);
  int64_t getLastProcessedChangeId(const std::string& connectionString,
                                   const std::string& dbEngine,
                                   const std::string& schemaName,
                                   const std::string& tableName);
  int64_t cleanupChangeLogTable(const std::string& connectionString,
                                const std::string& dbEngine,
                                const std::string& schemaName,
                                const std::string& tableName,
                                int64_t beforeChangeId,
                                int batchSize);
  std::vector<std::pair<std::string, std::string>> getChangeLogTables(
      const std::string& connectionString, const std::string& dbEngine);
};

#endif // CDC_CLEANUP_MANAGER_H
