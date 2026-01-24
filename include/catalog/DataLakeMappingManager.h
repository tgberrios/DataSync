#ifndef DATALAKE_MAPPING_MANAGER_H
#define DATALAKE_MAPPING_MANAGER_H

#include "core/logger.h"
#include "third_party/json.hpp"
#include <chrono>
#include <string>
#include <vector>

using json = nlohmann::json;

// DataLakeMappingManager: Tracking preciso de origen de tablas y refresh rate
class DataLakeMappingManager {
public:
  enum class RefreshRateType {
    MANUAL,
    SCHEDULED,
    REAL_TIME,
    ON_DEMAND
  };

  struct Mapping {
    int mappingId;
    std::string targetSchema;
    std::string targetTable;
    std::string sourceSystem; // 'mariadb', 'mssql', 'oracle', 'postgresql', 'mongodb', 'api', 'csv', 'google_sheets'
    std::string sourceConnection;
    std::string sourceSchema;
    std::string sourceTable;
    RefreshRateType refreshRateType;
    std::string refreshSchedule; // Cron expression
    std::chrono::system_clock::time_point lastRefreshAt;
    std::chrono::system_clock::time_point nextRefreshAt;
    double refreshDurationAvg; // milliseconds
    int refreshSuccessCount;
    int refreshFailureCount;
    double refreshSuccessRate; // percentage
    json metadata;
  };

  struct RefreshStats {
    int totalMappings;
    int scheduledMappings;
    int realTimeMappings;
    double averageSuccessRate;
    double averageDuration;
    int totalRefreshes;
    int successfulRefreshes;
    int failedRefreshes;
  };

  explicit DataLakeMappingManager(const std::string& connectionString);
  ~DataLakeMappingManager() = default;

  // Registrar o actualizar mapeo
  int createOrUpdateMapping(const Mapping& mapping);

  // Obtener mapeo por target table
  std::unique_ptr<Mapping> getMapping(const std::string& targetSchema,
                                      const std::string& targetTable);

  // Listar todos los mapeos
  std::vector<Mapping> listMappings(const std::string& sourceSystem = "",
                                  RefreshRateType refreshType = RefreshRateType::MANUAL);

  // Actualizar refresh rate
  bool updateRefreshRate(const std::string& targetSchema,
                        const std::string& targetTable,
                        RefreshRateType refreshType,
                        const std::string& refreshSchedule = "");

  // Registrar actualización (llamado después de sync exitoso)
  bool recordRefresh(const std::string& targetSchema,
                    const std::string& targetTable,
                    bool success,
                    int64_t durationMs);

  // Obtener estadísticas
  RefreshStats getRefreshStats();

  // Calcular próxima actualización programada
  std::chrono::system_clock::time_point calculateNextRefresh(
      const std::string& refreshSchedule);

private:
  std::string connectionString_;

  std::string refreshRateTypeToString(RefreshRateType type);
  RefreshRateType stringToRefreshRateType(const std::string& str);
  bool saveMappingToDatabase(const Mapping& mapping);
  std::unique_ptr<Mapping> loadMappingFromDatabase(const std::string& targetSchema,
                                                   const std::string& targetTable);
};

#endif // DATALAKE_MAPPING_MANAGER_H
