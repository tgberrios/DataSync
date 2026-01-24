#ifndef PARTITIONING_MANAGER_H
#define PARTITIONING_MANAGER_H

#include "third_party/json.hpp"
#include "core/logger.h"
#include <string>
#include <vector>
#include <map>
#include <chrono>

using json = nlohmann::json;

// PartitioningManager: Detección automática y gestión de particiones
class PartitioningManager {
public:
  enum class PartitionType {
    DATE,           // Partición por fecha (year/month/day)
    REGION,         // Partición por región geográfica
    RANGE,          // Partición por rangos de valores
    HASH,           // Partición por hash
    LIST            // Partición por lista de valores
  };

  struct PartitionInfo {
    std::string columnName;
    PartitionType type;
    std::string format;              // Para DATE: "year", "year-month", "year-month-day"
    std::vector<std::string> values;  // Para LIST o RANGE
    json metadata;
  };

  struct PartitionDetectionResult {
    bool hasPartitions{false};
    std::vector<PartitionInfo> partitions;
    std::string recommendedPartitionColumn;
    PartitionType recommendedType;
  };

  // Detectar particiones automáticamente en una tabla
  static PartitionDetectionResult detectPartitions(
    const std::string& schemaName,
    const std::string& tableName,
    const std::vector<std::string>& columnNames,
    const std::vector<std::string>& columnTypes
  );

  // Detectar si una columna es de fecha
  static bool isDateColumn(const std::string& columnName, const std::string& columnType);

  // Detectar si una columna es de región/geografía
  static bool isRegionColumn(const std::string& columnName, const std::string& columnType);

  // Generar SQL para particionado
  static std::string generatePartitionSQL(
    const std::string& tableName,
    const PartitionInfo& partitionInfo
  );

  // Obtener lista de particiones modificadas desde última ejecución
  static std::vector<std::string> getModifiedPartitions(
    const std::string& tableName,
    const PartitionInfo& partitionInfo,
    const std::chrono::system_clock::time_point& lastExecutionTime
  );

  // Procesar solo particiones específicas
  static std::string generatePartitionFilter(
    const PartitionInfo& partitionInfo,
    const std::vector<std::string>& partitionValues
  );

  // Dynamic Partitioning: Crear particiones automáticamente
  struct DynamicPartitionConfig {
    bool enabled{false};
    std::chrono::hours partitionInterval{24};  // Crear partición cada X horas
    size_t maxPartitions{1000};  // Límite máximo de particiones
    bool autoDropOldPartitions{false};
    int partitionRetentionDays{90};  // Retener particiones por X días
  };

  // Crear partición dinámicamente basado en datos nuevos
  static bool createDynamicPartition(
    const std::string& schemaName,
    const std::string& tableName,
    const PartitionInfo& partitionInfo,
    const std::string& partitionValue,
    const std::string& connectionString
  );

  // Obtener particiones existentes
  static std::vector<std::string> getExistingPartitions(
    const std::string& schemaName,
    const std::string& tableName,
    const PartitionInfo& partitionInfo,
    const std::string& connectionString
  );

  // Detectar si necesita crear nueva partición
  static bool needsNewPartition(
    const std::string& partitionValue,
    const std::vector<std::string>& existingPartitions,
    const PartitionInfo& partitionInfo
  );

  // Generar nombre de partición
  static std::string generatePartitionName(
    const PartitionInfo& partitionInfo,
    const std::string& partitionValue
  );

  // Eliminar particiones antiguas
  static size_t dropOldPartitions(
    const std::string& schemaName,
    const std::string& tableName,
    const PartitionInfo& partitionInfo,
    int retentionDays,
    const std::string& connectionString
  );
};

#endif // PARTITIONING_MANAGER_H
