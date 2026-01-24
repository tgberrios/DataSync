#ifndef SCHEMA_EVOLUTION_MANAGER_H
#define SCHEMA_EVOLUTION_MANAGER_H

#include "third_party/json.hpp"
#include "core/logger.h"
#include <string>
#include <vector>
#include <map>

using json = nlohmann::json;

// SchemaEvolutionManager: Manejo de cambios de esquema
class SchemaEvolutionManager {
public:
  enum class ChangeType {
    COLUMN_ADDED,
    COLUMN_REMOVED,
    COLUMN_MODIFIED,
    COLUMN_RENAMED,
    TYPE_CHANGED,
    NULLABLE_CHANGED
  };

  enum class CompatibilityLevel {
    BACKWARD_COMPATIBLE,  // Cambios compatibles hacia atrás
    FORWARD_COMPATIBLE,   // Cambios compatibles hacia adelante
    BREAKING              // Cambios que rompen compatibilidad
  };

  struct SchemaChange {
    ChangeType type;
    std::string columnName;
    std::string oldType;
    std::string newType;
    std::string oldValue;
    std::string newValue;
    CompatibilityLevel compatibility;
  };

  struct SchemaVersion {
    int version;
    std::string timestamp;
    std::vector<std::string> columns;
    std::map<std::string, std::string> columnTypes;
    json metadata;
  };

  // Detectar cambios de esquema entre dos versiones
  static std::vector<SchemaChange> detectChanges(
    const SchemaVersion& oldSchema,
    const SchemaVersion& newSchema
  );

  // Determinar nivel de compatibilidad de cambios
  static CompatibilityLevel determineCompatibility(const std::vector<SchemaChange>& changes);

  // Generar SQL de migración
  static std::string generateMigrationSQL(
    const std::string& tableName,
    const std::vector<SchemaChange>& changes,
    CompatibilityLevel compatibility
  );

  // Aplicar migración automática (si es posible)
  static bool applyMigration(
    const std::string& tableName,
    const std::vector<SchemaChange>& changes
  );
};

#endif // SCHEMA_EVOLUTION_MANAGER_H
