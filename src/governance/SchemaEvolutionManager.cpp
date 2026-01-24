#include "governance/SchemaEvolutionManager.h"
#include "core/logger.h"
#include <algorithm>
#include <sstream>
#include <set>

std::vector<SchemaEvolutionManager::SchemaChange>
SchemaEvolutionManager::detectChanges(
    const SchemaVersion& oldSchema,
    const SchemaVersion& newSchema) {
  
  std::vector<SchemaChange> changes;
  std::set<std::string> oldColumns(oldSchema.columns.begin(), oldSchema.columns.end());
  std::set<std::string> newColumns(newSchema.columns.begin(), newSchema.columns.end());
  
  // Detectar columnas agregadas
  for (const auto& col : newColumns) {
    if (oldColumns.find(col) == oldColumns.end()) {
      SchemaChange change;
      change.type = ChangeType::COLUMN_ADDED;
      change.columnName = col;
      change.newType = newSchema.columnTypes.at(col);
      change.compatibility = CompatibilityLevel::BACKWARD_COMPATIBLE;
      changes.push_back(change);
    }
  }
  
  // Detectar columnas eliminadas
  for (const auto& col : oldColumns) {
    if (newColumns.find(col) == newColumns.end()) {
      SchemaChange change;
      change.type = ChangeType::COLUMN_REMOVED;
      change.columnName = col;
      change.oldType = oldSchema.columnTypes.at(col);
      change.compatibility = CompatibilityLevel::BREAKING;
      changes.push_back(change);
    }
  }
  
  // Detectar columnas modificadas
  for (const auto& col : oldColumns) {
    if (newColumns.find(col) != newColumns.end()) {
      std::string oldType = oldSchema.columnTypes.at(col);
      std::string newType = newSchema.columnTypes.at(col);
      
      if (oldType != newType) {
        SchemaChange change;
        change.type = ChangeType::TYPE_CHANGED;
        change.columnName = col;
        change.oldType = oldType;
        change.newType = newType;
        
        // Determinar si el cambio de tipo es compatible
        // (ej: INT -> BIGINT es compatible, pero VARCHAR -> INT no)
        if ((oldType.find("int") != std::string::npos && 
             newType.find("int") != std::string::npos) ||
            (oldType.find("varchar") != std::string::npos && 
             newType.find("varchar") != std::string::npos)) {
          change.compatibility = CompatibilityLevel::BACKWARD_COMPATIBLE;
        } else {
          change.compatibility = CompatibilityLevel::BREAKING;
        }
        
        changes.push_back(change);
      }
    }
  }
  
  Logger::info(LogCategory::GOVERNANCE, "SchemaEvolutionManager",
               "Detected " + std::to_string(changes.size()) + " schema changes");
  
  return changes;
}

SchemaEvolutionManager::CompatibilityLevel
SchemaEvolutionManager::determineCompatibility(const std::vector<SchemaChange>& changes) {
  for (const auto& change : changes) {
    if (change.compatibility == CompatibilityLevel::BREAKING) {
      return CompatibilityLevel::BREAKING;
    }
  }
  
  // Si hay columnas eliminadas, es breaking
  for (const auto& change : changes) {
    if (change.type == ChangeType::COLUMN_REMOVED) {
      return CompatibilityLevel::BREAKING;
    }
  }
  
  // Si solo hay agregados, es backward compatible
  bool onlyAdditions = true;
  for (const auto& change : changes) {
    if (change.type != ChangeType::COLUMN_ADDED) {
      onlyAdditions = false;
      break;
    }
  }
  
  if (onlyAdditions) {
    return CompatibilityLevel::BACKWARD_COMPATIBLE;
  }
  
  return CompatibilityLevel::FORWARD_COMPATIBLE;
}

std::string SchemaEvolutionManager::generateMigrationSQL(
    const std::string& tableName,
    const std::vector<SchemaChange>& changes,
    CompatibilityLevel compatibility) {
  
  std::ostringstream sql;
  
  if (compatibility == CompatibilityLevel::BREAKING) {
    sql << "-- BREAKING CHANGES DETECTED - Manual intervention required\n";
    sql << "-- Table: " << tableName << "\n";
    sql << "-- Changes:\n";
    for (const auto& change : changes) {
      sql << "--   " << change.columnName << ": " << 
             (change.type == ChangeType::COLUMN_REMOVED ? "REMOVED" : "MODIFIED") << "\n";
    }
    return sql.str();
  }
  
  // Generar ALTER TABLE statements para cambios compatibles
  for (const auto& change : changes) {
    if (change.type == ChangeType::COLUMN_ADDED) {
      sql << "ALTER TABLE " << tableName << " ADD COLUMN " 
          << change.columnName << " " << change.newType;
      // Agregar default si es necesario
      sql << " DEFAULT NULL";
      sql << ";\n";
    } else if (change.type == ChangeType::TYPE_CHANGED && 
               change.compatibility == CompatibilityLevel::BACKWARD_COMPATIBLE) {
      sql << "ALTER TABLE " << tableName << " ALTER COLUMN " 
          << change.columnName << " TYPE " << change.newType << ";\n";
    }
  }
  
  return sql.str();
}

bool SchemaEvolutionManager::applyMigration(
    const std::string& tableName,
    const std::vector<SchemaChange>& changes) {
  
  CompatibilityLevel compatibility = determineCompatibility(changes);
  
  if (compatibility == CompatibilityLevel::BREAKING) {
    Logger::error(LogCategory::GOVERNANCE, "SchemaEvolutionManager",
                 "Cannot auto-apply breaking schema changes for table: " + tableName);
    return false;
  }
  
  std::string migrationSQL = generateMigrationSQL(tableName, changes, compatibility);
  
  Logger::info(LogCategory::GOVERNANCE, "SchemaEvolutionManager",
               "Generated migration SQL for " + tableName + ":\n" + migrationSQL);
  
  // En implementación real, se ejecutaría el SQL
  // Por ahora, solo logueamos
  
  return true;
}
