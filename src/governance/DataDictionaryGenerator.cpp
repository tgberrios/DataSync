#include "governance/DataDictionaryGenerator.h"
#include <pqxx/pqxx>
#include <algorithm>
#include <regex>

DataDictionaryGenerator::DataDictionaryGenerator(
    const std::string& connectionString,
    BusinessGlossaryManager& glossaryManager)
    : connectionString_(connectionString), glossaryManager_(glossaryManager) {
}

std::vector<DataDictionaryEntry> DataDictionaryGenerator::generateForTable(
    const std::string& schemaName,
    const std::string& tableName,
    const GenerationConfig& config) {
  
  std::vector<DataDictionaryEntry> entries;

  try {
    // Obtener metadata de columnas
    std::vector<ColumnMetadata> columnMetas = getColumnMetadata(schemaName, tableName);

    for (const auto& columnMeta : columnMetas) {
      // Verificar si ya existe entrada
      DataDictionaryEntry existing = glossaryManager_.getDictionaryEntry(
          schemaName, tableName, columnMeta.columnName);

      if (!existing.business_description.empty() && !config.overwriteExisting) {
        // Ya existe y no sobrescribir, saltar
        continue;
      }

      // Crear nueva entrada
      DataDictionaryEntry entry = createDictionaryEntry(
          schemaName, tableName, columnMeta, config);

      // Guardar o actualizar
      if (existing.business_description.empty()) {
        glossaryManager_.addDictionaryEntry(entry);
      } else {
        glossaryManager_.updateDictionaryEntry(schemaName, tableName,
                                               columnMeta.columnName, entry);
      }

      entries.push_back(entry);
    }

    Logger::info(LogCategory::GOVERNANCE, "DataDictionaryGenerator",
                 "Generated " + std::to_string(entries.size()) + 
                 " dictionary entries for " + schemaName + "." + tableName);
  } catch (const std::exception& e) {
    Logger::error(LogCategory::GOVERNANCE, "DataDictionaryGenerator",
                  "Error generating dictionary for table: " + std::string(e.what()));
  }

  return entries;
}

std::map<std::string, std::vector<DataDictionaryEntry>> 
DataDictionaryGenerator::generateForSchema(
    const std::string& schemaName,
    const GenerationConfig& config) {
  
  std::map<std::string, std::vector<DataDictionaryEntry>> schemaEntries;

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    // Obtener todas las tablas del schema
    auto result = txn.exec_params(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = $1 AND table_type = 'BASE TABLE'",
        schemaName
    );

    for (const auto& row : result) {
      std::string tableName = row["table_name"].as<std::string>();
      std::vector<DataDictionaryEntry> tableEntries = 
          generateForTable(schemaName, tableName, config);
      schemaEntries[tableName] = tableEntries;
    }

    txn.commit();
  } catch (const std::exception& e) {
    Logger::error(LogCategory::GOVERNANCE, "DataDictionaryGenerator",
                  "Error generating dictionary for schema: " + std::string(e.what()));
  }

  return schemaEntries;
}

void DataDictionaryGenerator::syncWithSchemaChanges(
    const std::string& schemaName,
    const std::string& tableName,
    const GenerationConfig& config) {
  
  // Regenerar para tabla (solo nuevas columnas si no overwrite)
  GenerationConfig syncConfig = config;
  syncConfig.overwriteExisting = false;  // No sobrescribir existentes
  
  generateForTable(schemaName, tableName, syncConfig);
}

std::string DataDictionaryGenerator::inferBusinessType(
    const std::string& technicalType) {
  
  std::string lowerType = technicalType;
  std::transform(lowerType.begin(), lowerType.end(), lowerType.begin(), ::tolower);

  // Mapeo de tipos técnicos a tipos de negocio
  if (lowerType.find("int") != std::string::npos ||
      lowerType.find("bigint") != std::string::npos ||
      lowerType.find("smallint") != std::string::npos) {
    return "Integer";
  }
  if (lowerType.find("decimal") != std::string::npos ||
      lowerType.find("numeric") != std::string::npos ||
      lowerType.find("float") != std::string::npos ||
      lowerType.find("double") != std::string::npos) {
    return "Decimal";
  }
  if (lowerType.find("varchar") != std::string::npos ||
      lowerType.find("text") != std::string::npos ||
      lowerType.find("char") != std::string::npos) {
    return "Text";
  }
  if (lowerType.find("date") != std::string::npos) {
    return "Date";
  }
  if (lowerType.find("timestamp") != std::string::npos ||
      lowerType.find("datetime") != std::string::npos) {
    return "DateTime";
  }
  if (lowerType.find("bool") != std::string::npos ||
      lowerType.find("bit") != std::string::npos) {
    return "Boolean";
  }

  return "Unknown";
}

std::string DataDictionaryGenerator::generateDescription(
    const std::string& columnName) {
  
  std::string description = columnName;
  
  // Convertir snake_case o camelCase a descripción legible
  std::regex snakeRegex(R"(_([a-z]))");
  description = std::regex_replace(description, snakeRegex, " $1");
  
  std::regex camelRegex(R"([A-Z])");
  description = std::regex_replace(description, camelRegex, " $&");
  
  // Capitalizar primera letra
  if (!description.empty()) {
    description[0] = std::toupper(description[0]);
  }

  // Agregar contexto común basado en nombres
  std::string lowerName = columnName;
  std::transform(lowerName.begin(), lowerName.end(), lowerName.begin(), ::tolower);

  if (lowerName.find("id") != std::string::npos) {
    description += " - Unique identifier";
  } else if (lowerName.find("name") != std::string::npos) {
    description += " - Name or label";
  } else if (lowerName.find("date") != std::string::npos ||
             lowerName.find("time") != std::string::npos) {
    description += " - Date or timestamp";
  } else if (lowerName.find("email") != std::string::npos) {
    description += " - Email address";
  } else if (lowerName.find("phone") != std::string::npos) {
    description += " - Phone number";
  } else if (lowerName.find("address") != std::string::npos) {
    description += " - Physical or mailing address";
  }

  return description;
}

std::vector<DataDictionaryGenerator::ColumnMetadata> 
DataDictionaryGenerator::getColumnMetadata(
    const std::string& schemaName,
    const std::string& tableName) {
  
  std::vector<ColumnMetadata> columns;

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    // Obtener información de columnas
    auto result = txn.exec_params(
        "SELECT c.column_name, c.data_type, c.is_nullable, c.column_default, "
        "CASE WHEN pk.column_name IS NOT NULL THEN true ELSE false END as is_pk, "
        "fk.target_table, fk.target_column "
        "FROM information_schema.columns c "
        "LEFT JOIN ("
        "  SELECT ku.column_name "
        "  FROM information_schema.table_constraints tc "
        "  JOIN information_schema.key_column_usage ku "
        "    ON tc.constraint_name = ku.constraint_name "
        "  WHERE tc.table_schema = $1 AND tc.table_name = $2 "
        "    AND tc.constraint_type = 'PRIMARY KEY'"
        ") pk ON c.column_name = pk.column_name "
        "LEFT JOIN ("
        "  SELECT ku.column_name, ccu.table_name as target_table, "
        "         ccu.column_name as target_column "
        "  FROM information_schema.table_constraints tc "
        "  JOIN information_schema.key_column_usage ku "
        "    ON tc.constraint_name = ku.constraint_name "
        "  JOIN information_schema.constraint_column_usage ccu "
        "    ON tc.constraint_name = ccu.constraint_name "
        "  WHERE tc.table_schema = $1 AND tc.table_name = $2 "
        "    AND tc.constraint_type = 'FOREIGN KEY'"
        ") fk ON c.column_name = fk.column_name "
        "WHERE c.table_schema = $1 AND c.table_name = $2 "
        "ORDER BY c.ordinal_position",
        schemaName, tableName
    );

    for (const auto& row : result) {
      ColumnMetadata meta;
      meta.columnName = row["column_name"].as<std::string>();
      meta.dataType = row["data_type"].as<std::string>();
      meta.isNullable = row["is_nullable"].as<std::string>() == "YES";
      if (!row["column_default"].is_null()) {
        meta.defaultValue = row["column_default"].as<std::string>();
      }
      meta.isPrimaryKey = row["is_pk"].as<bool>();
      if (!row["target_table"].is_null()) {
        meta.isForeignKey = true;
        meta.foreignKeyTable = row["target_table"].as<std::string>();
        meta.foreignKeyColumn = row["target_column"].as<std::string>();
      }

      columns.push_back(meta);
    }

    txn.commit();
  } catch (const std::exception& e) {
    Logger::error(LogCategory::GOVERNANCE, "DataDictionaryGenerator",
                  "Error getting column metadata: " + std::string(e.what()));
  }

  return columns;
}

DataDictionaryEntry DataDictionaryGenerator::createDictionaryEntry(
    const std::string& schemaName,
    const std::string& tableName,
    const ColumnMetadata& columnMeta,
    const GenerationConfig& config) {
  
  DataDictionaryEntry entry;
  entry.schema_name = schemaName;
  entry.table_name = tableName;
  entry.column_name = columnMeta.columnName;
  entry.business_name = generateDescription(columnMeta.columnName);
  entry.business_description = generateDescription(columnMeta.columnName);
  
  if (config.inferBusinessTypes) {
    entry.data_type_business = inferBusinessType(columnMeta.dataType);
  } else {
    entry.data_type_business = columnMeta.dataType;
  }

  // Documentar constraints
  if (config.detectConstraints) {
    std::stringstream rules;
    if (columnMeta.isPrimaryKey) {
      rules << "Primary Key. ";
    }
    if (columnMeta.isForeignKey) {
      rules << "Foreign Key to " << columnMeta.foreignKeyTable 
            << "." << columnMeta.foreignKeyColumn << ". ";
    }
    if (!columnMeta.isNullable) {
      rules << "Not Null. ";
    }
    if (!columnMeta.defaultValue.empty()) {
      rules << "Default: " << columnMeta.defaultValue << ". ";
    }
    entry.business_rules = rules.str();
  }

  entry.owner = config.defaultOwner;
  entry.steward = config.defaultSteward;

  return entry;
}
