#ifndef DATA_DICTIONARY_GENERATOR_H
#define DATA_DICTIONARY_GENERATOR_H

#include "governance/BusinessGlossaryManager.h"
#include "third_party/json.hpp"
#include "core/logger.h"
#include <string>
#include <vector>
#include <map>

using json = nlohmann::json;

// DataDictionaryGenerator: Genera entradas de diccionario automáticamente desde schemas
class DataDictionaryGenerator {
public:
  struct GenerationConfig {
    bool overwriteExisting;
    bool inferBusinessTypes;
    bool detectConstraints;
    bool detectForeignKeys;
    std::string defaultOwner;
    std::string defaultSteward;

    GenerationConfig()
      : overwriteExisting(false),
        inferBusinessTypes(true),
        detectConstraints(true),
        detectForeignKeys(true),
        defaultOwner(""),
        defaultSteward("") {}
  };

  struct ColumnMetadata {
    std::string columnName;
    std::string dataType;
    bool isNullable;
    std::string defaultValue;
    bool isPrimaryKey;
    bool isForeignKey;
    std::string foreignKeyTable;
    std::string foreignKeyColumn;
    std::vector<std::string> constraints;
  };

  explicit DataDictionaryGenerator(
      const std::string& connectionString,
      BusinessGlossaryManager& glossaryManager);
  ~DataDictionaryGenerator() = default;

  // Generar diccionario para una tabla
  std::vector<DataDictionaryEntry> generateForTable(
      const std::string& schemaName,
      const std::string& tableName,
      const GenerationConfig& config = GenerationConfig()
  );

  // Generar diccionario para un schema completo
  std::map<std::string, std::vector<DataDictionaryEntry>> generateForSchema(
      const std::string& schemaName,
      const GenerationConfig& config = GenerationConfig()
  );

  // Sincronizar diccionario con cambios de schema
  void syncWithSchemaChanges(
      const std::string& schemaName,
      const std::string& tableName,
      const GenerationConfig& config = GenerationConfig()
  );

  // Inferir tipo de dato de negocio desde tipo técnico
  static std::string inferBusinessType(const std::string& technicalType);

  // Generar descripción sugerida desde nombre de columna
  static std::string generateDescription(const std::string& columnName);

private:
  std::string connectionString_;
  BusinessGlossaryManager& glossaryManager_;

  // Helper methods
  std::vector<ColumnMetadata> getColumnMetadata(
      const std::string& schemaName,
      const std::string& tableName
  );

  DataDictionaryEntry createDictionaryEntry(
      const std::string& schemaName,
      const std::string& tableName,
      const ColumnMetadata& columnMeta,
      const GenerationConfig& config
  );
};

#endif // DATA_DICTIONARY_GENERATOR_H
