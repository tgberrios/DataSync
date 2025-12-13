#ifndef SCHEMASYNC_H
#define SCHEMASYNC_H

#include "core/logger.h"
#include <pqxx/pqxx>
#include <string>
#include <unordered_map>
#include <vector>

struct ColumnInfo {
  std::string name;
  std::string dataType;
  std::string pgType;
  bool isNullable;
  std::string defaultValue;
  int ordinalPosition;
  std::string maxLength;
  std::string numericPrecision;
  std::string numericScale;
  bool isPrimaryKey;

  bool operator==(const ColumnInfo &other) const {
    return name == other.name && pgType == other.pgType &&
           isNullable == other.isNullable;
  }
};

struct SchemaDiff {
  std::vector<ColumnInfo> columnsToAdd;
  std::vector<std::string> columnsToDrop;
  std::vector<std::pair<ColumnInfo, ColumnInfo>> columnsToModify;
  bool hasChanges() const {
    return !columnsToAdd.empty() || !columnsToDrop.empty() ||
           !columnsToModify.empty();
  }
};

class SchemaSync {
public:
  SchemaSync() = default;

  static std::vector<ColumnInfo>
  getTableColumnsPostgres(pqxx::connection &pgConn,
                          const std::string &schemaName,
                          const std::string &tableName);

  static SchemaDiff
  detectSchemaChanges(const std::vector<ColumnInfo> &sourceColumns,
                      const std::vector<ColumnInfo> &targetColumns);

  static bool applySchemaChanges(pqxx::connection &pgConn,
                                 const std::string &schemaName,
                                 const std::string &tableName,
                                 const SchemaDiff &diff,
                                 const std::string &dbEngine);

  static bool syncSchema(pqxx::connection &pgConn,
                         const std::string &schemaName,
                         const std::string &tableName,
                         const std::vector<ColumnInfo> &sourceColumns,
                         const std::string &dbEngine);

private:
  static bool addMissingColumns(pqxx::connection &pgConn,
                                const std::string &schemaName,
                                const std::string &tableName,
                                const std::vector<ColumnInfo> &columnsToAdd);

  static bool dropRemovedColumns(pqxx::connection &pgConn,
                                 const std::string &schemaName,
                                 const std::string &tableName,
                                 const std::vector<std::string> &columnsToDrop);

  static bool updateColumnTypes(
      pqxx::connection &pgConn, const std::string &schemaName,
      const std::string &tableName,
      const std::vector<std::pair<ColumnInfo, ColumnInfo>> &columnsToModify);

  static bool isTypeChangeCompatible(const std::string &oldType,
                                     const std::string &newType);

  static std::string buildColumnDefinition(const ColumnInfo &col);
};

#endif
