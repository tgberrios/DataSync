#include "sync/SchemaSync.h"
#include "core/logger.h"
#include <algorithm>
#include <unordered_map>
#include <unordered_set>

std::vector<ColumnInfo>
SchemaSync::getTableColumnsPostgres(pqxx::connection &pgConn,
                                    const std::string &schemaName,
                                    const std::string &tableName) {
  std::vector<ColumnInfo> columns;
  try {
    pqxx::work txn(pgConn);
    std::string lowerSchema = schemaName;
    std::transform(lowerSchema.begin(), lowerSchema.end(), lowerSchema.begin(),
                   ::tolower);
    std::string lowerTable = tableName;
    std::transform(lowerTable.begin(), lowerTable.end(), lowerTable.begin(),
                   ::tolower);

    std::string query =
        "SELECT column_name, data_type, is_nullable, column_default, "
        "ordinal_position, character_maximum_length, numeric_precision, "
        "numeric_scale "
        "FROM information_schema.columns "
        "WHERE table_schema = " +
        txn.quote(lowerSchema) + " AND table_name = " + txn.quote(lowerTable) +
        " ORDER BY ordinal_position";

    auto result = txn.exec(query);

    std::string pkQuery =
        "SELECT column_name FROM information_schema.table_constraints tc "
        "INNER JOIN information_schema.key_column_usage kcu "
        "ON tc.constraint_name = kcu.constraint_name "
        "AND tc.table_schema = kcu.table_schema "
        "WHERE tc.table_schema = " +
        txn.quote(lowerSchema) +
        " AND tc.table_name = " + txn.quote(lowerTable) +
        " AND tc.constraint_type = 'PRIMARY KEY' "
        "ORDER BY kcu.ordinal_position";

    auto pkResult = txn.exec(pkQuery);
    txn.commit();

    std::unordered_set<std::string> pkSet;
    for (const auto &row : pkResult) {
      if (!row[0].is_null()) {
        std::string pkCol = row[0].as<std::string>();
        std::transform(pkCol.begin(), pkCol.end(), pkCol.begin(), ::tolower);
        pkSet.insert(pkCol);
      }
    }

    for (const auto &row : result) {
      ColumnInfo col;
      col.name = row[0].as<std::string>();
      std::transform(col.name.begin(), col.name.end(), col.name.begin(),
                     ::tolower);
      col.dataType = row[1].as<std::string>();
      col.isNullable = (row[2].as<std::string>() == "YES");
      col.defaultValue = row[3].is_null() ? "" : row[3].as<std::string>();
      col.ordinalPosition = row[4].as<int>();
      col.maxLength = row[5].is_null() ? "" : row[5].as<std::string>();
      col.numericPrecision =
          row[6].is_null() ? "" : std::to_string(row[6].as<int>());
      col.numericScale =
          row[7].is_null() ? "" : std::to_string(row[7].as<int>());
      col.isPrimaryKey = pkSet.find(col.name) != pkSet.end();

      std::string pgType = col.dataType;
      if (pgType == "character varying" || pgType == "varchar") {
        if (!col.maxLength.empty()) {
          pgType = "VARCHAR(" + col.maxLength + ")";
        } else {
          pgType = "VARCHAR";
        }
      } else if (pgType == "character" || pgType == "char") {
        if (!col.maxLength.empty()) {
          pgType = "CHAR(" + col.maxLength + ")";
        } else {
          pgType = "CHAR(1)";
        }
      } else if (pgType == "numeric") {
        if (!col.numericPrecision.empty() && !col.numericScale.empty()) {
          pgType =
              "NUMERIC(" + col.numericPrecision + "," + col.numericScale + ")";
        } else {
          pgType = "NUMERIC";
        }
      }

      col.pgType = pgType;
      columns.push_back(col);
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "getTableColumnsPostgres",
                  "Error getting PostgreSQL columns: " + std::string(e.what()));
  }

  return columns;
}

SchemaDiff
SchemaSync::detectSchemaChanges(const std::vector<ColumnInfo> &sourceColumns,
                                const std::vector<ColumnInfo> &targetColumns) {
  SchemaDiff diff;

  std::unordered_map<std::string, ColumnInfo> sourceMap;
  std::unordered_map<std::string, ColumnInfo> targetMap;

  for (const auto &col : sourceColumns) {
    sourceMap[col.name] = col;
  }

  for (const auto &col : targetColumns) {
    targetMap[col.name] = col;
  }

  for (const auto &sourceCol : sourceColumns) {
    auto it = targetMap.find(sourceCol.name);
    if (it == targetMap.end()) {
      diff.columnsToAdd.push_back(sourceCol);
    } else {
      const ColumnInfo &targetCol = it->second;
      if (sourceCol.pgType != targetCol.pgType ||
          sourceCol.isNullable != targetCol.isNullable) {
        diff.columnsToModify.push_back({targetCol, sourceCol});
      }
    }
  }

  for (const auto &targetCol : targetColumns) {
    if (sourceMap.find(targetCol.name) == sourceMap.end()) {
      diff.columnsToDrop.push_back(targetCol.name);
    }
  }

  return diff;
}

bool SchemaSync::isTypeChangeCompatible(const std::string &oldType,
                                        const std::string &newType) {
  std::string oldUpper = oldType;
  std::string newUpper = newType;
  std::transform(oldUpper.begin(), oldUpper.end(), oldUpper.begin(), ::toupper);
  std::transform(newUpper.begin(), newUpper.end(), newUpper.begin(), ::toupper);

  if (oldUpper == newUpper) {
    return true;
  }

  if (oldUpper.find("VARCHAR") != std::string::npos &&
      newUpper.find("VARCHAR") != std::string::npos) {
    return true;
  }

  if (oldUpper.find("CHAR") != std::string::npos &&
      newUpper.find("CHAR") != std::string::npos) {
    return true;
  }

  if (oldUpper.find("NUMERIC") != std::string::npos &&
      newUpper.find("NUMERIC") != std::string::npos) {
    return true;
  }

  if (oldUpper.find("INTEGER") != std::string::npos &&
      newUpper.find("BIGINT") != std::string::npos) {
    return true;
  }

  if (oldUpper.find("SMALLINT") != std::string::npos &&
      (newUpper.find("INTEGER") != std::string::npos ||
       newUpper.find("BIGINT") != std::string::npos)) {
    return true;
  }

  return false;
}

std::string SchemaSync::buildColumnDefinition(const ColumnInfo &col) {
  std::string def = "\"" + col.name + "\" " + col.pgType;
  if (!col.isNullable) {
    def += " NOT NULL";
  }
  if (!col.defaultValue.empty() && col.defaultValue != "NULL") {
    def += " DEFAULT " + col.defaultValue;
  }
  return def;
}

bool SchemaSync::addMissingColumns(
    pqxx::connection &pgConn, const std::string &schemaName,
    const std::string &tableName, const std::vector<ColumnInfo> &columnsToAdd) {
  if (columnsToAdd.empty()) {
    return true;
  }

  try {
    pqxx::work txn(pgConn);
    std::string lowerSchema = schemaName;
    std::transform(lowerSchema.begin(), lowerSchema.end(), lowerSchema.begin(),
                   ::tolower);
    std::string lowerTable = tableName;
    std::transform(lowerTable.begin(), lowerTable.end(), lowerTable.begin(),
                   ::tolower);

    for (const auto &col : columnsToAdd) {
      std::string alterQuery = "ALTER TABLE " + txn.quote_name(lowerSchema) +
                               "." + txn.quote_name(lowerTable) +
                               " ADD COLUMN " + buildColumnDefinition(col);

      Logger::info(LogCategory::TRANSFER, "addMissingColumns",
                   "Adding column " + col.name + " to " + schemaName + "." +
                       tableName);
      txn.exec(alterQuery);
    }

    txn.commit();
    return true;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "addMissingColumns",
                  "Error adding columns: " + std::string(e.what()));
    return false;
  }
}

bool SchemaSync::dropRemovedColumns(
    pqxx::connection &pgConn, const std::string &schemaName,
    const std::string &tableName,
    const std::vector<std::string> &columnsToDrop) {
  if (columnsToDrop.empty()) {
    return true;
  }

  try {
    pqxx::work txn(pgConn);
    std::string lowerSchema = schemaName;
    std::transform(lowerSchema.begin(), lowerSchema.end(), lowerSchema.begin(),
                   ::tolower);
    std::string lowerTable = tableName;
    std::transform(lowerTable.begin(), lowerTable.end(), lowerTable.begin(),
                   ::tolower);

    for (const auto &colName : columnsToDrop) {
      std::string lowerColName = colName;
      std::transform(lowerColName.begin(), lowerColName.end(),
                     lowerColName.begin(), ::tolower);

      Logger::warning(LogCategory::TRANSFER, "dropRemovedColumns",
                      "Dropping column " + colName + " from " + schemaName +
                          "." + tableName + " (no longer exists in source)");

      std::string alterQuery = "ALTER TABLE " + txn.quote_name(lowerSchema) +
                               "." + txn.quote_name(lowerTable) +
                               " DROP COLUMN " + txn.quote_name(lowerColName);

      txn.exec(alterQuery);
    }

    txn.commit();
    return true;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "dropRemovedColumns",
                  "Error dropping columns: " + std::string(e.what()));
    return false;
  }
}

bool SchemaSync::updateColumnTypes(
    pqxx::connection &pgConn, const std::string &schemaName,
    const std::string &tableName,
    const std::vector<std::pair<ColumnInfo, ColumnInfo>> &columnsToModify) {
  if (columnsToModify.empty()) {
    return true;
  }

  try {
    pqxx::work txn(pgConn);
    std::string lowerSchema = schemaName;
    std::transform(lowerSchema.begin(), lowerSchema.end(), lowerSchema.begin(),
                   ::tolower);
    std::string lowerTable = tableName;
    std::transform(lowerTable.begin(), lowerTable.end(), lowerTable.begin(),
                   ::tolower);

    for (const auto &pair : columnsToModify) {
      const ColumnInfo &oldCol = pair.first;
      const ColumnInfo &newCol = pair.second;

      if (!isTypeChangeCompatible(oldCol.pgType, newCol.pgType)) {
        Logger::warning(LogCategory::TRANSFER, "updateColumnTypes",
                        "Incompatible type change for " + schemaName + "." +
                            tableName + "." + newCol.name + ": " +
                            oldCol.pgType + " -> " + newCol.pgType +
                            " - skipping");
        continue;
      }

      std::string lowerColName = newCol.name;
      std::transform(lowerColName.begin(), lowerColName.end(),
                     lowerColName.begin(), ::tolower);

      Logger::info(LogCategory::TRANSFER, "updateColumnTypes",
                   "Updating column type " + newCol.name + " in " + schemaName +
                       "." + tableName + ": " + oldCol.pgType + " -> " +
                       newCol.pgType);

      std::string alterQuery = "ALTER TABLE " + txn.quote_name(lowerSchema) +
                               "." + txn.quote_name(lowerTable) +
                               " ALTER COLUMN " + txn.quote_name(lowerColName) +
                               " TYPE " + newCol.pgType;

      txn.exec(alterQuery);

      if (oldCol.isNullable != newCol.isNullable) {
        std::string nullQuery = "ALTER TABLE " + txn.quote_name(lowerSchema) +
                                "." + txn.quote_name(lowerTable) +
                                " ALTER COLUMN " + txn.quote_name(lowerColName);
        if (newCol.isNullable) {
          nullQuery += " DROP NOT NULL";
        } else {
          nullQuery += " SET NOT NULL";
        }
        txn.exec(nullQuery);
      }
    }

    txn.commit();
    return true;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "updateColumnTypes",
                  "Error updating column types: " + std::string(e.what()));
    return false;
  }
}

bool SchemaSync::applySchemaChanges(pqxx::connection &pgConn,
                                    const std::string &schemaName,
                                    const std::string &tableName,
                                    const SchemaDiff &diff,
                                    const std::string &dbEngine) {
  if (!diff.hasChanges()) {
    return true;
  }

  Logger::info(LogCategory::TRANSFER, "applySchemaChanges",
               "Applying schema changes to " + schemaName + "." + tableName +
                   ": " + std::to_string(diff.columnsToAdd.size()) +
                   " to add, " + std::to_string(diff.columnsToDrop.size()) +
                   " to drop, " + std::to_string(diff.columnsToModify.size()) +
                   " to modify");

  bool success = true;

  for (const auto &col : diff.columnsToDrop) {
    for (const auto &sourceCol : diff.columnsToAdd) {
      if (sourceCol.isPrimaryKey && sourceCol.name == col) {
        Logger::warning(LogCategory::TRANSFER, "applySchemaChanges",
                        "Cannot drop primary key column " + col +
                            " - marking table for FULL_LOAD");
        return false;
      }
    }
  }

  if (!addMissingColumns(pgConn, schemaName, tableName, diff.columnsToAdd)) {
    success = false;
  }

  if (!dropRemovedColumns(pgConn, schemaName, tableName, diff.columnsToDrop)) {
    success = false;
  }

  if (!updateColumnTypes(pgConn, schemaName, tableName, diff.columnsToModify)) {
    success = false;
  }

  return success;
}

bool SchemaSync::syncSchema(pqxx::connection &pgConn,
                            const std::string &schemaName,
                            const std::string &tableName,
                            const std::vector<ColumnInfo> &sourceColumns,
                            const std::string &dbEngine) {
  try {
    std::vector<ColumnInfo> targetColumns =
        getTableColumnsPostgres(pgConn, schemaName, tableName);

    if (targetColumns.empty()) {
      Logger::debug(
          LogCategory::TRANSFER, "syncSchema",
          "Table " + schemaName + "." + tableName +
              " does not exist in PostgreSQL - will be created during sync");
      return true;
    }

    SchemaDiff diff = detectSchemaChanges(sourceColumns, targetColumns);

    if (!diff.hasChanges()) {
      return true;
    }

    return applySchemaChanges(pgConn, schemaName, tableName, diff, dbEngine);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "syncSchema",
                  "Error syncing schema for " + schemaName + "." + tableName +
                      ": " + std::string(e.what()));
    return false;
  }
}
