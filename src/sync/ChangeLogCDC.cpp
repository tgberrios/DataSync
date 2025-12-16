#include "sync/ChangeLogCDC.h"
#include "core/Config.h"
#include "core/logger.h"
#include <algorithm>
#include <mutex>
#include <pqxx/pqxx>

long long ChangeLogCDC::getLastChangeId(pqxx::connection &pgConn,
                                        const TableInfo &table,
                                        const std::string &dbEngine) {
  long long lastChangeId = 0;
  try {
    pqxx::work txn(pgConn);
    std::string query =
        "SELECT sync_metadata->>'last_change_id' FROM metadata.catalog "
        "WHERE schema_name=" +
        txn.quote(table.schema_name) +
        " AND table_name=" + txn.quote(table.table_name) +
        " AND db_engine=" + txn.quote(dbEngine);
    auto res = txn.exec(query);
    txn.commit();

    if (!res.empty() && !res[0][0].is_null()) {
      std::string value = res[0][0].as<std::string>();
      if (!value.empty() && value.size() <= 20) {
        try {
          lastChangeId = std::stoll(value);
        } catch (const std::exception &e) {
          Logger::error(LogCategory::TRANSFER, "getLastChangeId",
                        "Failed to parse last_change_id for " +
                            table.schema_name + "." + table.table_name + ": " +
                            std::string(e.what()));
          lastChangeId = 0;
        }
      }
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "getLastChangeId",
                  "Error getting last_change_id for " + table.schema_name +
                      "." + table.table_name + ": " + std::string(e.what()));
    lastChangeId = 0;
  }
  return lastChangeId;
}

void ChangeLogCDC::processChangeLogBatch(
    pqxx::connection &pgConn, const TableInfo &table,
    const std::vector<ChangeLogEntry> &changes,
    const std::vector<std::string> &columnNames,
    const std::vector<std::string> &columnTypes, const std::string &dbEngine) {
  std::vector<std::string> pkColumns =
      getPKColumnsFromCatalog(pgConn, table.schema_name, table.table_name);
  if (pkColumns.empty()) {
    Logger::warning(LogCategory::TRANSFER, "processChangeLogBatch",
                    "No PK columns found for " + table.schema_name + "." +
                        table.table_name);
    return;
  }

  std::vector<std::vector<std::string>> deletedPKs;
  std::vector<std::vector<std::string>> recordsToUpsert;

  for (const auto &change : changes) {
    try {
      std::vector<std::string> pkValues;
      for (const auto &pkCol : pkColumns) {
        if (change.pk_values.contains(pkCol) &&
            !change.pk_values[pkCol].is_null()) {
          if (change.pk_values[pkCol].is_string()) {
            pkValues.push_back(change.pk_values[pkCol].get<std::string>());
          } else {
            pkValues.push_back(change.pk_values[pkCol].dump());
          }
        } else {
          pkValues.push_back("NULL");
        }
      }

      if (pkValues.size() != pkColumns.size()) {
        continue;
      }

      if (change.operation == 'D') {
        deletedPKs.push_back(pkValues);
      } else if (change.operation == 'I' || change.operation == 'U') {
        bool useRowData = false;
        std::vector<std::string> record;

        if (!change.row_data.is_null() && !change.row_data.empty()) {
          try {
            record.reserve(columnNames.size());
            bool allColumnsFound = true;

            for (const auto &colName : columnNames) {
              if (change.row_data.contains(colName) &&
                  !change.row_data[colName].is_null()) {
                if (change.row_data[colName].is_string()) {
                  record.push_back(change.row_data[colName].get<std::string>());
                } else {
                  record.push_back(change.row_data[colName].dump());
                }
              } else {
                record.push_back("");
                allColumnsFound = false;
              }
            }

            if (allColumnsFound && record.size() == columnNames.size()) {
              recordsToUpsert.push_back(record);
              useRowData = true;
            }
          } catch (const std::exception &e) {
            Logger::warning(LogCategory::TRANSFER, "processChangeLogBatch",
                            "Failed to parse row_data for " +
                                table.schema_name + "." + table.table_name +
                                ": " + std::string(e.what()) +
                                ", falling back to SELECT");
          }
        }

        if (!useRowData) {
          std::string whereClause = "";
          for (size_t i = 0; i < pkColumns.size(); ++i) {
            if (i > 0) {
              whereClause += " AND ";
            }
            std::string pkValue = pkValues[i];
            if (pkValue == "NULL") {
              whereClause += escapeIdentifier(pkColumns[i]) + " IS NULL";
            } else {
              whereClause += escapeIdentifier(pkColumns[i]) + " = '" +
                             escapeSQL(pkValue) + "'";
            }
          }

          std::string selectQuery =
              "SELECT * FROM " + escapeIdentifier(table.schema_name) + "." +
              escapeIdentifier(table.table_name) + " WHERE " + whereClause;

          std::vector<std::vector<std::string>> recordResult =
              executeSourceQuery(selectQuery);

          if (!recordResult.empty() &&
              recordResult[0].size() == columnNames.size()) {
            recordsToUpsert.push_back(recordResult[0]);
          } else {
            Logger::warning(LogCategory::TRANSFER, "processChangeLogBatch",
                            "Record not found in source for " +
                                table.schema_name + "." + table.table_name +
                                " operation " +
                                std::string(1, change.operation));
          }
        }
      }
    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER, "processChangeLogBatch",
                    "Failed to process change for " + table.schema_name + "." +
                        table.table_name + ": " + std::string(e.what()));
    }
  }

  size_t deletedCount = 0;
  if (!deletedPKs.empty()) {
    std::string lowerSchemaName = table.schema_name;
    std::transform(lowerSchemaName.begin(), lowerSchemaName.end(),
                   lowerSchemaName.begin(), ::tolower);
    std::string lowerTableName = table.table_name;
    std::transform(lowerTableName.begin(), lowerTableName.end(),
                   lowerTableName.begin(), ::tolower);
    deletedCount = deleteRecordsByPrimaryKey(
        pgConn, lowerSchemaName, lowerTableName, deletedPKs, pkColumns);
  }

  size_t upsertedCount = 0;
  if (!recordsToUpsert.empty()) {
    std::string lowerSchemaName = table.schema_name;
    std::transform(lowerSchemaName.begin(), lowerSchemaName.end(),
                   lowerSchemaName.begin(), ::tolower);
    std::string lowerTableName = table.table_name;
    std::transform(lowerTableName.begin(), lowerTableName.end(),
                   lowerTableName.begin(), ::tolower);
    try {
      performBulkUpsert(pgConn, recordsToUpsert, columnNames, columnTypes,
                        lowerSchemaName, lowerTableName, table.schema_name);
      upsertedCount = recordsToUpsert.size();
    } catch (const std::exception &e) {
      Logger::error(LogCategory::TRANSFER, "processChangeLogBatch",
                    "Failed to upsert records for " + table.schema_name + "." +
                        table.table_name + ": " + std::string(e.what()));
    }
  }

  Logger::info(LogCategory::TRANSFER, "processChangeLogBatch",
               "Processed batch for " + table.schema_name + "." +
                   table.table_name + " with " +
                   std::to_string(changes.size()) +
                   " changes: " + std::to_string(upsertedCount) + " upserts, " +
                   std::to_string(deletedCount) + " deletes");
}

void ChangeLogCDC::updateLastChangeId(pqxx::connection &pgConn,
                                      const TableInfo &table,
                                      long long changeId,
                                      const std::string &dbEngine) {
  try {
    std::lock_guard<std::mutex> lock(metadataUpdateMutex);
    pqxx::work txn(pgConn);
    std::string updateQuery =
        "UPDATE metadata.catalog SET sync_metadata = "
        "COALESCE(sync_metadata, '{}'::jsonb) || "
        "jsonb_build_object('last_change_id', " +
        std::to_string(changeId) +
        ") WHERE schema_name=" + txn.quote(table.schema_name) +
        " AND table_name=" + txn.quote(table.table_name) +
        " AND db_engine=" + txn.quote(dbEngine);
    txn.exec(updateQuery);
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "updateLastChangeId",
                  "Error updating last_change_id for " + table.schema_name +
                      "." + table.table_name + ": " + std::string(e.what()));
  }
}
