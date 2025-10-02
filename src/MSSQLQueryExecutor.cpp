#include "MSSQLQueryExecutor.h"
#include <algorithm>
#include <set>
#include <sstream>

std::vector<TableInfo>
MSSQLQueryExecutor::getActiveTables(pqxx::connection &pgConn) {
  std::vector<TableInfo> data;

  try {
    pqxx::work txn(pgConn);
    auto results = txn.exec(
        "SELECT schema_name, table_name, cluster_name, db_engine, "
        "connection_string, last_sync_time, last_sync_column, "
        "status, last_offset, last_processed_pk, pk_strategy, "
        "pk_columns, candidate_columns, has_pk, table_size "
        "FROM metadata.catalog "
        "WHERE active=true AND db_engine='MSSQL' AND status != 'NO_DATA' "
        "ORDER BY table_size ASC, schema_name, table_name;");
    txn.commit();

    for (const auto &row : results) {
      if (row.size() < 15)
        continue;

      TableInfo t;
      t.schema_name = row[0].is_null() ? "" : row[0].as<std::string>();
      t.table_name = row[1].is_null() ? "" : row[1].as<std::string>();
      t.cluster_name = row[2].is_null() ? "" : row[2].as<std::string>();
      t.db_engine = row[3].is_null() ? "" : row[3].as<std::string>();
      t.connection_string = row[4].is_null() ? "" : row[4].as<std::string>();
      t.last_sync_time = row[5].is_null() ? "" : row[5].as<std::string>();
      t.last_sync_column = row[6].is_null() ? "" : row[6].as<std::string>();
      t.status = row[7].is_null() ? "" : row[7].as<std::string>();
      t.last_offset = row[8].is_null() ? "" : row[8].as<std::string>();
      t.last_processed_pk = row[9].is_null() ? "" : row[9].as<std::string>();
      t.pk_strategy = row[10].is_null() ? "" : row[10].as<std::string>();
      t.pk_columns = row[11].is_null() ? "" : row[11].as<std::string>();
      t.candidate_columns = row[12].is_null() ? "" : row[12].as<std::string>();
      t.has_pk = row[13].is_null() ? false : row[13].as<bool>();
      data.push_back(t);
    }
  } catch (const pqxx::sql_error &e) {
    Logger::getInstance().error(
        LogCategory::TRANSFER, "getActiveTables",
        "SQL ERROR getting active tables: " + std::string(e.what()) +
            " [SQL State: " + e.sqlstate() + "]");
  } catch (const pqxx::broken_connection &e) {
    Logger::getInstance().error(LogCategory::TRANSFER, "getActiveTables",
                                "CONNECTION ERROR getting active tables: " +
                                    std::string(e.what()));
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::TRANSFER, "getActiveTables",
                                "ERROR getting active tables: " +
                                    std::string(e.what()));
  }

  return data;
}

std::vector<std::string>
MSSQLQueryExecutor::getPrimaryKeyColumns(SQLHDBC mssqlConn,
                                         const std::string &schema_name,
                                         const std::string &table_name) {
  std::vector<std::string> pkColumns;

  // Validate input parameters
  if (!mssqlConn) {
    Logger::getInstance().error(LogCategory::TRANSFER, "getPrimaryKeyColumns",
                                "MSSQL connection is null");
    return pkColumns;
  }

  if (schema_name.empty() || table_name.empty()) {
    Logger::getInstance().error(LogCategory::TRANSFER, "getPrimaryKeyColumns",
                                "Schema name or table name is empty");
    return pkColumns;
  }

  std::string query =
      "SELECT c.name AS COLUMN_NAME "
      "FROM sys.columns c "
      "INNER JOIN sys.tables t ON c.object_id = t.object_id "
      "INNER JOIN sys.schemas s ON t.schema_id = s.schema_id "
      "INNER JOIN sys.index_columns ic ON c.object_id = ic.object_id AND "
      "c.column_id = ic.column_id "
      "INNER JOIN sys.indexes i ON ic.object_id = i.object_id AND "
      "ic.index_id = i.index_id "
      "WHERE s.name = '" +
      escapeSQL(schema_name) + "' AND t.name = '" + escapeSQL(table_name) +
      "' "
      "AND i.is_primary_key = 1 "
      "ORDER BY ic.key_ordinal;";

  std::vector<std::vector<std::string>> results =
      connectionManager.executeQueryMSSQL(mssqlConn, query);

  for (const auto &row : results) {
    if (!row.empty()) {
      std::string colName = row[0];
      std::transform(colName.begin(), colName.end(), colName.begin(),
                     ::tolower);
      pkColumns.push_back(colName);
    }
  }

  return pkColumns;
}

std::vector<std::vector<std::string>>
MSSQLQueryExecutor::findDeletedPrimaryKeys(
    SQLHDBC mssqlConn, const std::string &schema_name,
    const std::string &table_name,
    const std::vector<std::vector<std::string>> &pgPKs,
    const std::vector<std::string> &pkColumns) {

  std::vector<std::vector<std::string>> deletedPKs;

  if (pgPKs.empty() || pkColumns.empty()) {
    return deletedPKs;
  }

  // Procesar en batches para evitar consultas muy largas
  const size_t CHECK_BATCH_SIZE = 500;

  for (size_t batchStart = 0; batchStart < pgPKs.size();
       batchStart += CHECK_BATCH_SIZE) {
    size_t batchEnd = std::min(batchStart + CHECK_BATCH_SIZE, pgPKs.size());

    // Construir query para verificar existencia en MSSQL
    std::string checkQuery = "SELECT ";
    for (size_t i = 0; i < pkColumns.size(); ++i) {
      if (i > 0)
        checkQuery += ", ";
      checkQuery += "[" + pkColumns[i] + "]";
    }
    checkQuery += " FROM [" + schema_name + "].[" + table_name + "] WHERE (";

    for (size_t i = batchStart; i < batchEnd; ++i) {
      if (i > batchStart)
        checkQuery += " OR ";
      checkQuery += "(";
      for (size_t j = 0; j < pkColumns.size(); ++j) {
        if (j > 0)
          checkQuery += " AND ";
        std::string value = pgPKs[i][j];
        if (value == "NULL") {
          checkQuery += "[" + pkColumns[j] + "] IS NULL";
        } else {
          checkQuery += "[" + pkColumns[j] + "] = '" + escapeSQL(value) + "'";
        }
      }
      checkQuery += ")";
    }
    checkQuery += ");";

    // Ejecutar query en MSSQL
    auto existingResults =
        connectionManager.executeQueryMSSQL(mssqlConn, checkQuery);

    // Crear set de PKs que SÍ existen en MSSQL
    std::set<std::vector<std::string>> existingPKs;
    for (const auto &row : existingResults) {
      std::vector<std::string> pkValues;
      for (size_t i = 0; i < pkColumns.size() && i < row.size(); ++i) {
        pkValues.push_back(row[i]);
      }
      existingPKs.insert(pkValues);
    }

    // Encontrar PKs que NO existen en MSSQL (deleted)
    for (size_t i = batchStart; i < batchEnd; ++i) {
      if (existingPKs.find(pgPKs[i]) == existingPKs.end()) {
        deletedPKs.push_back(pgPKs[i]);
      }
    }
  }

  return deletedPKs;
}

std::string
MSSQLQueryExecutor::getPKStrategyFromCatalog(pqxx::connection &pgConn,
                                             const std::string &schema_name,
                                             const std::string &table_name) {
  try {
    pqxx::work txn(pgConn);
    auto result = txn.exec(
        "SELECT pk_strategy FROM metadata.catalog WHERE schema_name='" +
        escapeSQL(schema_name) + "' AND table_name='" + escapeSQL(table_name) +
        "';");
    txn.commit();

    if (!result.empty() && !result[0][0].is_null()) {
      return result[0][0].as<std::string>();
    }
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::TRANSFER,
                                "Error getting PK strategy: " +
                                    std::string(e.what()));
  }
  return "OFFSET";
}

std::vector<std::string>
MSSQLQueryExecutor::getPKColumnsFromCatalog(pqxx::connection &pgConn,
                                            const std::string &schema_name,
                                            const std::string &table_name) {
  try {
    pqxx::work txn(pgConn);
    auto result =
        txn.exec("SELECT pk_columns FROM metadata.catalog WHERE schema_name='" +
                 escapeSQL(schema_name) + "' AND table_name='" +
                 escapeSQL(table_name) + "';");
    txn.commit();

    if (!result.empty() && !result[0][0].is_null()) {
      std::string pkColumnsJson = result[0][0].as<std::string>();
      return parseJSONArray(pkColumnsJson);
    }
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::TRANSFER,
                                "Error getting PK columns: " +
                                    std::string(e.what()));
  }
  return {};
}

std::vector<std::string> MSSQLQueryExecutor::getCandidateColumnsFromCatalog(
    pqxx::connection &pgConn, const std::string &schema_name,
    const std::string &table_name) {
  try {
    pqxx::work txn(pgConn);
    auto result = txn.exec(
        "SELECT candidate_columns FROM metadata.catalog WHERE schema_name='" +
        escapeSQL(schema_name) + "' AND table_name='" + escapeSQL(table_name) +
        "';");
    txn.commit();

    if (!result.empty() && !result[0][0].is_null()) {
      std::string candidateColumnsJson = result[0][0].as<std::string>();
      return parseJSONArray(candidateColumnsJson);
    }
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::TRANSFER,
                                "Error getting candidate columns: " +
                                    std::string(e.what()));
  }
  return {};
}

std::string MSSQLQueryExecutor::getLastProcessedPKFromCatalog(
    pqxx::connection &pgConn, const std::string &schema_name,
    const std::string &table_name) {
  try {
    pqxx::work txn(pgConn);
    auto result = txn.exec(
        "SELECT last_processed_pk FROM metadata.catalog WHERE schema_name='" +
        escapeSQL(schema_name) + "' AND table_name='" + escapeSQL(table_name) +
        "';");
    txn.commit();

    if (!result.empty() && !result[0][0].is_null()) {
      return result[0][0].as<std::string>();
    }
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::TRANSFER,
                                "Error getting last processed PK: " +
                                    std::string(e.what()));
  }
  return "";
}

std::vector<std::string>
MSSQLQueryExecutor::parseJSONArray(const std::string &jsonArray) {
  std::vector<std::string> result;
  if (jsonArray.empty() || jsonArray == "[]")
    return result;

  std::string content = jsonArray;
  if (content.front() == '[')
    content = content.substr(1);
  if (content.back() == ']')
    content = content.substr(0, content.length() - 1);

  std::istringstream ss(content);
  std::string item;
  while (std::getline(ss, item, ',')) {
    item = item.substr(item.find_first_not_of(" \t\""));
    item = item.substr(0, item.find_last_not_of(" \t\"") + 1);
    if (!item.empty()) {
      result.push_back(item);
    }
  }
  return result;
}

std::string MSSQLQueryExecutor::getLastPKFromResults(
    const std::vector<std::vector<std::string>> &results,
    const std::vector<std::string> &pkColumns,
    const std::vector<std::string> &columnNames) {
  if (results.empty())
    return "";

  const auto &lastRow = results.back();
  std::string lastPK;

  for (size_t i = 0; i < pkColumns.size(); ++i) {
    if (i > 0)
      lastPK += "|";

    // Find the index of this PK column in the results
    size_t pkIndex = 0;
    for (size_t j = 0; j < columnNames.size(); ++j) {
      if (columnNames[j] == pkColumns[i]) {
        pkIndex = j;
        break;
      }
    }

    if (pkIndex < lastRow.size()) {
      lastPK += lastRow[pkIndex];
    }
  }

  return lastPK;
}

std::vector<std::string>
MSSQLQueryExecutor::parseLastPK(const std::string &lastPK) {
  std::vector<std::string> result;
  if (lastPK.empty())
    return result;

  std::istringstream ss(lastPK);
  std::string item;
  while (std::getline(ss, item, '|')) {
    if (!item.empty()) {
      result.push_back(item);
    }
  }
  return result;
}

std::string MSSQLQueryExecutor::escapeSQL(const std::string &value) {
  std::string escaped = value;
  size_t pos = 0;
  while ((pos = escaped.find("'", pos)) != std::string::npos) {
    escaped.replace(pos, 1, "''");
    pos += 2;
  }
  return escaped;
}
