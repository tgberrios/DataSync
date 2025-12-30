#include "engines/redshift_engine.h"
#include "third_party/json.hpp"
#include <algorithm>
#include <pqxx/pqxx>

RedshiftEngine::RedshiftEngine(std::string connectionString)
    : connectionString_(std::move(connectionString)) {}

std::unique_ptr<pqxx::connection> RedshiftEngine::getConnection() {
  try {
    return std::make_unique<pqxx::connection>(connectionString_);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "RedshiftEngine::getConnection",
                  "Failed to create connection: " + std::string(e.what()));
    throw;
  }
}

bool RedshiftEngine::testConnection() {
  try {
    auto conn = getConnection();
    if (!conn->is_open()) {
      return false;
    }
    pqxx::work txn(*conn);
    txn.exec("SELECT 1");
    txn.commit();
    return true;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "RedshiftEngine::testConnection",
                  "Connection test failed: " + std::string(e.what()));
    return false;
  }
}

std::string RedshiftEngine::mapDataType(const std::string &dataType) {
  std::string upperType = dataType;
  std::transform(upperType.begin(), upperType.end(), upperType.begin(),
                 ::toupper);

  if (upperType.find("VARCHAR") != std::string::npos ||
      upperType.find("CHAR") != std::string::npos ||
      upperType.find("TEXT") != std::string::npos) {
    return "VARCHAR(MAX)";
  }
  if (upperType.find("INTEGER") != std::string::npos ||
      upperType.find("INT") != std::string::npos) {
    return "INTEGER";
  }
  if (upperType.find("BIGINT") != std::string::npos) {
    return "BIGINT";
  }
  if (upperType.find("DECIMAL") != std::string::npos ||
      upperType.find("NUMERIC") != std::string::npos) {
    return "DECIMAL(18,2)";
  }
  if (upperType.find("DOUBLE") != std::string::npos ||
      upperType.find("FLOAT") != std::string::npos ||
      upperType.find("REAL") != std::string::npos) {
    return "DOUBLE PRECISION";
  }
  if (upperType.find("BOOLEAN") != std::string::npos ||
      upperType.find("BOOL") != std::string::npos) {
    return "BOOLEAN";
  }
  if (upperType.find("DATE") != std::string::npos) {
    return "DATE";
  }
  if (upperType.find("TIMESTAMP") != std::string::npos ||
      upperType.find("DATETIME") != std::string::npos) {
    return "TIMESTAMP";
  }
  if (upperType.find("JSON") != std::string::npos ||
      upperType.find("JSONB") != std::string::npos) {
    return "SUPER";
  }

  return "VARCHAR(MAX)";
}

void RedshiftEngine::createSchema(const std::string &schemaName) {
  try {
    auto conn = getConnection();
    pqxx::work txn(*conn);
    std::string lowerSchema = schemaName;
    std::transform(lowerSchema.begin(), lowerSchema.end(), lowerSchema.begin(),
                   ::tolower);
    txn.exec("CREATE SCHEMA IF NOT EXISTS " + txn.quote_name(lowerSchema));
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "RedshiftEngine::createSchema",
                  "Error creating schema: " + std::string(e.what()));
    throw;
  }
}

void RedshiftEngine::createTable(
    const std::string &schemaName, const std::string &tableName,
    const std::vector<WarehouseColumnInfo> &columns,
    const std::vector<std::string> &primaryKeys) {
  try {
    auto conn = getConnection();
    pqxx::work txn(*conn);

    std::string lowerSchema = schemaName;
    std::transform(lowerSchema.begin(), lowerSchema.end(), lowerSchema.begin(),
                   ::tolower);
    std::string lowerTable = tableName;
    std::transform(lowerTable.begin(), lowerTable.end(), lowerTable.begin(),
                   ::tolower);

    std::string createSQL = "CREATE TABLE IF NOT EXISTS " +
                            txn.quote_name(lowerSchema) + "." +
                            txn.quote_name(lowerTable) + " (";

    bool first = true;
    for (const auto &col : columns) {
      if (!first)
        createSQL += ", ";
      std::string lowerCol = col.name;
      std::transform(lowerCol.begin(), lowerCol.end(), lowerCol.begin(),
                     ::tolower);
      std::string dataType = mapDataType(col.data_type);
      std::string nullable = col.is_nullable ? "" : " NOT NULL";
      createSQL += txn.quote_name(lowerCol) + " " + dataType + nullable;
      first = false;
    }

    if (!primaryKeys.empty()) {
      createSQL += ", PRIMARY KEY (";
      for (size_t i = 0; i < primaryKeys.size(); ++i) {
        if (i > 0)
          createSQL += ", ";
        std::string lowerPK = primaryKeys[i];
        std::transform(lowerPK.begin(), lowerPK.end(), lowerPK.begin(),
                       ::tolower);
        createSQL += txn.quote_name(lowerPK);
      }
      createSQL += ")";
    }

    createSQL += ")";

    txn.exec(createSQL);
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "RedshiftEngine::createTable",
                  "Error creating table: " + std::string(e.what()));
    throw;
  }
}

void RedshiftEngine::insertData(
    const std::string &schemaName, const std::string &tableName,
    const std::vector<std::string> &columns,
    const std::vector<std::vector<std::string>> &rows) {
  if (rows.empty())
    return;

  try {
    auto conn = getConnection();
    pqxx::work txn(*conn);

    std::string lowerSchema = schemaName;
    std::transform(lowerSchema.begin(), lowerSchema.end(), lowerSchema.begin(),
                   ::tolower);
    std::string lowerTable = tableName;
    std::transform(lowerTable.begin(), lowerTable.end(), lowerTable.begin(),
                   ::tolower);

    std::string insertSQL = "INSERT INTO " + txn.quote_name(lowerSchema) + "." +
                            txn.quote_name(lowerTable) + " (";

    for (size_t i = 0; i < columns.size(); ++i) {
      if (i > 0)
        insertSQL += ", ";
      std::string lowerCol = columns[i];
      std::transform(lowerCol.begin(), lowerCol.end(), lowerCol.begin(),
                     ::tolower);
      insertSQL += txn.quote_name(lowerCol);
    }
    insertSQL += ") VALUES ";

    for (size_t rowIdx = 0; rowIdx < rows.size(); ++rowIdx) {
      if (rowIdx > 0)
        insertSQL += ", ";
      insertSQL += "(";
      for (size_t colIdx = 0; colIdx < columns.size(); ++colIdx) {
        if (colIdx > 0)
          insertSQL += ", ";
        if (colIdx < rows[rowIdx].size() && !rows[rowIdx][colIdx].empty()) {
          insertSQL += txn.quote(rows[rowIdx][colIdx]);
        } else {
          insertSQL += "NULL";
        }
      }
      insertSQL += ")";
    }

    txn.exec(insertSQL);
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "RedshiftEngine::insertData",
                  "Error inserting data: " + std::string(e.what()));
    throw;
  }
}

void RedshiftEngine::upsertData(
    const std::string &schemaName, const std::string &tableName,
    const std::vector<std::string> &columns,
    const std::vector<std::string> &primaryKeys,
    const std::vector<std::vector<std::string>> &rows) {
  if (rows.empty())
    return;

  try {
    auto conn = getConnection();
    pqxx::work txn(*conn);

    std::string lowerSchema = schemaName;
    std::transform(lowerSchema.begin(), lowerSchema.end(), lowerSchema.begin(),
                   ::tolower);
    std::string lowerTable = tableName;
    std::transform(lowerTable.begin(), lowerTable.end(), lowerTable.begin(),
                   ::tolower);

    for (const auto &row : rows) {
      std::string upsertSQL = "BEGIN TRANSACTION; ";
      upsertSQL += "DELETE FROM " + txn.quote_name(lowerSchema) + "." +
                   txn.quote_name(lowerTable) + " WHERE ";

      for (size_t i = 0; i < primaryKeys.size(); ++i) {
        if (i > 0)
          upsertSQL += " AND ";
        std::string lowerPK = primaryKeys[i];
        std::transform(lowerPK.begin(), lowerPK.end(), lowerPK.begin(),
                       ::tolower);
        auto pkIdx = std::find(columns.begin(), columns.end(), primaryKeys[i]);
        if (pkIdx != columns.end()) {
          size_t idx = std::distance(columns.begin(), pkIdx);
          if (idx < row.size()) {
            upsertSQL += txn.quote_name(lowerPK) + " = " + txn.quote(row[idx]);
          }
        }
      }
      upsertSQL += "; ";

      upsertSQL += "INSERT INTO " + txn.quote_name(lowerSchema) + "." +
                   txn.quote_name(lowerTable) + " (";
      for (size_t i = 0; i < columns.size(); ++i) {
        if (i > 0)
          upsertSQL += ", ";
        std::string lowerCol = columns[i];
        std::transform(lowerCol.begin(), lowerCol.end(), lowerCol.begin(),
                       ::tolower);
        upsertSQL += txn.quote_name(lowerCol);
      }
      upsertSQL += ") VALUES (";
      for (size_t i = 0; i < columns.size(); ++i) {
        if (i > 0)
          upsertSQL += ", ";
        if (i < row.size() && !row[i].empty()) {
          upsertSQL += txn.quote(row[i]);
        } else {
          upsertSQL += "NULL";
        }
      }
      upsertSQL += "); COMMIT;";

      txn.exec(upsertSQL);
    }
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "RedshiftEngine::upsertData",
                  "Error upserting data: " + std::string(e.what()));
    throw;
  }
}

void RedshiftEngine::createIndex(const std::string &schemaName,
                                 const std::string &tableName,
                                 const std::vector<std::string> &indexColumns,
                                 const std::string &indexName) {
  try {
    auto conn = getConnection();
    pqxx::work txn(*conn);

    std::string lowerSchema = schemaName;
    std::transform(lowerSchema.begin(), lowerSchema.end(), lowerSchema.begin(),
                   ::tolower);
    std::string lowerTable = tableName;
    std::transform(lowerTable.begin(), lowerTable.end(), lowerTable.begin(),
                   ::tolower);

    std::string idxName = indexName.empty()
                              ? "idx_" + lowerTable + "_" + indexColumns[0]
                              : indexName;
    std::transform(idxName.begin(), idxName.end(), idxName.begin(), ::tolower);

    std::string createIndexSQL =
        "CREATE INDEX IF NOT EXISTS " + txn.quote_name(idxName) + " ON " +
        txn.quote_name(lowerSchema) + "." + txn.quote_name(lowerTable) + " (";

    for (size_t i = 0; i < indexColumns.size(); ++i) {
      if (i > 0)
        createIndexSQL += ", ";
      std::string lowerCol = indexColumns[i];
      std::transform(lowerCol.begin(), lowerCol.end(), lowerCol.begin(),
                     ::tolower);
      createIndexSQL += txn.quote_name(lowerCol);
    }
    createIndexSQL += ")";

    txn.exec(createIndexSQL);
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "RedshiftEngine::createIndex",
                  "Error creating index: " + std::string(e.what()));
    throw;
  }
}

void RedshiftEngine::createPartition(const std::string &schemaName,
                                     const std::string &tableName,
                                     const std::string &partitionColumn) {
  Logger::warning(LogCategory::TRANSFER, "RedshiftEngine::createPartition",
                  "Partitioning not directly supported in Redshift. "
                  "Use DISTKEY and SORTKEY in table creation instead.");
}

void RedshiftEngine::executeStatement(const std::string &statement) {
  try {
    auto conn = getConnection();
    pqxx::work txn(*conn);
    txn.exec(statement);
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "RedshiftEngine::executeStatement",
                  "Error executing statement: " + std::string(e.what()));
    throw;
  }
}

std::vector<json> RedshiftEngine::executeQuery(const std::string &query) {
  std::vector<json> results;
  try {
    auto conn = getConnection();
    pqxx::work txn(*conn);
    auto rows = txn.exec(query);

    if (rows.empty()) {
      return results;
    }

    std::vector<std::string> columnNames;
    for (const auto &field : rows[0]) {
      columnNames.push_back(field.name());
    }

    for (const auto &row : rows) {
      json rowObj = json::object();
      for (size_t i = 0; i < columnNames.size(); ++i) {
        if (row[i].is_null()) {
          rowObj[columnNames[i]] = nullptr;
        } else {
          rowObj[columnNames[i]] = row[i].as<std::string>();
        }
      }
      results.push_back(rowObj);
    }
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "RedshiftEngine::executeQuery",
                  "Error executing query: " + std::string(e.what()));
    throw;
  }
  return results;
}

std::string RedshiftEngine::quoteIdentifier(const std::string &identifier) {
  try {
    auto conn = getConnection();
    pqxx::work txn(*conn);
    return txn.quote_name(identifier);
  } catch (...) {
    return "\"" + identifier + "\"";
  }
}

std::string RedshiftEngine::quoteValue(const std::string &value) {
  try {
    auto conn = getConnection();
    pqxx::work txn(*conn);
    return txn.quote(value);
  } catch (...) {
    std::string escaped = value;
    size_t pos = 0;
    while ((pos = escaped.find('\'', pos)) != std::string::npos) {
      escaped.replace(pos, 1, "''");
      pos += 2;
    }
    return "'" + escaped + "'";
  }
}
