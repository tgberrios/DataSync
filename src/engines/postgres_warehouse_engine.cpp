#include "engines/postgres_warehouse_engine.h"
#include "third_party/json.hpp"
#include <algorithm>
#include <pqxx/pqxx>

PostgresWarehouseEngine::PostgresWarehouseEngine(std::string connectionString)
    : connectionString_(std::move(connectionString)) {}

std::unique_ptr<pqxx::connection> PostgresWarehouseEngine::getConnection() {
  try {
    return std::make_unique<pqxx::connection>(connectionString_);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER,
                  "PostgresWarehouseEngine::getConnection",
                  "Failed to create connection: " + std::string(e.what()));
    throw;
  }
}

bool PostgresWarehouseEngine::testConnection() {
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
    Logger::error(LogCategory::TRANSFER,
                  "PostgresWarehouseEngine::testConnection",
                  "Connection test failed: " + std::string(e.what()));
    return false;
  }
}

void PostgresWarehouseEngine::createSchema(const std::string &schemaName) {
  try {
    auto conn = getConnection();
    pqxx::work txn(*conn);
    std::string lowerSchema = schemaName;
    std::transform(lowerSchema.begin(), lowerSchema.end(), lowerSchema.begin(),
                   ::tolower);
    txn.exec("CREATE SCHEMA IF NOT EXISTS " + txn.quote_name(lowerSchema));
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER,
                  "PostgresWarehouseEngine::createSchema",
                  "Error creating schema: " + std::string(e.what()));
    throw;
  }
}

void PostgresWarehouseEngine::createTable(
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
      std::string nullable = col.is_nullable ? "" : " NOT NULL";
      createSQL += txn.quote_name(lowerCol) + " " + col.data_type + nullable;
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
    Logger::error(LogCategory::TRANSFER, "PostgresWarehouseEngine::createTable",
                  "Error creating table: " + std::string(e.what()));
    throw;
  }
}

void PostgresWarehouseEngine::insertData(
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
    Logger::error(LogCategory::TRANSFER, "PostgresWarehouseEngine::insertData",
                  "Error inserting data: " + std::string(e.what()));
    throw;
  }
}

void PostgresWarehouseEngine::upsertData(
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

    std::string upsertSQL = "INSERT INTO " + txn.quote_name(lowerSchema) + "." +
                            txn.quote_name(lowerTable) + " (";

    for (size_t i = 0; i < columns.size(); ++i) {
      if (i > 0)
        upsertSQL += ", ";
      std::string lowerCol = columns[i];
      std::transform(lowerCol.begin(), lowerCol.end(), lowerCol.begin(),
                     ::tolower);
      upsertSQL += txn.quote_name(lowerCol);
    }
    upsertSQL += ") VALUES ";

    for (size_t rowIdx = 0; rowIdx < rows.size(); ++rowIdx) {
      if (rowIdx > 0)
        upsertSQL += ", ";
      upsertSQL += "(";
      for (size_t colIdx = 0; colIdx < columns.size(); ++colIdx) {
        if (colIdx > 0)
          upsertSQL += ", ";
        if (colIdx < rows[rowIdx].size() && !rows[rowIdx][colIdx].empty()) {
          upsertSQL += txn.quote(rows[rowIdx][colIdx]);
        } else {
          upsertSQL += "NULL";
        }
      }
      upsertSQL += ")";
    }

    if (!primaryKeys.empty()) {
      upsertSQL += " ON CONFLICT (";
      for (size_t i = 0; i < primaryKeys.size(); ++i) {
        if (i > 0)
          upsertSQL += ", ";
        std::string lowerPK = primaryKeys[i];
        std::transform(lowerPK.begin(), lowerPK.end(), lowerPK.begin(),
                       ::tolower);
        upsertSQL += txn.quote_name(lowerPK);
      }
      upsertSQL += ") DO UPDATE SET ";
      for (size_t i = 0; i < columns.size(); ++i) {
        if (i > 0)
          upsertSQL += ", ";
        std::string lowerCol = columns[i];
        std::transform(lowerCol.begin(), lowerCol.end(), lowerCol.begin(),
                       ::tolower);
        upsertSQL += txn.quote_name(lowerCol) + " = EXCLUDED." +
                     txn.quote_name(lowerCol);
      }
    }

    txn.exec(upsertSQL);
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "PostgresWarehouseEngine::upsertData",
                  "Error upserting data: " + std::string(e.what()));
    throw;
  }
}

void PostgresWarehouseEngine::createIndex(
    const std::string &schemaName, const std::string &tableName,
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
    Logger::error(LogCategory::TRANSFER, "PostgresWarehouseEngine::createIndex",
                  "Error creating index: " + std::string(e.what()));
    throw;
  }
}

void PostgresWarehouseEngine::createPartition(
    const std::string &schemaName, const std::string &tableName,
    const std::string &partitionColumn) {
  Logger::info(LogCategory::TRANSFER,
               "PostgresWarehouseEngine::createPartition",
               "Partitioning should be specified during table creation. "
               "Use PARTITION BY in CREATE TABLE.");
}

void PostgresWarehouseEngine::executeStatement(const std::string &statement) {
  try {
    auto conn = getConnection();
    pqxx::work txn(*conn);
    txn.exec(statement);
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER,
                  "PostgresWarehouseEngine::executeStatement",
                  "Error executing statement: " + std::string(e.what()));
    throw;
  }
}

std::vector<json>
PostgresWarehouseEngine::executeQuery(const std::string &query) {
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
    Logger::error(LogCategory::TRANSFER,
                  "PostgresWarehouseEngine::executeQuery",
                  "Error executing query: " + std::string(e.what()));
    throw;
  }
  return results;
}

std::string
PostgresWarehouseEngine::quoteIdentifier(const std::string &identifier) {
  try {
    auto conn = getConnection();
    pqxx::work txn(*conn);
    return txn.quote_name(identifier);
  } catch (...) {
    return "\"" + identifier + "\"";
  }
}

std::string PostgresWarehouseEngine::quoteValue(const std::string &value) {
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
