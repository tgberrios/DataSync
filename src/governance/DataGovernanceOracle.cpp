#include "governance/DataGovernanceOracle.h"
#include "core/database_config.h"
#include "core/logger.h"
#include "engines/oracle_engine.h"
#include "utils/connection_utils.h"
#include <algorithm>
#include <iomanip>
#include <pqxx/pqxx>
#include <sstream>

DataGovernanceOracle::DataGovernanceOracle(const std::string &connectionString)
    : connectionString_(connectionString) {}

DataGovernanceOracle::~DataGovernanceOracle() {}

std::string
DataGovernanceOracle::extractServerName(const std::string &connectionString) {
  std::istringstream ss(connectionString);
  std::string token;
  while (std::getline(ss, token, ';')) {
    auto pos = token.find('=');
    if (pos == std::string::npos)
      continue;
    std::string key = token.substr(0, pos);
    std::string value = token.substr(pos + 1);
    key.erase(0, key.find_first_not_of(" \t\r\n"));
    key.erase(key.find_last_not_of(" \t\r\n") + 1);
    value.erase(0, value.find_first_not_of(" \t\r\n"));
    value.erase(value.find_last_not_of(" \t\r\n") + 1);
    if (key == "host" || key == "HOST") {
      return value;
    }
  }
  return "UNKNOWN";
}

std::string
DataGovernanceOracle::extractSchemaName(const std::string &connectionString) {
  std::istringstream ss(connectionString);
  std::string token;
  while (std::getline(ss, token, ';')) {
    auto pos = token.find('=');
    if (pos == std::string::npos)
      continue;
    std::string key = token.substr(0, pos);
    std::string value = token.substr(pos + 1);
    key.erase(0, key.find_first_not_of(" \t\r\n"));
    key.erase(key.find_last_not_of(" \t\r\n") + 1);
    value.erase(0, value.find_first_not_of(" \t\r\n"));
    value.erase(value.find_last_not_of(" \t\r\n") + 1);
    if (key == "user" || key == "USER") {
      return value;
    }
  }
  return "";
}

std::string DataGovernanceOracle::escapeSQL(const std::string &str) {
  std::string escaped = str;
  size_t pos = 0;
  while ((pos = escaped.find('\'', pos)) != std::string::npos) {
    escaped.replace(pos, 1, "''");
    pos += 2;
  }
  return escaped;
}

std::vector<std::vector<std::string>>
DataGovernanceOracle::executeQuery(OCIConnection *conn,
                                   const std::string &query) {
  std::vector<std::vector<std::string>> results;
  if (!conn || !conn->isValid()) {
    Logger::error(LogCategory::GOVERNANCE, "DataGovernanceOracle",
                  "Invalid Oracle connection");
    return results;
  }

  OCIStmt *stmt = nullptr;
  OCIError *err = conn->getErr();
  OCISvcCtx *svc = conn->getSvc();
  OCIEnv *env = conn->getEnv();
  sword status =
      OCIHandleAlloc((dvoid *)env, (dvoid **)&stmt, OCI_HTYPE_STMT, 0, nullptr);
  if (status != OCI_SUCCESS) {
    Logger::error(LogCategory::GOVERNANCE, "DataGovernanceOracle",
                  "OCIHandleAlloc(STMT) failed");
    return results;
  }

  status = OCIStmtPrepare(stmt, err, (OraText *)query.c_str(), query.length(),
                          OCI_NTV_SYNTAX, OCI_DEFAULT);
  if (status != OCI_SUCCESS) {
    Logger::error(LogCategory::GOVERNANCE, "DataGovernanceOracle",
                  "OCIStmtPrepare failed for query: " + query);
    OCIHandleFree(stmt, OCI_HTYPE_STMT);
    return results;
  }

  status = OCIStmtExecute(svc, stmt, err, 0, 0, nullptr, nullptr, OCI_DEFAULT);
  if (status != OCI_SUCCESS && status != OCI_SUCCESS_WITH_INFO) {
    char errbuf[512];
    sb4 errcode = 0;
    OCIErrorGet(err, 1, nullptr, &errcode, (OraText *)errbuf, sizeof(errbuf),
                OCI_HTYPE_ERROR);
    Logger::error(LogCategory::GOVERNANCE, "DataGovernanceOracle",
                  "OCIStmtExecute failed for query: " + query +
                      " - Error: " + std::string(errbuf) +
                      " (code: " + std::to_string(errcode) + ")");
    OCIHandleFree(stmt, OCI_HTYPE_STMT);
    return results;
  }

  ub4 numCols = 0;
  status = OCIAttrGet(stmt, OCI_HTYPE_STMT, &numCols, nullptr,
                      OCI_ATTR_PARAM_COUNT, err);
  if (status != OCI_SUCCESS || numCols == 0) {
    Logger::error(LogCategory::GOVERNANCE, "DataGovernanceOracle",
                  "OCIAttrGet(PARAM_COUNT) failed or no columns");
    OCIHandleFree(stmt, OCI_HTYPE_STMT);
    return results;
  }

  constexpr ub4 MAX_COLUMN_SIZE = 32768;
  std::vector<OCIDefine *> defines(numCols);
  std::vector<std::vector<char>> buffers(numCols);
  std::vector<ub2> lengths(numCols);
  std::vector<sb2> inds(numCols);

  for (ub4 i = 0; i < numCols; ++i) {
    buffers[i].resize(MAX_COLUMN_SIZE);
    status = OCIDefineByPos(stmt, &defines[i], err, i + 1, buffers[i].data(),
                            MAX_COLUMN_SIZE, SQLT_STR, &inds[i], &lengths[i],
                            nullptr, OCI_DEFAULT);
    if (status != OCI_SUCCESS) {
      Logger::error(LogCategory::GOVERNANCE, "DataGovernanceOracle",
                    "OCIDefineByPos failed for column " +
                        std::to_string(i + 1));
      OCIHandleFree(stmt, OCI_HTYPE_STMT);
      return results;
    }
  }

  sword fetchStatus;
  while ((fetchStatus = OCIStmtFetch(stmt, err, 1, OCI_FETCH_NEXT,
                                     OCI_DEFAULT)) == OCI_SUCCESS ||
         fetchStatus == OCI_SUCCESS_WITH_INFO) {
    std::vector<std::string> row;
    for (ub4 i = 0; i < numCols; ++i) {
      if (inds[i] == -1 || inds[i] == OCI_IND_NULL) {
        row.push_back("");
      } else if (lengths[i] > 0) {
        ub4 copyLen =
            (lengths[i] < MAX_COLUMN_SIZE) ? lengths[i] : MAX_COLUMN_SIZE;
        std::string cellValue(buffers[i].data(), copyLen);
        if (lengths[i] >= MAX_COLUMN_SIZE) {
          cellValue += "...(truncated)";
        }
        row.push_back(cellValue);
      } else {
        row.push_back("");
      }
    }
    results.push_back(row);
  }

  if (fetchStatus != OCI_NO_DATA && fetchStatus != OCI_SUCCESS) {
    char errbuf[512];
    sb4 errcode = 0;
    OCIErrorGet(err, 1, nullptr, &errcode, (OraText *)errbuf, sizeof(errbuf),
                OCI_HTYPE_ERROR);
    Logger::error(LogCategory::GOVERNANCE, "DataGovernanceOracle",
                  "OCIStmtFetch failed: " + std::string(errbuf) +
                      " (code: " + std::to_string(errcode) + ")");
  }

  OCIHandleFree(stmt, OCI_HTYPE_STMT);
  return results;
}

void DataGovernanceOracle::collectGovernanceData() {
  Logger::info(LogCategory::GOVERNANCE, "DataGovernanceOracle",
               "Starting governance data collection for Oracle");

  {
    std::lock_guard<std::mutex> lock(governanceDataMutex_);
    governanceData_.clear();
  }

  try {
    queryTableStats();
    queryServerConfig();
    queryIndexStats();
    calculateHealthScores();

    Logger::info(LogCategory::GOVERNANCE, "DataGovernanceOracle",
                 "Governance data collection completed. Collected " +
                     std::to_string([this]() {
                       std::lock_guard<std::mutex> lock(governanceDataMutex_);
                       return governanceData_.size();
                     }()) +
                     " records");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "DataGovernanceOracle",
                  "Error collecting governance data: " + std::string(e.what()));
  }
}

void DataGovernanceOracle::queryServerConfig() {
  try {
    auto conn = std::make_unique<OCIConnection>(connectionString_);
    if (!conn || !conn->isValid()) {
      Logger::error(LogCategory::GOVERNANCE, "DataGovernanceOracle",
                    "Failed to connect to Oracle for server config");
      return;
    }

    std::string serverName = extractServerName(connectionString_);
    std::string schemaName = extractSchemaName(connectionString_);

    std::string version = "";
    try {
      std::string query =
          "SELECT banner FROM v$version WHERE banner LIKE 'Oracle%'";
      auto results = executeQuery(conn.get(), query);
      if (!results.empty() && !results[0].empty()) {
        version = results[0][0];
      }
    } catch (const std::exception &e) {
      Logger::warning(
          LogCategory::GOVERNANCE, "DataGovernanceOracle",
          "Could not query v$version (may require special privileges): " +
              std::string(e.what()));
    }

    int blockSize = 8192;
    try {
      std::string blockSizeQuery =
          "SELECT value FROM v$parameter WHERE name = 'db_block_size'";
      auto blockSizeResults = executeQuery(conn.get(), blockSizeQuery);
      if (!blockSizeResults.empty() && !blockSizeResults[0].empty()) {
        try {
          blockSize = std::stoi(blockSizeResults[0][0]);
        } catch (...) {
        }
      }
    } catch (const std::exception &e) {
      Logger::warning(LogCategory::GOVERNANCE, "DataGovernanceOracle",
                      "Could not query v$parameter (may require special "
                      "privileges), using default block size 8192: " +
                          std::string(e.what()));
    }

    {
      std::lock_guard<std::mutex> lock(governanceDataMutex_);
      for (auto &data : governanceData_) {
        if (data.schema_name == schemaName) {
          data.version = version;
          data.block_size = blockSize;
        }
      }
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "DataGovernanceOracle",
                  "Error querying server config: " + std::string(e.what()));
  }
}

void DataGovernanceOracle::queryTableStats() {
  try {
    auto conn = std::make_unique<OCIConnection>(connectionString_);
    if (!conn || !conn->isValid()) {
      Logger::error(LogCategory::GOVERNANCE, "DataGovernanceOracle",
                    "Failed to connect to Oracle for table stats");
      return;
    }

    std::string serverName = extractServerName(connectionString_);
    std::string schemaName = extractSchemaName(connectionString_);

    std::string upperSchema = schemaName;
    std::transform(upperSchema.begin(), upperSchema.end(), upperSchema.begin(),
                   ::toupper);
    std::string escapedSchema = escapeSQL(upperSchema);

    std::string query = "SELECT "
                        "owner, "
                        "table_name, "
                        "tablespace_name, "
                        "num_rows, "
                        "blocks, "
                        "empty_blocks, "
                        "avg_row_len, "
                        "chain_cnt, "
                        "avg_space, "
                        "compression, "
                        "logging, "
                        "partitioned, "
                        "iot_type, "
                        "temporary "
                        "FROM all_tables "
                        "WHERE owner = '" +
                        escapedSchema + "'";

    auto results = executeQuery(conn.get(), query);

    for (const auto &row : results) {
      if (row.size() >= 14) {
        OracleGovernanceData data;
        data.server_name = serverName;
        data.schema_name = row[0];
        data.table_name = row[1];
        data.tablespace_name = row[2];

        try {
          if (!row[3].empty() && row[3] != "NULL")
            data.num_rows = std::stoll(row[3]);
          if (!row[4].empty() && row[4] != "NULL")
            data.blocks = std::stoll(row[4]);
          if (!row[5].empty() && row[5] != "NULL")
            data.empty_blocks = std::stoll(row[5]);
          if (!row[6].empty() && row[6] != "NULL")
            data.avg_row_len = std::stoll(row[6]);
          if (!row[7].empty() && row[7] != "NULL")
            data.chain_cnt = std::stoll(row[7]);
          if (!row[8].empty() && row[8] != "NULL")
            data.avg_space = std::stoll(row[8]);
        } catch (const std::exception &e) {
          Logger::warning(LogCategory::GOVERNANCE, "DataGovernanceOracle",
                          "Error parsing table stats: " +
                              std::string(e.what()));
        }

        data.compression = row[9];
        data.logging = row[10];
        data.partitioned = row[11];
        data.iot_type = row[12];
        data.temporary = row[13];

        // Calculate table size (approximate)
        // Oracle stores size in blocks, convert to MB
        // Assuming default block size of 8KB
        int blockSize = 8192;
        if (data.blocks > 0) {
          data.table_size_mb = (data.blocks * blockSize) / (1024.0 * 1024.0);
        }

        data.row_count = data.num_rows;
        data.total_size_mb = data.table_size_mb;

        {
          std::lock_guard<std::mutex> lock(governanceDataMutex_);
          governanceData_.push_back(data);
        }
      }
    }

    Logger::info(LogCategory::GOVERNANCE, "DataGovernanceOracle",
                 "Collected " + std::to_string(results.size()) +
                     " table stats records");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "DataGovernanceOracle",
                  "Error querying table stats: " + std::string(e.what()));
  }
}

void DataGovernanceOracle::queryIndexStats() {
  try {
    auto conn = std::make_unique<OCIConnection>(connectionString_);
    if (!conn || !conn->isValid()) {
      return;
    }

    std::string serverName = extractServerName(connectionString_);
    std::string schemaName = extractSchemaName(connectionString_);

    std::string upperSchema = schemaName;
    std::transform(upperSchema.begin(), upperSchema.end(), upperSchema.begin(),
                   ::toupper);
    std::string escapedSchema = escapeSQL(upperSchema);

    std::string query =
        "SELECT "
        "i.owner, "
        "i.table_name, "
        "i.index_name, "
        "LISTAGG(ic.column_name, ',') WITHIN GROUP (ORDER BY "
        "ic.column_position) AS index_columns, "
        "i.uniqueness, "
        "i.index_type, "
        "i.tablespace_name, "
        "i.num_rows, "
        "i.leaf_blocks "
        "FROM all_indexes i "
        "INNER JOIN all_ind_columns ic ON i.owner = ic.index_owner AND "
        "i.index_name = ic.index_name "
        "WHERE i.owner = '" +
        escapedSchema +
        "' "
        "GROUP BY i.owner, i.table_name, i.index_name, i.uniqueness, "
        "i.index_type, i.tablespace_name, i.num_rows, i.leaf_blocks";

    auto results = executeQuery(conn.get(), query);

    for (const auto &row : results) {
      if (row.size() >= 9) {
        std::string schemaName = row[0];
        std::string tableName = row[1];
        std::string indexName = row[2];

        bool found = false;
        {
          std::lock_guard<std::mutex> lock(governanceDataMutex_);
          for (auto &data : governanceData_) {
            if (data.schema_name == schemaName &&
                data.table_name == tableName && data.index_name.empty()) {
              data.index_name = indexName;
              data.index_columns = row[3];
              data.index_unique = (row[4] == "UNIQUE");
              data.index_type = row[5];
              found = true;
              break;
            }
          }

          if (!found && !indexName.empty()) {
            OracleGovernanceData indexData;
            indexData.server_name = serverName;
            indexData.schema_name = schemaName;
            indexData.table_name = tableName;
            indexData.index_name = indexName;
            indexData.index_columns = row[3];
            indexData.index_unique = (row[4] == "UNIQUE");
            indexData.index_type = row[5];

            // Copy table stats from main table record
            for (const auto &tableData : governanceData_) {
              if (tableData.schema_name == schemaName &&
                  tableData.table_name == tableName &&
                  tableData.index_name.empty()) {
                indexData.row_count = tableData.row_count;
                indexData.table_size_mb = tableData.table_size_mb;
                indexData.total_size_mb = tableData.total_size_mb;
                indexData.version = tableData.version;
                indexData.block_size = tableData.block_size;
                indexData.num_rows = tableData.num_rows;
                indexData.blocks = tableData.blocks;
                indexData.tablespace_name = tableData.tablespace_name;
                break;
              }
            }

            // Calculate index size
            int blockSize =
                indexData.block_size > 0 ? indexData.block_size : 8192;
            if (!row[8].empty() && row[8] != "NULL") {
              try {
                long long leafBlocks = std::stoll(row[8]);
                indexData.index_size_mb =
                    (leafBlocks * blockSize) / (1024.0 * 1024.0);
              } catch (...) {
              }
            }

            governanceData_.push_back(indexData);
          }
        }
      }
    }

    Logger::info(LogCategory::GOVERNANCE, "DataGovernanceOracle",
                 "Updated " + std::to_string(results.size()) +
                     " index stats records");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "DataGovernanceOracle",
                  "Error querying index stats: " + std::string(e.what()));
  }
}

void DataGovernanceOracle::calculateHealthScores() {
  std::lock_guard<std::mutex> lock(governanceDataMutex_);
  for (auto &data : governanceData_) {
    double score = 100.0;

    // Check fragmentation (chain count indicates row chaining)
    if (data.chain_cnt > 0 && data.num_rows > 0) {
      double chainRatio =
          (static_cast<double>(data.chain_cnt) / data.num_rows) * 100.0;
      if (chainRatio > 5.0) {
        double penalty = (chainRatio - 5.0) * 2.0;
        score -= std::min(penalty, 30.0);
      }
    }

    // Check empty blocks ratio
    if (data.blocks > 0 && data.empty_blocks > 0) {
      double emptyRatio =
          (static_cast<double>(data.empty_blocks) / data.blocks) * 100.0;
      if (emptyRatio > 20.0) {
        score -= 15.0;
      }
    }

    // Check if table is not logged (NOLOGGING)
    if (data.logging == "NO") {
      score -= 10.0;
    }

    data.health_score = std::max(0.0, std::min(100.0, score));

    if (data.health_score >= 80.0) {
      data.health_status = "HEALTHY";
    } else if (data.health_score >= 60.0) {
      data.health_status = "WARNING";
    } else {
      data.health_status = "CRITICAL";
    }

    if (data.recommendation_summary.empty()) {
      if (data.chain_cnt > 0 && data.num_rows > 0) {
        double chainRatio =
            (static_cast<double>(data.chain_cnt) / data.num_rows) * 100.0;
        if (chainRatio > 5.0) {
          data.recommendation_summary =
              "Consider reorganizing table due to row chaining (" +
              std::to_string(static_cast<int>(chainRatio)) + "%)";
        }
      } else if (data.blocks > 0 && data.empty_blocks > 0) {
        double emptyRatio =
            (static_cast<double>(data.empty_blocks) / data.blocks) * 100.0;
        if (emptyRatio > 20.0) {
          data.recommendation_summary =
              "Consider reorganizing table to reclaim empty blocks";
        }
      } else if (data.logging == "NO") {
        data.recommendation_summary = "Table is set to NOLOGGING - consider "
                                      "enabling logging for data protection";
      }
    }
  }
}

void DataGovernanceOracle::storeGovernanceData() {
  std::vector<OracleGovernanceData> dataCopy;
  {
    std::lock_guard<std::mutex> lock(governanceDataMutex_);
    if (governanceData_.empty()) {
      Logger::warning(LogCategory::GOVERNANCE, "DataGovernanceOracle",
                      "No governance data to store");
      return;
    }
    dataCopy = governanceData_;
  }

  try {
    pqxx::connection conn(DatabaseConfig::getPostgresConnectionString());
    if (!conn.is_open()) {
      Logger::error(LogCategory::GOVERNANCE, "DataGovernanceOracle",
                    "Failed to connect to PostgreSQL");
      return;
    }

    pqxx::work txn(conn);
    std::string createTableSQL =
        "CREATE TABLE IF NOT EXISTS metadata.data_governance_catalog_oracle ("
        "id SERIAL PRIMARY KEY,"
        "server_name VARCHAR(200) NOT NULL,"
        "schema_name VARCHAR(100) NOT NULL,"
        "table_name VARCHAR(100) NOT NULL,"
        "index_name VARCHAR(200),"
        "index_columns TEXT,"
        "index_unique BOOLEAN DEFAULT false,"
        "index_type VARCHAR(50),"
        "row_count BIGINT,"
        "table_size_mb DECIMAL(10,2),"
        "index_size_mb DECIMAL(10,2),"
        "total_size_mb DECIMAL(10,2),"
        "data_free_mb DECIMAL(10,2),"
        "fragmentation_pct DECIMAL(5,2),"
        "tablespace_name VARCHAR(100),"
        "version VARCHAR(100),"
        "block_size INTEGER,"
        "num_rows BIGINT,"
        "blocks BIGINT,"
        "empty_blocks BIGINT,"
        "avg_row_len BIGINT,"
        "chain_cnt BIGINT,"
        "avg_space BIGINT,"
        "compression VARCHAR(50),"
        "logging VARCHAR(10),"
        "partitioned VARCHAR(10),"
        "iot_type VARCHAR(50),"
        "temporary VARCHAR(10),"
        "access_frequency VARCHAR(20),"
        "health_status VARCHAR(20),"
        "recommendation_summary TEXT,"
        "health_score DECIMAL(5,2),"
        "snapshot_date TIMESTAMP DEFAULT NOW(),"
        "CONSTRAINT unique_oracle_governance UNIQUE (server_name, "
        "schema_name, table_name, index_name)"
        ");";

    txn.exec(createTableSQL);

    std::string createIndexSQL =
        "CREATE INDEX IF NOT EXISTS idx_oracle_gov_server_schema "
        "ON metadata.data_governance_catalog_oracle (server_name, schema_name);"
        "CREATE INDEX IF NOT EXISTS idx_oracle_gov_table "
        "ON metadata.data_governance_catalog_oracle (table_name);"
        "CREATE INDEX IF NOT EXISTS idx_oracle_gov_health "
        "ON metadata.data_governance_catalog_oracle (health_status);";

    txn.exec(createIndexSQL);
    txn.commit();

    int successCount = 0;
    int errorCount = 0;

    for (const auto &data : dataCopy) {
      if (data.server_name.empty() || data.schema_name.empty() ||
          data.table_name.empty()) {
        Logger::warning(LogCategory::GOVERNANCE, "DataGovernanceOracle",
                        "Skipping record with missing required fields");
        errorCount++;
        continue;
      }

      try {
        pqxx::work insertTxn(conn);

        std::ostringstream insertQuery;
        insertQuery
            << "INSERT INTO metadata.data_governance_catalog_oracle ("
            << "server_name, schema_name, table_name, "
            << "index_name, index_columns, index_unique, index_type, "
            << "row_count, table_size_mb, index_size_mb, total_size_mb, "
            << "data_free_mb, fragmentation_pct, tablespace_name, version, "
            << "block_size, num_rows, blocks, empty_blocks, avg_row_len, "
            << "chain_cnt, avg_space, compression, logging, partitioned, "
            << "iot_type, temporary, access_frequency, health_status, "
            << "recommendation_summary, health_score, snapshot_date"
            << ") VALUES (" << insertTxn.quote(data.server_name) << ", "
            << insertTxn.quote(data.schema_name) << ", "
            << insertTxn.quote(data.table_name) << ", "
            << (data.index_name.empty() ? "NULL"
                                        : insertTxn.quote(data.index_name))
            << ", "
            << (data.index_columns.empty()
                    ? "NULL"
                    : insertTxn.quote(data.index_columns))
            << ", " << (data.index_unique ? "true" : "false") << ", "
            << (data.index_type.empty() ? "NULL"
                                        : insertTxn.quote(data.index_type))
            << ", "
            << (data.row_count == 0 ? "NULL" : std::to_string(data.row_count))
            << ", "
            << (data.table_size_mb == 0.0 ? "NULL"
                                          : std::to_string(data.table_size_mb))
            << ", "
            << (data.index_size_mb == 0.0 ? "NULL"
                                          : std::to_string(data.index_size_mb))
            << ", "
            << (data.total_size_mb == 0.0 ? "NULL"
                                          : std::to_string(data.total_size_mb))
            << ", "
            << (data.data_free_mb == 0.0 ? "NULL"
                                         : std::to_string(data.data_free_mb))
            << ", "
            << (data.fragmentation_pct == 0.0
                    ? "NULL"
                    : std::to_string(data.fragmentation_pct))
            << ", "
            << (data.tablespace_name.empty()
                    ? "NULL"
                    : insertTxn.quote(data.tablespace_name))
            << ", "
            << (data.version.empty() ? "NULL" : insertTxn.quote(data.version))
            << ", "
            << (data.block_size == 0 ? "NULL" : std::to_string(data.block_size))
            << ", "
            << (data.num_rows == 0 ? "NULL" : std::to_string(data.num_rows))
            << ", " << (data.blocks == 0 ? "NULL" : std::to_string(data.blocks))
            << ", "
            << (data.empty_blocks == 0 ? "NULL"
                                       : std::to_string(data.empty_blocks))
            << ", "
            << (data.avg_row_len == 0 ? "NULL"
                                      : std::to_string(data.avg_row_len))
            << ", "
            << (data.chain_cnt == 0 ? "NULL" : std::to_string(data.chain_cnt))
            << ", "
            << (data.avg_space == 0 ? "NULL" : std::to_string(data.avg_space))
            << ", "
            << (data.compression.empty() ? "NULL"
                                         : insertTxn.quote(data.compression))
            << ", "
            << (data.logging.empty() ? "NULL" : insertTxn.quote(data.logging))
            << ", "
            << (data.partitioned.empty() ? "NULL"
                                         : insertTxn.quote(data.partitioned))
            << ", "
            << (data.iot_type.empty() ? "NULL" : insertTxn.quote(data.iot_type))
            << ", "
            << (data.temporary.empty() ? "NULL"
                                       : insertTxn.quote(data.temporary))
            << ", "
            << (data.access_frequency.empty()
                    ? "NULL"
                    : insertTxn.quote(data.access_frequency))
            << ", "
            << (data.health_status.empty()
                    ? "NULL"
                    : insertTxn.quote(data.health_status))
            << ", "
            << (data.recommendation_summary.empty()
                    ? "NULL"
                    : insertTxn.quote(data.recommendation_summary))
            << ", "
            << (data.health_score == 0.0 ? "NULL"
                                         : std::to_string(data.health_score))
            << ", "
            << "NOW()"
            << ") ON CONFLICT DO NOTHING;";

        insertTxn.exec(insertQuery.str());
        insertTxn.commit();
        successCount++;
      } catch (const std::exception &e) {
        Logger::error(LogCategory::GOVERNANCE, "DataGovernanceOracle",
                      "Error inserting record: " + std::string(e.what()));
        errorCount++;
        try {
          pqxx::work rollbackTxn(conn);
          rollbackTxn.abort();
        } catch (...) {
        }
      }
    }

    Logger::info(LogCategory::GOVERNANCE, "DataGovernanceOracle",
                 "Stored " + std::to_string(successCount) +
                     " governance records in PostgreSQL (errors: " +
                     std::to_string(errorCount) + ")");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "DataGovernanceOracle",
                  "Error storing governance data: " + std::string(e.what()));
  }
}

void DataGovernanceOracle::generateReport() {
  std::vector<OracleGovernanceData> dataCopy;
  {
    std::lock_guard<std::mutex> lock(governanceDataMutex_);
    dataCopy = governanceData_;
  }
  Logger::info(LogCategory::GOVERNANCE, "DataGovernanceOracle",
               "Generating governance report for " +
                   std::to_string(dataCopy.size()) + " records");

  int healthyCount = 0;
  int warningCount = 0;
  int criticalCount = 0;

  for (const auto &data : dataCopy) {
    if (data.health_status == "HEALTHY") {
      healthyCount++;
    } else if (data.health_status == "WARNING") {
      warningCount++;
    } else if (data.health_status == "CRITICAL") {
      criticalCount++;
    }
  }

  Logger::info(LogCategory::GOVERNANCE, "DataGovernanceOracle",
               "Report: Healthy=" + std::to_string(healthyCount) +
                   ", Warning=" + std::to_string(warningCount) +
                   ", Critical=" + std::to_string(criticalCount));
}
