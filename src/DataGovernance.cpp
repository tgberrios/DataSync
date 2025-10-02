#include "DataGovernance.h"
#include <algorithm>
#include <chrono>
#include <iomanip>
#include <sstream>

void DataGovernance::initialize() {
  createGovernanceTable();
  Logger::getInstance().info(LogCategory::GOVERNANCE, "Data Governance system initialized");
}

void DataGovernance::createGovernanceTable() {
  try {
    pqxx::connection conn(DatabaseConfig::getPostgresConnectionString());
    pqxx::work txn(conn);

    std::string createTableSQL =
        "CREATE TABLE IF NOT EXISTS metadata.data_governance_catalog ("
        "id SERIAL PRIMARY KEY,"
        "schema_name VARCHAR(100) NOT NULL,"
        "table_name VARCHAR(100) NOT NULL,"
        "total_columns INTEGER,"
        "total_rows BIGINT,"
        "table_size_mb DECIMAL(10,2),"
        "primary_key_columns VARCHAR(200),"
        "index_count INTEGER,"
        "constraint_count INTEGER,"
        "data_quality_score DECIMAL(5,2),"
        "null_percentage DECIMAL(5,2),"
        "duplicate_percentage DECIMAL(5,2),"
        "completeness_score DECIMAL(5,2),"
        "accuracy_score DECIMAL(5,2),"
        "consistency_score DECIMAL(5,2),"
        "validity_score DECIMAL(5,2),"
        "timeliness_score DECIMAL(5,2),"
        "uniqueness_score DECIMAL(5,2),"
        "integrity_score DECIMAL(5,2),"
        "inferred_source_engine VARCHAR(50),"
        "first_discovered TIMESTAMP DEFAULT NOW(),"
        "last_analyzed TIMESTAMP,"
        "last_accessed TIMESTAMP,"
        "access_frequency VARCHAR(20),"
        "query_count_daily INTEGER,"
        "data_category VARCHAR(50),"
        "business_domain VARCHAR(100),"
        "sensitivity_level VARCHAR(20),"
        "data_classification VARCHAR(50),"
        "retention_policy VARCHAR(50),"
        "backup_frequency VARCHAR(20),"
        "health_status VARCHAR(20),"
        "last_vacuum TIMESTAMP,"
        "fragmentation_percentage DECIMAL(5,2),"
        "table_owner VARCHAR(100),"
        "creation_date TIMESTAMP,"
        "last_modified TIMESTAMP,"
        "table_comment TEXT,"
        "data_lineage TEXT,"
        "compliance_requirements TEXT,"
        "created_at TIMESTAMP DEFAULT NOW(),"
        "updated_at TIMESTAMP DEFAULT NOW(),"
        "CONSTRAINT unique_table UNIQUE (schema_name, table_name)"
        ");";

    txn.exec(createTableSQL);

    std::string createIndexesSQL =
        "CREATE INDEX IF NOT EXISTS idx_data_governance_schema_table "
        "ON metadata.data_governance_catalog (schema_name, table_name);"
        "CREATE INDEX IF NOT EXISTS idx_data_governance_source_engine "
        "ON metadata.data_governance_catalog (inferred_source_engine);"
        "CREATE INDEX IF NOT EXISTS idx_data_governance_health_status "
        "ON metadata.data_governance_catalog (health_status);";

    txn.exec(createIndexesSQL);
    txn.commit();

  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::GOVERNANCE, "createGovernanceTable",
                  "Error creating governance table: " + std::string(e.what()));
  }
}

void DataGovernance::runDiscovery() {
  try {
    std::vector<TableMetadata> tables = discoverTables();
    Logger::getInstance().info(LogCategory::GOVERNANCE,
                 "Discovered " + std::to_string(tables.size()) + " tables");

    for (const auto &table : tables) {
      try {
        storeMetadata(table);
      } catch (const std::exception &e) {
        Logger::getInstance().error(LogCategory::GOVERNANCE,
                      "Error processing table " + table.schema_name + "." +
                          table.table_name + ": " + std::string(e.what()));
      }
    }

    Logger::getInstance().info(LogCategory::GOVERNANCE,
                 "Data governance discovery completed");
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::GOVERNANCE,
                  "Error in discovery process: " + std::string(e.what()));
  }
}

std::vector<TableMetadata> DataGovernance::discoverTables() {
  std::vector<TableMetadata> tables;

  try {
    pqxx::connection conn(DatabaseConfig::getPostgresConnectionString());
    pqxx::work txn(conn);

    std::string discoverQuery =
        "SELECT table_schema, table_name "
        "FROM information_schema.tables "
        "WHERE table_schema NOT IN ('information_schema', 'pg_catalog', "
        "'pg_toast', 'pg_temp_1', 'pg_toast_temp_1', 'metadata') "
        "AND table_type = 'BASE TABLE' "
        "ORDER BY table_schema, table_name;";

    auto result = txn.exec(discoverQuery);
    txn.commit();

    for (const auto &row : result) {
      std::string schema_name = row[0].as<std::string>();
      std::string table_name = row[1].as<std::string>();

      TableMetadata metadata = extractTableMetadata(schema_name, table_name);
      tables.push_back(metadata);
    }

    Logger::getInstance().info(LogCategory::GOVERNANCE, "Discovered " +
                                              std::to_string(tables.size()) +
                                              " tables from DataLake");
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::GOVERNANCE,
                  "Error discovering tables: " + std::string(e.what()));
  }

  return tables;
}

TableMetadata
DataGovernance::extractTableMetadata(const std::string &schema_name,
                                     const std::string &table_name) {
  // Validate input parameters
  if (schema_name.empty() || table_name.empty()) {
    throw std::invalid_argument("Schema name and table name cannot be empty");
  }

  // Basic validation for SQL injection prevention
  if (schema_name.find("'") != std::string::npos ||
      schema_name.find(";") != std::string::npos ||
      table_name.find("'") != std::string::npos ||
      table_name.find(";") != std::string::npos) {
    throw std::invalid_argument(
        "Schema name and table name contain invalid characters");
  }

  TableMetadata metadata;
  metadata.schema_name = schema_name;
  metadata.table_name = table_name;

  try {
    pqxx::connection conn(DatabaseConfig::getPostgresConnectionString());

    // Verify that the table exists before analyzing
    pqxx::work checkTxn(conn);
    std::string checkTableQuery =
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema = '" +
        escapeSQL(schema_name) +
        "' "
        "AND table_name = '" +
        escapeSQL(table_name) + "';";

    auto checkResult = checkTxn.exec(checkTableQuery);
    checkTxn.commit();

    if (checkResult.empty() || checkResult[0][0].as<int>() == 0) {
      throw std::runtime_error("Table '" + schema_name + "." + table_name +
                               "' does not exist");
    }

    analyzeTableStructure(conn, schema_name, table_name, metadata);
    analyzeDataQuality(conn, schema_name, table_name, metadata);
    analyzeUsageStatistics(conn, schema_name, table_name, metadata);
    analyzeHealthStatus(conn, schema_name, table_name, metadata);

    classifier.classifyTable(metadata);
    inferSourceEngine(metadata);

    // Calculate enhanced quality scores using the quality calculator
    qualityCalculator.calculateQualityScores(metadata);
    metadata.last_analyzed = getCurrentTimestamp();

  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::GOVERNANCE, "Error extracting metadata for " +
                                               schema_name + "." + table_name +
                                               ": " + std::string(e.what()));
  }

  return metadata;
}

void DataGovernance::analyzeTableStructure(pqxx::connection &conn,
                                           const std::string &schema_name,
                                           const std::string &table_name,
                                           TableMetadata &metadata) {
  try {
    pqxx::work txn(conn);

    std::string columnCountQuery =
        "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = "
        "'" +
        escapeSQL(schema_name) + "' AND table_name = '" +
        escapeSQL(table_name) + "';";
    auto columnResult = txn.exec(columnCountQuery);
    if (!columnResult.empty()) {
      metadata.total_columns = columnResult[0][0].as<int>();
    }

    std::string rowCountQuery = "SELECT COUNT(*) FROM \"" +
                                escapeSQL(schema_name) + "\".\"" +
                                escapeSQL(table_name) + "\";";
    auto rowResult = txn.exec(rowCountQuery);
    if (!rowResult.empty()) {
      metadata.total_rows = rowResult[0][0].as<long long>();
    }

    std::string sizeQuery =
        "SELECT COALESCE(pg_total_relation_size(to_regclass('" +
        escapeSQL(schema_name) + ".\"" + escapeSQL(table_name) +
        "\"')), 0) as size_bytes;";
    auto sizeResult = txn.exec(sizeQuery);
    if (!sizeResult.empty()) {
      try {
        long long sizeBytes = sizeResult[0][0].as<long long>();
        metadata.table_size_mb =
            static_cast<double>(sizeBytes) / (1024.0 * 1024.0);
      } catch (...) {
        metadata.table_size_mb = 0.0;
      }
    }

    std::string pkQuery = "SELECT string_agg(column_name, ',') "
                          "FROM information_schema.table_constraints tc "
                          "JOIN information_schema.key_column_usage kcu ON "
                          "tc.constraint_name = kcu.constraint_name "
                          "WHERE tc.table_schema = '" +
                          escapeSQL(schema_name) +
                          "' "
                          "AND tc.table_name = '" +
                          escapeSQL(table_name) +
                          "' "
                          "AND tc.constraint_type = 'PRIMARY KEY';";
    auto pkResult = txn.exec(pkQuery);
    if (!pkResult.empty() && !pkResult[0][0].is_null()) {
      metadata.primary_key_columns = pkResult[0][0].as<std::string>();
    }

    std::string indexQuery =
        "SELECT COUNT(*) FROM pg_indexes WHERE schemaname = '" +
        escapeSQL(schema_name) + "' AND tablename = '" + escapeSQL(table_name) +
        "';";
    auto indexResult = txn.exec(indexQuery);
    if (!indexResult.empty()) {
      metadata.index_count = indexResult[0][0].as<int>();
    }

    std::string constraintQuery =
        "SELECT COUNT(*) FROM information_schema.table_constraints WHERE "
        "table_schema = '" +
        escapeSQL(schema_name) + "' AND table_name = '" +
        escapeSQL(table_name) + "';";
    auto constraintResult = txn.exec(constraintQuery);
    if (!constraintResult.empty()) {
      metadata.constraint_count = constraintResult[0][0].as<int>();
    }

    txn.commit();
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::GOVERNANCE,
                  "Error analyzing table structure: " + std::string(e.what()));
  }
}

void DataGovernance::analyzeDataQuality(pqxx::connection &conn,
                                        const std::string &schema_name,
                                        const std::string &table_name,
                                        TableMetadata &metadata) {
  try {
    pqxx::work txn(conn);

    // Analyze actual NULL values in data, not just nullable columns
    std::string nullQuery = "SELECT column_name, data_type, is_nullable "
                            "FROM information_schema.columns "
                            "WHERE table_schema = '" +
                            escapeSQL(schema_name) +
                            "' "
                            "AND table_name = '" +
                            escapeSQL(table_name) +
                            "' "
                            "ORDER BY ordinal_position;";

    auto nullResult = txn.exec(nullQuery);
    if (!nullResult.empty()) {
      int totalColumns = nullResult.size();
      int nullableColumns = 0;
      int columnsWithNulls = 0;

      for (const auto &columnRow : nullResult) {
        std::string columnName = columnRow[0].as<std::string>();
        std::string dataType = columnRow[1].as<std::string>();
        std::string isNullable = columnRow[2].as<std::string>();

        if (isNullable == "YES") {
          nullableColumns++;

          // Check for actual NULL values in this column
          std::string columnNullQuery = "SELECT COUNT(*) as total_rows, "
                                        "COUNT(*) FILTER (WHERE \"" +
                                        escapeSQL(columnName) +
                                        "\" IS NULL) as null_count "
                                        "FROM \"" +
                                        escapeSQL(schema_name) + "\".\"" +
                                        escapeSQL(table_name) + "\";";

          try {
            auto columnNullResult = txn.exec(columnNullQuery);
            if (!columnNullResult.empty()) {
              long long totalRows = columnNullResult[0][0].as<long long>();
              long long nullCount = columnNullResult[0][1].as<long long>();

              if (totalRows > 0 && nullCount > 0) {
                columnsWithNulls++;
              }
            }
          } catch (const std::exception &e) {
            // Skip columns that can't be analyzed (e.g., complex types)
            Logger::getInstance().warning(LogCategory::GOVERNANCE, "analyzeDataQuality",
                            "Could not analyze NULLs in column " + columnName +
                                ": " + e.what());
          }
        }
      }

      if (totalColumns > 0) {
        // Calculate percentage based on columns that actually have NULL values
        metadata.null_percentage =
            (double)columnsWithNulls / totalColumns * 100.0;
      }
    }

    // Use sampling for large tables to improve performance
    std::string duplicateQuery;
    if (metadata.total_rows > 1000000) {
      duplicateQuery = "SELECT COUNT(*) - COUNT(DISTINCT ctid) FROM \"" +
                       escapeSQL(schema_name) + "\".\"" +
                       escapeSQL(table_name) +
                       "\" TABLESAMPLE SYSTEM(10);"; // 10% sample
    } else {
      duplicateQuery = "SELECT COUNT(*) - COUNT(DISTINCT ctid) FROM \"" +
                       escapeSQL(schema_name) + "\".\"" +
                       escapeSQL(table_name) + "\";";
    }

    try {
      auto duplicateResult = txn.exec(duplicateQuery);
      if (!duplicateResult.empty() && metadata.total_rows > 0) {
        long long duplicates = duplicateResult[0][0].as<long long>();
        // Adjust percentage for sampled data
        if (metadata.total_rows > 1000000) {
          metadata.duplicate_percentage =
              (double)duplicates / (metadata.total_rows * 0.1) * 100.0;
        } else {
          metadata.duplicate_percentage =
              (double)duplicates / metadata.total_rows * 100.0;
        }
      }
    } catch (const pqxx::sql_error &e) {
      Logger::getInstance().warning(LogCategory::GOVERNANCE, "analyzeDataQuality",
                      "SQL error calculating duplicates: " +
                          std::string(e.what()));
      metadata.duplicate_percentage = 0.0;
    } catch (const std::exception &e) {
      Logger::getInstance().error(LogCategory::GOVERNANCE, "analyzeDataQuality",
                    "Error calculating duplicates: " + std::string(e.what()));
      metadata.duplicate_percentage = 0.0;
    }

    txn.commit();
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::GOVERNANCE,
                  "Error analyzing data quality: " + std::string(e.what()));
  }
}

void DataGovernance::analyzeUsageStatistics(pqxx::connection &conn,
                                            const std::string &schema_name,
                                            const std::string &table_name,
                                            TableMetadata &metadata) {
  try {
    pqxx::work txn(conn);

    // Add timeout and limits for performance
    std::string usageQuery = "SELECT "
                             "last_autoanalyze,"
                             "last_autovacuum,"
                             "seq_scan,"
                             "seq_tup_read,"
                             "idx_scan,"
                             "idx_tup_fetch,"
                             "n_tup_ins,"
                             "n_tup_upd,"
                             "n_tup_del "
                             "FROM pg_stat_user_tables "
                             "WHERE schemaname = '" +
                             escapeSQL(schema_name) +
                             "' "
                             "AND relname = '" +
                             escapeSQL(table_name) +
                             "' "
                             "LIMIT 1;";

    auto usageResult = txn.exec(usageQuery);
    if (!usageResult.empty()) {
      if (!usageResult[0][0].is_null()) {
        metadata.last_accessed = usageResult[0][0].as<std::string>();
      }
      if (!usageResult[0][1].is_null()) {
        metadata.last_vacuum = usageResult[0][1].as<std::string>();
      }

      long long seq_scan =
          usageResult[0][2].is_null() ? 0 : usageResult[0][2].as<long long>();
      long long idx_scan =
          usageResult[0][4].is_null() ? 0 : usageResult[0][4].as<long long>();
      long long n_tup_ins =
          usageResult[0][6].is_null() ? 0 : usageResult[0][6].as<long long>();
      long long n_tup_upd =
          usageResult[0][7].is_null() ? 0 : usageResult[0][7].as<long long>();
      long long n_tup_del =
          usageResult[0][8].is_null() ? 0 : usageResult[0][8].as<long long>();

      metadata.query_count_daily =
          seq_scan + idx_scan + n_tup_ins + n_tup_upd + n_tup_del;
      metadata.access_frequency = determineAccessFrequency(
          static_cast<int>(metadata.query_count_daily));
    } else {
      metadata.query_count_daily = 0;
      metadata.access_frequency = "LOW";
    }

    txn.commit();
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::GOVERNANCE,
                  "Error analyzing usage statistics: " + std::string(e.what()));
  }
}

void DataGovernance::analyzeHealthStatus(pqxx::connection &conn,
                                         const std::string &schema_name,
                                         const std::string &table_name,
                                         TableMetadata &metadata) {
  try {
    pqxx::work txn(conn);

    std::string healthQuery = "SELECT "
                              "n_dead_tup,"
                              "n_live_tup,"
                              "last_vacuum,"
                              "last_autovacuum "
                              "FROM pg_stat_user_tables "
                              "WHERE schemaname = '" +
                              escapeSQL(schema_name) +
                              "' "
                              "AND relname = '" +
                              escapeSQL(table_name) +
                              "' "
                              "LIMIT 1;";

    auto healthResult = txn.exec(healthQuery);
    if (!healthResult.empty()) {
      long long deadTuples = healthResult[0][0].as<long long>();
      long long liveTuples = healthResult[0][1].as<long long>();

      if (liveTuples > 0) {
        metadata.fragmentation_percentage =
            (double)deadTuples / liveTuples * 100.0;
      }

      if (!healthResult[0][2].is_null()) {
        metadata.last_vacuum = healthResult[0][2].as<std::string>();
      } else if (!healthResult[0][3].is_null()) {
        metadata.last_vacuum = healthResult[0][3].as<std::string>();
      }
    }

    metadata.health_status = determineHealthStatus(metadata);

    txn.commit();
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::GOVERNANCE,
                  "Error analyzing health status: " + std::string(e.what()));
  }
}

void DataGovernance::inferSourceEngine(TableMetadata &metadata) {
  try {
    pqxx::connection conn(DatabaseConfig::getPostgresConnectionString());
    pqxx::work txn(conn);

    std::string query =
        "SELECT db_engine FROM metadata.catalog WHERE schema_name = '" +
        escapeSQL(metadata.schema_name) + "' AND table_name = '" +
        escapeSQL(metadata.table_name) + "' LIMIT 1;";

    auto result = txn.exec(query);
    txn.commit();

    if (!result.empty()) {
      metadata.inferred_source_engine = result[0][0].as<std::string>();
    } else {
      metadata.inferred_source_engine = "UNKNOWN";
    }
  } catch (const std::exception &e) {
    metadata.inferred_source_engine = "UNKNOWN";
  }
}

void DataGovernance::storeMetadata(const TableMetadata &metadata) {
  try {
    pqxx::connection conn(DatabaseConfig::getPostgresConnectionString());
    pqxx::work txn(conn);

    std::string checkQuery =
        "SELECT COUNT(*) FROM metadata.data_governance_catalog WHERE "
        "schema_name = '" +
        escapeSQL(metadata.schema_name) + "' AND table_name = '" +
        escapeSQL(metadata.table_name) + "';";
    auto checkResult = txn.exec(checkQuery);

    if (!checkResult.empty() && checkResult[0][0].as<int>() > 0) {
      updateExistingMetadata(metadata);
    } else {
      std::string insertQuery =
          "INSERT INTO metadata.data_governance_catalog ("
          "schema_name, table_name, total_columns, total_rows, table_size_mb,"
          "primary_key_columns, index_count, constraint_count,"
          "data_quality_score, null_percentage, duplicate_percentage,"
          "inferred_source_engine, last_analyzed,"
          "last_accessed, access_frequency, query_count_daily,"
          "data_category, business_domain, sensitivity_level,"
          "health_status, last_vacuum, fragmentation_percentage"
          ") VALUES ("
          "'" +
          escapeSQL(metadata.schema_name) + "', '" +
          escapeSQL(metadata.table_name) + "'," +
          std::to_string(metadata.total_columns) + ", " +
          std::to_string(metadata.total_rows) + ", " +
          std::to_string(metadata.table_size_mb) +
          ","
          "'" +
          escapeSQL(metadata.primary_key_columns) + "', " +
          std::to_string(metadata.index_count) + ", " +
          std::to_string(metadata.constraint_count) + "," +
          std::to_string(metadata.data_quality_score) + ", " +
          std::to_string(metadata.null_percentage) + ", " +
          std::to_string(metadata.duplicate_percentage) +
          ","
          "'" +
          escapeSQL(metadata.inferred_source_engine) + "', NOW()," +
          (metadata.last_accessed.empty()
               ? "NULL"
               : "'" + escapeSQL(metadata.last_accessed) + "'") +
          ", '" + escapeSQL(metadata.access_frequency) + "', " +
          std::to_string(metadata.query_count_daily) +
          ","
          "'" +
          escapeSQL(metadata.data_category) + "', '" +
          escapeSQL(metadata.business_domain) + "', '" +
          escapeSQL(metadata.sensitivity_level) +
          "',"
          "'" +
          escapeSQL(metadata.health_status) + "', " +
          (metadata.last_vacuum.empty()
               ? "NULL"
               : "'" + escapeSQL(metadata.last_vacuum) + "'") +
          ", " + std::to_string(metadata.fragmentation_percentage) + ");";

      txn.exec(insertQuery);
    }

    txn.commit();
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::GOVERNANCE,
                  "Error storing metadata: " + std::string(e.what()));
  }
}

void DataGovernance::updateExistingMetadata(const TableMetadata &metadata) {
  try {
    pqxx::connection conn(DatabaseConfig::getPostgresConnectionString());
    pqxx::work txn(conn);

    std::string updateQuery =
        "UPDATE metadata.data_governance_catalog SET "
        "total_columns = " +
        std::to_string(metadata.total_columns) +
        ","
        "total_rows = " +
        std::to_string(metadata.total_rows) +
        ","
        "table_size_mb = " +
        std::to_string(metadata.table_size_mb) +
        ","
        "primary_key_columns = '" +
        escapeSQL(metadata.primary_key_columns) +
        "',"
        "index_count = " +
        std::to_string(metadata.index_count) +
        ","
        "constraint_count = " +
        std::to_string(metadata.constraint_count) +
        ","
        "data_quality_score = " +
        std::to_string(metadata.data_quality_score) +
        ","
        "null_percentage = " +
        std::to_string(metadata.null_percentage) +
        ","
        "duplicate_percentage = " +
        std::to_string(metadata.duplicate_percentage) +
        ","
        "inferred_source_engine = '" +
        escapeSQL(metadata.inferred_source_engine) +
        "',"
        "last_analyzed = NOW()," +
        "last_accessed = " +
        (metadata.last_accessed.empty()
             ? "NULL"
             : "'" + escapeSQL(metadata.last_accessed) + "'") +
        ","
        "access_frequency = '" +
        escapeSQL(metadata.access_frequency) +
        "',"
        "query_count_daily = " +
        std::to_string(metadata.query_count_daily) +
        ","
        "data_category = '" +
        escapeSQL(metadata.data_category) +
        "',"
        "business_domain = '" +
        escapeSQL(metadata.business_domain) +
        "',"
        "sensitivity_level = '" +
        escapeSQL(metadata.sensitivity_level) +
        "',"
        "health_status = '" +
        escapeSQL(metadata.health_status) +
        "',"
        "last_vacuum = " +
        (metadata.last_vacuum.empty()
             ? "NULL"
             : "'" + escapeSQL(metadata.last_vacuum) + "'") +
        ","
        "fragmentation_percentage = " +
        std::to_string(metadata.fragmentation_percentage) +
        ","
        "updated_at = NOW() "
        "WHERE schema_name = '" +
        escapeSQL(metadata.schema_name) +
        "' "
        "AND table_name = '" +
        escapeSQL(metadata.table_name) + "';";

    txn.exec(updateQuery);
    txn.commit();

  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::GOVERNANCE,
                  "Error updating metadata: " + std::string(e.what()));
  }
}

void DataGovernance::generateReport() {
  try {
    pqxx::connection conn(DatabaseConfig::getPostgresConnectionString());
    pqxx::work txn(conn);

    std::string reportQuery =
        "SELECT "
        "COUNT(*) as total_tables,"
        "COUNT(*) FILTER (WHERE health_status = 'HEALTHY') as healthy_tables,"
        "COUNT(*) FILTER (WHERE health_status = 'WARNING') as warning_tables,"
        "COUNT(*) FILTER (WHERE health_status = 'CRITICAL') as critical_tables,"
        "AVG(data_quality_score) as avg_quality_score,"
        "SUM(total_rows) as total_rows,"
        "SUM(table_size_mb) as total_size_mb "
        "FROM metadata.data_governance_catalog;";

    auto result = txn.exec(reportQuery);
    txn.commit();

    if (!result.empty()) {
      auto row = result[0];
      int totalTables = row[0].as<int>();
      int healthyTables = row[1].as<int>();
      int warningTables = row[2].as<int>();
      int criticalTables = row[3].as<int>();
      double avgQuality = row[4].is_null() ? 0.0 : row[4].as<double>();
      long long totalRows =
          row[5].is_null() ? 0 : static_cast<long long>(row[5].as<double>());
      double totalSize = row[6].is_null() ? 0.0 : row[6].as<double>();
    }
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::GOVERNANCE,
                  "Error generating report: " + std::string(e.what()));
  }
}

std::string DataGovernance::escapeSQL(const std::string &value) {
  std::string escaped = value;
  size_t pos = 0;
  while ((pos = escaped.find("'", pos)) != std::string::npos) {
    escaped.replace(pos, 1, "''");
    pos += 2;
  }
  return escaped;
}

std::string DataGovernance::getCurrentTimestamp() {
  auto now = std::chrono::system_clock::now();
  auto time_t = std::chrono::system_clock::to_time_t(now);
  auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                now.time_since_epoch()) %
            1000;

  std::stringstream ss;
  ss << std::put_time(std::localtime(&time_t), "%Y-%m-%d %H:%M:%S");
  ss << "." << std::setfill('0') << std::setw(3) << ms.count();
  return ss.str();
}

std::string DataGovernance::determineAccessFrequency(int query_count) {
  if (query_count > 10000)
    return "REAL_TIME";
  if (query_count > 1000)
    return "HIGH";
  if (query_count > 100)
    return "MEDIUM";
  if (query_count > 10)
    return "LOW";
  if (query_count > 0)
    return "RARE";
  return "ARCHIVED";
}

std::string
DataGovernance::determineHealthStatus(const TableMetadata &metadata) {
  if (metadata.fragmentation_percentage > 80.0 ||
      metadata.duplicate_percentage > 50.0 || metadata.null_percentage > 70.0) {
    return "EMERGENCY";
  }
  if (metadata.fragmentation_percentage > 50.0 ||
      metadata.duplicate_percentage > 20.0 || metadata.null_percentage > 50.0) {
    return "CRITICAL";
  }
  if (metadata.fragmentation_percentage > 20.0 ||
      metadata.duplicate_percentage > 10.0 || metadata.null_percentage > 30.0) {
    return "WARNING";
  }
  if (metadata.fragmentation_percentage == 0.0 &&
      metadata.duplicate_percentage == 0.0 && metadata.null_percentage < 5.0) {
    return "EXCELLENT";
  }
  return "HEALTHY";
}
