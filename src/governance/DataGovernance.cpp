#include "governance/DataGovernance.h"
#include "catalog/metadata_repository.h"
#include "core/database_config.h"
#include "engines/database_engine.h"
#include "governance/ColumnCatalogCollector.h"
#include "governance/DataGovernanceMSSQL.h"
#include "governance/DataGovernanceMariaDB.h"
#include "governance/DataGovernanceMongoDB.h"
#include "governance/DataGovernanceOracle.h"
#include "governance/LineageExtractorMSSQL.h"
#include "governance/LineageExtractorMariaDB.h"
#include "governance/LineageExtractorMongoDB.h"
#include "governance/LineageExtractorOracle.h"
#include "utils/string_utils.h"
#include "utils/time_utils.h"
#include <algorithm>

// Constructor for DataGovernance. Initializes the governance system by
// creating a DataClassifier instance for table classification. The classifier
// is used to determine data categories, business domains, sensitivity levels,
// and data classifications based on governance rules.
DataGovernance::DataGovernance()
    : classifier_(std::make_unique<DataClassifier>()) {}

// Initializes the DataGovernance system by creating the necessary database
// table structure. This function should be called before using any other
// governance functionality. If initialization fails, an error is logged but
// the system continues to operate (subsequent operations may fail).
void DataGovernance::initialize() {
  try {
    createGovernanceTable();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "initialize",
                  "Error initializing DataGovernance: " +
                      std::string(e.what()));
  }
}

// Creates the metadata.data_governance_catalog table and required indexes if
// they do not already exist. This table stores comprehensive metadata about
// tables including structure, quality metrics, usage statistics, health
// status, and classification information. The table includes a unique
// constraint on (schema_name, table_name) to prevent duplicates. Creates
// indexes on schema/table, source engine, and health status for efficient
// querying.
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
    Logger::error(LogCategory::GOVERNANCE, "createGovernanceTable",
                  "Error creating governance table: " + std::string(e.what()));
  }
}

// Runs the discovery process to analyze all tables in the PostgreSQL database
// and store their metadata in the governance catalog. Discovers all user
// tables (excluding system schemas), extracts comprehensive metadata for each
// table, and stores it in the database. If processing a table fails, logs an
// error and continues with the next table. This function can be time-consuming
// for large databases.
void DataGovernance::runDiscovery() {

  try {
    std::vector<TableMetadata> tables = discoverTables();

    for (const auto &table : tables) {
      try {
        storeMetadata(table);
      } catch (const std::exception &e) {
        Logger::error(LogCategory::GOVERNANCE, "runDiscovery",
                      "Error processing table " + table.schema_name + "." +
                          table.table_name + ": " + std::string(e.what()));
      }
    }

    try {
      MetadataRepository repo(DatabaseConfig::getPostgresConnectionString());

      std::vector<std::string> mssqlConnections =
          repo.getConnectionStrings("MSSQL");
      for (const auto &connStr : mssqlConnections) {
        if (!connStr.empty()) {
          try {
            Logger::info(LogCategory::GOVERNANCE, "runDiscovery",
                         "Collecting MSSQL governance data for connection");
            DataGovernanceMSSQL mssqlGov(connStr);
            mssqlGov.collectGovernanceData();
            mssqlGov.storeGovernanceData();
            mssqlGov.generateReport();
          } catch (const std::exception &e) {
            Logger::error(LogCategory::GOVERNANCE, "runDiscovery",
                          "Error collecting MSSQL governance data: " +
                              std::string(e.what()));
          }

          try {
            Logger::info(LogCategory::GOVERNANCE, "runDiscovery",
                         "Extracting MSSQL lineage for connection");
            LineageExtractorMSSQL lineageExtractor(connStr);
            lineageExtractor.extractLineage();
            lineageExtractor.storeLineage();
            Logger::info(LogCategory::GOVERNANCE, "runDiscovery",
                         "MSSQL lineage extraction completed");
          } catch (const std::exception &e) {
            Logger::error(LogCategory::GOVERNANCE, "runDiscovery",
                          "Error extracting MSSQL lineage: " +
                              std::string(e.what()));
          }
        }
      }

      std::vector<std::string> mariadbConnections =
          repo.getConnectionStrings("MariaDB");
      for (const auto &connStr : mariadbConnections) {
        if (!connStr.empty()) {
          try {
            Logger::info(LogCategory::GOVERNANCE, "runDiscovery",
                         "Collecting MariaDB governance data for connection");
            DataGovernanceMariaDB mariadbGov(connStr);
            mariadbGov.collectGovernanceData();
            mariadbGov.storeGovernanceData();
            mariadbGov.generateReport();
          } catch (const std::exception &e) {
            Logger::error(LogCategory::GOVERNANCE, "runDiscovery",
                          "Error collecting MariaDB governance data: " +
                              std::string(e.what()));
          }

          try {
            Logger::info(LogCategory::GOVERNANCE, "runDiscovery",
                         "Extracting MariaDB lineage for connection");
            LineageExtractorMariaDB lineageExtractor(connStr);
            lineageExtractor.extractLineage();
            lineageExtractor.storeLineage();
            Logger::info(LogCategory::GOVERNANCE, "runDiscovery",
                         "MariaDB lineage extraction completed");
          } catch (const std::exception &e) {
            Logger::error(LogCategory::GOVERNANCE, "runDiscovery",
                          "Error extracting MariaDB lineage: " +
                              std::string(e.what()));
          }
        }
      }

      std::vector<std::string> mongodbConnections =
          repo.getConnectionStrings("MongoDB");
      for (const auto &connStr : mongodbConnections) {
        if (!connStr.empty()) {
          try {
            Logger::info(LogCategory::GOVERNANCE, "runDiscovery",
                         "Collecting MongoDB governance data for connection");
            DataGovernanceMongoDB mongodbGov(connStr);
            mongodbGov.collectGovernanceData();
            mongodbGov.storeGovernanceData();
            mongodbGov.generateReport();
          } catch (const std::exception &e) {
            Logger::error(LogCategory::GOVERNANCE, "runDiscovery",
                          "Error collecting MongoDB governance data: " +
                              std::string(e.what()));
          }

          try {
            Logger::info(LogCategory::GOVERNANCE, "runDiscovery",
                         "Extracting MongoDB lineage for connection");
            LineageExtractorMongoDB lineageExtractor(connStr);
            lineageExtractor.extractLineage();
            lineageExtractor.storeLineage();
            Logger::info(LogCategory::GOVERNANCE, "runDiscovery",
                         "MongoDB lineage extraction completed");
          } catch (const std::exception &e) {
            Logger::error(LogCategory::GOVERNANCE, "runDiscovery",
                          "Error extracting MongoDB lineage: " +
                              std::string(e.what()));
          }
        }
      }

      std::vector<std::string> oracleConnections =
          repo.getConnectionStrings("Oracle");
      for (const auto &connStr : oracleConnections) {
        if (!connStr.empty()) {
          try {
            Logger::info(LogCategory::GOVERNANCE, "runDiscovery",
                         "Collecting Oracle governance data for connection");
            DataGovernanceOracle oracleGov(connStr);
            oracleGov.collectGovernanceData();
            oracleGov.storeGovernanceData();
            oracleGov.generateReport();
          } catch (const std::exception &e) {
            Logger::error(LogCategory::GOVERNANCE, "runDiscovery",
                          "Error collecting Oracle governance data: " +
                              std::string(e.what()));
          }

          try {
            Logger::info(LogCategory::GOVERNANCE, "runDiscovery",
                         "Extracting Oracle lineage for connection");
            LineageExtractorOracle lineageExtractor(connStr);
            lineageExtractor.extractLineage();
            lineageExtractor.storeLineage();
            Logger::info(LogCategory::GOVERNANCE, "runDiscovery",
                         "Oracle lineage extraction completed");
          } catch (const std::exception &e) {
            Logger::error(LogCategory::GOVERNANCE, "runDiscovery",
                          "Error extracting Oracle lineage: " +
                              std::string(e.what()));
          }
        }
      }
    } catch (const std::exception &e) {
      Logger::error(LogCategory::GOVERNANCE, "runDiscovery",
                    "Error processing database governance: " +
                        std::string(e.what()));
    }

    try {
      Logger::info(LogCategory::GOVERNANCE, "runDiscovery",
                   "Collecting column catalog from all sources");
      ColumnCatalogCollector columnCollector(
          DatabaseConfig::getPostgresConnectionString());
      columnCollector.collectAllColumns();
      columnCollector.storeColumnMetadata();
      columnCollector.generateReport();
      Logger::info(LogCategory::GOVERNANCE, "runDiscovery",
                   "Column catalog collection completed");
    } catch (const std::exception &e) {
      Logger::error(LogCategory::GOVERNANCE, "runDiscovery",
                    "Error collecting column catalog: " +
                        std::string(e.what()));
    }

  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "runDiscovery",
                  "Error in discovery process: " + std::string(e.what()));
  }
}

// Discovers all user tables in the PostgreSQL database by querying
// information_schema.tables. Excludes system schemas (information_schema,
// pg_catalog, pg_toast, pg_temp_1, pg_toast_temp_1, metadata) and only
// includes BASE TABLE types. Returns a vector of TableMetadata objects, one
// for each discovered table. If discovery fails, returns an empty vector and
// logs an error.
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

  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "discoverTables",
                  "Error discovering tables: " + std::string(e.what()));
  }

  return tables;
}

// Extracts comprehensive metadata for a specific table. Validates input
// parameters, verifies table existence, and performs multiple analyses:
// table structure, data quality, usage statistics, and health status. Also
// classifies the table and infers the source database engine. Returns a
// TableMetadata object with all collected information. Throws
// std::invalid_argument if schema or table name is empty or contains invalid
// characters. Throws std::runtime_error if the table does not exist.
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
    std::string lowerSchema = schema_name;
    std::transform(lowerSchema.begin(), lowerSchema.end(), lowerSchema.begin(),
                   ::tolower);
    std::string lowerTable = table_name;
    std::transform(lowerTable.begin(), lowerTable.end(), lowerTable.begin(),
                   ::tolower);
    std::string checkTableQuery =
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema = $1 "
        "AND table_name = $2";

    auto checkResult =
        checkTxn.exec_params(checkTableQuery, lowerSchema, lowerTable);
    checkTxn.commit();

    if (checkResult.empty() || checkResult[0][0].as<int>() == 0) {
      throw std::runtime_error("Table '" + schema_name + "." + table_name +
                               "' does not exist");
    }

    analyzeTableStructure(conn, lowerSchema, lowerTable, metadata);
    analyzeDataQuality(conn, lowerSchema, lowerTable, metadata);
    analyzeUsageStatistics(conn, lowerSchema, lowerTable, metadata);
    analyzeHealthStatus(conn, lowerSchema, lowerTable, metadata);

    classifyTable(metadata);
    inferSourceEngine(metadata);

    // Calculate enhanced quality scores
    metadata.completeness_score = calculateCompletenessScore(metadata);
    metadata.accuracy_score = calculateAccuracyScore(metadata);
    metadata.consistency_score = calculateConsistencyScore(metadata);
    metadata.validity_score = calculateValidityScore(metadata);
    metadata.timeliness_score = calculateTimelinessScore(metadata);
    metadata.uniqueness_score = calculateUniquenessScore(metadata);
    metadata.integrity_score = calculateIntegrityScore(metadata);

    metadata.data_quality_score = calculateDataQualityScore(metadata);
    metadata.last_analyzed = TimeUtils::getCurrentTimestamp();

  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "extractTableMetadata",
                  "Error extracting metadata for " + schema_name + "." +
                      table_name + ": " + std::string(e.what()));
  }

  return metadata;
}

// Analyzes the structural properties of a table including column count, row
// count, table size, primary key columns, index count, and constraint count.
// Queries information_schema and pg_stat tables to gather this information.
// Updates the provided metadata object with the collected data. If analysis
// fails, logs an error but does not throw an exception.
void DataGovernance::analyzeTableStructure(pqxx::connection &conn,
                                           const std::string &schema_name,
                                           const std::string &table_name,
                                           TableMetadata &metadata) {
  try {
    pqxx::work txn(conn);

    std::string lowerSchema = schema_name;
    std::transform(lowerSchema.begin(), lowerSchema.end(), lowerSchema.begin(),
                   ::tolower);
    std::string lowerTable = table_name;
    std::transform(lowerTable.begin(), lowerTable.end(), lowerTable.begin(),
                   ::tolower);
    std::string columnCountQuery =
        "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = "
        "$1 AND table_name = $2";
    auto columnResult =
        txn.exec_params(columnCountQuery, lowerSchema, lowerTable);
    if (!columnResult.empty()) {
      metadata.total_columns = columnResult[0][0].as<int>();
    }

    std::string rowCountQuery = "SELECT COUNT(*) FROM " +
                                txn.quote_name(lowerSchema) + "." +
                                txn.quote_name(lowerTable);
    auto rowResult = txn.exec(rowCountQuery);
    if (!rowResult.empty()) {
      metadata.total_rows = rowResult[0][0].as<long long>();
    }

    std::string sizeQuery = "SELECT pg_total_relation_size(" +
                            txn.quote(lowerSchema + "." + lowerTable) +
                            "::regclass) as size_bytes";
    auto sizeResult = txn.exec(sizeQuery);
    if (!sizeResult.empty()) {
      try {
        long long sizeBytes = sizeResult[0][0].as<long long>();
        if (sizeBytes > 0) {
          metadata.table_size_mb =
              static_cast<double>(sizeBytes) / (1024.0 * 1024.0);
        } else {
          metadata.table_size_mb = 0.0;
        }
      } catch (const std::exception &e) {
        Logger::warning(LogCategory::GOVERNANCE, "DataGovernance",
                        "Failed to calculate table size: " +
                            std::string(e.what()));
        metadata.table_size_mb = 0.0;
      } catch (...) {
        Logger::warning(LogCategory::GOVERNANCE, "DataGovernance",
                        "Unknown error calculating table size");
        metadata.table_size_mb = 0.0;
      }
    }

    std::string pkQuery = "SELECT string_agg(column_name, ',') "
                          "FROM information_schema.table_constraints tc "
                          "JOIN information_schema.key_column_usage kcu ON "
                          "tc.constraint_name = kcu.constraint_name "
                          "WHERE tc.table_schema = $1 "
                          "AND tc.table_name = $2 "
                          "AND tc.constraint_type = 'PRIMARY KEY'";
    auto pkResult = txn.exec_params(pkQuery, lowerSchema, lowerTable);
    if (!pkResult.empty() && !pkResult[0][0].is_null()) {
      metadata.primary_key_columns = pkResult[0][0].as<std::string>();
    }

    std::string indexQuery = "SELECT COUNT(*) FROM pg_indexes WHERE schemaname "
                             "= $1 AND tablename = $2";
    auto indexResult = txn.exec_params(indexQuery, lowerSchema, lowerTable);
    if (!indexResult.empty()) {
      metadata.index_count = indexResult[0][0].as<int>();
    }

    std::string constraintQuery =
        "SELECT COUNT(*) FROM information_schema.table_constraints WHERE "
        "table_schema = $1 AND table_name = $2";
    auto constraintResult =
        txn.exec_params(constraintQuery, lowerSchema, lowerTable);
    if (!constraintResult.empty()) {
      metadata.constraint_count = constraintResult[0][0].as<int>();
    }

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "analyzeTableStructure",
                  "Error analyzing table structure: " + std::string(e.what()));
  }
}

// Analyzes data quality metrics for a table including NULL percentage and
// duplicate percentage. For NULL analysis, checks actual NULL values in
// nullable columns, not just column definitions. For duplicate analysis, uses
// sampling (10% TABLESAMPLE) for large tables (>1M rows) to improve
// performance. Updates the metadata object with null_percentage and
// duplicate_percentage. If analysis fails, logs an error but does not throw
// an exception.
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
            Logger::error(LogCategory::GOVERNANCE, "analyzeDataQuality",
                          "Error analyzing column nulls: " +
                              std::string(e.what()));
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
      Logger::error(LogCategory::GOVERNANCE, "analyzeDataQuality",
                    "SQL error calculating duplicates for " + schema_name +
                        "." + table_name + ": " + std::string(e.what()));
      metadata.duplicate_percentage = 0.0;
    } catch (const std::exception &e) {
      Logger::error(LogCategory::GOVERNANCE, "analyzeDataQuality",
                    "Error calculating duplicates: " + std::string(e.what()));
      metadata.duplicate_percentage = 0.0;
    }

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "analyzeDataQuality",
                  "Error analyzing data quality: " + std::string(e.what()));
  }
}

// Analyzes usage statistics for a table by querying pg_stat_user_tables.
// Collects information about sequential scans, index scans, tuple insertions,
// updates, and deletions. Calculates daily query count and determines access
// frequency (REAL_TIME, HIGH, MEDIUM, LOW, RARE, ARCHIVED) based on query
// count. Updates the metadata object with last_accessed, last_vacuum,
// access_frequency, and query_count_daily. If analysis fails, logs an error
// but does not throw an exception.
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
    Logger::error(LogCategory::GOVERNANCE, "analyzeUsageStatistics",
                  "Error analyzing usage statistics: " + std::string(e.what()));
  }
}

// Analyzes the health status of a table by examining dead tuples, live tuples,
// and vacuum history from pg_stat_user_tables. Calculates fragmentation
// percentage as the ratio of dead tuples to live tuples. Determines overall
// health status (EMERGENCY, CRITICAL, WARNING, HEALTHY, EXCELLENT) based on
// fragmentation, duplicate, and null percentages. Updates the metadata object
// with fragmentation_percentage, last_vacuum, and health_status. If analysis
// fails, logs an error but does not throw an exception.
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
    Logger::error(LogCategory::GOVERNANCE, "analyzeHealthStatus",
                  "Error analyzing health status: " + std::string(e.what()));
  }
}

// Classifies a table by determining its data category, business domain,
// sensitivity level, data classification, retention policy, backup frequency,
// and compliance requirements. Uses the DataClassifier to perform pattern
// matching on table and schema names. Updates the metadata object with all
// classification results. If classification fails, logs an error but does not
// throw an exception.
void DataGovernance::classifyTable(TableMetadata &metadata) {
  try {
    metadata.data_category =
        determineDataCategory(metadata.table_name, metadata.schema_name);
    metadata.business_domain =
        determineBusinessDomain(metadata.table_name, metadata.schema_name);
    metadata.sensitivity_level =
        determineSensitivityLevel(metadata.table_name, metadata.schema_name);
    metadata.data_classification =
        determineDataClassification(metadata.table_name, metadata.schema_name);
    metadata.retention_policy = determineRetentionPolicy(
        metadata.data_category, metadata.sensitivity_level);
    metadata.backup_frequency = determineBackupFrequency(
        metadata.data_category, metadata.access_frequency);
    metadata.compliance_requirements = determineComplianceRequirements(
        metadata.sensitivity_level, metadata.business_domain);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "classifyTable",
                  "Error classifying table: " + std::string(e.what()));
  }
}

// Infers the source database engine for a table by querying metadata.catalog
// to find matching schema_name entries. If a match is found, sets
// inferred_source_engine to the db_engine value. If no match is found, sets
// it to "UNKNOWN". This helps track which source database a table originated
// from. If inference fails, sets inferred_source_engine to "UNKNOWN" and logs
// an error.
void DataGovernance::inferSourceEngine(TableMetadata &metadata) {
  try {
    pqxx::connection conn(DatabaseConfig::getPostgresConnectionString());
    pqxx::work txn(conn);

    std::string query =
        "SELECT db_engine FROM metadata.catalog WHERE schema_name = $1 LIMIT 1";

    auto result = txn.exec_params(query, metadata.schema_name);
    txn.commit();

    if (!result.empty()) {
      metadata.inferred_source_engine = result[0][0].as<std::string>();
    } else {
      metadata.inferred_source_engine = "UNKNOWN";
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "inferSourceEngine",
                  "Error inferring source engine: " + std::string(e.what()));
    metadata.inferred_source_engine = "UNKNOWN";
  }
}

// Stores table metadata in the metadata.data_governance_catalog table. If a
// record with the same schema_name and table_name already exists, calls
// updateExistingMetadata to update it. Otherwise, inserts a new record with
// all metadata fields. Uses SQL escaping to prevent injection attacks. If
// storage fails, logs an error but does not throw an exception.
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
          "health_status, last_vacuum, fragmentation_percentage, snapshot_date"
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
          ", " + std::to_string(metadata.fragmentation_percentage) +
          ", NOW());";

      txn.exec(insertQuery);
    }

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "storeMetadata",
                  "Error storing metadata: " + std::string(e.what()));
  }
}

// Updates an existing metadata record in metadata.data_governance_catalog.
// Updates all fields including structure metrics, quality scores, usage
// statistics, health status, and classification information. Sets updated_at
// to the current timestamp. Uses SQL escaping to prevent injection attacks.
// If update fails, logs an error but does not throw an exception.
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
    Logger::error(LogCategory::GOVERNANCE, "updateExistingMetadata",
                  "Error updating metadata: " + std::string(e.what()));
  }
}

// Generates a summary report of governance metrics by aggregating data from
// metadata.data_governance_catalog. Calculates total tables, healthy/warning/
// critical table counts, average quality score, total rows, and total size.
// Currently extracts the data but does not output it (variables are assigned
// but not used). If report generation fails, logs an error but does not throw
// an exception.
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

      Logger::info(
          LogCategory::GOVERNANCE, "generateReport",
          "Governance Report: Total tables=" + std::to_string(totalTables) +
              ", Healthy=" + std::to_string(healthyTables) +
              ", Warning=" + std::to_string(warningTables) +
              ", Critical=" + std::to_string(criticalTables) +
              ", Avg Quality=" + std::to_string(avgQuality) +
              ", Total Rows=" + std::to_string(totalRows) +
              ", Total Size MB=" + std::to_string(totalSize));
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "generateReport",
                  "Error generating report: " + std::string(e.what()));
  }
}

// Calculates an overall data quality score (0-100) based on null percentage,
// duplicate percentage, and fragmentation percentage. Applies weighted
// deductions: null_percentage * 0.5, duplicate_percentage * 0.3,
// fragmentation_percentage * 0.2. Returns a value clamped between 0.0 and
// 100.0. Higher scores indicate better data quality.
double
DataGovernance::calculateDataQualityScore(const TableMetadata &metadata) {
  double score = 100.0;

  score -= metadata.null_percentage * 0.5;
  score -= metadata.duplicate_percentage * 0.3;
  score -= metadata.fragmentation_percentage * 0.2;

  return std::max(0.0, std::min(100.0, score));
}

// Determines access frequency category based on daily query count. Returns
// "REAL_TIME" for >10000 queries, "HIGH" for >1000, "MEDIUM" for >100,
// "LOW" for >10, "RARE" for >0, and "ARCHIVED" for 0 queries. This
// classification is used to determine backup frequency and retention policies.
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

// Determines the health status of a table based on fragmentation, duplicate,
// and null percentages. Returns "EMERGENCY" for severe issues (>80%
// fragmentation, >50% duplicates, >70% nulls), "CRITICAL" for serious issues
// (>50% fragmentation, >20% duplicates, >50% nulls), "WARNING" for moderate
// issues (>20% fragmentation, >10% duplicates, >30% nulls), "EXCELLENT" for
// perfect condition (0% fragmentation, 0% duplicates, <5% nulls), or "HEALTHY"
// otherwise.
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

// Determines the data category for a table by delegating to the DataClassifier.
// Returns categories such as "TRANSACTIONAL", "ANALYTICAL", "SPORTS", etc.
// based on pattern matching against governance rules.
std::string
DataGovernance::determineDataCategory(const std::string &table_name,
                                      const std::string &schema_name) {
  return classifier_->classifyDataCategory(table_name, schema_name);
}

// Determines the business domain for a table by delegating to the
// DataClassifier. Returns domains such as "FINANCE", "HEALTHCARE", "SPORTS",
// "GENERAL", etc. based on pattern matching against governance rules.
std::string
DataGovernance::determineBusinessDomain(const std::string &table_name,
                                        const std::string &schema_name) {
  return classifier_->classifyBusinessDomain(table_name, schema_name);
}

// Determines the sensitivity level for a table by delegating to the
// DataClassifier. Returns levels such as "PUBLIC", "PRIVATE", "CRITICAL",
// "HIGH", etc. based on pattern matching against governance rules.
std::string
DataGovernance::determineSensitivityLevel(const std::string &table_name,
                                          const std::string &schema_name) {
  return classifier_->classifySensitivityLevel(table_name, schema_name);
}

// Determines the data classification for a table by delegating to the
// DataClassifier. Returns classifications such as "PUBLIC", "CONFIDENTIAL",
// etc. based on pattern matching against governance rules.
std::string
DataGovernance::determineDataClassification(const std::string &table_name,
                                            const std::string &schema_name) {
  return classifier_->classifyDataClassification(table_name, schema_name);
}

// Determines the retention policy for a table based on data category and
// sensitivity level. Returns "7_YEARS" for CRITICAL sensitivity, "5_YEARS"
// for HIGH sensitivity, "3_YEARS" for ANALYTICAL or SPORTS categories,
// "2_YEARS" for TRANSACTIONAL, or "1_YEAR" as default. Sports data requires
// longer retention for compliance.
std::string
DataGovernance::determineRetentionPolicy(const std::string &data_category,
                                         const std::string &sensitivity_level) {
  if (sensitivity_level == "CRITICAL") {
    return "7_YEARS";
  }
  if (sensitivity_level == "HIGH") {
    return "5_YEARS";
  }
  if (data_category == "ANALYTICAL") {
    return "3_YEARS";
  }
  if (data_category == "TRANSACTIONAL") {
    return "2_YEARS";
  }
  if (data_category == "SPORTS") {
    return "3_YEARS"; // Sports data requires longer retention for compliance
  }
  return "1_YEAR";
}

// Determines the backup frequency for a table based on data category and
// access frequency. Returns "HOURLY" for REAL_TIME or HIGH access frequency,
// "DAILY" for TRANSACTIONAL, MASTER_DATA, or SPORTS categories, "WEEKLY" for
// ANALYTICAL, or "MONTHLY" as default. Sports data requires frequent backups
// due to high value.
std::string
DataGovernance::determineBackupFrequency(const std::string &data_category,
                                         const std::string &access_frequency) {
  if (access_frequency == "REAL_TIME" || access_frequency == "HIGH") {
    return "HOURLY";
  }
  if (data_category == "TRANSACTIONAL" || data_category == "MASTER_DATA") {
    return "DAILY";
  }
  if (data_category == "ANALYTICAL") {
    return "WEEKLY";
  }
  if (data_category == "SPORTS") {
    return "DAILY"; // Sports data requires frequent backups due to high value
  }
  return "MONTHLY";
}

// Determines compliance requirements for a table based on sensitivity level
// and business domain. Returns "HIPAA" for HEALTHCARE domain, "SOX,PCI" for
// FINANCE domain with CRITICAL/HIGH sensitivity, "GDPR,PCI,AML" for SPORTS
// domain with CRITICAL/HIGH sensitivity, "GDPR,AML" for SPORTS domain
// otherwise, "SOX" for FINANCE domain, "GDPR" for CRITICAL/HIGH sensitivity,
// or "GDPR" as default. Sports betting requires GDPR, PCI, and AML compliance.
std::string DataGovernance::determineComplianceRequirements(
    const std::string &sensitivity_level, const std::string &business_domain) {
  if (sensitivity_level == "CRITICAL" || sensitivity_level == "HIGH") {
    if (business_domain == "HEALTHCARE") {
      return "HIPAA";
    }
    if (business_domain == "FINANCE") {
      return "SOX,PCI";
    }
    if (business_domain == "SPORTS") {
      return "GDPR,PCI,AML"; // Sports betting requires GDPR, PCI, and AML
                             // compliance
    }
    return "GDPR";
  }
  if (business_domain == "HEALTHCARE") {
    return "HIPAA";
  }
  if (business_domain == "FINANCE") {
    return "SOX";
  }
  if (business_domain == "SPORTS") {
    return "GDPR,AML"; // Sports betting requires GDPR and AML compliance
  }
  return "GDPR";
}

// Calculates a completeness score (0-100) based on null percentage. Returns
// 0.0 if total_columns is 0. Otherwise, returns 100.0 minus null_percentage
// * 0.1. Higher scores indicate more complete data (fewer nulls).
double
DataGovernance::calculateCompletenessScore(const TableMetadata &metadata) {
  if (metadata.total_columns == 0)
    return 0.0;
  return 100.0 - (metadata.null_percentage * 0.1);
}

// Calculates an accuracy score (0-100) based on duplicate percentage.
// Returns 100.0 minus duplicate_percentage * 0.5. Higher scores indicate
// more accurate data (fewer duplicates).
double DataGovernance::calculateAccuracyScore(const TableMetadata &metadata) {
  return 100.0 - (metadata.duplicate_percentage * 0.5);
}

// Calculates a consistency score (0-100) based on fragmentation percentage.
// Returns 100.0 minus fragmentation_percentage * 0.2. Higher scores indicate
// more consistent data (less fragmentation).
double
DataGovernance::calculateConsistencyScore(const TableMetadata &metadata) {
  return 100.0 - (metadata.fragmentation_percentage * 0.2);
}

// Calculates a validity score (0-100) based on null percentage. Returns
// 100.0 minus null_percentage * 0.3. Higher scores indicate more valid data
// (fewer nulls in required fields).
double DataGovernance::calculateValidityScore(const TableMetadata &metadata) {
  return 100.0 - (metadata.null_percentage * 0.3);
}

// Calculates a timeliness score (0-100) based on fragmentation percentage.
// Returns 100.0 minus fragmentation_percentage * 0.1. Higher scores indicate
// more timely data (less fragmentation, better maintenance).
double DataGovernance::calculateTimelinessScore(const TableMetadata &metadata) {
  return 100.0 - (metadata.fragmentation_percentage * 0.1);
}

// Calculates a uniqueness score (0-100) based on duplicate percentage.
// Returns 100.0 minus duplicate_percentage * 0.8. Higher scores indicate
// more unique data (fewer duplicates).
double DataGovernance::calculateUniquenessScore(const TableMetadata &metadata) {
  return 100.0 - (metadata.duplicate_percentage * 0.8);
}

// Calculates an integrity score (0-100) based on fragmentation percentage.
// Returns 100.0 minus fragmentation_percentage * 0.3. Higher scores indicate
// better data integrity (less fragmentation, better referential integrity).
double DataGovernance::calculateIntegrityScore(const TableMetadata &metadata) {
  return 100.0 - (metadata.fragmentation_percentage * 0.3);
}
