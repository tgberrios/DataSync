#include "DataQuality.h"
#include <algorithm>

std::string cleanSchemaNameForPostgres(const std::string &schemaName) {
  std::string cleaned = schemaName;

  // Only remove truly problematic characters for SQL injection
  cleaned.erase(std::remove(cleaned.begin(), cleaned.end(), ';'),
                cleaned.end());
  cleaned.erase(std::remove(cleaned.begin(), cleaned.end(), '\''),
                cleaned.end());
  cleaned.erase(std::remove(cleaned.begin(), cleaned.end(), '"'),
                cleaned.end());
  cleaned.erase(std::remove(cleaned.begin(), cleaned.end(), '\n'),
                cleaned.end());
  cleaned.erase(std::remove(cleaned.begin(), cleaned.end(), '\r'),
                cleaned.end());
  cleaned.erase(std::remove(cleaned.begin(), cleaned.end(), '\t'),
                cleaned.end());

  // Trim leading/trailing whitespace
  cleaned.erase(0, cleaned.find_first_not_of(" \t"));
  cleaned.erase(cleaned.find_last_not_of(" \t") + 1);

  // If empty after cleaning, throw error instead of using default
  if (cleaned.empty()) {
    throw std::invalid_argument(
        "Schema name is empty or contains only invalid characters: " +
        schemaName);
  }

  // Lowercase for PostgreSQL identifier standardization
  std::transform(cleaned.begin(), cleaned.end(), cleaned.begin(), ::tolower);
  return cleaned;
}

bool DataQuality::validateTable(pqxx::connection &conn,
                                const std::string &schema,
                                const std::string &table,
                                const std::string &engine) {
  // Validate input parameters
  if (schema.empty() || table.empty() || engine.empty()) {
    Logger::error(LogCategory::QUALITY, "validateTable",
                  "Schema, table, and engine cannot be empty");
    return false;
  }

  // Validate connection
  if (!conn.is_open()) {
    Logger::error(LogCategory::QUALITY, "validateTable",
                  "Database connection is not open");
    return false;
  }

  // Basic SQL injection prevention
  if (schema.find("'") != std::string::npos ||
      schema.find(";") != std::string::npos ||
      table.find("'") != std::string::npos ||
      table.find(";") != std::string::npos) {
    Logger::error(LogCategory::QUALITY, "validateTable",
                  "Schema and table names contain invalid characters");
    return false;
  }

  auto start = std::chrono::high_resolution_clock::now();

  try {
    QualityMetrics metrics;
    std::string lowerSchema = cleanSchemaNameForPostgres(schema);
    std::string lowerTable = table;
    std::transform(lowerTable.begin(), lowerTable.end(), lowerTable.begin(),
                   ::tolower);
    metrics.schema_name = lowerSchema;
    metrics.table_name = lowerTable;
    metrics.source_db_engine = engine;

    // Collect all metrics
    metrics = collectMetrics(conn, lowerSchema, lowerTable);

    // Calculate duration
    auto end = std::chrono::high_resolution_clock::now();
    metrics.check_duration_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
            .count();

    // Determine final status and save
    metrics.validation_status = determineValidationStatus(metrics);
    return saveMetrics(conn, metrics);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::QUALITY, "validateTable",
                  "Error validating table " + schema + "." + table + ": " +
                      std::string(e.what()));
    return false;
  }
}

DataQuality::QualityMetrics
DataQuality::collectMetrics(pqxx::connection &conn, const std::string &schema,
                            const std::string &table) {
  QualityMetrics metrics;
  std::string lowerSchema = cleanSchemaNameForPostgres(schema);
  std::string lowerTable = table;
  std::transform(lowerTable.begin(), lowerTable.end(), lowerTable.begin(),
                 ::tolower);
  metrics.schema_name = lowerSchema;
  metrics.table_name = lowerTable;

  try {
    // Check data types and collect type-related metrics
    checkDataTypes(conn, metrics);

    // Check for nulls
    checkNullCounts(conn, metrics);

    // Check for duplicates
    checkDuplicates(conn, metrics);

    // Check constraints
    checkConstraints(conn, metrics);

    // Calculate final quality score
    calculateQualityScore(metrics);

  } catch (const pqxx::sql_error &e) {
    Logger::error(LogCategory::QUALITY, "collectMetrics",
                  "SQL error collecting metrics: " + std::string(e.what()) +
                      " [SQL State: " + e.sqlstate() + "]");
    metrics.error_details = "SQL Error: " + std::string(e.what());
    metrics.validation_status = "FAILED";
  } catch (const std::invalid_argument &e) {
    Logger::error(LogCategory::QUALITY, "collectMetrics",
                  "Invalid argument error: " + std::string(e.what()));
    metrics.error_details = "Invalid Argument: " + std::string(e.what());
    metrics.validation_status = "FAILED";
  } catch (const std::exception &e) {
    Logger::error(LogCategory::QUALITY, "collectMetrics",
                  "General error collecting metrics: " + std::string(e.what()));
    metrics.error_details = "General Error: " + std::string(e.what());
    metrics.validation_status = "FAILED";
  }

  return metrics;
}

bool DataQuality::checkDataTypes(pqxx::connection &conn,
                                 QualityMetrics &metrics) {
  try {
    pqxx::work txn(conn);

    // Get column information
    auto result =
        txn.exec("SELECT column_name, data_type, is_nullable "
                 "FROM information_schema.columns "
                 "WHERE table_schema = " +
                 txn.quote(metrics.schema_name) +
                 " AND table_name = " + txn.quote(metrics.table_name));

    json type_mismatches;

    for (const auto &row : result) {
      std::string column = row[0].as<std::string>();
      std::string type = row[1].as<std::string>();

      // Check for type-specific issues with improved performance
      std::string cleanSchema = cleanSchemaNameForPostgres(metrics.schema_name);

      // Use sampling for large tables to improve performance
      std::string typeQuery;
      try {
        // First check table size to determine sampling strategy
        auto sizeResult = txn.exec("SELECT COUNT(*) FROM \"" + cleanSchema +
                                   "\".\"" + metrics.table_name + "\"");
        size_t tableSize = sizeResult[0][0].as<size_t>();

        if (tableSize > 1000000) {
          // Use sampling for large tables
          typeQuery = "SELECT COUNT(*) FROM \"" + cleanSchema + "\".\"" +
                      metrics.table_name + "\" TABLESAMPLE SYSTEM(5) WHERE \"" +
                      column + "\" IS NOT NULL AND " + "NOT pg_typeof(\"" +
                      column + "\")::text = " + txn.quote(type);
        } else {
          // Use full table for smaller tables
          typeQuery = "SELECT COUNT(*) FROM \"" + cleanSchema + "\".\"" +
                      metrics.table_name + "\" WHERE \"" + column +
                      "\" IS NOT NULL AND " + "NOT pg_typeof(\"" + column +
                      "\")::text = " + txn.quote(type);
        }

        auto type_check = txn.exec(typeQuery);
        size_t invalid_count = type_check[0][0].as<size_t>();

        // Adjust count for sampled data
        if (tableSize > 1000000) {
          invalid_count = static_cast<size_t>(
              invalid_count * 20); // 5% sample -> multiply by 20
        }

        if (invalid_count > 0) {
          type_mismatches[column] = {{"expected_type", type},
                                     {"invalid_count", invalid_count}};
          metrics.invalid_type_count += invalid_count;
        }
      } catch (const pqxx::sql_error &e) {
        type_mismatches[column] = {{"expected_type", type},
                                   {"error", e.what()}};
      }
    }

    metrics.type_mismatch_details = type_mismatches;
    txn.commit();
    return true;
  } catch (const pqxx::sql_error &e) {
    Logger::error(LogCategory::QUALITY, "checkDataTypes",
                  "SQL error checking data types: " + std::string(e.what()) +
                      " [SQL State: " + e.sqlstate() + "]");
    return false;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::QUALITY, "checkDataTypes",
                  "Error checking data types: " + std::string(e.what()));
    return false;
  }
}

bool DataQuality::checkNullCounts(pqxx::connection &conn,
                                  QualityMetrics &metrics) {
  try {
    pqxx::work txn(conn);

    // Check if table exists first
    std::string cleanSchema = cleanSchemaNameForPostgres(metrics.schema_name);
    auto tableExists =
        txn.exec("SELECT COUNT(*) FROM information_schema.tables "
                 "WHERE table_schema = " +
                 txn.quote(cleanSchema) +
                 " AND table_name = " + txn.quote(metrics.table_name));

    if (tableExists[0][0].as<int>() == 0) {
      metrics.total_rows = 0;
      metrics.null_count = 0;
      txn.commit();
      return true;
    }

    // Get total rows efficiently
    auto totalResult = txn.exec("SELECT COUNT(*) FROM \"" + cleanSchema +
                                "\".\"" + metrics.table_name + "\"");
    metrics.total_rows =
        totalResult[0][0].is_null() ? 0 : totalResult[0][0].as<size_t>();

    // Calculate NULL count more efficiently by checking each column
    // individually
    metrics.null_count = 0;

    // Get column list
    auto columnsResult =
        txn.exec("SELECT column_name FROM information_schema.columns "
                 "WHERE table_schema = " +
                 txn.quote(cleanSchema) +
                 " AND table_name = " + txn.quote(metrics.table_name));

    for (const auto &colRow : columnsResult) {
      std::string columnName = colRow[0].as<std::string>();

      try {
        // Count NULL values in this specific column
        auto nullResult = txn.exec("SELECT COUNT(*) FROM \"" + cleanSchema +
                                   "\".\"" + metrics.table_name +
                                   "\" WHERE \"" + columnName + "\" IS NULL");

        size_t columnNulls = nullResult[0][0].as<size_t>();
        metrics.null_count += columnNulls;
      } catch (const std::exception &e) {
        Logger::error(LogCategory::QUALITY, "checkNullCounts",
                      "Error checking column nulls: " + std::string(e.what()));
      }
    }

    txn.commit();
    return true;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::QUALITY, "checkNullCounts",
                  "Error checking null counts: " + std::string(e.what()));
    return false;
  }
}

bool DataQuality::checkDuplicates(pqxx::connection &conn,
                                  QualityMetrics &metrics) {
  try {
    pqxx::work txn(conn);

    // Check if table exists first
    std::string cleanSchema = cleanSchemaNameForPostgres(metrics.schema_name);
    auto tableExists =
        txn.exec("SELECT COUNT(*) FROM information_schema.tables "
                 "WHERE table_schema = " +
                 txn.quote(cleanSchema) +
                 " AND table_name = " + txn.quote(metrics.table_name));

    if (tableExists[0][0].as<int>() == 0) {
      metrics.duplicate_count = 0;
      txn.commit();
      return true;
    }

    // Count duplicates with sampling for large tables
    std::string duplicateQuery;

    // First check table size
    auto sizeResult = txn.exec("SELECT COUNT(*) FROM \"" + cleanSchema +
                               "\".\"" + metrics.table_name + "\"");
    size_t tableSize = sizeResult[0][0].as<size_t>();

    if (tableSize > 1000000) {
      // Use sampling for large tables
      duplicateQuery = "SELECT COUNT(*) - COUNT(DISTINCT ctid) FROM \"" +
                       cleanSchema + "\".\"" + metrics.table_name +
                       "\" TABLESAMPLE SYSTEM(10)";
    } else {
      // Use full table for smaller tables
      duplicateQuery = "SELECT COUNT(*) - COUNT(DISTINCT ctid) FROM \"" +
                       cleanSchema + "\".\"" + metrics.table_name + "\"";
    }

    auto result = txn.exec(duplicateQuery);
    metrics.duplicate_count =
        result[0][0].is_null() ? 0 : result[0][0].as<size_t>();

    // Adjust count for sampled data
    if (tableSize > 1000000) {
      metrics.duplicate_count = static_cast<size_t>(
          metrics.duplicate_count * 10); // 10% sample -> multiply by 10
    }

    txn.commit();
    return true;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::QUALITY, "checkDuplicates",
                  "Error checking duplicates: " + std::string(e.what()));
    return false;
  }
}

bool DataQuality::checkConstraints(pqxx::connection &conn,
                                   QualityMetrics &metrics) {
  try {
    pqxx::work txn(conn);
    json constraint_issues;

    // Check foreign key constraints properly
    std::string fkQuery =
        "SELECT rc.constraint_name, kcu.column_name, "
        "ccu.table_name AS referenced_table, ccu.column_name AS "
        "referenced_column "
        "FROM information_schema.referential_constraints rc "
        "JOIN information_schema.key_column_usage kcu "
        "ON rc.constraint_name = kcu.constraint_name "
        "JOIN information_schema.constraint_column_usage ccu "
        "ON rc.unique_constraint_name = ccu.constraint_name "
        "WHERE kcu.table_schema = " +
        txn.quote(metrics.schema_name) +
        " AND kcu.table_name = " + txn.quote(metrics.table_name);

    auto fk_result = txn.exec(fkQuery);
    metrics.referential_integrity_errors = 0;

    // Check each foreign key constraint for violations
    for (const auto &fk_row : fk_result) {
      std::string constraintName = fk_row[0].as<std::string>();
      std::string columnName = fk_row[1].as<std::string>();
      std::string referencedTable = fk_row[2].as<std::string>();
      std::string referencedColumn = fk_row[3].as<std::string>();

      try {
        // Build proper violation check query
        std::string violationQuery =
            "SELECT COUNT(*) FROM \"" + metrics.schema_name + "\".\"" +
            metrics.table_name +
            "\" t "
            "WHERE \"" +
            columnName +
            "\" IS NOT NULL AND "
            "NOT EXISTS (SELECT 1 FROM \"" +
            metrics.schema_name + "\".\"" + referencedTable +
            "\" r "
            "WHERE r.\"" +
            referencedColumn + "\" = t.\"" + columnName + "\")";

        auto violations = txn.exec(violationQuery);
        size_t constraintViolations = violations[0][0].as<size_t>();
        metrics.referential_integrity_errors += constraintViolations;

        if (constraintViolations > 0) {
          constraint_issues[constraintName] = {
              {"column", columnName},
              {"referenced_table", referencedTable},
              {"referenced_column", referencedColumn},
              {"violations", constraintViolations}};
        }
      } catch (const std::exception &e) {
        Logger::error(LogCategory::QUALITY, "checkConstraints",
                      "Error checking constraint " + constraintName + ": " +
                          std::string(e.what()));
        constraint_issues[constraintName] = {{"error", e.what()}};
      }
    }

    metrics.integrity_check_details = constraint_issues;
    txn.commit();
    return true;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::QUALITY, "checkConstraints",
                  "Error checking constraints: " + std::string(e.what()));
    return false;
  }
}

void DataQuality::calculateQualityScore(QualityMetrics &metrics) {
  double score = 100.0;

  // Deduct points for various issues
  if (metrics.total_rows > 0) {
    // Deduct for null values
    score -=
        (static_cast<double>(metrics.null_count) / metrics.total_rows) * 20.0;

    // Deduct for duplicates
    score -=
        (static_cast<double>(metrics.duplicate_count) / metrics.total_rows) *
        20.0;

    // Deduct for type mismatches
    score -=
        (static_cast<double>(metrics.invalid_type_count) / metrics.total_rows) *
        30.0;

    // Deduct for integrity errors
    score -= (static_cast<double>(metrics.referential_integrity_errors) /
              metrics.total_rows) *
             30.0;
  }

  // Ensure score stays within 0-100 range
  metrics.quality_score = std::max(0.0, std::min(100.0, score));
}

std::string
DataQuality::determineValidationStatus(const QualityMetrics &metrics) {
  if (metrics.quality_score >= 90.0) {
    return "PASSED";
  } else if (metrics.quality_score >= 70.0) {
    return "WARNING";
  } else {
    return "FAILED";
  }
}

bool DataQuality::saveMetrics(pqxx::connection &conn,
                              const QualityMetrics &metrics) {
  try {
    pqxx::work txn(conn);

    txn.exec_params(
        "INSERT INTO metadata.data_quality ("
        "schema_name, table_name, source_db_engine, "
        "total_rows, null_count, duplicate_count, "
        "invalid_type_count, type_mismatch_details, out_of_range_count, "
        "referential_integrity_errors, constraint_violation_count, "
        "integrity_check_details, "
        "validation_status, error_details, quality_score, check_duration_ms"
        ") VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, "
        "$13, $14, $15, $16)",
        metrics.schema_name, metrics.table_name, metrics.source_db_engine,
        static_cast<int64_t>(metrics.total_rows),
        static_cast<int64_t>(metrics.null_count),
        static_cast<int64_t>(metrics.duplicate_count),
        static_cast<int64_t>(metrics.invalid_type_count),
        metrics.type_mismatch_details.dump(),
        static_cast<int64_t>(metrics.out_of_range_count),
        static_cast<int64_t>(metrics.referential_integrity_errors),
        static_cast<int64_t>(metrics.constraint_violation_count),
        metrics.integrity_check_details.dump(), metrics.validation_status,
        metrics.error_details, metrics.quality_score,
        metrics.check_duration_ms);

    txn.commit();
    return true;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::QUALITY, "saveMetrics",
                  "Error saving metrics: " + std::string(e.what()));
    return false;
  }
}

std::vector<DataQuality::QualityMetrics>
DataQuality::getLatestMetrics(pqxx::connection &conn,
                              const std::string &status) {

  std::vector<QualityMetrics> results;

  try {
    pqxx::work txn(conn);

    std::string query =
        "WITH latest_checks AS ("
        "  SELECT DISTINCT ON (schema_name, table_name) "
        "    schema_name, table_name, source_db_engine, "
        "    total_rows, null_count, duplicate_count, "
        "    invalid_type_count, type_mismatch_details, out_of_range_count, "
        "    referential_integrity_errors, constraint_violation_count, "
        "    integrity_check_details, validation_status, error_details, "
        "    quality_score, check_duration_ms "
        "  FROM metadata.data_quality "
        "  ORDER BY schema_name, table_name, check_timestamp DESC"
        ") SELECT * FROM latest_checks ";

    if (!status.empty()) {
      query += "WHERE validation_status = " + txn.quote(status);
    }

    query += " ORDER BY schema_name, table_name";

    auto result = txn.exec(query);

    for (const auto &row : result) {
      QualityMetrics metrics;
      metrics.schema_name = row["schema_name"].as<std::string>();
      metrics.table_name = row["table_name"].as<std::string>();
      metrics.source_db_engine = row["source_db_engine"].as<std::string>();
      metrics.total_rows = row["total_rows"].as<size_t>();
      metrics.null_count = row["null_count"].as<size_t>();
      metrics.duplicate_count = row["duplicate_count"].as<size_t>();
      metrics.invalid_type_count = row["invalid_type_count"].as<size_t>();
      metrics.type_mismatch_details =
          json::parse(row["type_mismatch_details"].as<std::string>());
      metrics.out_of_range_count = row["out_of_range_count"].as<size_t>();
      metrics.referential_integrity_errors =
          row["referential_integrity_errors"].as<size_t>();
      metrics.constraint_violation_count =
          row["constraint_violation_count"].as<size_t>();
      metrics.integrity_check_details =
          json::parse(row["integrity_check_details"].as<std::string>());
      metrics.validation_status = row["validation_status"].as<std::string>();
      metrics.error_details = row["error_details"].is_null()
                                  ? ""
                                  : row["error_details"].as<std::string>();
      metrics.quality_score = row["quality_score"].as<double>();
      metrics.check_duration_ms = row["check_duration_ms"].as<int64_t>();

      results.push_back(metrics);
    }

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::QUALITY, "getLatestMetrics",
                  "Error getting latest metrics: " + std::string(e.what()));
  }

  return results;
}
