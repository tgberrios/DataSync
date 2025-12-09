#include "governance/QueryStoreCollector.h"
#include "core/logger.h"
#include "core/database_config.h"
#include <pqxx/pqxx>
#include <algorithm>
#include <sstream>
#include <regex>
#include <iomanip>
#include <cmath>

QueryStoreCollector::QueryStoreCollector(const std::string &connectionString)
    : connectionString_(connectionString) {
}

QueryStoreCollector::~QueryStoreCollector() {
}

void QueryStoreCollector::collectQuerySnapshots() {
  Logger::info(LogCategory::GOVERNANCE, "QueryStoreCollector",
               "Starting query snapshot collection");

  snapshots_.clear();

  try {
    pqxx::connection conn(connectionString_);
    if (!conn.is_open()) {
      Logger::error(LogCategory::GOVERNANCE, "QueryStoreCollector",
                    "Failed to connect to PostgreSQL");
      return;
    }

    queryPgStatStatements(conn);

    for (auto &snapshot : snapshots_) {
      parseQueryText(snapshot);
      extractQueryMetadata(snapshot);
      calculateMetrics(snapshot);
    }

    Logger::info(LogCategory::GOVERNANCE, "QueryStoreCollector",
                 "Collected " + std::to_string(snapshots_.size()) + " query snapshots");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "QueryStoreCollector",
                  "Error collecting snapshots: " + std::string(e.what()));
  }
}

void QueryStoreCollector::queryPgStatStatements(pqxx::connection &conn) {
  try {
    pqxx::work txn(conn);

    std::string checkExtension = "SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements')";
    auto extResult = txn.exec(checkExtension);
    bool extensionExists = false;
    if (!extResult.empty() && !extResult[0][0].is_null()) {
      extensionExists = extResult[0][0].as<bool>();
    }

    if (!extensionExists) {
      Logger::warning(LogCategory::GOVERNANCE, "QueryStoreCollector",
                      "pg_stat_statements extension not enabled. Run: CREATE EXTENSION pg_stat_statements;");
      txn.commit();
      return;
    }

    std::string query = R"(
      SELECT 
        COALESCE(pg_database.datname::text, 'unknown') as datname,
        COALESCE(pg_user.usename::text, 'unknown') as usename,
        pss.queryid,
        pss.query,
        pss.calls,
        pss.total_exec_time,
        pss.mean_exec_time,
        pss.rows,
        pss.shared_blks_hit,
        pss.shared_blks_read,
        pss.shared_blks_dirtied,
        pss.shared_blks_written,
        pss.local_blks_hit,
        pss.local_blks_read,
        pss.local_blks_dirtied,
        pss.local_blks_written,
        pss.temp_blks_read,
        pss.temp_blks_written,
        pss.blk_read_time,
        pss.blk_write_time,
        pss.wal_records,
        pss.wal_fpi,
        pss.wal_bytes
      FROM pg_stat_statements pss
      LEFT JOIN pg_database ON pss.dbid = pg_database.oid
      LEFT JOIN pg_user ON pss.userid = pg_user.usesysid
      WHERE pss.query NOT LIKE '%pg_stat_statements%'
        AND pss.query NOT LIKE '%pg_catalog%'
      ORDER BY pss.total_exec_time DESC
      LIMIT 1000
    )";

    auto results = txn.exec(query);
    txn.commit();

    for (const auto &row : results) {
      QuerySnapshot snapshot;
      int col = 0;

      if (!row[col].is_null()) snapshot.dbname = row[col].as<std::string>();
      col++;
      if (!row[col].is_null()) snapshot.username = row[col].as<std::string>();
      col++;
      if (!row[col].is_null()) snapshot.queryid = row[col].as<long long>();
      col++;
      if (!row[col].is_null()) snapshot.query_text = row[col].as<std::string>();
      col++;
      if (!row[col].is_null()) snapshot.calls = row[col].as<long long>();
      col++;
      if (!row[col].is_null()) snapshot.total_time_ms = row[col].as<double>();
      col++;
      if (!row[col].is_null()) snapshot.mean_time_ms = row[col].as<double>();
      col++;
      if (!row[col].is_null()) snapshot.rows_returned = row[col].as<long long>();
      col++;
      if (!row[col].is_null()) snapshot.shared_blks_hit = row[col].as<long long>();
      col++;
      if (!row[col].is_null()) snapshot.shared_blks_read = row[col].as<long long>();
      col++;
      if (!row[col].is_null()) snapshot.shared_blks_dirtied = row[col].as<long long>();
      col++;
      if (!row[col].is_null()) snapshot.shared_blks_written = row[col].as<long long>();
      col++;
      if (!row[col].is_null()) snapshot.local_blks_hit = row[col].as<long long>();
      col++;
      if (!row[col].is_null()) snapshot.local_blks_read = row[col].as<long long>();
      col++;
      if (!row[col].is_null()) snapshot.local_blks_dirtied = row[col].as<long long>();
      col++;
      if (!row[col].is_null()) snapshot.local_blks_written = row[col].as<long long>();
      col++;
      if (!row[col].is_null()) snapshot.temp_blks_read = row[col].as<long long>();
      col++;
      if (!row[col].is_null()) snapshot.temp_blks_written = row[col].as<long long>();
      col++;
      if (!row[col].is_null()) snapshot.blk_read_time_ms = row[col].as<double>();
      col++;
      if (!row[col].is_null()) snapshot.blk_write_time_ms = row[col].as<double>();
      col++;
      if (!row[col].is_null()) snapshot.wal_records = row[col].as<long long>();
      col++;
      if (!row[col].is_null()) snapshot.wal_fpi = row[col].as<long long>();
      col++;
      if (!row[col].is_null()) snapshot.wal_bytes = row[col].as<double>();

      snapshots_.push_back(snapshot);
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "QueryStoreCollector",
                  "Error querying pg_stat_statements: " + std::string(e.what()));
  }
}

void QueryStoreCollector::parseQueryText(QuerySnapshot &snapshot) {
  std::string query = snapshot.query_text;
  std::transform(query.begin(), query.end(), query.begin(), ::tolower);

  snapshot.has_joins = (query.find(" join ") != std::string::npos ||
                        query.find("inner join") != std::string::npos ||
                        query.find("left join") != std::string::npos ||
                        query.find("right join") != std::string::npos);

  snapshot.has_subqueries = (query.find("(select") != std::string::npos ||
                             query.find("( select") != std::string::npos);

  snapshot.has_cte = (query.find("with ") != std::string::npos &&
                      query.find(" as (") != std::string::npos);

  snapshot.has_window_functions = (query.find("over (") != std::string::npos ||
                                   query.find("row_number()") != std::string::npos ||
                                   query.find("rank()") != std::string::npos);

  snapshot.has_functions = (query.find("(") != std::string::npos &&
                            (query.find("count(") != std::string::npos ||
                             query.find("sum(") != std::string::npos ||
                             query.find("avg(") != std::string::npos ||
                             query.find("max(") != std::string::npos ||
                             query.find("min(") != std::string::npos));

  std::regex tableRegex(R"(\bfrom\s+(\w+)|join\s+(\w+))", std::regex_constants::icase);
  std::sregex_iterator iter(query.begin(), query.end(), tableRegex);
  std::sregex_iterator end;
  snapshot.tables_count = std::distance(iter, end);
}

void QueryStoreCollector::extractQueryMetadata(QuerySnapshot &snapshot) {
  snapshot.operation_type = extractOperationType(snapshot.query_text);
  snapshot.query_fingerprint = generateFingerprint(snapshot.query_text);
  snapshot.query_category = categorizeQuery(snapshot.query_text);
}

void QueryStoreCollector::calculateMetrics(QuerySnapshot &snapshot) {
  long long total_blks = snapshot.shared_blks_hit + snapshot.shared_blks_read;
  if (total_blks > 0) {
    snapshot.cache_hit_ratio = (snapshot.shared_blks_hit * 100.0) / total_blks;
  }

  long long total_io = snapshot.shared_blks_read + snapshot.temp_blks_read;
  long long total_writes = snapshot.shared_blks_written + snapshot.temp_blks_written;
  if (snapshot.total_time_ms > 0) {
    snapshot.io_efficiency = (total_io + total_writes) / snapshot.total_time_ms;
  }

  double time_score = snapshot.mean_time_ms < 100 ? 100.0 : 
                      snapshot.mean_time_ms < 1000 ? 80.0 :
                      snapshot.mean_time_ms < 5000 ? 60.0 : 40.0;

  double cache_score = snapshot.cache_hit_ratio;
  double io_score = snapshot.io_efficiency > 0 ? std::min(100.0, 100.0 / snapshot.io_efficiency) : 50.0;

  snapshot.query_efficiency_score = (time_score * 0.4 + cache_score * 0.4 + io_score * 0.2);

  if (snapshot.calls > 0) {
    snapshot.min_time_ms = snapshot.mean_time_ms * 0.5;
    snapshot.max_time_ms = snapshot.mean_time_ms * 2.0;
  }
}

std::string QueryStoreCollector::categorizeQuery(const std::string &queryText) {
  std::string query = queryText;
  std::transform(query.begin(), query.end(), query.begin(), ::tolower);

  if (query.find("select") == 0) {
    if (query.find("count") != std::string::npos) return "ANALYTICS";
    if (query.find("sum") != std::string::npos || query.find("avg") != std::string::npos) return "ANALYTICS";
    if (query.find("join") != std::string::npos) return "JOIN_QUERY";
    return "SELECT";
  }
  if (query.find("insert") == 0) return "INSERT";
  if (query.find("update") == 0) return "UPDATE";
  if (query.find("delete") == 0) return "DELETE";
  if (query.find("create") == 0) return "DDL";
  if (query.find("alter") == 0) return "DDL";
  if (query.find("drop") == 0) return "DDL";

  return "OTHER";
}

std::string QueryStoreCollector::extractOperationType(const std::string &queryText) {
  std::string query = queryText;
  std::transform(query.begin(), query.end(), query.begin(), ::tolower);
  std::regex whitespace(R"(\s+)");
  query = std::regex_replace(query, whitespace, " ");

  if (query.find("select") == 0) return "SELECT";
  if (query.find("insert") == 0) return "INSERT";
  if (query.find("update") == 0) return "UPDATE";
  if (query.find("delete") == 0) return "DELETE";
  if (query.find("create") == 0) return "CREATE";
  if (query.find("alter") == 0) return "ALTER";
  if (query.find("drop") == 0) return "DROP";

  return "UNKNOWN";
}

std::string QueryStoreCollector::generateFingerprint(const std::string &queryText) {
  std::string fingerprint = queryText;
  std::regex numberRegex(R"(\b\d+\b)");
  fingerprint = std::regex_replace(fingerprint, numberRegex, "?");
  std::regex stringRegex(R"('([^']*)')");
  fingerprint = std::regex_replace(fingerprint, stringRegex, "'?'");
  std::transform(fingerprint.begin(), fingerprint.end(), fingerprint.begin(), ::tolower);
  return fingerprint;
}

void QueryStoreCollector::storeSnapshots() {
  if (snapshots_.empty()) {
    Logger::warning(LogCategory::GOVERNANCE, "QueryStoreCollector",
                    "No snapshots to store");
    return;
  }

  try {
    pqxx::connection conn(DatabaseConfig::getPostgresConnectionString());
    if (!conn.is_open()) {
      Logger::error(LogCategory::GOVERNANCE, "QueryStoreCollector",
                    "Failed to connect to PostgreSQL for storage");
      return;
    }

    int stored = 0;
    for (const auto &snapshot : snapshots_) {
      try {
        pqxx::work txn(conn);
        std::string query = R"(
          INSERT INTO metadata.query_performance (
            source_type, dbname, username, queryid, query_text, calls, total_time_ms, mean_time_ms,
            min_time_ms, max_time_ms, rows_returned, shared_blks_hit, shared_blks_read, shared_blks_dirtied,
            shared_blks_written, local_blks_hit, local_blks_read, local_blks_dirtied,
            local_blks_written, temp_blks_read, temp_blks_written, blk_read_time_ms,
            blk_write_time_ms, wal_records, wal_fpi, wal_bytes,
            operation_type, query_fingerprint, tables_count, has_joins, has_subqueries,
            has_cte, has_window_functions, has_functions, query_category
          ) VALUES (
            'snapshot', $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17,
            $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33
          )
        )";

        txn.exec_params(query,
          snapshot.dbname.empty() ? nullptr : snapshot.dbname.c_str(),
          snapshot.username.empty() ? nullptr : snapshot.username.c_str(),
          snapshot.queryid,
          snapshot.query_text.empty() ? nullptr : snapshot.query_text.c_str(),
          snapshot.calls,
          snapshot.total_time_ms,
          snapshot.mean_time_ms,
          snapshot.min_time_ms,
          snapshot.max_time_ms,
          snapshot.rows_returned,
          snapshot.shared_blks_hit,
          snapshot.shared_blks_read,
          snapshot.shared_blks_dirtied,
          snapshot.shared_blks_written,
          snapshot.local_blks_hit,
          snapshot.local_blks_read,
          snapshot.local_blks_dirtied,
          snapshot.local_blks_written,
          snapshot.temp_blks_read,
          snapshot.temp_blks_written,
          snapshot.blk_read_time_ms,
          snapshot.blk_write_time_ms,
          snapshot.wal_records,
          snapshot.wal_fpi,
          snapshot.wal_bytes,
          snapshot.operation_type.empty() ? nullptr : snapshot.operation_type.c_str(),
          snapshot.query_fingerprint.empty() ? nullptr : snapshot.query_fingerprint.c_str(),
          snapshot.tables_count,
          snapshot.has_joins,
          snapshot.has_subqueries,
          snapshot.has_cte,
          snapshot.has_window_functions,
          snapshot.has_functions,
          snapshot.query_category.empty() ? nullptr : snapshot.query_category.c_str()
        );
        txn.commit();
        stored++;
      } catch (const std::exception &e) {
        Logger::error(LogCategory::GOVERNANCE, "QueryStoreCollector",
                      "Error storing snapshot: " + std::string(e.what()));
      }
    }

    Logger::info(LogCategory::GOVERNANCE, "QueryStoreCollector",
                 "Stored " + std::to_string(stored) + " snapshots in unified table");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "QueryStoreCollector",
                  "Error storing snapshots: " + std::string(e.what()));
  }
}

void QueryStoreCollector::analyzeQueries() {
  Logger::info(LogCategory::GOVERNANCE, "QueryStoreCollector",
               "Analyzing " + std::to_string(snapshots_.size()) + " queries");

  int slowQueries = 0;
  int highIOQueries = 0;
  int lowCacheHitQueries = 0;

  for (const auto &snapshot : snapshots_) {
    if (snapshot.mean_time_ms > 1000) slowQueries++;
    if (snapshot.shared_blks_read > 10000) highIOQueries++;
    if (snapshot.cache_hit_ratio < 80) lowCacheHitQueries++;
  }

  Logger::info(LogCategory::GOVERNANCE, "QueryStoreCollector",
               "Analysis: " + std::to_string(slowQueries) + " slow queries, " +
               std::to_string(highIOQueries) + " high IO queries, " +
               std::to_string(lowCacheHitQueries) + " low cache hit queries");
}
