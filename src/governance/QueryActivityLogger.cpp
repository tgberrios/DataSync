#include "governance/QueryActivityLogger.h"
#include "core/logger.h"
#include "core/database_config.h"
#include <pqxx/pqxx>
#include <algorithm>
#include <sstream>
#include <regex>
#include <cmath>

QueryActivityLogger::QueryActivityLogger(const std::string &connectionString)
    : connectionString_(connectionString) {
}

QueryActivityLogger::~QueryActivityLogger() {
}

void QueryActivityLogger::logActiveQueries() {
  Logger::info(LogCategory::GOVERNANCE, "QueryActivityLogger",
               "Starting active query logging");

  activities_.clear();

  try {
    pqxx::connection conn(connectionString_);
    if (!conn.is_open()) {
      Logger::error(LogCategory::GOVERNANCE, "QueryActivityLogger",
                    "Failed to connect to PostgreSQL");
      return;
    }

    queryPgStatActivity(conn);

    for (auto &activity : activities_) {
      extractQueryInfo(activity);
      calculateMetrics(activity);
    }

    categorizeQueries();

    Logger::info(LogCategory::GOVERNANCE, "QueryActivityLogger",
                 "Collected " + std::to_string(activities_.size()) + " active queries");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "QueryActivityLogger",
                  "Error logging active queries: " + std::string(e.what()));
  }
}

void QueryActivityLogger::queryPgStatActivity(pqxx::connection &conn) {
  try {
    pqxx::work txn(conn);

    std::string query = R"(
      SELECT 
        pid,
        datname,
        usename,
        application_name,
        client_addr::text,
        state,
        wait_event_type,
        wait_event,
        query,
        EXTRACT(EPOCH FROM (NOW() - query_start)) * 1000 as query_duration_ms
      FROM pg_stat_activity
      WHERE state IN ('active', 'idle in transaction', 'idle in transaction (aborted)')
        AND query NOT LIKE '%pg_stat_activity%'
        AND query NOT LIKE '%pg_catalog%'
        AND pid != pg_backend_pid()
    )";

    auto results = txn.exec(query);
    txn.commit();

    for (const auto &row : results) {
      QueryActivity activity;
      int col = 0;

      if (!row[col].is_null()) activity.pid = row[col].as<int>();
      col++;
      if (!row[col].is_null()) activity.dbname = row[col].as<std::string>();
      col++;
      if (!row[col].is_null()) activity.username = row[col].as<std::string>();
      col++;
      if (!row[col].is_null()) activity.application_name = row[col].as<std::string>();
      col++;
      if (!row[col].is_null()) activity.client_addr = row[col].as<std::string>();
      col++;
      if (!row[col].is_null()) activity.state = row[col].as<std::string>();
      col++;
      if (!row[col].is_null()) activity.wait_event_type = row[col].as<std::string>();
      col++;
      if (!row[col].is_null()) activity.wait_event = row[col].as<std::string>();
      col++;
      if (!row[col].is_null()) activity.query_text = row[col].as<std::string>();
      col++;
      if (!row[col].is_null()) activity.query_duration_ms = row[col].as<double>();

      activities_.push_back(activity);
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "QueryActivityLogger",
                  "Error querying pg_stat_activity: " + std::string(e.what()));
  }
}

void QueryActivityLogger::extractQueryInfo(QueryActivity &activity) {
  activity.operation_type = extractOperationType(activity.query_text);
  activity.query_category = categorizeQuery(activity.query_text);
  activity.is_long_running = (activity.query_duration_ms > 60000);
  activity.is_blocking = (!activity.wait_event_type.empty() && 
                          activity.wait_event_type != "ClientRead");
}

void QueryActivityLogger::calculateMetrics(QueryActivity &activity) {
  double duration_score = activity.query_duration_ms < 1000 ? 100.0 :
                          activity.query_duration_ms < 5000 ? 80.0 :
                          activity.query_duration_ms < 30000 ? 60.0 : 40.0;

  double state_score = (activity.state == "active") ? 100.0 : 60.0;
  double blocking_score = activity.is_blocking ? 40.0 : 100.0;

  activity.query_efficiency_score = (duration_score * 0.5 + state_score * 0.3 + blocking_score * 0.2);
}

void QueryActivityLogger::categorizeQueries() {
  for (auto &activity : activities_) {
    if (activity.query_category.empty()) {
      activity.query_category = categorizeQuery(activity.query_text);
    }
  }
}

std::string QueryActivityLogger::categorizeQuery(const std::string &queryText) {
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

std::string QueryActivityLogger::extractOperationType(const std::string &queryText) {
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

void QueryActivityLogger::storeActivityLog() {
  if (activities_.empty()) {
    Logger::warning(LogCategory::GOVERNANCE, "QueryActivityLogger",
                    "No activities to store");
    return;
  }

  try {
    pqxx::connection conn(DatabaseConfig::getPostgresConnectionString());
    if (!conn.is_open()) {
      Logger::error(LogCategory::GOVERNANCE, "QueryActivityLogger",
                    "Failed to connect to PostgreSQL for storage");
      return;
    }

    int stored = 0;
    for (const auto &activity : activities_) {
      try {
        pqxx::work txn(conn);
        std::string query = R"(
          INSERT INTO metadata.query_activity_log (
            pid, dbname, username, application_name, client_addr, state,
            wait_event_type, wait_event, query_text, query_duration_ms,
            operation_type, query_category
          ) VALUES (
            $1, $2, $3, $4, $5::inet, $6, $7, $8, $9, $10, $11, $12
          )
        )";

        txn.exec_params(query,
          activity.pid,
          activity.dbname.empty() ? nullptr : activity.dbname.c_str(),
          activity.username.empty() ? nullptr : activity.username.c_str(),
          activity.application_name.empty() ? nullptr : activity.application_name.c_str(),
          activity.client_addr.empty() ? nullptr : activity.client_addr.c_str(),
          activity.state.empty() ? nullptr : activity.state.c_str(),
          activity.wait_event_type.empty() ? nullptr : activity.wait_event_type.c_str(),
          activity.wait_event.empty() ? nullptr : activity.wait_event.c_str(),
          activity.query_text.empty() ? nullptr : activity.query_text.c_str(),
          activity.query_duration_ms,
          activity.operation_type.empty() ? nullptr : activity.operation_type.c_str(),
          activity.query_category.empty() ? nullptr : activity.query_category.c_str()
        );
        txn.commit();
        stored++;
      } catch (const std::exception &e) {
        Logger::error(LogCategory::GOVERNANCE, "QueryActivityLogger",
                      "Error storing activity: " + std::string(e.what()));
      }
    }

    Logger::info(LogCategory::GOVERNANCE, "QueryActivityLogger",
                 "Stored " + std::to_string(stored) + " activity records");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "QueryActivityLogger",
                  "Error storing activities: " + std::string(e.what()));
  }
}

void QueryActivityLogger::analyzeActivity() {
  Logger::info(LogCategory::GOVERNANCE, "QueryActivityLogger",
               "Analyzing " + std::to_string(activities_.size()) + " active queries");

  int longRunning = 0;
  int blocking = 0;
  int idleInTransaction = 0;

  for (const auto &activity : activities_) {
    if (activity.is_long_running) longRunning++;
    if (activity.is_blocking) blocking++;
    if (activity.state == "idle in transaction") idleInTransaction++;
  }

  Logger::info(LogCategory::GOVERNANCE, "QueryActivityLogger",
               "Analysis: " + std::to_string(longRunning) + " long-running queries, " +
               std::to_string(blocking) + " blocking queries, " +
               std::to_string(idleInTransaction) + " idle in transaction");
}
