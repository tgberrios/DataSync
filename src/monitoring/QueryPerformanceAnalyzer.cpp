#include "monitoring/QueryPerformanceAnalyzer.h"
#include "core/database_config.h"
#include "core/logger.h"
#include <pqxx/pqxx>
#include <sstream>
#include <regex>
#include <algorithm>

QueryPerformanceAnalyzer::QueryPerformanceAnalyzer(const std::string& connectionString)
    : connectionString_(connectionString) {
  // Tables are created in migration
}

std::unique_ptr<QueryPerformanceAnalyzer::QueryAnalysis> QueryPerformanceAnalyzer::analyzeQuery(
    const std::string& queryId, const std::string& queryText) {
  auto analysis = std::make_unique<QueryAnalysis>();
  analysis->queryId = queryId;
  analysis->queryText = queryText;
  analysis->queryFingerprint = generateFingerprint(queryText);
  analysis->explainPlan = executeExplainAnalyze(queryText);
  analysis->issues = detectIssues(analysis->explainPlan);
  analysis->recommendations = generateRecommendations(analysis->explainPlan, analysis->issues);
  analysis->analyzedAt = std::chrono::system_clock::now();

  saveAnalysisToDatabase(*analysis);
  return analysis;
}

std::string QueryPerformanceAnalyzer::normalizeQuery(const std::string& queryText) {
  std::string normalized = queryText;
  // Remove extra whitespace
  normalized = std::regex_replace(normalized, std::regex("\\s+"), " ");
  // Normalize case
  std::transform(normalized.begin(), normalized.end(), normalized.begin(), ::tolower);
  // Replace parameter values with placeholders
  normalized = std::regex_replace(normalized, std::regex("\\d+"), "?");
  normalized = std::regex_replace(normalized, std::regex("'[^']*'"), "'?'");
  return normalized;
}

std::string QueryPerformanceAnalyzer::generateFingerprint(const std::string& queryText) {
  std::string normalized = normalizeQuery(queryText);
  // Simple hash (in production, use proper hash function)
  std::hash<std::string> hasher;
  return std::to_string(hasher(normalized));
}

json QueryPerformanceAnalyzer::executeExplainAnalyze(const std::string& queryText) {
  json plan;
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string explainQuery = "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + queryText;
    auto result = txn.exec(explainQuery);

    if (!result.empty()) {
      plan = json::parse(result[0][0].as<std::string>());
    }
  } catch (const std::exception& e) {
    Logger::error(LogCategory::MONITORING, "QueryPerformanceAnalyzer",
                  "Error executing EXPLAIN ANALYZE: " + std::string(e.what()));
  }

  return plan;
}

std::vector<std::string> QueryPerformanceAnalyzer::detectIssues(const json& explainPlan) {
  std::vector<std::string> issues;

  if (explainPlan.empty()) {
    return issues;
  }

  // Check for sequential scans
  std::string planStr = explainPlan.dump();
  if (planStr.find("Seq Scan") != std::string::npos) {
    issues.push_back("seq_scan");
  }

  // Check for missing indexes
  if (planStr.find("Index Scan") == std::string::npos && planStr.find("Index Only Scan") == std::string::npos) {
    if (planStr.find("Seq Scan") != std::string::npos) {
      issues.push_back("missing_index");
    }
  }

  // Check for nested loops (potential N+1)
  if (planStr.find("Nested Loop") != std::string::npos) {
    issues.push_back("n_plus_one");
  }

  return issues;
}

std::vector<std::string> QueryPerformanceAnalyzer::generateRecommendations(
    const json& explainPlan, const std::vector<std::string>& issues) {
  std::vector<std::string> recommendations;

  for (const auto& issue : issues) {
    if (issue == "seq_scan") {
      recommendations.push_back("Consider adding an index on the filtered columns");
    } else if (issue == "missing_index") {
      recommendations.push_back("Add indexes to improve query performance");
    } else if (issue == "n_plus_one") {
      recommendations.push_back("Consider using JOINs instead of nested queries");
    }
  }

  return recommendations;
}

bool QueryPerformanceAnalyzer::saveAnalysisToDatabase(const QueryAnalysis& analysis) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    json issuesJson = json::array();
    for (const auto& issue : analysis.issues) {
      issuesJson.push_back(issue);
    }

    json recommendationsJson = json::array();
    for (const auto& rec : analysis.recommendations) {
      recommendationsJson.push_back(rec);
    }

    txn.exec_params(
        "INSERT INTO metadata.query_performance_analysis "
        "(query_id, query_text, query_fingerprint, explain_plan, execution_time, rows_examined, "
        "rows_returned, issues, recommendations) "
        "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) "
        "ON CONFLICT (query_id) DO UPDATE SET "
        "query_text = EXCLUDED.query_text, explain_plan = EXCLUDED.explain_plan, "
        "execution_time = EXCLUDED.execution_time, issues = EXCLUDED.issues, "
        "recommendations = EXCLUDED.recommendations, analyzed_at = NOW()",
        analysis.queryId, analysis.queryText, analysis.queryFingerprint, analysis.explainPlan.dump(),
        analysis.executionTime, analysis.rowsExamined, analysis.rowsReturned, issuesJson.dump(),
        recommendationsJson.dump());

    txn.commit();
    return true;
  } catch (const std::exception& e) {
    Logger::error(LogCategory::MONITORING, "QueryPerformanceAnalyzer",
                  "Error saving analysis: " + std::string(e.what()));
    return false;
  }
}

std::vector<QueryPerformanceAnalyzer::Regression> QueryPerformanceAnalyzer::detectRegressions(
    int days) {
  std::vector<Regression> regressions;
  // TODO: Implement regression detection
  return regressions;
}

std::vector<QueryPerformanceAnalyzer::OptimizationSuggestion>
QueryPerformanceAnalyzer::generateSuggestions(const std::string& queryFingerprint) {
  std::vector<OptimizationSuggestion> suggestions;
  // TODO: Implement suggestion generation
  return suggestions;
}

std::unique_ptr<QueryPerformanceAnalyzer::QueryAnalysis> QueryPerformanceAnalyzer::getAnalysis(
    const std::string& queryId) {
  // TODO: Implement
  return nullptr;
}

std::vector<QueryPerformanceAnalyzer::OptimizationSuggestion>
QueryPerformanceAnalyzer::getSuggestions(const std::string& queryFingerprint) {
  std::vector<OptimizationSuggestion> suggestions;
  // TODO: Implement
  return suggestions;
}

bool QueryPerformanceAnalyzer::saveSuggestionToDatabase(
    const OptimizationSuggestion& suggestion) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    txn.exec_params(
        "INSERT INTO metadata.query_optimization_suggestions "
        "(suggestion_id, query_fingerprint, type, description, sql_suggestion, estimated_improvement) "
        "VALUES ($1, $2, $3, $4, $5, $6) "
        "ON CONFLICT (suggestion_id) DO UPDATE SET "
        "description = EXCLUDED.description, sql_suggestion = EXCLUDED.sql_suggestion, "
        "estimated_improvement = EXCLUDED.estimated_improvement",
        suggestion.id, suggestion.queryFingerprint, suggestion.type, suggestion.description,
        suggestion.sqlSuggestion, suggestion.estimatedImprovement);

    txn.commit();
    return true;
  } catch (const std::exception& e) {
    Logger::error(LogCategory::MONITORING, "QueryPerformanceAnalyzer",
                  "Error saving suggestion: " + std::string(e.what()));
    return false;
  }
}
