#include "governance/QueryActivityLogger.h"
#include "core/logger.h"

QueryActivityLogger::QueryActivityLogger() {}

QueryActivityLogger::~QueryActivityLogger() {}

void QueryActivityLogger::logActiveQueries(
    const std::string &connectionString) {
  Logger::info(LogCategory::GOVERNANCE, "QueryActivityLogger",
               "Logging active queries");
}

void QueryActivityLogger::storeActivityLog() {
  Logger::info(LogCategory::GOVERNANCE, "QueryActivityLogger",
               "Storing activity log");
}

void QueryActivityLogger::analyzeActivity() {
  Logger::info(LogCategory::GOVERNANCE, "QueryActivityLogger",
               "Analyzing query activity");
}

void QueryActivityLogger::queryPgStatActivity() {}

void QueryActivityLogger::extractQueryInfo() {}

void QueryActivityLogger::categorizeQueries() {}
