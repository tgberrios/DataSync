#include "governance/QueryStoreCollector.h"
#include "core/logger.h"

QueryStoreCollector::QueryStoreCollector() {}

QueryStoreCollector::~QueryStoreCollector() {}

void QueryStoreCollector::collectQuerySnapshots(
    const std::string &connectionString) {
  Logger::info(LogCategory::GOVERNANCE, "QueryStoreCollector",
               "Collecting query store snapshots");
}

void QueryStoreCollector::storeSnapshots() {
  Logger::info(LogCategory::GOVERNANCE, "QueryStoreCollector",
               "Storing query snapshots");
}

void QueryStoreCollector::analyzeQueries() {
  Logger::info(LogCategory::GOVERNANCE, "QueryStoreCollector",
               "Analyzing queries");
}

void QueryStoreCollector::queryPgStatStatements() {}

void QueryStoreCollector::parseQueryText() {}

void QueryStoreCollector::extractQueryMetadata() {}
