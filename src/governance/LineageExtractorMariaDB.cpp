#include "governance/LineageExtractorMariaDB.h"
#include "core/logger.h"

LineageExtractorMariaDB::LineageExtractorMariaDB() {}

LineageExtractorMariaDB::~LineageExtractorMariaDB() {}

void LineageExtractorMariaDB::extractLineage(
    const std::string &connectionString) {
  Logger::info(LogCategory::GOVERNANCE, "LineageExtractorMariaDB",
               "Extracting lineage for MariaDB");
}

void LineageExtractorMariaDB::storeLineage() {
  Logger::info(LogCategory::GOVERNANCE, "LineageExtractorMariaDB",
               "Storing lineage data");
}

void LineageExtractorMariaDB::analyzeDependencies() {
  Logger::info(LogCategory::GOVERNANCE, "LineageExtractorMariaDB",
               "Analyzing dependencies");
}

void LineageExtractorMariaDB::extractTableDependencies() {}

void LineageExtractorMariaDB::extractViewDependencies() {}

void LineageExtractorMariaDB::extractTriggerDependencies() {}

void LineageExtractorMariaDB::buildDependencyGraph() {}
