#include "governance/LineageExtractorMSSQL.h"
#include "core/logger.h"

LineageExtractorMSSQL::LineageExtractorMSSQL() {}

LineageExtractorMSSQL::~LineageExtractorMSSQL() {}

void LineageExtractorMSSQL::extractLineage(
    const std::string &connectionString) {
  Logger::info(LogCategory::GOVERNANCE, "LineageExtractorMSSQL",
               "Extracting lineage for MSSQL");
}

void LineageExtractorMSSQL::storeLineage() {
  Logger::info(LogCategory::GOVERNANCE, "LineageExtractorMSSQL",
               "Storing lineage data");
}

void LineageExtractorMSSQL::analyzeDependencies() {
  Logger::info(LogCategory::GOVERNANCE, "LineageExtractorMSSQL",
               "Analyzing dependencies");
}

void LineageExtractorMSSQL::extractTableDependencies() {}

void LineageExtractorMSSQL::extractStoredProcedureDependencies() {}

void LineageExtractorMSSQL::extractViewDependencies() {}

void LineageExtractorMSSQL::buildDependencyGraph() {}
