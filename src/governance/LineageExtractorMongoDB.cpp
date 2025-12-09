#include "governance/LineageExtractorMongoDB.h"
#include "core/logger.h"

LineageExtractorMongoDB::LineageExtractorMongoDB() {}

LineageExtractorMongoDB::~LineageExtractorMongoDB() {}

void LineageExtractorMongoDB::extractLineage(
    const std::string &connectionString) {
  Logger::info(LogCategory::GOVERNANCE, "LineageExtractorMongoDB",
               "Extracting lineage for MongoDB");
}

void LineageExtractorMongoDB::storeLineage() {
  Logger::info(LogCategory::GOVERNANCE, "LineageExtractorMongoDB",
               "Storing lineage data");
}

void LineageExtractorMongoDB::analyzeDependencies() {
  Logger::info(LogCategory::GOVERNANCE, "LineageExtractorMongoDB",
               "Analyzing dependencies");
}

void LineageExtractorMongoDB::extractCollectionDependencies() {}

void LineageExtractorMongoDB::extractViewDependencies() {}

void LineageExtractorMongoDB::extractPipelineDependencies() {}

void LineageExtractorMongoDB::buildDependencyGraph() {}
