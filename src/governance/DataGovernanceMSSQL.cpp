#include "governance/DataGovernanceMSSQL.h"
#include "core/logger.h"

DataGovernanceMSSQL::DataGovernanceMSSQL() {}

DataGovernanceMSSQL::~DataGovernanceMSSQL() {}

void DataGovernanceMSSQL::collectGovernanceData(
    const std::string &connectionString) {
  Logger::info(LogCategory::GOVERNANCE, "DataGovernanceMSSQL",
               "Collecting governance data for MSSQL");
}

void DataGovernanceMSSQL::storeGovernanceData() {
  Logger::info(LogCategory::GOVERNANCE, "DataGovernanceMSSQL",
               "Storing governance data");
}

void DataGovernanceMSSQL::generateReport() {
  Logger::info(LogCategory::GOVERNANCE, "DataGovernanceMSSQL",
               "Generating governance report");
}

void DataGovernanceMSSQL::queryIndexPhysicalStats() {}

void DataGovernanceMSSQL::queryIndexUsageStats() {}

void DataGovernanceMSSQL::queryMissingIndexes() {}

void DataGovernanceMSSQL::queryBackupInfo() {}

void DataGovernanceMSSQL::queryDatabaseConfig() {}

void DataGovernanceMSSQL::queryStoredProcedures() {}

void DataGovernanceMSSQL::calculateHealthScores() {}
