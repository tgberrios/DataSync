#include "engines/iceberg_engine.h"
#include "core/logger.h"

#ifdef HAVE_ICEBERG

IcebergEngine::IcebergEngine(std::shared_ptr<SparkEngine> sparkEngine)
    : sparkEngine_(sparkEngine) {
  Logger::info(LogCategory::SYSTEM, "IcebergEngine",
               "Initializing IcebergEngine");
}

json IcebergEngine::readTable(const std::string& tablePath, const std::string& snapshotId) {
  std::string sql = "SELECT * FROM iceberg.`" + tablePath + "`";
  if (!snapshotId.empty()) {
    sql += " FOR SYSTEM_TIME AS OF " + snapshotId;
  }
  
  SparkEngine::SparkJobResult result = sparkEngine_->executeSQL(sql, "");
  return result.metadata;
}

bool IcebergEngine::writeTable(const std::string& tablePath, const json& data) {
  std::string sql = "INSERT INTO iceberg.`" + tablePath + "` VALUES (...)";
  SparkEngine::SparkJobResult result = sparkEngine_->executeSQL(sql, "");
  return result.success;
}

json IcebergEngine::timeTravelQuery(const std::string& tablePath, const std::string& snapshotId) {
  std::string sql = "SELECT * FROM iceberg.`" + tablePath + "` FOR SYSTEM_TIME AS OF " + snapshotId;
  SparkEngine::SparkJobResult result = sparkEngine_->executeSQL(sql, "");
  return result.metadata;
}

bool IcebergEngine::evolveSchema(const std::string& tablePath, const json& newSchema) {
  // Iceberg schema evolution
  std::string sql = "ALTER TABLE iceberg.`" + tablePath + "` ADD COLUMN ...";
  SparkEngine::SparkJobResult result = sparkEngine_->executeSQL(sql, "");
  return result.success;
}

#endif // HAVE_ICEBERG
