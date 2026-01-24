#include "engines/delta_lake_engine.h"
#include "core/logger.h"

#ifdef HAVE_DELTA_LAKE

DeltaLakeEngine::DeltaLakeEngine(std::shared_ptr<SparkEngine> sparkEngine)
    : sparkEngine_(sparkEngine) {
  Logger::info(LogCategory::SYSTEM, "DeltaLakeEngine",
               "Initializing DeltaLakeEngine");
}

json DeltaLakeEngine::readTable(const std::string& tablePath, const std::string& version) {
  std::string sql = "SELECT * FROM delta.`" + tablePath + "`";
  if (!version.empty()) {
    sql += " VERSION AS OF " + version;
  }
  
  SparkEngine::SparkJobResult result = sparkEngine_->executeSQL(sql, "");
  return result.metadata;
}

bool DeltaLakeEngine::writeTable(const std::string& tablePath, const json& data, const std::string& mode) {
  std::string sql = "INSERT INTO delta.`" + tablePath + "` VALUES (...)";
  // Placeholder - implementación real generaría SQL desde data
  SparkEngine::SparkJobResult result = sparkEngine_->executeSQL(sql, "");
  return result.success;
}

json DeltaLakeEngine::timeTravelQuery(const std::string& tablePath, const std::string& timestamp) {
  std::string sql = "SELECT * FROM delta.`" + tablePath + "` TIMESTAMP AS OF '" + timestamp + "'";
  SparkEngine::SparkJobResult result = sparkEngine_->executeSQL(sql, "");
  return result.metadata;
}

bool DeltaLakeEngine::merge(const std::string& targetPath, const std::string& sourcePath, const std::string& condition) {
  std::string sql = "MERGE INTO delta.`" + targetPath + "` AS target "
                   "USING delta.`" + sourcePath + "` AS source "
                   "ON " + condition;
  SparkEngine::SparkJobResult result = sparkEngine_->executeSQL(sql, "");
  return result.success;
}

#endif // HAVE_DELTA_LAKE
