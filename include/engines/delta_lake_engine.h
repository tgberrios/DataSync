#ifndef DELTA_LAKE_ENGINE_H
#define DELTA_LAKE_ENGINE_H

#include "engines/spark_engine.h"
#include "third_party/json.hpp"
#include "core/logger.h"
#include <string>
#include <memory>

using json = nlohmann::json;

#ifdef HAVE_DELTA_LAKE

// DeltaLakeEngine: Integración con Delta Lake
class DeltaLakeEngine {
public:
  explicit DeltaLakeEngine(std::shared_ptr<SparkEngine> sparkEngine);
  ~DeltaLakeEngine() = default;

  // Leer tabla Delta Lake
  json readTable(const std::string& tablePath, const std::string& version = "");

  // Escribir a tabla Delta Lake
  bool writeTable(const std::string& tablePath, const json& data, const std::string& mode = "append");

  // Time travel query
  json timeTravelQuery(const std::string& tablePath, const std::string& timestamp);

  // Merge operation
  bool merge(const std::string& targetPath, const std::string& sourcePath, const std::string& condition);

private:
  std::shared_ptr<SparkEngine> sparkEngine_;
};

#else

// Stub cuando Delta Lake no está disponible
class DeltaLakeEngine {
public:
  explicit DeltaLakeEngine(std::shared_ptr<SparkEngine>) {
    Logger::warning(LogCategory::SYSTEM, "DeltaLakeEngine",
                    "Delta Lake support not compiled");
  }
  ~DeltaLakeEngine() = default;
  json readTable(const std::string&, const std::string& = "") { return json::object(); }
  bool writeTable(const std::string&, const json&, const std::string& = "") { return false; }
  json timeTravelQuery(const std::string&, const std::string&) { return json::object(); }
  bool merge(const std::string&, const std::string&, const std::string&) { return false; }
};

#endif // HAVE_DELTA_LAKE

#endif // DELTA_LAKE_ENGINE_H
