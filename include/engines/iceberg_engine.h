#ifndef ICEBERG_ENGINE_H
#define ICEBERG_ENGINE_H

#include "engines/spark_engine.h"
#include "third_party/json.hpp"
#include "core/logger.h"
#include <string>
#include <memory>

using json = nlohmann::json;

#ifdef HAVE_ICEBERG

// IcebergEngine: Integración con Apache Iceberg
class IcebergEngine {
public:
  explicit IcebergEngine(std::shared_ptr<SparkEngine> sparkEngine);
  ~IcebergEngine() = default;

  // Leer tabla Iceberg
  json readTable(const std::string& tablePath, const std::string& snapshotId = "");

  // Escribir a tabla Iceberg
  bool writeTable(const std::string& tablePath, const json& data);

  // Time travel query
  json timeTravelQuery(const std::string& tablePath, const std::string& snapshotId);

  // Schema evolution
  bool evolveSchema(const std::string& tablePath, const json& newSchema);

private:
  std::shared_ptr<SparkEngine> sparkEngine_;
};

#else

// Stub cuando Iceberg no está disponible
class IcebergEngine {
public:
  explicit IcebergEngine(std::shared_ptr<SparkEngine>) {
    Logger::warning(LogCategory::SYSTEM, "IcebergEngine",
                    "Iceberg support not compiled");
  }
  ~IcebergEngine() = default;
  json readTable(const std::string&, const std::string& = "") { return json::object(); }
  bool writeTable(const std::string&, const json&) { return false; }
  json timeTravelQuery(const std::string&, const std::string&) { return json::object(); }
  bool evolveSchema(const std::string&, const json&) { return false; }
};

#endif // HAVE_ICEBERG

#endif // ICEBERG_ENGINE_H
