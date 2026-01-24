#ifndef SPARK_TRANSFORMATION_H
#define SPARK_TRANSFORMATION_H

#include "transformation_engine.h"
#include "transformations/spark_translator.h"
#include "engines/spark_engine.h"
#include "third_party/json.hpp"
#include "core/logger.h"
#include <string>
#include <vector>
#include <memory>

using json = nlohmann::json;

#ifdef HAVE_SPARK

// SparkTransformation: Ejecuta transformaciones usando Spark
class SparkTransformation : public Transformation {
public:
  explicit SparkTransformation(std::shared_ptr<SparkEngine> sparkEngine);
  ~SparkTransformation() override = default;
  
  std::vector<json> execute(
    const std::vector<json>& inputData,
    const json& config
  ) override;
  
  std::string getType() const override { return "spark"; }
  
  bool validateConfig(const json& config) const override;

private:
  std::shared_ptr<SparkEngine> sparkEngine_;
  
  // Ejecutar transformación en Spark
  std::vector<json> executeInSpark(const json& transformationConfig, const std::vector<json>& inputData);
};

#endif // HAVE_SPARK

#endif // SPARK_TRANSFORMATION_H
