#ifndef TRANSFORMATION_ENGINE_H
#define TRANSFORMATION_ENGINE_H

#include "third_party/json.hpp"
#include "transformations/spark_translator.h"
#include "sync/JoinOptimizer.h"
#include "utils/MemoryManager.h"
#include <string>
#include <vector>
#include <memory>
#include <map>

using json = nlohmann::json;

// Base class for all transformations
class Transformation {
public:
  virtual ~Transformation() = default;
  
  // Execute the transformation on input data
  virtual std::vector<json> execute(
    const std::vector<json>& inputData,
    const json& config
  ) = 0;
  
  // Get the transformation type name
  virtual std::string getType() const = 0;
  
  // Validate the transformation configuration
  virtual bool validateConfig(const json& config) const = 0;
};

// Transformation engine that orchestrates multiple transformations
class TransformationEngine {
public:
  TransformationEngine();
  ~TransformationEngine();
  
  // Register a transformation type
  void registerTransformation(std::unique_ptr<Transformation> transformation);
  
  // Execute a pipeline of transformations
  std::vector<json> executePipeline(
    const std::vector<json>& inputData,
    const json& pipelineConfig
  );
  
  // Execute a single transformation
  std::vector<json> executeTransformation(
    const std::vector<json>& inputData,
    const std::string& transformationType,
    const json& config
  );
  
  // Execute pipeline using Spark (if available and configured)
  std::vector<json> executePipelineWithSpark(
    const std::vector<json>& inputData,
    const json& pipelineConfig
  );
  
  // Validate a pipeline configuration
  bool validatePipeline(const json& pipelineConfig) const;

private:
  std::map<std::string, std::unique_ptr<Transformation>> transformations_;
  std::unique_ptr<MemoryManager> memoryManager_;
  
  // Check if pipeline should use Spark
  bool shouldUseSpark(const json& pipelineConfig) const;
  
  // Optimize join operations
  std::vector<json> optimizeJoin(
    const std::vector<json>& leftData,
    const std::vector<json>& rightData,
    const json& joinConfig
  );
};

#endif // TRANSFORMATION_ENGINE_H
