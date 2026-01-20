#include "transformations/transformation_engine.h"
#include "transformations/lookup_transformation.h"
#include "transformations/aggregate_transformation.h"
#include "third_party/json.hpp"
#include <cassert>
#include <iostream>

using json = nlohmann::json;

void testEngineRegistration() {
  std::cout << "Testing TransformationEngine - Registration...\n";
  
  TransformationEngine engine;
  
  engine.registerTransformation(std::make_unique<LookupTransformation>());
  engine.registerTransformation(std::make_unique<AggregateTransformation>());
  
  std::cout << "✓ TransformationEngine registration test passed\n";
}

void testEnginePipeline() {
  std::cout << "Testing TransformationEngine - Pipeline execution...\n";
  
  TransformationEngine engine;
  engine.registerTransformation(std::make_unique<AggregateTransformation>());
  
  std::vector<json> inputData = {
    {{"category", "A"}, {"value", 10}},
    {{"category", "A"}, {"value", 20}},
    {{"category", "B"}, {"value", 15}}
  };
  
  json pipelineConfig;
  pipelineConfig["transformations"] = json::array({
    {
      {"type", "aggregate"},
      {"config", {
        {"group_by", json::array({"category"})},
        {"aggregations", json::array({
          {{"column", "value"}, {"function", "sum"}, {"alias", "total"}}
        })}
      }}
    }
  });
  
  assert(engine.validatePipeline(pipelineConfig) && "Pipeline should be valid");
  
  auto result = engine.executePipeline(inputData, pipelineConfig);
  assert(result.size() == 2 && "Pipeline should return 2 groups");
  
  std::cout << "✓ TransformationEngine pipeline test passed\n";
}

int main() {
  try {
    testEngineRegistration();
    testEnginePipeline();
    std::cout << "\n✅ All TransformationEngine tests passed!\n";
    return 0;
  } catch (const std::exception& e) {
    std::cerr << "❌ Test failed: " << e.what() << "\n";
    return 1;
  }
}
