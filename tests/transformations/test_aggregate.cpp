#include "transformations/aggregate_transformation.h"
#include "third_party/json.hpp"
#include <cassert>
#include <iostream>
#include <cmath>

using json = nlohmann::json;

void testAggregateSum() {
  std::cout << "Testing AggregateTransformation - SUM...\n";
  
  AggregateTransformation aggregate;
  
  std::vector<json> inputData = {
    {{"category", "A"}, {"value", 10}},
    {{"category", "A"}, {"value", 20}},
    {{"category", "B"}, {"value", 15}},
    {{"category", "B"}, {"value", 25}}
  };
  
  json config;
  config["group_by"] = json::array({"category"});
  config["aggregations"] = json::array({
    {{"column", "value"}, {"function", "sum"}, {"alias", "total"}}
  });
  
  assert(aggregate.validateConfig(config) && "Config should be valid");
  
  auto result = aggregate.execute(inputData, config);
  assert(result.size() == 2 && "Should have 2 groups");
  
  std::cout << "✓ AggregateTransformation SUM test passed\n";
}

void testAggregateAvg() {
  std::cout << "Testing AggregateTransformation - AVG...\n";
  
  AggregateTransformation aggregate;
  
  std::vector<json> inputData = {
    {{"value", 10}},
    {{"value", 20}},
    {{"value", 30}}
  };
  
  json config;
  config["aggregations"] = json::array({
    {{"column", "value"}, {"function", "avg"}, {"alias", "average"}}
  });
  
  auto result = aggregate.execute(inputData, config);
  assert(result.size() == 1 && "Should have 1 result");
  assert(result[0].contains("average") && "Should have average column");
  
  double avg = result[0]["average"];
  assert(std::abs(avg - 20.0) < 0.001 && "Average should be 20");
  
  std::cout << "✓ AggregateTransformation AVG test passed\n";
}

void testAggregateValidation() {
  std::cout << "Testing AggregateTransformation - Validation...\n";
  
  AggregateTransformation aggregate;
  
  // Invalid - missing aggregations
  json invalid1;
  assert(!aggregate.validateConfig(invalid1) && "Should reject missing aggregations");
  
  // Invalid - empty aggregations
  json invalid2;
  invalid2["aggregations"] = json::array();
  assert(!aggregate.validateConfig(invalid2) && "Should reject empty aggregations");
  
  // Invalid - bad function
  json invalid3;
  invalid3["aggregations"] = json::array({
    {{"column", "value"}, {"function", "invalid_func"}}
  });
  assert(!aggregate.validateConfig(invalid3) && "Should reject invalid function");
  
  std::cout << "✓ AggregateTransformation validation test passed\n";
}

int main() {
  try {
    testAggregateSum();
    testAggregateAvg();
    testAggregateValidation();
    std::cout << "\n✅ All AggregateTransformation tests passed!\n";
    return 0;
  } catch (const std::exception& e) {
    std::cerr << "❌ Test failed: " << e.what() << "\n";
    return 1;
  }
}
