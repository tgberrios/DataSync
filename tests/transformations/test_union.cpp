#include "transformations/union_transformation.h"
#include "third_party/json.hpp"
#include <cassert>
#include <iostream>

using json = nlohmann::json;

void testUnionAll() {
  std::cout << "Testing UnionTransformation - UNION ALL...\n";
  
  UnionTransformation unionTransform;
  
  std::vector<json> inputData = {
    {{"id", 1}, {"name", "Alice"}},
    {{"id", 2}, {"name", "Bob"}}
  };
  
  json config;
  config["union_type"] = "union_all";
  config["additional_data"] = json::array({
    json::array({
      {{"id", 3}, {"name", "Charlie"}}
    })
  });
  
  assert(unionTransform.validateConfig(config) && "Config should be valid");
  
  auto result = unionTransform.execute(inputData, config);
  assert(result.size() == 3 && "Union ALL should return 3 rows");
  
  std::cout << "✓ UnionTransformation UNION ALL test passed\n";
}

void testUnion() {
  std::cout << "Testing UnionTransformation - UNION (deduplicate)...\n";
  
  UnionTransformation unionTransform;
  
  std::vector<json> inputData = {
    {{"id", 1}, {"name", "Alice"}},
    {{"id", 2}, {"name", "Bob"}}
  };
  
  json config;
  config["union_type"] = "union";
  config["additional_data"] = json::array({
    json::array({
      {{"id", 1}, {"name", "Alice"}},  // Duplicate
      {{"id", 3}, {"name", "Charlie"}}
    })
  });
  
  auto result = unionTransform.execute(inputData, config);
  assert(result.size() <= 3 && "Union should deduplicate");
  
  std::cout << "✓ UnionTransformation UNION test passed\n";
}

void testUnionValidation() {
  std::cout << "Testing UnionTransformation - Validation...\n";
  
  UnionTransformation unionTransform;
  
  // Invalid - missing additional_data
  json invalid1;
  assert(!unionTransform.validateConfig(invalid1) && "Should reject missing additional_data");
  
  std::cout << "✓ UnionTransformation validation test passed\n";
}

int main() {
  try {
    testUnionAll();
    testUnion();
    testUnionValidation();
    std::cout << "\n✅ All UnionTransformation tests passed!\n";
    return 0;
  } catch (const std::exception& e) {
    std::cerr << "❌ Test failed: " << e.what() << "\n";
    return 1;
  }
}
