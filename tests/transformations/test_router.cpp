#include "transformations/router_transformation.h"
#include "third_party/json.hpp"
#include <cassert>
#include <iostream>

using json = nlohmann::json;

void testRouterBasic() {
  std::cout << "Testing RouterTransformation - Basic routing...\n";
  
  RouterTransformation router;
  
  std::vector<json> inputData = {
    {{"value", 10}},
    {{"value", 20}},
    {{"value", 30}}
  };
  
  json config;
  config["routes"] = json::array({
    {
      {"name", "low"},
      {"condition", {
        {"column", "value"},
        {"op", "<"},
        {"value", 15}
      }}
    },
    {
      {"name", "high"},
      {"condition", {
        {"column", "value"},
        {"op", ">="},
        {"value", 15}
      }}
    }
  });
  
  assert(router.validateConfig(config) && "Config should be valid");
  
  auto result = router.execute(inputData, config);
  assert(result.size() == 3 && "Should route all rows");
  
  std::cout << "✓ RouterTransformation basic test passed\n";
}

void testRouterValidation() {
  std::cout << "Testing RouterTransformation - Validation...\n";
  
  RouterTransformation router;
  
  // Invalid - missing routes
  json invalid1;
  assert(!router.validateConfig(invalid1) && "Should reject missing routes");
  
  // Invalid - bad operator
  json invalid2;
  invalid2["routes"] = json::array({
    {
      {"name", "test"},
      {"condition", {
        {"column", "value"},
        {"op", "INVALID"},
        {"value", 10}
      }}
    }
  });
  assert(!router.validateConfig(invalid2) && "Should reject invalid operator");
  
  std::cout << "✓ RouterTransformation validation test passed\n";
}

int main() {
  try {
    testRouterBasic();
    testRouterValidation();
    std::cout << "\n✅ All RouterTransformation tests passed!\n";
    return 0;
  } catch (const std::exception& e) {
    std::cerr << "❌ Test failed: " << e.what() << "\n";
    return 1;
  }
}
