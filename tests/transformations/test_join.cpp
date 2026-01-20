#include "transformations/join_transformation.h"
#include "third_party/json.hpp"
#include <cassert>
#include <iostream>

using json = nlohmann::json;

void testJoinInner() {
  std::cout << "Testing JoinTransformation - INNER JOIN...\n";
  
  JoinTransformation join;
  
  std::vector<json> leftData = {
    {{"id", 1}, {"name", "Alice"}},
    {{"id", 2}, {"name", "Bob"}},
    {{"id", 3}, {"name", "Charlie"}}
  };
  
  json config;
  config["join_type"] = "inner";
  config["right_data"] = json::array({
    {{"id", 1}, {"dept", "Engineering"}},
    {{"id", 2}, {"dept", "Sales"}}
  });
  config["left_columns"] = json::array({"id"});
  config["right_columns"] = json::array({"id"});
  
  assert(join.validateConfig(config) && "Config should be valid");
  
  auto result = join.execute(leftData, config);
  assert(result.size() == 2 && "Inner join should return 2 rows");
  
  std::cout << "✓ JoinTransformation INNER JOIN test passed\n";
}

void testJoinLeft() {
  std::cout << "Testing JoinTransformation - LEFT JOIN...\n";
  
  JoinTransformation join;
  
  std::vector<json> leftData = {
    {{"id", 1}, {"name", "Alice"}},
    {{"id", 2}, {"name", "Bob"}},
    {{"id", 3}, {"name", "Charlie"}}
  };
  
  json config;
  config["join_type"] = "left";
  config["right_data"] = json::array({
    {{"id", 1}, {"dept", "Engineering"}}
  });
  config["left_columns"] = json::array({"id"});
  config["right_columns"] = json::array({"id"});
  
  auto result = join.execute(leftData, config);
  assert(result.size() >= 3 && "Left join should return at least 3 rows");
  
  std::cout << "✓ JoinTransformation LEFT JOIN test passed\n";
}

void testJoinValidation() {
  std::cout << "Testing JoinTransformation - Validation...\n";
  
  JoinTransformation join;
  
  // Invalid - missing right_data
  json invalid1;
  invalid1["join_type"] = "inner";
  assert(!join.validateConfig(invalid1) && "Should reject missing right_data");
  
  // Invalid - bad join type
  json invalid2;
  invalid2["join_type"] = "invalid";
  invalid2["right_data"] = json::array();
  invalid2["left_columns"] = json::array({"id"});
  invalid2["right_columns"] = json::array({"id"});
  assert(!join.validateConfig(invalid2) && "Should reject invalid join type");
  
  std::cout << "✓ JoinTransformation validation test passed\n";
}

int main() {
  try {
    testJoinInner();
    testJoinLeft();
    testJoinValidation();
    std::cout << "\n✅ All JoinTransformation tests passed!\n";
    return 0;
  } catch (const std::exception& e) {
    std::cerr << "❌ Test failed: " << e.what() << "\n";
    return 1;
  }
}
