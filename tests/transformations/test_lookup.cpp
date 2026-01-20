#include "transformations/lookup_transformation.h"
#include "third_party/json.hpp"
#include <cassert>
#include <iostream>

using json = nlohmann::json;

void testLookupBasic() {
  std::cout << "Testing LookupTransformation - Basic lookup...\n";
  
  LookupTransformation lookup;
  
  // Config
  json config;
  config["lookup_table"] = "departments";
  config["connection_string"] = "test";
  config["db_engine"] = "PostgreSQL";
  config["source_columns"] = json::array({"id"});
  config["lookup_columns"] = json::array({"id"});
  config["return_columns"] = json::array({"department"});
  
  // Test validation
  bool isValid = lookup.validateConfig(config);
  assert(isValid && "Lookup config should be valid");
  
  std::cout << "✓ LookupTransformation basic test passed\n";
}

void testLookupValidation() {
  std::cout << "Testing LookupTransformation - Validation...\n";
  
  LookupTransformation lookup;
  
  // Invalid config - missing lookup_table
  json invalidConfig1;
  invalidConfig1["source_columns"] = json::array({"id"});
  assert(!lookup.validateConfig(invalidConfig1) && "Should reject missing lookup_table");
  
  // Invalid config - mismatched column sizes
  json invalidConfig2;
  invalidConfig2["lookup_table"] = "test";
  invalidConfig2["source_columns"] = json::array({"id"});
  invalidConfig2["lookup_columns"] = json::array({"id", "name"});
  invalidConfig2["return_columns"] = json::array({"department"});
  assert(!lookup.validateConfig(invalidConfig2) && "Should reject mismatched column sizes");
  
  std::cout << "✓ LookupTransformation validation test passed\n";
}

int main() {
  try {
    testLookupBasic();
    testLookupValidation();
    std::cout << "\n✅ All LookupTransformation tests passed!\n";
    return 0;
  } catch (const std::exception& e) {
    std::cerr << "❌ Test failed: " << e.what() << "\n";
    return 1;
  }
}
