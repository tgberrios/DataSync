#!/bin/bash

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║         Running Transformation Tests (Backend C++)           ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

# Compiler flags - using same as CMakeLists.txt
CXX_FLAGS="-std=c++17 -I./include -I./include/transformations -I./include/core -I./include/third_party -I/usr/include/libmongoc-1.0 -I/usr/include/libbson-1.0 -I/usr/include/mariadb -I/usr/include/mariadb/mysql -I/opt/microsoft/msodbcsql/include -Wall -Wextra -DJSON_USE_IMPLICIT_CONVERSIONS=1"
LIBS="-lpqxx -lpq -lmariadb -lmysqlclient -lpthread"

# Source files needed (including dependencies)
TRANSFORMATION_SOURCES="src/transformations/transformation_engine.cpp src/transformations/lookup_transformation.cpp src/transformations/aggregate_transformation.cpp src/transformations/join_transformation.cpp src/transformations/router_transformation.cpp src/transformations/union_transformation.cpp src/core/logger.cpp src/core/database_config.cpp src/core/database_log_writer.cpp"

# Test files
TESTS=(
  "tests/transformations/test_lookup.cpp"
  "tests/transformations/test_aggregate.cpp"
  "tests/transformations/test_join.cpp"
  "tests/transformations/test_router.cpp"
  "tests/transformations/test_union.cpp"
  "tests/transformations/test_engine.cpp"
)

PASSED=0
FAILED=0

for test_file in "${TESTS[@]}"; do
  test_name=$(basename "$test_file" .cpp)
  echo "▸ Compiling and running $test_name..."
  
  # Try compilation (suppress warnings)
  g++ $CXX_FLAGS "$test_file" $TRANSFORMATION_SOURCES -o "/tmp/$test_name" $LIBS 2>&1 | grep -E "(error|undefined)" | head -10
  
  if [ ${PIPESTATUS[0]} -eq 0 ]; then
    echo "  [OK] Compilation successful"
    if /tmp/$test_name 2>&1; then
      echo "  ✅ $test_name PASSED"
      ((PASSED++))
    else
      echo "  ❌ $test_name FAILED"
      ((FAILED++))
    fi
    rm -f "/tmp/$test_name"
  else
    echo "  ❌ $test_name COMPILATION FAILED"
    ((FAILED++))
  fi
  echo ""
done

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║                    TEST SUMMARY                                ║"
echo "╠════════════════════════════════════════════════════════════════╣"
echo "║  ✅ Passed: $PASSED"
echo "║  ❌ Failed: $FAILED"
echo "║  📊 Total:  $((PASSED + FAILED))"
echo "╚════════════════════════════════════════════════════════════════╝"

if [ $FAILED -eq 0 ]; then
  exit 0
else
  exit 1
fi
