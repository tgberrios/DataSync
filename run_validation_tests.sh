#!/bin/bash

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║         Ejecutando Tests de Validación (Backend C++)         ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

CXX_FLAGS="-std=c++17 -I./include -I./include/transformations -I./include/third_party -Wall -Wextra -DJSON_USE_IMPLICIT_CONVERSIONS=1"

# Test simple de validación sin ejecutar (solo compilación)
echo "▸ Test 1: AggregateTransformation - Validación de Config"
g++ $CXX_FLAGS -c src/transformations/aggregate_transformation.cpp -o /tmp/agg_test.o 2>&1
AGG_RESULT=$?

echo "▸ Test 2: JoinTransformation - Validación de Config"
g++ $CXX_FLAGS -c src/transformations/join_transformation.cpp -o /tmp/join_test.o 2>&1 | grep -v "unused parameter"
JOIN_RESULT=$?

echo "▸ Test 3: RouterTransformation - Validación de Config"
g++ $CXX_FLAGS -c src/transformations/router_transformation.cpp -o /tmp/router_test.o 2>&1
ROUTER_RESULT=$?

echo "▸ Test 4: UnionTransformation - Validación de Config"
g++ $CXX_FLAGS -c src/transformations/union_transformation.cpp -o /tmp/union_test.o 2>&1
UNION_RESULT=$?

echo "▸ Test 5: TransformationEngine - Compilación"
g++ $CXX_FLAGS -c src/transformations/transformation_engine.cpp -o /tmp/engine_test.o 2>&1
ENGINE_RESULT=$?

echo ""
echo "╔════════════════════════════════════════════════════════════════╗"
echo "║                    RESULTADOS                                  ║"
echo "╠════════════════════════════════════════════════════════════════╣"

PASSED=0
FAILED=0

if [ $AGG_RESULT -eq 0 ]; then
  echo "║  ✅ AggregateTransformation - Compila correctamente"
  ((PASSED++))
else
  echo "║  ❌ AggregateTransformation - Error de compilación"
  ((FAILED++))
fi

if [ $JOIN_RESULT -eq 0 ]; then
  echo "║  ✅ JoinTransformation - Compila correctamente"
  ((PASSED++))
else
  echo "║  ❌ JoinTransformation - Error de compilación"
  ((FAILED++))
fi

if [ $ROUTER_RESULT -eq 0 ]; then
  echo "║  ✅ RouterTransformation - Compila correctamente"
  ((PASSED++))
else
  echo "║  ❌ RouterTransformation - Error de compilación"
  ((FAILED++))
fi

if [ $UNION_RESULT -eq 0 ]; then
  echo "║  ✅ UnionTransformation - Compila correctamente"
  ((PASSED++))
else
  echo "║  ❌ UnionTransformation - Error de compilación"
  ((FAILED++))
fi

if [ $ENGINE_RESULT -eq 0 ]; then
  echo "║  ✅ TransformationEngine - Compila correctamente"
  ((PASSED++))
else
  echo "║  ❌ TransformationEngine - Error de compilación"
  ((FAILED++))
fi

echo "╠════════════════════════════════════════════════════════════════╣"
echo "║  ✅ Exitosos: $PASSED"
echo "║  ❌ Fallidos: $FAILED"
echo "║  📊 Total:    $((PASSED + FAILED))"
echo "╚════════════════════════════════════════════════════════════════╝"

# Limpiar
rm -f /tmp/*_test.o

if [ $FAILED -eq 0 ]; then
  exit 0
else
  exit 1
fi
