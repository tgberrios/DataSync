#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo ""
echo "╔════════════════════════════════════════════════════════════════╗"
echo "║            DataSync Build System (DEBUG MODE)                 ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

echo "▸ Project directory: $SCRIPT_DIR"
echo ""

echo "▸ Step 1/3: Creating build directory..."
mkdir -p build-debug
echo "   [OK] Build directory ready"
echo ""

cd build-debug

echo "▸ Step 2/3: Running CMake configuration (DEBUG)..."
if cmake -DCMAKE_BUILD_TYPE=Debug .. > /tmp/cmake_debug_output.log 2>&1; then
    echo "   [OK] CMake configuration successful"
else
    echo "   [FAIL] CMake configuration failed!"
    cat /tmp/cmake_debug_output.log
    rm -f /tmp/cmake_debug_output.log
    exit 1
fi
echo ""

echo "▸ Step 3/3: Compiling DataSync (DEBUG)..."
echo "   (Using $(nproc) parallel jobs)"
echo ""

START_TIME=$(date +%s)

if make -j$(nproc) 2>&1 | tee /tmp/make_debug_output.log | tail -20; then
    END_TIME=$(date +%s)
    DURATION=$((END_TIME - START_TIME))
    echo ""
    echo "   [OK] Build completed successfully in ${DURATION}s"
    echo ""
    echo "▸ Executable location: $SCRIPT_DIR/DataSync"
    echo "▸ To run with GDB: gdb $SCRIPT_DIR/DataSync"
    echo "▸ To run with valgrind: valgrind --leak-check=full $SCRIPT_DIR/DataSync"
    echo ""
    rm -f /tmp/make_debug_output.log
    rm -f /tmp/cmake_debug_output.log
    exit 0
else
    echo ""
    echo "   [FAIL] Build failed!"
    echo ""
    echo "▸ Last 50 lines of build output:"
    tail -50 /tmp/make_debug_output.log
    rm -f /tmp/make_debug_output.log
    rm -f /tmp/cmake_debug_output.log
    exit 1
fi

