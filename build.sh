#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo ""
echo "╔════════════════════════════════════════════════════════════════╗"
echo "║                  DataSync Build System                         ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

echo "▸ Project directory: $SCRIPT_DIR"
echo ""

echo "▸ Step 1/3: Creating build directory..."
mkdir -p build
echo "   [OK] Build directory ready"
echo ""

cd build

echo "▸ Step 2/3: Running CMake configuration..."
if cmake .. > /tmp/cmake_output.log 2>&1; then
    echo "   [OK] CMake configuration successful"
else
    echo "   [FAIL] CMake configuration failed!"
    cat /tmp/cmake_output.log
    rm -f /tmp/cmake_output.log
    exit 1
fi
echo ""

echo "▸ Step 3/3: Compiling DataSync..."
echo "   (Using $(nproc) parallel jobs)"
echo ""

START_TIME=$(date +%s)

if make -j$(nproc) 2>&1 | tee /tmp/make_output.log | tail -20; then
    END_TIME=$(date +%s)
    DURATION=$((END_TIME - START_TIME))
    
    echo ""
    echo "╔════════════════════════════════════════════════════════════════╗"
    echo "║                    BUILD SUCCESSFUL                            ║"
    echo "╠════════════════════════════════════════════════════════════════╣"
    echo "║                                                                ║"
    echo "║  ► Build time: ${DURATION}s                                    ║"
    echo "║  ► Executable: ./DataSync                                      ║"
    echo "║  ► Ready to run: ./DataSync                                    ║"
    echo "║                                                                ║"
    echo "╚════════════════════════════════════════════════════════════════╝"
    echo ""
else
    echo ""
    echo "╔════════════════════════════════════════════════════════════════╗"
    echo "║                    BUILD FAILED                                ║"
    echo "╚════════════════════════════════════════════════════════════════╝"
    echo ""
    echo "▸ Error details:"
    tail -50 /tmp/make_output.log
    rm -f /tmp/cmake_output.log /tmp/make_output.log
    exit 1
fi

rm -f /tmp/cmake_output.log /tmp/make_output.log
