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

# Función para procesar la salida de make y mostrar progreso
process_make_output() {
    local last_percent=0
    local last_line=""
    local milestone_percent=0
    local show_all=false
    
    while IFS= read -r line; do
        # Guardar todas las líneas para el log
        echo "$line" >> /tmp/make_output.log
        
        # Mostrar líneas de progreso con porcentaje
        if [[ "$line" =~ \[[[:space:]]*([0-9]+)%[[:space:]]*\] ]]; then
            local percent="${BASH_REMATCH[1]}"
            # Mostrar hitos importantes (0%, 10%, 20%, ..., 80%, linking, built target)
            if [[ "$percent" -eq 0 ]] || \
               [[ "$percent" -ge 10 && "$percent" -lt 20 && "$last_percent" -lt 10 ]] || \
               [[ "$percent" -ge 20 && "$percent" -lt 30 && "$last_percent" -lt 20 ]] || \
               [[ "$percent" -ge 30 && "$percent" -lt 40 && "$last_percent" -lt 30 ]] || \
               [[ "$percent" -ge 40 && "$percent" -lt 50 && "$last_percent" -lt 40 ]] || \
               [[ "$percent" -ge 50 && "$percent" -lt 60 && "$last_percent" -lt 50 ]] || \
               [[ "$percent" -ge 60 && "$percent" -lt 70 && "$last_percent" -lt 60 ]] || \
               [[ "$percent" -ge 70 && "$percent" -lt 80 && "$last_percent" -lt 70 ]] || \
               [[ "$percent" -ge 80 ]] || \
               [[ "$line" =~ (Linking|Built target) ]]; then
                echo "$line"
            fi
            last_percent=$percent
        # Mostrar errores (siempre)
        elif [[ "$line" =~ (error:|Error) ]]; then
            echo "❌ $line"
        # Mostrar el mensaje final de "Built target" (siempre)
        elif [[ "$line" =~ Built[[:space:]]+target ]]; then
            echo "$line"
        # Mostrar líneas de linking (siempre)
        elif [[ "$line" =~ Linking ]]; then
            echo "$line"
        fi
        
        last_line="$line"
    done
    
    # Asegurar que se muestre el 100% si no se mostró
    if [[ "$last_line" =~ Built[[:space:]]+target ]] && [[ ! "$last_line" =~ ^\[[[:space:]]*100% ]]; then
        echo "[100%] Build completed successfully"
    fi
}

if make -j$(nproc) 2>&1 | process_make_output; then
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
