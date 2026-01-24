#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo ""
echo "╔════════════════════════════════════════════════════════════════╗"
echo "║        Compilación Manual de Librerías Faltantes              ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

echo "▸ Compilando libxlsxwriter..."
"$SCRIPT_DIR/compile_libxlsxwriter.sh"
echo ""

echo "▸ Compilando Apache Avro C++..."
"$SCRIPT_DIR/compile_avro_cpp.sh"
echo ""

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║              Compilación Manual Completada                     ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""
echo "Verifica las instalaciones:"
echo "  ldconfig -p | grep -E '(xlsxwriter|avro)'"
