#!/bin/bash
set -e

echo ""
echo "╔════════════════════════════════════════════════════════════════╗"
echo "║          Compilando libxlsxwriter manualmente                  ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

BUILD_DIR="/tmp/libxlsxwriter_build"
INSTALL_PREFIX="/usr/local"

if [ -d "$BUILD_DIR" ]; then
    echo "▸ Limpiando build anterior..."
    rm -rf "$BUILD_DIR"
fi

mkdir -p "$BUILD_DIR"
cd "$BUILD_DIR"

echo "▸ Clonando repositorio..."
if [ -d "libxlsxwriter" ]; then
    cd libxlsxwriter && git pull && cd ..
else
    git clone https://github.com/jmcnamara/libxlsxwriter.git
fi

cd libxlsxwriter
echo "▸ Configurando build..."
mkdir -p build && cd build

cmake .. \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX="$INSTALL_PREFIX" \
    -DCMAKE_POLICY_DEFAULT_CMP0025=NEW

echo "▸ Compilando (esto puede tardar unos minutos)..."
make -j$(nproc)

echo "▸ Instalando..."
make install

echo "▸ Actualizando cache de librerías..."
ldconfig

echo ""
echo "╔════════════════════════════════════════════════════════════════╗"
echo "║              libxlsxwriter instalado exitosamente             ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""
