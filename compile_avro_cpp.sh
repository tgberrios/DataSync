#!/bin/bash
set -e

echo ""
echo "╔════════════════════════════════════════════════════════════════╗"
echo "║     Compilando Apache Avro C++ desde Git (versión más reciente) ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

BUILD_DIR="/tmp/avro_cpp_build"
INSTALL_PREFIX="/usr/local"

if ! pacman -Q boost-libs &>/dev/null; then
    echo "▸ Instalando boost-libs..."
    pacman -S --noconfirm boost-libs
fi

if [ -d "$BUILD_DIR" ]; then
    echo "▸ Limpiando build anterior..."
    rm -rf "$BUILD_DIR"
fi

mkdir -p "$BUILD_DIR"
cd "$BUILD_DIR"

echo "▸ Clonando Apache Avro desde Git..."
if [ -d "avro" ]; then
    cd avro
    git pull
    cd ..
else
    git clone https://github.com/apache/avro.git
fi

cd avro/lang/c++

echo "▸ Configurando build..."
mkdir -p build && cd build

# Usar el nuevo sistema de Boost que viene con Boost 1.89.0
cmake .. \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX="$INSTALL_PREFIX"

echo "▸ Compilando (esto puede tardar varios minutos)..."
make -j$(nproc)

echo "▸ Instalando..."
make install

echo "▸ Actualizando cache de librerías..."
ldconfig

echo ""
echo "╔════════════════════════════════════════════════════════════════╗"
echo "║            Apache Avro C++ instalado exitosamente              ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""
