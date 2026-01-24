#!/bin/bash

# Script para instalar todas las dependencias necesarias en Arch Linux
# Para los nuevos engines: S3, Azure Blob, GCS, FTP, Email, y formatos de archivo

set -e

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║     Instalación de Dependencias para DataSync (Arch Linux)     ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

# Verificar si el usuario es root o tiene sudo
if [ "$EUID" -eq 0 ]; then
    SUDO=""
else
    SUDO="sudo"
    echo "▸ Se requerirán privilegios de administrador"
    echo ""
fi

# Colores para output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Función para verificar si un paquete está instalado
check_package() {
    pacman -Qi "$1" &>/dev/null
}

# Función para instalar paquete si no está instalado
install_if_missing() {
    if check_package "$1"; then
        echo -e "${GREEN}✓${NC} $1 ya está instalado"
    else
        echo -e "${YELLOW}▸${NC} Instalando $1..."
        $SUDO pacman -S --noconfirm "$1"
    fi
}

# Función para instalar desde AUR usando yay
install_aur() {
    if command -v yay &> /dev/null; then
        if yay -Qi "$1" &>/dev/null; then
            echo -e "${GREEN}✓${NC} $1 (AUR) ya está instalado"
            return 0
        else
            echo -e "${YELLOW}▸${NC} Instalando $1 desde AUR..."
            if yay -S --noconfirm "$1" 2>&1; then
                return 0
            else
                echo -e "${RED}✗${NC} Error instalando $1 desde AUR (puede requerir instalación manual)"
                return 1
            fi
        fi
    else
        echo -e "${YELLOW}▸${NC} yay no está instalado. Instalando yay primero..."
        cd /tmp
        if [ -d "yay" ]; then
            rm -rf yay
        fi
        git clone https://aur.archlinux.org/yay.git
        cd yay
        if makepkg -si --noconfirm; then
            cd -
            echo -e "${YELLOW}▸${NC} Instalando $1 desde AUR..."
            if yay -S --noconfirm "$1" 2>&1; then
                return 0
            else
                echo -e "${RED}✗${NC} Error instalando $1 desde AUR"
                return 1
            fi
        else
            echo -e "${RED}✗${NC} Error instalando yay"
            cd -
            return 1
        fi
    fi
}

echo "▸ Paso 1/6: Librerías básicas y compresión"
echo "─────────────────────────────────────────────"
install_if_missing "base-devel"
install_if_missing "git"
install_if_missing "cmake"
install_if_missing "pkg-config"
install_if_missing "zlib"
install_if_missing "bzip2"
install_if_missing "lz4"
echo ""

echo "▸ Paso 2/6: Librerías de red y protocolos"
echo "─────────────────────────────────────────────"
install_if_missing "curl"
install_if_missing "libssh2"
# Verificar si curl tiene soporte SFTP
if curl --version | grep -q "sftp"; then
    echo -e "${GREEN}✓${NC} curl tiene soporte SFTP"
else
    echo -e "${YELLOW}⚠${NC} curl puede no tener soporte SFTP completo"
fi
echo ""

echo "▸ Paso 3/6: Librerías de parsing y formatos"
echo "─────────────────────────────────────────────"
install_if_missing "pugixml"
# libxlsxwriter puede estar en AUR
if pacman -Ss libxlsxwriter &>/dev/null; then
    install_if_missing "libxlsxwriter"
else
    echo -e "${YELLOW}▸${NC} libxlsxwriter no está en repos oficiales, intentando AUR..."
    if ! install_aur "libxlsxwriter"; then
        echo -e "${YELLOW}⚠${NC} libxlsxwriter falló. Puedes instalarlo manualmente más tarde."
        echo -e "${YELLOW}  ${NC} O compilar desde: https://github.com/jmcnamara/libxlsxwriter"
    fi
fi
echo ""

echo "▸ Paso 4/6: Librerías de formatos de datos"
echo "─────────────────────────────────────────────"
# Apache Avro C++
if pacman -Ss avro-cpp &>/dev/null; then
    install_if_missing "avro-cpp"
else
    echo -e "${YELLOW}▸${NC} avro-cpp no está en repos oficiales, intentando AUR..."
    if ! install_aur "avro-cpp"; then
        echo -e "${YELLOW}⚠${NC} avro-cpp falló. Puedes instalarlo manualmente más tarde."
    fi
fi

# Apache Parquet C++
if pacman -Ss parquet-cpp &>/dev/null; then
    install_if_missing "parquet-cpp"
else
    echo -e "${YELLOW}▸${NC} parquet-cpp no está en repos oficiales, intentando AUR..."
    if ! install_aur "parquet-cpp"; then
        echo -e "${YELLOW}⚠${NC} parquet-cpp falló. Puedes instalarlo manualmente más tarde."
        echo -e "${YELLOW}  ${NC} O compilar desde: https://github.com/apache/arrow"
    fi
fi
echo ""

echo "▸ Paso 5/6: AWS SDK (C++)"
echo "─────────────────────────────────────────────"
if pacman -Ss aws-sdk-cpp &>/dev/null; then
    install_if_missing "aws-sdk-cpp"
else
    echo -e "${YELLOW}▸${NC} aws-sdk-cpp no está en repos oficiales, intentando AUR..."
    if ! install_aur "aws-sdk-cpp"; then
        echo -e "${YELLOW}⚠${NC} aws-sdk-cpp falló. Puedes instalarlo manualmente:"
        echo -e "${YELLOW}  ${NC} https://github.com/aws/aws-sdk-cpp"
        echo -e "${YELLOW}  ${NC} O usar: yay -S aws-sdk-cpp-git"
    fi
fi
echo ""

echo "▸ Paso 6/6: Azure SDK y Google Cloud SDK"
echo "─────────────────────────────────────────────"
# Azure Storage C++ SDK
if pacman -Ss azure-storage-cpp &>/dev/null; then
    install_if_missing "azure-storage-cpp"
else
    echo -e "${YELLOW}▸${NC} azure-storage-cpp no está en repos oficiales, intentando AUR..."
    if ! install_aur "azure-storage-cpp"; then
        echo -e "${YELLOW}⚠${NC} azure-storage-cpp no disponible. Instalación manual requerida:"
        echo -e "${YELLOW}  ${NC} https://github.com/Azure/azure-storage-cpp"
    fi
fi

# Google Cloud Storage C++ SDK
if pacman -Ss google-cloud-cpp &>/dev/null; then
    install_if_missing "google-cloud-cpp"
else
    echo -e "${YELLOW}▸${NC} google-cloud-cpp no está en repos oficiales, intentando AUR..."
    if ! install_aur "google-cloud-cpp"; then
        echo -e "${YELLOW}⚠${NC} google-cloud-cpp falló. Puedes instalarlo manualmente:"
        echo -e "${YELLOW}  ${NC} https://github.com/googleapis/google-cloud-cpp"
    fi
fi
echo ""

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║                    INSTALACIÓN COMPLETADA                      ║"
echo "╠════════════════════════════════════════════════════════════════╣"
echo "║                                                                ║"
echo "║  Notas importantes:                                            ║"
echo "║  • Algunas librerías pueden requerir instalación manual       ║"
echo "║  • Verifica que todas las librerías estén en los paths         ║"
echo "║    configurados en CMakeLists.txt                              ║"
echo "║  • Ejecuta './build.sh' para compilar el proyecto             ║"
echo "║                                                                ║"
echo "╚════════════════════════════════════════════════════════════════╝"
