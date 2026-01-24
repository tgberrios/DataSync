#!/bin/bash
# Script para instalar el SDK de DynamoDB para DataSync

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║  Instalación del SDK de DynamoDB para DataSync                ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

echo "Instalando AWS SDK para C++ (incluye DynamoDB)..."
echo "NOTA: Este paquete es grande (~500MB+) pero incluye todos los servicios de AWS"
echo ""

# Instalar el paquete completo de AWS SDK
sudo pacman -S aws-sdk-cpp

echo ""
echo "Verificando instalación..."

# Verificar instalación
if ls /usr/lib/libaws-cpp-sdk-dynamodb* 2>/dev/null | grep -q .; then
    echo "✓ SDK de DynamoDB encontrado en /usr/lib"
    ls -lh /usr/lib/libaws-cpp-sdk-dynamodb* | head -3
elif find /usr/include/aws -name "*dynamodb*" -type d 2>/dev/null | grep -q .; then
    echo "✓ Headers de DynamoDB encontrados en /usr/include/aws"
    find /usr/include/aws -name "*dynamodb*" -type d | head -3
else
    echo "⚠ SDK de DynamoDB no encontrado en ubicaciones estándar."
    echo "  El SDK puede estar en el paquete completo aws-sdk-cpp."
    echo "  Verifica con: pacman -Ql aws-sdk-cpp | grep dynamodb"
fi

echo ""
echo "Para verificar la instalación completa, ejecuta:"
echo "  ls -la /usr/lib/libaws-cpp-sdk-dynamodb*"
echo "  find /usr/include/aws -name '*dynamodb*'"
echo ""
echo "Luego recompila DataSync:"
echo "  cd /home/iks/Documents/DataSync && ./build.sh"
echo ""
echo "El sistema detectará automáticamente el SDK y habilitará DynamoDBEngine."
