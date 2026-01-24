# Instalación del SDK de DynamoDB para DataSync

## Opción 1: Instalación desde repositorio oficial (Recomendado)

El SDK de DynamoDB está incluido en el paquete completo `aws-sdk-cpp` de Arch Linux:

```bash
sudo pacman -S aws-sdk-cpp
```

**Nota:** El paquete `aws-sdk-cpp` incluye TODOS los servicios de AWS, incluyendo DynamoDB. Es un paquete grande (~500MB+) pero incluye todo lo necesario.

Alternativamente, si solo necesitas DynamoDB y S3, puedes instalar los paquetes individuales (aunque DynamoDB puede no estar disponible como paquete separado):

```bash
# Instalar paquetes base necesarios
sudo pacman -S aws-sdk-cpp-core aws-sdk-cpp-s3

# DynamoDB puede estar incluido en aws-sdk-cpp o requerir compilación manual
```

## Opción 2: Verificación de instalación

Después de instalar, verifica que las librerías de DynamoDB estén disponibles:

```bash
# Verificar librerías
ls -la /usr/lib/libaws-cpp-sdk-dynamodb*

# Verificar headers
find /usr/include/aws -name "*dynamodb*" -type d
```

## Opción 3: Compilación manual (si es necesario)

Si las opciones anteriores no funcionan, puedes compilar el SDK manualmente:

```bash
# Clonar el repositorio
git clone https://github.com/aws/aws-sdk-cpp.git
cd aws-sdk-cpp

# Crear directorio de build
mkdir build && cd build

# Configurar CMake (solo DynamoDB)
cmake .. -DCMAKE_BUILD_TYPE=Release \
         -DBUILD_ONLY=dynamodb \
         -DCMAKE_INSTALL_PREFIX=/usr/local

# Compilar e instalar
make -j$(nproc)
sudo make install
```

## Verificación después de la instalación

Después de instalar el SDK, recompila DataSync:

```bash
cd /home/iks/Documents/DataSync
./build.sh
```

El sistema debería detectar automáticamente el SDK de DynamoDB y habilitar la funcionalidad completa de `DynamoDBEngine`.

## Nota

- El SDK de DynamoDB requiere que el SDK base de AWS (`aws-cpp-sdk-core`) esté instalado
- Asegúrate de tener las dependencias necesarias: `openssl`, `curl`, `zlib`
- Si encuentras problemas, verifica que `/usr/lib/pkgconfig/aws-cpp-sdk-dynamodb.pc` exista
