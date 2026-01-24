# Instalación de Dependencias para DataSync (Arch Linux)

Este documento lista todas las dependencias necesarias para compilar DataSync con soporte completo para todos los engines.

## Dependencias Básicas (Ya instaladas)

Estas dependencias ya están instaladas en tu sistema:
- ✅ base-devel
- ✅ git
- ✅ cmake
- ✅ pkg-config
- ✅ zlib
- ✅ bzip2
- ✅ lz4
- ✅ curl
- ✅ libssh2
- ✅ pugixml

## Dependencias que Necesitan Instalación

### 1. Librerías de Formatos de Archivo

#### libxlsxwriter (para Excel)
```bash
# Opción 1: Desde AUR (puede fallar por problemas de CMake)
yay -S libxlsxwriter

# Opción 2: Compilar manualmente
cd /tmp
git clone https://github.com/jmcnamara/libxlsxwriter.git
cd libxlsxwriter
mkdir build && cd build
cmake ..
make
sudo make install
```

### 2. Formatos de Datos

#### Apache Avro C++
```bash
yay -S avro-cpp
# O compilar desde: https://github.com/apache/avro
```

#### Apache Parquet C++
```bash
# Parquet está incluido en Apache Arrow
yay -S apache-arrow
# O compilar desde: https://github.com/apache/arrow
```

### 3. Cloud SDKs

#### AWS SDK C++
```bash
# Opción 1: Desde AUR
yay -S aws-sdk-cpp

# Opción 2: Versión git (más reciente)
yay -S aws-sdk-cpp-git

# Opción 3: Compilar manualmente
# https://github.com/aws/aws-sdk-cpp
```

#### Azure Storage C++ SDK
```bash
# No está disponible en repos oficiales ni AUR común
# Instalación manual requerida:
cd /tmp
git clone https://github.com/Azure/azure-storage-cpp.git
cd azure-storage-cpp
# Seguir instrucciones en README.md
```

#### Google Cloud Storage C++ SDK
```bash
# Opción 1: Desde AUR
yay -S google-cloud-cpp

# Opción 2: Compilar manualmente
# https://github.com/googleapis/google-cloud-cpp
```

## Instalación Rápida (Recomendada)

**Nota**: Muchas de estas librerías no están disponibles directamente en los repositorios de Arch Linux. Aquí están las opciones:

### Opción 1: Instalar lo que esté disponible

```bash
# Librerías básicas (ya instaladas)
sudo pacman -S zlib bzip2 lz4 libssh2 pugixml curl

# Intentar desde AUR (puede fallar)
yay -S libxlsxwriter avro-cpp
```

### Opción 2: Compilar manualmente (Recomendado para producción)

Para las librerías que no están en repos, necesitarás compilarlas manualmente:

1. **Apache Arrow** (incluye Parquet):
   ```bash
   cd /tmp
   git clone https://github.com/apache/arrow.git
   cd arrow/cpp
   mkdir build && cd build
   cmake -DCMAKE_INSTALL_PREFIX=/usr/local ..
   make -j$(nproc)
   sudo make install
   ```

2. **AWS SDK C++**:
   ```bash
   cd /tmp
   git clone https://github.com/aws/aws-sdk-cpp.git
   cd aws-sdk-cpp
   mkdir build && cd build
   cmake -DCMAKE_INSTALL_PREFIX=/usr/local -DBUILD_ONLY=s3 ..
   make -j$(nproc)
   sudo make install
   ```

3. **Google Cloud C++ SDK**:
   ```bash
   cd /tmp
   git clone https://github.com/googleapis/google-cloud-cpp.git
   cd google-cloud-cpp
   mkdir build && cd build
   cmake -DCMAKE_INSTALL_PREFIX=/usr/local ..
   make -j$(nproc)
   sudo make install
   ```

4. **Azure Storage C++ SDK**:
   Ver: https://github.com/Azure/azure-storage-cpp

## Verificación

Después de instalar, verifica que las librerías estén disponibles:

```bash
# Verificar librerías instaladas
pkg-config --list-all | grep -E "(avro|parquet|arrow|xlsxwriter|aws|azure|google)"

# Verificar paths de librerías
ldconfig -p | grep -E "(avro|parquet|arrow|xlsxwriter|aws|azure|google)"
```

## Notas Importantes

1. **Azure SDK**: No tiene un paquete oficial en Arch Linux. Necesita compilación manual o usar vcpkg/conan.

2. **Parquet**: Está incluido en Apache Arrow, así que instala `apache-arrow` en lugar de buscar `parquet-cpp` específicamente.

3. **AWS SDK**: Es muy grande (~500MB). La compilación puede tardar 30-60 minutos.

4. **Google Cloud SDK**: También es grande y puede tardar en compilar.

## Compilación sin Todas las Librerías

Si no instalas todas las librerías, el proyecto aún compilará, pero:
- Los engines que requieren librerías faltantes mostrarán warnings
- Funcionalidad limitada (stubs que retornan vacío)
- Los engines funcionarán cuando las librerías estén disponibles

## Actualizar CMakeLists.txt

El `CMakeLists.txt` ya está configurado para detectar automáticamente las librerías instaladas. No necesitas modificar nada manualmente.
