# DataSync

**DataSync** es una plataforma completa de sincronización y gobernanza de datos que permite sincronizar datos entre múltiples fuentes (PostgreSQL, MariaDB, MSSQL, MongoDB, Oracle) y APIs REST hacia PostgreSQL, proporcionando gobernanza de datos, catalogación, lineage y calidad de datos.

---

## 📋 Tabla de Contenidos

- [Descripción del Proyecto](#descripción-del-proyecto)
- [Características Principales](#características-principales)
- [Requisitos del Sistema](#requisitos-del-sistema)
- [Quick Start](#quick-start)
- [Instalación Detallada](#instalación-detallada)
- [Configuración](#configuración)
- [Uso Básico](#uso-básico)
- [Documentación Adicional](#documentación-adicional)
- [Estructura del Proyecto](#estructura-del-proyecto)
- [Desarrollo](#desarrollo)
- [Seguridad](#seguridad)

---

## 🎯 Descripción del Proyecto

DataSync es una solución enterprise para:

- **Sincronización de Datos**: Sincronización bidireccional entre múltiples bases de datos y APIs
- **Catálogo de Datos**: Catálogo completo de tablas, columnas y APIs con metadatos
- **Lineage de Datos**: Trazabilidad completa del origen y destino de los datos
- **Gobernanza de Datos**: Clasificación, calidad y cumplimiento de datos
- **Custom Jobs**: Ejecución de trabajos personalizados para transformaciones y sincronizaciones específicas
- **Monitoreo**: Métricas, logs y seguridad en tiempo real

### Arquitectura

- **Backend C++**: Core de sincronización de alto rendimiento
- **Frontend React/TypeScript**: Interfaz web moderna y responsive
- **PostgreSQL**: Base de datos central para metadatos y datos sincronizados
- **API REST**: API completa para integraciones y automatización

---

## ✨ Características Principales

### 🗄️ Soportes de Base de Datos

- PostgreSQL
- MariaDB / MySQL
- Microsoft SQL Server
- MongoDB
- Oracle Database
- APIs REST

### 🔐 Seguridad

- Autenticación JWT
- Autorización basada en roles (admin, user, viewer)
- Rate limiting
- HTTPS y headers de seguridad (CSP, HSTS, X-Frame-Options)
- Sanitización de errores
- Validación completa de entrada

### 📊 Funcionalidades

- Sincronización en tiempo real y programada
- Catálogo de metadatos completo
- Data lineage con visualización
- Data quality metrics
- Custom jobs con scripts personalizados
- API catalog para APIs REST
- Monitoring y logging
- User management

---

## 💻 Requisitos del Sistema

### Backend C++

- **Compilador**: GCC 9+ o Clang 10+ con soporte C++17
- **CMake**: 3.15 o superior
- **Librerías de Base de Datos**:
  - PostgreSQL: libpqxx-dev
  - MariaDB/MySQL: libmariadb-dev o libmysqlclient-dev
  - MSSQL: unixODBC + ODBC Driver for SQL Server
  - MongoDB: libmongoc-1.0-dev, libbson-1.0-dev
  - Oracle: Oracle Instant Client (libclntsh.so)

### ⚠️ Requisitos Especiales por Motor de Base de Datos

#### MongoDB - Replica Set Obligatorio

**MongoDB requiere estar configurado como Replica Set (incluso single-node) para habilitar Change Streams**, que son necesarios para CDC (Change Data Capture).

**Pasos para configurar MongoDB como Single-Node Replica Set:**

1. **Editar configuración de MongoDB:**

   ```bash
   sudo nano /etc/mongod.conf
   ```

2. **Agregar o descomentar la sección `replication`:**

   ```yaml
   replication:
     replSetName: "rs0"
   ```

3. **Reiniciar MongoDB:**

   ```bash
   sudo systemctl restart mongod
   ```

4. **Iniciar el Replica Set:**

   ```bash
   mongosh
   rs.initiate()
   ```

5. **Verificar el estado:**
   ```javascript
   rs.status();
   ```

El prompt debería cambiar a `rs0 [primary] test>`, indicando que el replica set está activo.

**Nota:** Sin esta configuración, Change Streams no funcionarán y el CDC de MongoDB no estará disponible.

### Frontend

- **Node.js**: 18.x o superior
- **npm**: 9.x o superior
- **PostgreSQL**: 12+ (para la base de datos de metadatos)

### Sistema Operativo

- Linux (Ubuntu 20.04+, Debian 11+, CentOS 8+)
- macOS (10.15+)
- Windows (con WSL2 recomendado)

---

## 🚀 Quick Start

### 1. Clonar el Repositorio

```bash
git clone <repository-url>
cd DataSync
```

### 2. Configurar Base de Datos PostgreSQL

```bash
# Crear base de datos
createdb DataLake

# O usando psql
psql -U postgres -c "CREATE DATABASE DataLake;"
```

### 3. Configurar Variables de Entorno

```bash
# Copiar archivo de ejemplo
cp config.example.json config.json

# Editar config.json con tus credenciales
nano config.json
```

### 4. Compilar Backend C++

```bash
mkdir -p build
cd build
cmake ..
make -j$(nproc)
```

### 5. Inicializar Base de Datos

```bash
# Desde el directorio build
./DataSync --init-db
```

### 6. Instalar y Ejecutar Frontend

```bash
cd frontend
npm install
npm run dev
```

### 7. Acceder a la Aplicación

Abre tu navegador en: `http://localhost:5173`

**Credenciales por defecto:**

- Username: `admin`
- Password: `admin123` (⚠️ Cambiar en producción)

---

## 📦 Instalación Detallada

### Instalación de Dependencias del Sistema (Ubuntu/Debian)

```bash
# Actualizar sistema
sudo apt update && sudo apt upgrade -y

# Instalar herramientas de compilación
sudo apt install -y build-essential cmake git

# PostgreSQL
sudo apt install -y postgresql postgresql-contrib libpqxx-dev

# MariaDB/MySQL
sudo apt install -y libmariadb-dev

# MongoDB
sudo apt install -y libmongoc-1.0-dev libbson-1.0-dev

# ⚠️ IMPORTANTE: MongoDB debe estar configurado como Replica Set
# Ver sección "Requisitos Especiales por Motor de Base de Datos" arriba

# ODBC para MSSQL (opcional)
sudo apt install -y unixodbc-dev
# Descargar e instalar ODBC Driver for SQL Server desde Microsoft
```

### Instalación de Node.js

```bash
# Usando nvm (recomendado)
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.0/install.sh | bash
nvm install 18
nvm use 18

# O usando repositorio oficial
curl -fsSL https://deb.nodesource.com/setup_18.x | sudo -E bash -
sudo apt install -y nodejs
```

### Compilación del Backend

```bash
# Navegar al directorio del proyecto
cd DataSync

# Crear directorio de build
mkdir -p build && cd build

# Configurar con CMake
cmake .. -DCMAKE_BUILD_TYPE=Release

# Compilar
make -j$(nproc)

# Verificar que se compiló correctamente
./DataSync --version
```

### Instalación del Frontend

```bash
# Desde el directorio raíz del proyecto
cd frontend

# Instalar dependencias
npm install

# Construir para producción (opcional)
npm run build

# Ejecutar en modo desarrollo
npm run dev
```

---

## ⚙️ Configuración

### Archivo config.json

El archivo `config.json` en la raíz del proyecto contiene la configuración principal:

```json
{
  "database": {
    "postgres": {
      "host": "localhost",
      "port": "5432",
      "database": "DataLake",
      "user": "postgres",
      "password": "your_password"
    }
  },
  "sync": {
    "chunk_size": 25000,
    "sync_interval_seconds": 30,
    "max_workers": 4,
    "max_tables_per_cycle": 1000
  }
}
```

### Variables de Entorno

Alternativamente, puedes usar variables de entorno:

```bash
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
export POSTGRES_DB=DataLake
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=your_password
```

### Configuración del Servidor Frontend

Variables de entorno del servidor Node.js (opcionales):

```bash
# Puerto del servidor (default: 3000)
export PORT=3000

# Entorno (development, production)
export NODE_ENV=production

# JWT Secret (generar uno seguro en producción)
export JWT_SECRET=your-secret-key-here

# Password del admin por defecto (solo para primera creación)
export DEFAULT_ADMIN_PASSWORD=admin123

# Orígenes permitidos para CORS (producción)
export ALLOWED_ORIGINS=https://yourdomain.com,https://www.yourdomain.com
```

### Configuración de Seguridad en Producción

1. **Cambiar credenciales por defecto**:

   ```bash
   cd frontend
   node scripts/create-user.js admin new-secure-password admin
   ```

2. **Configurar HTTPS**:

   - Usar un proxy inverso (nginx, Apache) con certificados SSL
   - O configurar certificados directamente en el servidor

3. **JWT Secret seguro**:
   ```bash
   # Generar un secret seguro
   openssl rand -base64 32
   export JWT_SECRET=<generated-secret>
   ```

---

## 📖 Uso Básico

### 1. Iniciar Sesión

1. Abre `http://localhost:5173` en tu navegador
2. Inicia sesión con tus credenciales
3. Serás redirigido al Dashboard

### 2. Añadir Tablas al Catálogo

Ver [USER_GUIDE.md](USER_GUIDE.md#añadir-tablas-al-catálogo) para instrucciones detalladas.

### 3. Configurar Sincronización

Ver [USER_GUIDE.md](USER_GUIDE.md#configurar-sincronización) para instrucciones detalladas.

### 4. Usar API Catalog

Ver [USER_GUIDE.md](USER_GUIDE.md#usar-api-catalog) para instrucciones detalladas.

### 5. Crear Custom Jobs

Ver [USER_GUIDE.md](USER_GUIDE.md#crear-custom-jobs) para instrucciones detalladas.

---

## 📚 Documentación Adicional

- **[USER_GUIDE.md](USER_GUIDE.md)**: Guía completa de usuario con ejemplos paso a paso
- **[API_DOCUMENTATION.md](API_DOCUMENTATION.md)**: Documentación completa de la API REST
- **[README_TESTS.md](frontend/README_TESTS.md)**: Guía para ejecutar y escribir tests
- **[COMMERCIALIZATION_ROADMAP.md](COMMERCIALIZATION_ROADMAP.md)**: Roadmap de comercialización

---

## 🏗️ Estructura del Proyecto

```
DataSync/
├── build/                  # Archivos compilados (generado)
├── frontend/              # Aplicación web React/TypeScript
│   ├── src/              # Código fuente del frontend
│   ├── server.js         # Servidor Express.js
│   ├── server-utils/     # Utilidades del servidor
│   └── __tests__/        # Tests del frontend
├── include/              # Headers C++ (.h)
│   ├── core/            # Core del sistema
│   ├── engines/         # Motores de base de datos
│   ├── sync/            # Lógica de sincronización
│   ├── governance/      # Gobernanza de datos
│   └── catalog/         # Catálogo de metadatos
├── src/                 # Código fuente C++ (.cpp)
├── scripts/             # Scripts auxiliares
├── config.json          # Configuración (no versionado)
├── config.example.json  # Ejemplo de configuración
└── CMakeLists.txt       # Configuración CMake
```

---

## 🔧 Desarrollo

### Ejecutar Tests

```bash
# Tests del frontend
cd frontend
npm test

# Tests en modo watch
npm run test:watch
```

### Ejecutar en Modo Desarrollo

```bash
# Frontend con hot reload
cd frontend
npm run dev

# Backend (en otra terminal)
cd build
./DataSync
```

### Linting

```bash
cd frontend
npm run lint
```

### Build de Producción

```bash
# Frontend
cd frontend
npm run build

# Backend
cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)
```

---

## 🔒 Seguridad

DataSync implementa múltiples capas de seguridad:

- ✅ Autenticación JWT con tokens seguros
- ✅ Autorización basada en roles
- ✅ Rate limiting para prevenir abuso
- ✅ Validación completa de entrada
- ✅ Sanitización de errores (no expone información sensible)
- ✅ HTTPS y headers de seguridad (CSP, HSTS, X-Frame-Options)
- ✅ Password hashing con bcrypt
- ✅ Prepared statements para prevenir SQL injection

**⚠️ IMPORTANTE**: En producción, asegúrate de:

1. Cambiar todas las contraseñas por defecto
2. Usar un JWT_SECRET seguro y único
3. Configurar HTTPS con certificados válidos
4. Configurar CORS apropiadamente
5. Revisar y ajustar rate limits según tu caso de uso
6. Mantener PostgreSQL actualizado y seguro

---

## 📝 Licencia

Ver archivo [LICENSE](LICENSE) para más detalles.

---

## 🤝 Contribuir

Las contribuciones son bienvenidas. Por favor:

1. Fork el proyecto
2. Crea una rama para tu feature (`git checkout -b feature/AmazingFeature`)
3. Commit tus cambios (`git commit -m 'Add some AmazingFeature'`)
4. Push a la rama (`git push origin feature/AmazingFeature`)
5. Abre un Pull Request

---

## 📞 Soporte

Para soporte, por favor abre un issue en el repositorio o contacta al equipo de desarrollo.

---

## 🎯 Roadmap

Ver [COMMERCIALIZATION_ROADMAP.md](COMMERCIALIZATION_ROADMAP.md) para el roadmap completo de desarrollo y comercialización.

---

**Última actualización**: 2024
