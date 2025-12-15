# Guía de Usuario - DataSync

Esta guía te ayudará a utilizar DataSync para sincronizar y gestionar tus datos de manera eficiente.

---

## 📋 Tabla de Contenidos

- [Primeros Pasos](#primeros-pasos)
- [Añadir Tablas al Catálogo](#añadir-tablas-al-catálogo)
- [Configurar Sincronización](#configurar-sincronización)
- [Usar API Catalog](#usar-api-catalog)
- [Crear Custom Jobs](#crear-custom-jobs)
- [Gestión de Usuarios](#gestión-de-usuarios)
- [Visualizar Data Lineage](#visualizar-data-lineage)
- [Monitorear Calidad de Datos](#monitorear-calidad-de-datos)
- [Logs y Seguridad](#logs-y-seguridad)

---

## 🚀 Primeros Pasos

### Iniciar Sesión

1. Abre tu navegador y navega a la URL de DataSync (por defecto: `http://localhost:5173`)
2. Ingresa tus credenciales:
   - **Username**: Tu nombre de usuario
   - **Password**: Tu contraseña
3. Haz clic en **"Login"**

### Navegar por la Interfaz

El menú lateral contiene las siguientes secciones:

- **Dashboard**: Vista general del sistema
- **Catalog**: Gestión de tablas sincronizadas
- **Column Catalog**: Catálogo de columnas
- **Data Lineage**: Visualización de relaciones entre datos
- **API Catalog**: Gestión de APIs REST
- **Custom Jobs**: Trabajos personalizados
- **Quality**: Métricas de calidad de datos
- **Security**: Monitoreo de seguridad
- **Logs**: Visualización de logs del sistema
- **Config**: Configuración del sistema
- **User Management**: Gestión de usuarios (solo admin)

---

## 📊 Añadir Tablas al Catálogo

El catálogo es donde registras las tablas que deseas sincronizar desde diferentes fuentes de datos.

### Paso 1: Acceder al Catálogo

1. En el menú lateral, haz clic en **"Catalog"**
2. Verás una tabla con todas las tablas registradas actualmente

### Paso 2: Añadir una Nueva Tabla

1. Haz clic en el botón **"Add Table"** en la esquina superior derecha
2. Completa el formulario con la información de tu tabla:

#### Campos Requeridos:

- **Schema Name**: Nombre del esquema donde se encuentra la tabla
  - Ejemplo: `public`, `dbo`, `mydb`
- **Table Name**: Nombre de la tabla
  - Ejemplo: `users`, `orders`, `products`
- **Database Engine**: Selecciona el tipo de base de datos:

  - PostgreSQL
  - MariaDB
  - MSSQL
  - MongoDB
  - Oracle

- **Connection String**: Cadena de conexión a la base de datos

#### Ejemplos de Connection Strings:

**PostgreSQL:**

```
host=localhost port=5432 dbname=mydb user=postgres password=secret123
```

**MariaDB/MySQL:**

```
host=localhost port=3306 dbname=mydb user=root password=secret123
```

**MSSQL:**

```
host=localhost port=1433 dbname=mydb user=sa password=Secret123! uid=sa pwd=Secret123! driver={ODBC Driver 17 for SQL Server}
```

**MongoDB:**

```
mongodb://username:password@localhost:27017/mydb
```

o para MongoDB Atlas:

```
mongodb+srv://username:password@cluster0.xxxxx.mongodb.net/mydb
```

**Oracle:**

```
host=localhost port=1521 sid=orcl user=system password=secret123
```

#### Campos Opcionales:

- **Cluster Name**: Nombre del cluster si aplica (para alta disponibilidad)
- **PK Strategy**: Estrategia de clave primaria:
  - `OFFSET`: Usa offset para sincronización incremental
  - `TIMESTAMP`: Usa columna de timestamp
  - `AUTO`: Detección automática
- **Last Sync Column**: Nombre de la columna que contiene el timestamp de última actualización (para sincronización incremental)
- **Active**: Activa/desactiva la sincronización de esta tabla (checked = activa)
- **Status**: Estado inicial (`PENDING`, `ACTIVE`, `ERROR`)

### Paso 3: Guardar

1. Revisa la información ingresada
2. Haz clic en **"Save"**
3. La tabla aparecerá en el catálogo con estado `PENDING`

### Paso 4: Verificar Sincronización

1. La tabla debería cambiar su estado a `ACTIVE` después de la primera sincronización
2. Puedes ver el tiempo de última sincronización en la columna **"Last Sync"**

### Filtrar y Buscar Tablas

- **Búsqueda**: Usa el campo de búsqueda para filtrar por nombre de tabla o esquema
- **Filtros**: Usa los dropdowns para filtrar por:
  - Database Engine
  - Status (PENDING, ACTIVE, ERROR, INACTIVE)
  - Active/Inactive
- **Ordenamiento**: Haz clic en los encabezados de columna para ordenar

---

## ⚙️ Configurar Sincronización

### Configuración Global

La sincronización se configura en el archivo `config.json`:

```json
{
  "sync": {
    "chunk_size": 25000,
    "sync_interval_seconds": 30,
    "max_workers": 4,
    "max_tables_per_cycle": 1000
  }
}
```

- **chunk_size**: Número de registros procesados por lote
- **sync_interval_seconds**: Intervalo entre sincronizaciones (en segundos)
- **max_workers**: Número máximo de threads para procesamiento paralelo
- **max_tables_per_cycle**: Máximo de tablas sincronizadas por ciclo

### Sincronización Incremental

Para habilitar sincronización incremental:

1. Asegúrate de que tu tabla tenga una columna de timestamp (ej: `updated_at`, `modified_at`)
2. Al añadir la tabla, especifica esta columna en **"Last Sync Column"**
3. Selecciona **PK Strategy**: `TIMESTAMP` o `AUTO`
4. DataSync solo sincronizará registros modificados después de la última sincronización

### Activar/Desactivar Tablas

1. En el catálogo, encuentra la tabla que deseas modificar
2. Haz clic en **"Edit"**
3. Marca/desmarca **"Active"**
4. Haz clic en **"Save"**

Las tablas inactivas no se sincronizarán pero permanecerán en el catálogo.

---

## 🌐 Usar API Catalog

El API Catalog te permite sincronizar datos desde APIs REST hacia bases de datos.

### Paso 1: Acceder al API Catalog

1. En el menú lateral, haz clic en **"API Catalog"**
2. Verás una lista de todas las APIs configuradas

### Paso 2: Añadir una Nueva API

1. Haz clic en **"Add API"**
2. Completa el formulario:

#### Información Básica:

- **API Name**: Nombre descriptivo para esta API
- **API Type**: Tipo de API (actualmente solo `REST`)
- **Base URL**: URL base de la API
  - Ejemplo: `https://api.example.com/v1`
- **Endpoint**: Endpoint específico
  - Ejemplo: `/users`, `/orders`
- **HTTP Method**: Método HTTP (`GET`, `POST`, `PUT`, `PATCH`)

#### Autenticación:

- **Auth Type**: Tipo de autenticación:

  - `NONE`: Sin autenticación
  - `API_KEY`: API Key en headers o query params
  - `BASIC`: Autenticación HTTP Basic
  - `BEARER`: Token Bearer
  - `OAUTH2`: OAuth 2.0

- **Auth Config**: Configuración JSON según el tipo:

**API_KEY:**

```json
{
  "key": "X-API-Key",
  "value": "your-api-key-here",
  "location": "header"
}
```

**BEARER:**

```json
{
  "token": "your-bearer-token-here"
}
```

**BASIC:**

```json
{
  "username": "user",
  "password": "pass"
}
```

#### Destino:

- **Target DB Engine**: Base de datos destino (PostgreSQL, MariaDB, etc.)
- **Target Connection String**: Cadena de conexión al destino
- **Target Schema**: Esquema destino
- **Target Table**: Tabla destino

#### Configuración Avanzada:

- **Request Headers**: Headers HTTP adicionales en formato JSON

  ```json
  {
    "Content-Type": "application/json",
    "Accept": "application/json"
  }
  ```

- **Query Parameters**: Parámetros de consulta en formato JSON

  ```json
  {
    "page": "1",
    "limit": "100"
  }
  ```

- **Request Body**: Cuerpo de la petición (para POST/PUT/PATCH)
- **Sync Interval**: Intervalo de sincronización en segundos (default: 3600 = 1 hora)
- **Active**: Activar/desactivar sincronización

### Paso 3: Ejemplo Completo

**Ejemplo: Sincronizar usuarios desde una API REST**

```yaml
API Name: GitHub Users API
Base URL: https://api.github.com
Endpoint: /users
HTTP Method: GET
Auth Type: BEARER
Auth Config: { "token": "ghp_xxxxxxxxxxxx" }
Target DB Engine: PostgreSQL
Target Connection String: host=localhost port=5432 dbname=DataLake user=postgres password=secret
Target Schema: public
Target Table: github_users
Sync Interval: 3600
Active: true
```

### Paso 4: Monitorear Sincronización

1. El estado de la API aparecerá en la tabla:
   - `PENDING`: Esperando primera ejecución
   - `ACTIVE`: Sincronizando correctamente
   - `ERROR`: Error en la sincronización
2. Revisa los logs para ver detalles de la sincronización

---

## 🔧 Crear Custom Jobs

Los Custom Jobs permiten ejecutar scripts personalizados para transformaciones y sincronizaciones específicas.

### Paso 1: Acceder a Custom Jobs

1. En el menú lateral, haz clic en **"Custom Jobs"**
2. Verás una lista de todos los jobs configurados

### Paso 2: Crear un Nuevo Custom Job

1. Haz clic en **"Add Job"**
2. Completa el formulario:

#### Campos Requeridos:

- **Job Name**: Nombre único para el job

  - Ejemplo: `transform_sales_data`, `export_monthly_report`

- **Script**: Código del script a ejecutar

#### Ejemplo de Script:

```javascript
// Ejemplo: Transformar datos de ventas
async function execute() {
  // Acceder a la conexión de PostgreSQL
  const pool = await getPostgresPool();

  // Query para obtener datos
  const result = await pool.query(`
    SELECT 
      DATE_TRUNC('month', sale_date) as month,
      SUM(amount) as total_sales,
      COUNT(*) as transaction_count
    FROM sales
    WHERE sale_date >= CURRENT_DATE - INTERVAL '12 months'
    GROUP BY DATE_TRUNC('month', sale_date)
    ORDER BY month
  `);

  // Insertar datos transformados
  for (const row of result.rows) {
    await pool.query(
      `
      INSERT INTO monthly_sales_summary (month, total_sales, transaction_count)
      VALUES ($1, $2, $3)
      ON CONFLICT (month) DO UPDATE
      SET total_sales = EXCLUDED.total_sales,
          transaction_count = EXCLUDED.transaction_count
    `,
      [row.month, row.total_sales, row.transaction_count]
    );
  }

  return {
    success: true,
    recordsProcessed: result.rows.length,
  };
}
```

#### Configuración:

- **Schedule**: Programación del job (cron expression o intervalo)

  - Ejemplo: `0 0 * * *` (diario a medianoche)
  - Ejemplo: `*/30 * * * *` (cada 30 minutos)
  - Ejemplo: `3600` (cada hora en segundos)

- **Active**: Activar/desactivar el job

### Paso 3: Ejecutar un Job Manualmente

1. En la lista de jobs, encuentra el job deseado
2. Haz clic en **"Execute"**
3. El job se ejecutará inmediatamente
4. Revisa el resultado en la sección de historial

### Paso 4: Ver Historial de Ejecución

1. Haz clic en **"History"** para un job específico
2. Verás todas las ejecuciones pasadas con:
   - Timestamp
   - Estado (SUCCESS, ERROR, RUNNING)
   - Resultado/error
   - Duración

### Scripts Disponibles

Los scripts tienen acceso a:

- `getPostgresPool()`: Pool de conexiones PostgreSQL
- Funciones de logging
- Funciones de utilidad para transformación de datos

---

## 👥 Gestión de Usuarios

Solo los administradores pueden gestionar usuarios.

### Acceder a User Management

1. En el menú lateral, expande **"System"**
2. Haz clic en **"User Management"**

### Crear un Nuevo Usuario

1. Haz clic en **"Add User"**
2. Completa el formulario:
   - **Username**: Nombre de usuario único
   - **Email**: Correo electrónico
   - **Password**: Contraseña (mínimo 8 caracteres)
   - **Role**:
     - `admin`: Acceso completo
     - `user`: Acceso estándar (sin gestión de usuarios)
     - `viewer`: Solo lectura
3. Haz clic en **"Save"**

### Editar Usuario

1. En la lista, haz clic en **"Edit"** para el usuario deseado
2. Modifica los campos:
   - Username
   - Email
   - Role
   - Active/Inactive
3. Haz clic en **"Save"**

### Resetear Contraseña

1. Haz clic en **"Reset Password"** para el usuario
2. Ingresa la nueva contraseña
3. Haz clic en **"Save"**

### Desactivar Usuario

1. Haz clic en **"Edit"**
2. Desmarca **"Active"**
3. El usuario no podrá iniciar sesión

### Eliminar Usuario

1. Haz clic en **"Delete"**
2. Confirma la eliminación
3. ⚠️ Esta acción no se puede deshacer

### Filtrar y Buscar Usuarios

- Usa la búsqueda para filtrar por username o email
- Usa los filtros para buscar por role o estado (active/inactive)

---

## 🔗 Visualizar Data Lineage

Data Lineage muestra las relaciones y dependencias entre tablas y columnas.

### Acceder a Data Lineage

1. En el menú lateral, expande **"Data Lineage"**
2. Selecciona el tipo de base de datos:
   - MariaDB
   - MSSQL
   - MongoDB
   - Oracle

### Ver Lineage por Servidor

1. Selecciona un servidor de la lista
2. Verás todas las tablas en ese servidor
3. Haz clic en una tabla para ver sus relaciones:
   - **Parent Tables**: Tablas de las que depende
   - **Child Tables**: Tablas que dependen de esta
   - **Relations**: Relaciones detectadas

### Navegar por Esquemas

1. Selecciona un esquema del dropdown
2. Las tablas se filtrarán por ese esquema

### Exportar Lineage

1. Usa el botón **"Export"** para descargar el lineage en formato JSON o CSV

---

## 📈 Monitorear Calidad de Datos

La sección Quality muestra métricas de calidad de datos.

### Acceder a Quality

1. En el menú lateral, haz clic en **"Quality"**

### Métricas Disponibles

- **Completeness**: Porcentaje de campos no nulos
- **Uniqueness**: Porcentaje de valores únicos
- **Validity**: Porcentaje de valores que cumplen reglas de validación
- **Consistency**: Consistencia entre diferentes fuentes
- **Accuracy**: Precisión de los datos

### Ver Detalles por Tabla

1. Haz clic en una tabla para ver métricas detalladas
2. Revisa las columnas y sus puntajes de calidad
3. Identifica problemas y toma acciones correctivas

---

## 📋 Logs y Seguridad

### Ver Logs

1. En el menú lateral, expande **"System"**
2. Haz clic en **"Logs"**
3. Filtra por:
   - Nivel (INFO, WARNING, ERROR)
   - Categoría (CONFIG, SYNC, API, etc.)
   - Rango de fechas
   - Búsqueda de texto

### Monitorear Seguridad

1. En el menú lateral, haz clic en **"Security"**
2. Verás:
   - Intentos de acceso
   - Actividad de usuarios
   - Alertas de seguridad
   - Conexiones activas

### Exportar Logs

1. En la sección de Logs, usa el botón **"Export"**
2. Selecciona el formato (JSON, CSV, TXT)
3. Los logs se descargarán

---

## 💡 Consejos y Mejores Prácticas

### Naming Conventions

- Usa nombres descriptivos para tablas y APIs
- Mantén consistencia en los nombres de esquemas
- Documenta conexiones complejas

### Sincronización

- Empieza con tablas pequeñas para probar
- Usa sincronización incremental cuando sea posible
- Monitorea el rendimiento y ajusta `chunk_size` según sea necesario

### Seguridad

- Usa contraseñas seguras
- Limita el acceso según roles (admin, user, viewer)
- Revisa logs regularmente
- Mantén las credenciales de conexión seguras

### Rendimiento

- Configura `max_workers` según tu hardware
- Usa filtros para reducir la carga en sincronizaciones
- Monitorea el uso de recursos en el dashboard

---

## ❓ Solución de Problemas

### La tabla no se sincroniza

1. Verifica que la tabla esté **Active**
2. Revisa el **Connection String**
3. Verifica que la base de datos origen esté accesible
4. Revisa los logs para ver errores específicos

### Error de autenticación en API

1. Verifica el **Auth Type** y **Auth Config**
2. Prueba la API con curl o Postman primero
3. Verifica que el token/key no haya expirado

### Custom Job falla

1. Revisa la sintaxis del script
2. Verifica los permisos en la base de datos
3. Revisa el historial de ejecución para ver el error específico

### Performance lento

1. Reduce el `chunk_size` en config.json
2. Aumenta `sync_interval_seconds` para reducir frecuencia
3. Verifica la carga en las bases de datos origen y destino

---

## 📞 Obtener Ayuda

Si necesitas ayuda adicional:

1. Revisa los logs para detalles de errores
2. Consulta la documentación de API
3. Contacta al equipo de soporte

---

**Última actualización**: 2024
