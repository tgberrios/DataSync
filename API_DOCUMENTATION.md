# Documentación de API - DataSync

Documentación completa de la API REST de DataSync.

**Base URL**: `http://localhost:3000/api` (desarrollo) o `https://yourdomain.com/api` (producción)

---

## 📋 Tabla de Contenidos

- [Autenticación](#autenticación)
- [Endpoints de Catálogo](#endpoints-de-catálogo)
- [Endpoints de API Catalog](#endpoints-de-api-catalog)
- [Endpoints de Custom Jobs](#endpoints-de-custom-jobs)
- [Endpoints de Dashboard](#endpoints-de-dashboard)
- [Endpoints de Data Lineage](#endpoints-de-data-lineage)
- [Endpoints de Quality](#endpoints-de-quality)
- [Endpoints de Configuración](#endpoints-de-configuración)
- [Endpoints de Usuarios](#endpoints-de-usuarios)
- [Códigos de Estado HTTP](#códigos-de-estado-http)
- [Manejo de Errores](#manejo-de-errores)

---

## 🔐 Autenticación

La mayoría de los endpoints requieren autenticación mediante JWT. Incluye el token en el header `Authorization`:

```
Authorization: Bearer <your-jwt-token>
```

### POST /api/auth/login

Inicia sesión y obtiene un token JWT.

**Request:**

```json
{
  "username": "admin",
  "password": "admin123"
}
```

**Response 200:**

```json
{
  "user": {
    "userId": 1,
    "username": "admin",
    "role": "admin"
  },
  "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
}
```

**Response 401:**

```json
{
  "error": "Invalid credentials"
}
```

**Ejemplo cURL:**

```bash
curl -X POST http://localhost:3000/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"admin123"}'
```

---

### GET /api/auth/me

Obtiene información del usuario actualmente autenticado.

**Headers:**

```
Authorization: Bearer <token>
```

**Response 200:**

```json
{
  "user": {
    "userId": 1,
    "username": "admin",
    "role": "admin"
  }
}
```

**Ejemplo cURL:**

```bash
curl -X GET http://localhost:3000/api/auth/me \
  -H "Authorization: Bearer <token>"
```

---

### POST /api/auth/logout

Cierra la sesión del usuario actual.

**Headers:**

```
Authorization: Bearer <token>
```

**Response 200:**

```json
{
  "message": "Logged out successfully"
}
```

---

### POST /api/auth/change-password

Cambia la contraseña del usuario actual.

**Headers:**

```
Authorization: Bearer <token>
```

**Request:**

```json
{
  "oldPassword": "oldpass123",
  "newPassword": "newpass123"
}
```

**Response 200:**

```json
{
  "message": "Password changed successfully"
}
```

**Response 400:**

```json
{
  "error": "Invalid old password"
}
```

---

## 📊 Endpoints de Catálogo

### GET /api/catalog

Obtiene la lista de entradas del catálogo con paginación y filtros.

**Headers:**

```
Authorization: Bearer <token>
```

**Query Parameters:**

- `page` (integer, default: 1): Número de página
- `limit` (integer, default: 10, max: 100): Elementos por página
- `engine` (string, optional): Filtrar por motor de base de datos (PostgreSQL, MariaDB, MSSQL, MongoDB, Oracle)
- `status` (string, optional): Filtrar por estado (PENDING, ACTIVE, ERROR, INACTIVE)
- `active` (string, optional): Filtrar por activo ("true" o "false")
- `search` (string, optional): Búsqueda en nombre de tabla o esquema
- `sort_field` (string, optional): Campo para ordenar (default: "active")
- `sort_direction` (string, optional): Dirección de ordenamiento ("asc" o "desc", default: "desc")

**Response 200:**

```json
{
  "data": [
    {
      "id": 1,
      "schema_name": "public",
      "table_name": "users",
      "db_engine": "PostgreSQL",
      "connection_string": "host=localhost port=5432 dbname=mydb user=postgres password=***",
      "active": true,
      "status": "ACTIVE",
      "cluster_name": "",
      "pk_strategy": "OFFSET",
      "last_sync_column": "updated_at",
      "last_sync_time": "2024-01-15T10:30:00Z",
      "updated_at": "2024-01-15T10:30:00Z"
    }
  ],
  "pagination": {
    "page": 1,
    "limit": 10,
    "total": 45,
    "totalPages": 5
  }
}
```

**Ejemplo cURL:**

```bash
curl -X GET "http://localhost:3000/api/catalog?page=1&limit=10&engine=PostgreSQL&status=ACTIVE" \
  -H "Authorization: Bearer <token>"
```

---

### POST /api/catalog

Crea una nueva entrada en el catálogo.

**Headers:**

```
Authorization: Bearer <token>
```

**Request:**

```json
{
  "schema_name": "public",
  "table_name": "users",
  "db_engine": "PostgreSQL",
  "connection_string": "host=localhost port=5432 dbname=mydb user=postgres password=secret123",
  "active": true,
  "status": "PENDING",
  "cluster_name": "",
  "pk_strategy": "TIMESTAMP",
  "last_sync_column": "updated_at"
}
```

**Response 201:**

```json
{
  "entry": {
    "id": 1,
    "schema_name": "public",
    "table_name": "users",
    "db_engine": "PostgreSQL",
    "active": true,
    "status": "PENDING",
    "updated_at": "2024-01-15T10:30:00Z"
  }
}
```

**Response 400:**

```json
{
  "error": "Schema name is required"
}
```

---

### PATCH /api/catalog/:id

Actualiza una entrada del catálogo.

**Headers:**

```
Authorization: Bearer <token>
```

**Path Parameters:**

- `id` (integer): ID de la entrada del catálogo

**Request:**

```json
{
  "active": false,
  "status": "INACTIVE"
}
```

**Response 200:**

```json
{
  "entry": {
    "id": 1,
    "schema_name": "public",
    "table_name": "users",
    "active": false,
    "status": "INACTIVE",
    "updated_at": "2024-01-15T11:00:00Z"
  }
}
```

---

### DELETE /api/catalog/:id

Elimina una entrada del catálogo.

**Headers:**

```
Authorization: Bearer <token>
```

**Path Parameters:**

- `id` (integer): ID de la entrada del catálogo

**Response 200:**

```json
{
  "message": "Catalog entry deleted successfully"
}
```

---

### GET /api/catalog/schemas

Obtiene la lista de esquemas disponibles.

**Headers:**

```
Authorization: Bearer <token>
```

**Response 200:**

```json
{
  "schemas": ["public", "dbo", "mydb", "sales"]
}
```

---

## 🌐 Endpoints de API Catalog

### GET /api/api-catalog

Obtiene la lista de APIs configuradas.

**Headers:**

```
Authorization: Bearer <token>
```

**Query Parameters:**

- `page` (integer, default: 1)
- `limit` (integer, default: 10, max: 100)
- `api_type` (string, optional): Filtrar por tipo de API
- `status` (string, optional): Filtrar por estado
- `active` (string, optional): Filtrar por activo
- `search` (string, optional): Búsqueda en nombre de API

**Response 200:**

```json
{
  "data": [
    {
      "id": 1,
      "api_name": "GitHub Users API",
      "api_type": "REST",
      "base_url": "https://api.github.com",
      "endpoint": "/users",
      "http_method": "GET",
      "auth_type": "BEARER",
      "auth_config": { "token": "***" },
      "target_db_engine": "PostgreSQL",
      "target_schema": "public",
      "target_table": "github_users",
      "sync_interval": 3600,
      "status": "ACTIVE",
      "active": true,
      "last_sync_time": "2024-01-15T10:30:00Z",
      "updated_at": "2024-01-15T10:30:00Z"
    }
  ],
  "pagination": {
    "page": 1,
    "limit": 10,
    "total": 5,
    "totalPages": 1
  }
}
```

---

### POST /api/api-catalog

Crea una nueva entrada en el API catalog.

**Headers:**

```
Authorization: Bearer <token>
```

**Request:**

```json
{
  "api_name": "GitHub Users API",
  "api_type": "REST",
  "base_url": "https://api.github.com",
  "endpoint": "/users",
  "http_method": "GET",
  "auth_type": "BEARER",
  "auth_config": { "token": "ghp_xxxxxxxxxxxx" },
  "target_db_engine": "PostgreSQL",
  "target_connection_string": "host=localhost port=5432 dbname=DataLake user=postgres password=secret",
  "target_schema": "public",
  "target_table": "github_users",
  "request_headers": { "Accept": "application/json" },
  "query_params": {},
  "request_body": "",
  "sync_interval": 3600,
  "status": "PENDING",
  "active": true
}
```

**Response 201:**

```json
{
  "entry": {
    "id": 1,
    "api_name": "GitHub Users API",
    "status": "PENDING",
    "updated_at": "2024-01-15T10:30:00Z"
  }
}
```

---

## 🔧 Endpoints de Custom Jobs

### GET /api/custom-jobs

Obtiene la lista de custom jobs.

**Headers:**

```
Authorization: Bearer <token>
```

**Query Parameters:**

- `page` (integer, default: 1)
- `limit` (integer, default: 10, max: 100)
- `active` (string, optional)
- `search` (string, optional)

**Response 200:**

```json
{
  "data": [
    {
      "id": 1,
      "job_name": "transform_sales_data",
      "script": "async function execute() { ... }",
      "schedule": "0 0 * * *",
      "active": true,
      "last_run_time": "2024-01-15T00:00:00Z",
      "next_run_time": "2024-01-16T00:00:00Z",
      "updated_at": "2024-01-15T10:30:00Z"
    }
  ],
  "pagination": {
    "page": 1,
    "limit": 10,
    "total": 3,
    "totalPages": 1
  }
}
```

---

### POST /api/custom-jobs

Crea un nuevo custom job.

**Headers:**

```
Authorization: Bearer <token>
```

**Request:**

```json
{
  "job_name": "transform_sales_data",
  "script": "async function execute() {\n  const pool = await getPostgresPool();\n  const result = await pool.query('SELECT * FROM sales');\n  return { success: true, records: result.rows.length };\n}",
  "schedule": "0 0 * * *",
  "active": true
}
```

**Response 201:**

```json
{
  "job": {
    "id": 1,
    "job_name": "transform_sales_data",
    "active": true,
    "updated_at": "2024-01-15T10:30:00Z"
  }
}
```

---

### POST /api/custom-jobs/:jobName/execute

Ejecuta un custom job manualmente.

**Headers:**

```
Authorization: Bearer <token>
```

**Path Parameters:**

- `jobName` (string): Nombre del job

**Response 200:**

```json
{
  "message": "Job executed successfully",
  "result": {
    "success": true,
    "records": 150
  }
}
```

---

### GET /api/custom-jobs/:jobName/history

Obtiene el historial de ejecución de un job.

**Headers:**

```
Authorization: Bearer <token>
```

**Path Parameters:**

- `jobName` (string): Nombre del job

**Query Parameters:**

- `page` (integer, default: 1)
- `limit` (integer, default: 20)

**Response 200:**

```json
{
  "data": [
    {
      "id": 1,
      "job_name": "transform_sales_data",
      "execution_time": "2024-01-15T00:00:00Z",
      "status": "SUCCESS",
      "result": { "success": true, "records": 150 },
      "duration_ms": 1250
    }
  ],
  "pagination": {
    "page": 1,
    "limit": 20,
    "total": 30,
    "totalPages": 2
  }
}
```

---

## 📈 Endpoints de Dashboard

### GET /api/dashboard/stats

Obtiene estadísticas del dashboard.

**Headers:**

```
Authorization: Bearer <token>
```

**Response 200:**

```json
{
  "stats": {
    "totalTables": 45,
    "activeTables": 38,
    "totalAPIs": 5,
    "activeAPIs": 4,
    "totalJobs": 3,
    "activeJobs": 2,
    "lastSyncTime": "2024-01-15T10:30:00Z"
  }
}
```

---

## 🔗 Endpoints de Data Lineage

### GET /api/data-lineage/:engine/schemas/:serverName

Obtiene los esquemas disponibles para un servidor específico.

**Headers:**

```
Authorization: Bearer <token>
```

**Path Parameters:**

- `engine` (string): Motor de base de datos (mariadb, mssql, mongodb, oracle)
- `serverName` (string): Nombre del servidor

**Response 200:**

```json
{
  "schemas": ["dbo", "sales", "hr"]
}
```

---

### GET /api/data-lineage/:engine/tables/:serverName/:schemaName

Obtiene las tablas de un esquema específico.

**Headers:**

```
Authorization: Bearer <token>
```

**Path Parameters:**

- `engine` (string): Motor de base de datos
- `serverName` (string): Nombre del servidor
- `schemaName` (string): Nombre del esquema

**Query Parameters:**

- `page` (integer, default: 1)
- `limit` (integer, default: 50)

**Response 200:**

```json
{
  "data": [
    {
      "table_name": "users",
      "parent_tables": ["departments"],
      "child_tables": ["user_profiles", "user_sessions"],
      "relations": [
        {
          "type": "foreign_key",
          "referenced_table": "departments",
          "referenced_column": "id",
          "referencing_column": "department_id"
        }
      ]
    }
  ],
  "pagination": {
    "page": 1,
    "limit": 50,
    "total": 25,
    "totalPages": 1
  }
}
```

---

## 📊 Endpoints de Quality

### GET /api/quality/metrics

Obtiene métricas de calidad de datos.

**Headers:**

```
Authorization: Bearer <token>
```

**Query Parameters:**

- `page` (integer, default: 1)
- `limit` (integer, default: 20)

**Response 200:**

```json
{
  "data": [
    {
      "table_name": "users",
      "schema_name": "public",
      "quality_score": 0.95,
      "completeness": 0.98,
      "uniqueness": 1.0,
      "validity": 0.92,
      "consistency": 0.9,
      "updated_at": "2024-01-15T10:30:00Z"
    }
  ],
  "pagination": {
    "page": 1,
    "limit": 20,
    "total": 45,
    "totalPages": 3
  }
}
```

---

## ⚙️ Endpoints de Configuración

### GET /api/config

Obtiene la configuración del sistema.

**Headers:**

```
Authorization: Bearer <token>
```

**Response 200:**

```json
{
  "config": {
    "sync": {
      "chunk_size": 25000,
      "sync_interval_seconds": 30,
      "max_workers": 4,
      "max_tables_per_cycle": 1000
    }
  }
}
```

---

### POST /api/config

Actualiza la configuración del sistema (solo admin).

**Headers:**

```
Authorization: Bearer <token>
```

**Request:**

```json
{
  "sync": {
    "chunk_size": 30000,
    "sync_interval_seconds": 60,
    "max_workers": 8,
    "max_tables_per_cycle": 1500
  }
}
```

**Response 200:**

```json
{
  "message": "Configuration updated successfully"
}
```

---

## 👥 Endpoints de Usuarios

### GET /api/auth/users

Obtiene la lista de usuarios (solo admin).

**Headers:**

```
Authorization: Bearer <token>
```

**Query Parameters:**

- `page` (integer, default: 1)
- `limit` (integer, default: 20, max: 100)
- `role` (string, optional): Filtrar por rol
- `active` (string, optional): Filtrar por activo
- `search` (string, optional): Búsqueda en username o email

**Response 200:**

```json
{
  "data": [
    {
      "id": 1,
      "username": "admin",
      "email": "admin@example.com",
      "role": "admin",
      "active": true,
      "created_at": "2024-01-01T00:00:00Z",
      "updated_at": "2024-01-15T10:30:00Z",
      "last_login": "2024-01-15T09:00:00Z"
    }
  ],
  "pagination": {
    "page": 1,
    "limit": 20,
    "total": 5,
    "totalPages": 1
  }
}
```

---

### PATCH /api/auth/users/:id

Actualiza un usuario (solo admin).

**Headers:**

```
Authorization: Bearer <token>
```

**Path Parameters:**

- `id` (integer): ID del usuario

**Request:**

```json
{
  "role": "user",
  "active": true
}
```

**Response 200:**

```json
{
  "user": {
    "id": 1,
    "username": "admin",
    "email": "admin@example.com",
    "role": "user",
    "active": true,
    "updated_at": "2024-01-15T11:00:00Z"
  }
}
```

---

### DELETE /api/auth/users/:id

Elimina un usuario (solo admin).

**Headers:**

```
Authorization: Bearer <token>
```

**Path Parameters:**

- `id` (integer): ID del usuario

**Response 200:**

```json
{
  "message": "User deleted successfully"
}
```

---

### POST /api/auth/users/:id/reset-password

Resetea la contraseña de un usuario (solo admin).

**Headers:**

```
Authorization: Bearer <token>
```

**Path Parameters:**

- `id` (integer): ID del usuario

**Request:**

```json
{
  "newPassword": "newSecurePassword123"
}
```

**Response 200:**

```json
{
  "message": "User password reset successfully"
}
```

---

## 📡 Códigos de Estado HTTP

| Código | Significado           | Descripción                                 |
| ------ | --------------------- | ------------------------------------------- |
| 200    | OK                    | Solicitud exitosa                           |
| 201    | Created               | Recurso creado exitosamente                 |
| 400    | Bad Request           | Error en la solicitud (validación, formato) |
| 401    | Unauthorized          | No autenticado o token inválido             |
| 403    | Forbidden             | Sin permisos para la operación              |
| 404    | Not Found             | Recurso no encontrado                       |
| 429    | Too Many Requests     | Rate limit excedido                         |
| 500    | Internal Server Error | Error interno del servidor                  |

---

## ⚠️ Manejo de Errores

Todos los errores siguen el siguiente formato:

```json
{
  "error": "Error message description"
}
```

### Ejemplos de Errores

**400 Bad Request:**

```json
{
  "error": "Schema name is required"
}
```

**401 Unauthorized:**

```json
{
  "error": "Invalid credentials"
}
```

**403 Forbidden:**

```json
{
  "error": "Admin role required"
}
```

**404 Not Found:**

```json
{
  "error": "Catalog entry not found"
}
```

**429 Too Many Requests:**

```json
{
  "error": "Too many requests from this IP, please try again later."
}
```

**500 Internal Server Error:**

```json
{
  "error": "An error occurred while processing your request"
}
```

---

## 🔒 Rate Limiting

La API implementa rate limiting para prevenir abuso:

- **Endpoints generales**: 100 requests por 15 minutos (desarrollo: 10,000)
- **Endpoints de autenticación**: 5 requests por 15 minutos (desarrollo: 500)
- **Endpoints estrictos**: 50 requests por 15 minutos (desarrollo: 5,000)

Cuando se excede el límite, se retorna un código `429 Too Many Requests`.

---

## 📝 Notas Adicionales

### Paginación

Todos los endpoints que retornan listas soportan paginación:

- `page`: Número de página (empezando en 1)
- `limit`: Elementos por página (máximo 100)

### Ordenamiento

Algunos endpoints soportan ordenamiento:

- `sort_field`: Campo por el cual ordenar
- `sort_direction`: "asc" o "desc"

### Filtros

Muchos endpoints soportan filtros adicionales como:

- Búsqueda de texto
- Filtros por estado
- Filtros por tipo/categoría

### Seguridad

- Todos los endpoints (excepto `/api/auth/login` y `/api/health`) requieren autenticación
- Algunos endpoints requieren roles específicos (admin, user, viewer)
- Las contraseñas y tokens nunca se exponen en respuestas

---

## 📚 Ejemplos de Uso

### Flujo Completo: Añadir Tabla y Sincronizar

```bash
# 1. Iniciar sesión
TOKEN=$(curl -X POST http://localhost:3000/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"admin123"}' | jq -r '.token')

# 2. Añadir tabla al catálogo
curl -X POST http://localhost:3000/api/catalog \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "schema_name": "public",
    "table_name": "users",
    "db_engine": "PostgreSQL",
    "connection_string": "host=localhost port=5432 dbname=mydb user=postgres password=secret",
    "active": true,
    "status": "PENDING",
    "pk_strategy": "TIMESTAMP",
    "last_sync_column": "updated_at"
  }'

# 3. Verificar estado
curl -X GET "http://localhost:3000/api/catalog?status=ACTIVE" \
  -H "Authorization: Bearer $TOKEN"
```

---

**Última actualización**: 2024
