# Guía de Tests - DataSync

## Ejecutar Tests

```bash
# Ejecutar todos los tests
npm test

# Ejecutar tests en modo watch
npm run test:watch

# Ejecutar un archivo específico
npm test -- --testPathPatterns=validation.test.js
```

## Estructura de Tests

### Tests de Validación (`validation.test.js`)

- Tests para todas las funciones de validación
- `sanitizeSearch`, `validatePage`, `validateLimit`, `validateBoolean`, `validateIdentifier`, `validateEnum`

### Tests de Autenticación (`auth.test.js`)

- POST `/api/auth/login` - Login con credenciales
- GET `/api/auth/me` - Obtener información del usuario actual
- Validación de tokens JWT
- Manejo de errores de autenticación

### Tests de Catalog (`catalog.test.js`)

- GET `/api/catalog` - Obtener catálogo con paginación
- POST `/api/catalog` - Crear nueva entrada en catálogo
- Validación de parámetros (page, limit, filters)
- Validación de campos requeridos

### Tests de API Catalog (`api-catalog.test.js`)

- GET `/api/api-catalog` - Obtener catálogo de APIs
- POST `/api/api-catalog` - Crear nueva API
- Validación de filtros y enums

### Tests de Custom Jobs (`custom-jobs.test.js`)

- GET `/api/custom-jobs` - Obtener jobs personalizados
- POST `/api/custom-jobs` - Crear/actualizar job
- Validación de campos requeridos

## Variables de Entorno para Tests

Los tests usan estas variables de entorno (se configuran automáticamente):

```bash
NODE_ENV=test
JWT_SECRET=test-secret-key-for-testing
DEFAULT_ADMIN_PASSWORD=admin123
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DATABASE=DataLake
POSTGRES_USER=postgres
POSTGRES_PASSWORD=
```

## Notas

- Los tests requieren una conexión a PostgreSQL funcionando
- El usuario admin se crea automáticamente en la base de datos si no existe
- Los tests usan tokens JWT reales para autenticación
- Los tests validan tanto éxito como casos de error
