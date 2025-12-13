# Errores y Problemas Pendientes - Frontend

## 🔴 CRÍTICOS (Arreglar Inmediatamente)

### 1. Validación de Entrada en TODOS los Endpoints

**Estado**: ⚠️ PARCIAL - Solo algunos endpoints tienen validación

**Problema**:

- Solo `/api/catalog`, `/api/catalog/status`, y `/api/catalog/sync` tienen validación
- El resto de endpoints (~50+) no validan entrada

**Endpoints sin validación**:

- `/api/column-catalog/*`
- `/api/data-lineage/*` (MariaDB, MSSQL, MongoDB, Oracle)
- `/api/governance-catalog/*` (MariaDB, MSSQL, MongoDB, Oracle)
- `/api/api-catalog/*`
- `/api/custom-jobs/*`
- `/api/monitor/*`
- `/api/maintenance/*`
- `/api/quality/*`
- `/api/security/*`
- `/api/logs/*`
- Y muchos más...

**Solución**: Aplicar validación a todos los endpoints usando `server-utils/validation.js`

### 2. Manejo de Errores Inconsistente en server.js

**Estado**: ⚠️ PARCIAL - Solo algunos endpoints usan `sanitizeError`

**Problema**:

- Algunos endpoints usan `sanitizeError(err, ...)`
- Otros usan `err.message` directamente
- Inconsistencia en formato de respuesta

**Solución**: Reemplazar todos los `err.message` con `sanitizeError()`

---

## ⚠️ ALTOS (Arreglar Esta Semana)

### 3. Componentes Sin Refactorizar (Código Duplicado)

**Estado**: 🔴 PENDIENTE - Solo 2 de 27 componentes refactorizados

**Componentes con código duplicado**:

- ✅ `Dashboard.tsx` - REFACTORIZADO
- ✅ `Catalog.tsx` - REFACTORIZADO
- ❌ `APICatalog.tsx` - Tiene Header, Table, Select, etc. duplicados
- ❌ `CustomJobs.tsx` - Tiene Header, Table, Select, etc. duplicados
- ❌ `DataLineageMariaDB.tsx` - Tiene código duplicado
- ❌ `DataLineageMSSQL.tsx` - Tiene código duplicado
- ❌ `DataLineageMongoDB.tsx` - Tiene código duplicado
- ❌ `DataLineageOracle.tsx` - Tiene código duplicado
- ❌ `GovernanceCatalogMariaDB.tsx` - Tiene código duplicado
- ❌ `GovernanceCatalogMSSQL.tsx` - Tiene código duplicado
- ❌ `GovernanceCatalogMongoDB.tsx` - Tiene código duplicado
- ❌ `GovernanceCatalogOracle.tsx` - Tiene código duplicado
- ❌ `Monitor.tsx` - Tiene código duplicado
- ❌ `QueryPerformance.tsx` - Tiene código duplicado
- ❌ `Maintenance.tsx` - Tiene código duplicado
- ❌ `ColumnCatalog.tsx` - Tiene código duplicado
- ❌ `CatalogLocks.tsx` - Tiene código duplicado
- ❌ Y ~10 componentes más...

**Impacto**: ~1500+ líneas de código duplicado aún pendientes

### 4. Validación de Arrays Opcionales

**Estado**: ⚠️ PARCIAL - Algunos lugares no usan optional chaining

**Problemas**:

- `stats.metricsCards?.topTablesThroughput?.slice(0, 5) || []` - Ya corregido en Dashboard
- Otros componentes pueden tener el mismo problema

**Ubicaciones a revisar**:

- Todos los componentes que acceden a arrays opcionales
- Verificar uso de `?.` y `|| []`

### 5. Manejo de Valores Null/Undefined

**Estado**: ⚠️ PARCIAL - Algunos componentes no manejan nulls

**Problemas**:

- No todos los componentes usan `|| 0` o `|| ""` para valores opcionales
- Algunos pueden causar errores en runtime

---

## ⚠️ MEDIOS (Arreglar Próximas Semanas)

### 6. Funciones Demasiado Largas

**Estado**: ⚠️ PENDIENTE

**Problemas**:

- `server.js` tiene endpoints de 90+ líneas
- Algunos componentes tienen funciones muy largas

**Solución**: Extraer lógica a funciones helper

### 7. Falta de Documentación JSDoc

**Estado**: ⚠️ PARCIAL - Solo algunos archivos documentados

**Pendiente**:

- Documentar todas las funciones en `server.js`
- Documentar componentes restantes
- Documentar funciones de utilidades del servidor

### 8. Race Conditions Potenciales

**Estado**: ⚠️ PARCIAL - Dashboard ya tiene `isMountedRef`

**Problemas**:

- Otros componentes pueden tener race conditions
- Actualizaciones de estado después de desmontar

**Solución**: Agregar `isMountedRef` a todos los componentes con `useEffect` async

---

## 📋 RESUMEN DE PRIORIDADES

### Inmediato (Hoy)

1. ✅ Arreglar error de `require` en ES modules - COMPLETADO
2. ✅ Arreglar error de `dependency_level` - COMPLETADO
3. ⚠️ **Aplicar validación a TODOS los endpoints restantes**
4. ⚠️ **Reemplazar todos los `err.message` con `sanitizeError()`**

### Esta Semana

5. ⚠️ **Refactorizar `APICatalog.tsx` y `CustomJobs.tsx`**
6. ⚠️ **Refactorizar componentes de DataLineage (4 archivos)**
7. ⚠️ **Refactorizar componentes de GovernanceCatalog (4 archivos)**
8. ⚠️ **Agregar validación de arrays opcionales en todos los componentes**

### Próximas Semanas

9. ⚠️ Refactorizar componentes restantes
10. ⚠️ Documentar funciones del servidor
11. ⚠️ Agregar `isMountedRef` a componentes restantes
12. ⚠️ Extraer funciones helper para endpoints largos

---

## 📊 ESTADÍSTICAS ACTUALES

- **Componentes refactorizados**: 2 de 27 (7%)
- **Endpoints con validación**: 3 de ~50+ (6%)
- **Endpoints con sanitización de errores**: ~10 de ~50+ (20%)
- **Código duplicado eliminado**: ~500 líneas de ~2000+ (25%)
- **Documentación agregada**: ~6 archivos de ~30+ (20%)

---

## 🎯 OBJETIVOS INMEDIATOS

1. **Validar TODOS los endpoints** - Aplicar `validatePage`, `validateLimit`, `sanitizeSearch`, etc.
2. **Sanitizar TODOS los errores** - Reemplazar `err.message` con `sanitizeError()`
3. **Refactorizar componentes más usados** - APICatalog, CustomJobs, DataLineage, GovernanceCatalog
