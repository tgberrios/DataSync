# Guía de Implementación - Refactorización del Frontend

## Resumen

Este documento describe los cambios implementados y cómo usar las nuevas utilidades y componentes.

## ✅ Cambios Implementados

### 1. Sistema de Diseño Centralizado

**Archivos creados**:

- `src/theme/theme.ts` - Definición del tema con colores, espaciado, sombras, etc.
- `src/components/shared/BaseComponents.tsx` - Componentes base reutilizables

**Uso**:

```typescript
import {
  Container,
  Header,
  Table,
  Button,
} from "../components/shared/BaseComponents";
import { theme } from "../theme/theme";

// En lugar de definir styled-components en cada componente
const MyComponent = () => (
  <Container>
    <Header>My Title</Header>
    <Button $variant="primary">Click me</Button>
  </Container>
);
```

### 2. Hooks Personalizados

**Archivos creados**:

- `src/hooks/usePagination.ts` - Hook para manejar paginación
- `src/hooks/useTableFilters.ts` - Hook para manejar filtros de tabla

**Uso**:

```typescript
import { usePagination } from "../hooks/usePagination";
import { useTableFilters } from "../hooks/useTableFilters";

const MyComponent = () => {
  const { page, limit, offset, setPage, setLimit, calculateTotalPages } =
    usePagination(1, 20);
  const { filters, setFilter, clearFilters, hasActiveFilters } =
    useTableFilters({
      engine: "",
      status: "",
    });

  // ...
};
```

### 3. Utilidades de Validación

**Archivos creados**:

- `src/utils/validation.ts` - Funciones de validación para el frontend
- `src/utils/errorHandler.ts` - Manejo de errores sanitizado

**Uso**:

```typescript
import {
  sanitizeSearch,
  validatePage,
  validateLimit,
} from "../utils/validation";
import { extractApiError } from "../utils/errorHandler";

const search = sanitizeSearch(userInput, 100);
const page = validatePage(req.query.page, 1);

try {
  await api.get("/endpoint");
} catch (err) {
  const message = extractApiError(err);
  setError(message);
}
```

### 4. Mejoras de Seguridad en el Servidor

**Archivos creados**:

- `server-utils/validation.js` - Validación de entrada del servidor
- `server-utils/errorHandler.js` - Sanitización de errores

**Cambios aplicados**:

- ✅ Endpoint `/api/catalog` ahora valida y sanitiza todos los parámetros
- ✅ Endpoints `/api/catalog/status` y `/api/catalog/sync` validan entrada
- ✅ Mensajes de error sanitizados para no exponer información sensible
- ✅ Credenciales ahora usan variables de entorno (con fallback)

**Variables de entorno necesarias** (crear `.env`):

```
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DATABASE=DataLake
POSTGRES_USER=your_user
POSTGRES_PASSWORD=your_password
NODE_ENV=development
```

## 📋 Próximos Pasos

### FASE 1: Refactorizar Componentes Existentes

1. **Reemplazar styled-components duplicados**:

   - Buscar todos los componentes que definen `Header`, `Table`, `Button`, etc.
   - Reemplazar con imports de `BaseComponents.tsx`
   - Eliminar definiciones duplicadas

2. **Usar hooks personalizados**:

   - Reemplazar lógica de paginación manual con `usePagination`
   - Reemplazar lógica de filtros manual con `useTableFilters`

3. **Usar utilidades de validación**:
   - Reemplazar validaciones manuales con funciones de `validation.ts`
   - Usar `errorHandler.ts` para manejo de errores consistente

### FASE 2: Documentar Funciones

Agregar JSDoc a todas las funciones siguiendo este formato:

````typescript
/**
 * Descripción breve de la función
 *
 * @param param1 - Descripción del parámetro 1
 * @param param2 - Descripción del parámetro 2
 * @returns Descripción del valor de retorno
 *
 * @example
 * ```typescript
 * const result = myFunction('value1', 123);
 * ```
 */
function myFunction(param1: string, param2: number): boolean {
  // ...
}
````

### FASE 3: Optimización

1. **Lazy Loading**:

```typescript
import { lazy, Suspense } from "react";

const Dashboard = lazy(() => import("./components/Dashboard"));
const Catalog = lazy(() => import("./components/Catalog"));

// En App.tsx
<Suspense fallback={<div>Loading...</div>}>
  <Routes>
    <Route path="/" element={<Dashboard />} />
    <Route path="/catalog" element={<Catalog />} />
  </Routes>
</Suspense>;
```

2. **Memoización**:

```typescript
import { useMemo, useCallback } from "react";

const expensiveValue = useMemo(() => {
  return computeExpensiveValue(data);
}, [data]);

const handleClick = useCallback(() => {
  doSomething(id);
}, [id]);
```

## 🔍 Archivos a Refactorizar

### Prioridad Alta (Componentes más usados):

1. `src/components/Dashboard.tsx`
2. `src/components/Catalog.tsx`
3. `src/components/APICatalog.tsx`
4. `src/components/CustomJobs.tsx`

### Prioridad Media:

5. `src/components/DataLineage*.tsx` (4 archivos)
6. `src/components/GovernanceCatalog*.tsx` (4 archivos)
7. `src/components/Monitor.tsx`
8. `src/components/QueryPerformance.tsx`

### Prioridad Baja:

9. Resto de componentes

## 📝 Notas Importantes

1. **No romper funcionalidad existente**: Los cambios deben ser incrementales
2. **Mantener consistencia visual**: Usar siempre el tema centralizado
3. **Validar entrada**: Nunca confiar en datos del usuario sin validar
4. **Sanitizar errores**: Nunca exponer información sensible en errores
5. **Documentar cambios**: Agregar comentarios cuando sea necesario

## 🐛 Problemas Conocidos

1. **TypeScript errors en useTableFilters**: Corregido usando `as any` (temporal, mejorar tipos)
2. **process.env en errorHandler.ts**: Requiere pasar `isProduction` como parámetro
3. **server.js usa require()**: Ya está usando ES modules, los imports están correctos

## 📚 Referencias

- [Documentación de styled-components](https://styled-components.com/docs)
- [React Hooks](https://react.dev/reference/react)
- [TypeScript Handbook](https://www.typescriptlang.org/docs/)
- [Express Best Practices](https://expressjs.com/en/advanced/best-practice-security.html)
