import { useState, useCallback, useMemo } from "react";

/**
 * Tipo para definir filtros de tabla
 */
export interface TableFilters {
  [key: string]: string | number | boolean | null | undefined;
}

/**
 * Hook personalizado para manejar filtros de tabla
 *
 * @param initialFilters - Filtros iniciales
 * @returns Objeto con estado y funciones de filtros
 *
 * @example
 * ```typescript
 * const { filters, setFilter, clearFilters, hasActiveFilters } = useTableFilters({
 *   engine: '',
 *   status: '',
 *   active: '',
 * });
 * ```
 */
export const useTableFilters = <T extends TableFilters>(initialFilters: T) => {
  const [filters, setFilters] = useState<T>(initialFilters);

  /**
   * Establece un valor para un filtro específico
   *
   * @param key - Clave del filtro
   * @param value - Valor del filtro
   */
  const setFilter = useCallback(<K extends keyof T>(key: K, value: T[K]) => {
    setFilters((prev) => ({ ...prev, [key]: value }));
  }, []);

  /**
   * Limpia todos los filtros a sus valores iniciales
   */
  const clearFilters = useCallback(() => {
    setFilters(initialFilters);
  }, [initialFilters]);

  /**
   * Verifica si hay filtros activos
   */
  const hasActiveFilters = useMemo(() => {
    return Object.keys(filters).some((key) => {
      const value = filters[key];
      if (value === null || value === undefined || value === "") {
        return false;
      }
      if (typeof value === "string" && value.trim() === "") {
        return false;
      }
      return true;
    });
  }, [filters]);

  /**
   * Obtiene los filtros activos (sin valores vacíos)
   */
  const activeFilters = useMemo(() => {
    const active: Partial<T> = {};
    Object.keys(filters).forEach((key) => {
      const value = filters[key as keyof T];
      if (value !== null && value !== undefined && value !== "") {
        if (typeof value === "string" && value.trim() !== "") {
          (active as any)[key] = value;
        } else if (typeof value !== "string") {
          (active as any)[key] = value;
        }
      }
    });
    return active;
  }, [filters]);

  return {
    filters,
    setFilter,
    clearFilters,
    hasActiveFilters,
    activeFilters,
  };
};
