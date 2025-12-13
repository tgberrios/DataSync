import { useState, useMemo } from "react";

/**
 * Hook personalizado para manejar paginación de tablas
 *
 * @param initialPage - Página inicial (default: 1)
 * @param initialLimit - Límite de items por página (default: 10)
 * @param maxLimit - Límite máximo permitido (default: 100)
 * @returns Objeto con estado y funciones de paginación
 *
 * @example
 * ```typescript
 * const { page, limit, offset, setPage, setLimit, totalPages } = usePagination(1, 20);
 * ```
 */
export const usePagination = (
  initialPage: number = 1,
  initialLimit: number = 10,
  maxLimit: number = 100
) => {
  const [page, setPage] = useState<number>(Math.max(1, initialPage));
  const [limit, setLimit] = useState<number>(
    Math.min(maxLimit, Math.max(1, initialLimit))
  );

  const offset = useMemo(() => (page - 1) * limit, [page, limit]);

  const handlePageChange = (newPage: number) => {
    setPage(Math.max(1, newPage));
  };

  const handleLimitChange = (newLimit: number) => {
    const validatedLimit = Math.min(maxLimit, Math.max(1, newLimit));
    setLimit(validatedLimit);
    setPage(1);
  };

  const calculateTotalPages = (total: number): number => {
    return Math.max(1, Math.ceil(total / limit));
  };

  return {
    page,
    limit,
    offset,
    setPage: handlePageChange,
    setLimit: handleLimitChange,
    calculateTotalPages,
  };
};
