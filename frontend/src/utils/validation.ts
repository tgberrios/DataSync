/**
 * Utilidades de validación para el frontend
 */

/**
 * Valida y sanitiza un string de búsqueda
 *
 * @param search - String a validar
 * @param maxLength - Longitud máxima permitida (default: 100)
 * @returns String sanitizado o string vacío si es inválido
 *
 * @example
 * ```typescript
 * const sanitized = sanitizeSearch(userInput, 50);
 * ```
 */
export const sanitizeSearch = (
  search: string | null | undefined,
  maxLength: number = 100
): string => {
  if (!search || typeof search !== "string") {
    return "";
  }

  return search.trim().substring(0, maxLength);
};

/**
 * Valida y sanitiza un número de página
 *
 * @param page - Página a validar
 * @param min - Página mínima (default: 1)
 * @returns Número de página válido
 *
 * @example
 * ```typescript
 * const validPage = validatePage(req.query.page, 1);
 * ```
 */
export const validatePage = (
  page: string | number | null | undefined,
  min: number = 1
): number => {
  if (typeof page === "number") {
    return Math.max(min, page);
  }

  if (typeof page === "string") {
    const parsed = parseInt(page, 10);
    if (!isNaN(parsed)) {
      return Math.max(min, parsed);
    }
  }

  return min;
};

/**
 * Valida y sanitiza un límite de items por página
 *
 * @param limit - Límite a validar
 * @param min - Límite mínimo (default: 1)
 * @param max - Límite máximo (default: 100)
 * @returns Límite válido
 *
 * @example
 * ```typescript
 * const validLimit = validateLimit(req.query.limit, 1, 50);
 * ```
 */
export const validateLimit = (
  limit: string | number | null | undefined,
  min: number = 1,
  max: number = 100
): number => {
  if (typeof limit === "number") {
    return Math.min(max, Math.max(min, limit));
  }

  if (typeof limit === "string") {
    const parsed = parseInt(limit, 10);
    if (!isNaN(parsed)) {
      return Math.min(max, Math.max(min, parsed));
    }
  }

  return min;
};

/**
 * Valida que un string sea un valor válido de un enum
 *
 * @param value - Valor a validar
 * @param allowedValues - Array de valores permitidos
 * @param defaultValue - Valor por defecto si no es válido
 * @returns Valor válido o defaultValue
 *
 * @example
 * ```typescript
 * const status = validateEnum(req.query.status, ['active', 'inactive'], 'active');
 * ```
 */
export const validateEnum = <T extends string>(
  value: string | null | undefined,
  allowedValues: readonly T[],
  defaultValue: T
): T => {
  if (!value || typeof value !== "string") {
    return defaultValue;
  }

  if (allowedValues.includes(value as T)) {
    return value as T;
  }

  return defaultValue;
};

/**
 * Valida que un valor sea un boolean
 *
 * @param value - Valor a validar
 * @param defaultValue - Valor por defecto si no es válido
 * @returns Boolean válido
 *
 * @example
 * ```typescript
 * const active = validateBoolean(req.body.active, false);
 * ```
 */
export const validateBoolean = (
  value: string | boolean | null | undefined,
  defaultValue: boolean = false
): boolean => {
  if (typeof value === "boolean") {
    return value;
  }

  if (typeof value === "string") {
    const lower = value.toLowerCase();
    if (lower === "true" || lower === "1" || lower === "yes") {
      return true;
    }
    if (lower === "false" || lower === "0" || lower === "no") {
      return false;
    }
  }

  return defaultValue;
};

/**
 * Valida y sanitiza un identificador de schema o tabla
 *
 * @param identifier - Identificador a validar
 * @param maxLength - Longitud máxima (default: 100)
 * @returns Identificador sanitizado o null si es inválido
 *
 * @example
 * ```typescript
 * const schema = validateIdentifier(req.body.schema_name, 100);
 * ```
 */
export const validateIdentifier = (
  identifier: string | null | undefined,
  maxLength: number = 100
): string | null => {
  if (!identifier || typeof identifier !== "string") {
    return null;
  }

  const trimmed = identifier.trim();
  if (trimmed.length === 0 || trimmed.length > maxLength) {
    return null;
  }

  return trimmed.toLowerCase();
};
