/**
 * Utilidades de validación para el servidor Express
 */

/**
 * Valida y sanitiza un string de búsqueda
 *
 * @param {string|null|undefined} search - String a validar
 * @param {number} maxLength - Longitud máxima permitida (default: 100)
 * @returns {string} String sanitizado o string vacío si es inválido
 */
function sanitizeSearch(search, maxLength = 100) {
  if (!search || typeof search !== "string") {
    return "";
  }

  return search.trim().substring(0, maxLength);
}

/**
 * Valida y sanitiza un número de página
 *
 * @param {string|number|null|undefined} page - Página a validar
 * @param {number} min - Página mínima (default: 1)
 * @returns {number} Número de página válido
 */
function validatePage(page, min = 1) {
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
}

/**
 * Valida y sanitiza un límite de items por página
 *
 * @param {string|number|null|undefined} limit - Límite a validar
 * @param {number} min - Límite mínimo (default: 1)
 * @param {number} max - Límite máximo (default: 100)
 * @returns {number} Límite válido
 */
function validateLimit(limit, min = 1, max = 100) {
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
}

/**
 * Valida que un valor sea un boolean
 *
 * @param {string|boolean|null|undefined} value - Valor a validar
 * @param {boolean} defaultValue - Valor por defecto si no es válido
 * @returns {boolean} Boolean válido
 */
function validateBoolean(value, defaultValue = false) {
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
}

/**
 * Valida y sanitiza un identificador de schema o tabla
 *
 * @param {string|null|undefined} identifier - Identificador a validar
 * @param {number} maxLength - Longitud máxima (default: 100)
 * @returns {string|null} Identificador sanitizado o null si es inválido
 */
function validateIdentifier(identifier, maxLength = 100) {
  if (!identifier || typeof identifier !== "string") {
    return null;
  }

  const trimmed = identifier.trim();
  if (trimmed.length === 0 || trimmed.length > maxLength) {
    return null;
  }

  return trimmed.toLowerCase();
}

/**
 * Valida que un string sea un valor válido de un array
 *
 * @param {string|null|undefined} value - Valor a validar
 * @param {string[]} allowedValues - Array de valores permitidos
 * @param {string} defaultValue - Valor por defecto si no es válido
 * @returns {string} Valor válido o defaultValue
 */
function validateEnum(value, allowedValues, defaultValue) {
  if (!value || typeof value !== "string") {
    return defaultValue;
  }

  if (allowedValues.includes(value)) {
    return value;
  }

  return defaultValue;
}

export {
  sanitizeSearch,
  validatePage,
  validateLimit,
  validateBoolean,
  validateIdentifier,
  validateEnum,
};
