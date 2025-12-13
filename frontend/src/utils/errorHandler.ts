/**
 * Utilidades para manejo de errores
 */

/**
 * Sanitiza un mensaje de error para no exponer información sensible
 *
 * @param error - Error a sanitizar
 * @param defaultMessage - Mensaje por defecto si no se puede sanitizar
 * @returns Mensaje de error seguro para mostrar al usuario
 *
 * @example
 * ```typescript
 * const safeError = sanitizeError(err, 'An error occurred');
 * ```
 */
export const sanitizeError = (
  error: unknown,
  defaultMessage: string = "An error occurred",
  isProduction: boolean = false
): string => {
  if (error instanceof Error) {
    const message = error.message;

    if (isProduction) {
      if (
        message.includes("password") ||
        message.includes("credential") ||
        message.includes("authentication") ||
        message.includes("connection") ||
        message.includes("ECONNREFUSED") ||
        message.includes("ENOTFOUND")
      ) {
        return "Database connection error. Please contact support.";
      }

      if (
        message.includes("syntax error") ||
        message.includes("relation") ||
        message.includes("column") ||
        message.includes("constraint")
      ) {
        return "Database query error. Please contact support.";
      }

      return defaultMessage;
    }

    return message;
  }

  if (typeof error === "string") {
    return error;
  }

  return defaultMessage;
};

/**
 * Tipo para respuestas de error de API
 */
export interface ApiError {
  error: string;
  details?: string;
  code?: string;
}

/**
 * Extrae un mensaje de error de una respuesta de API
 *
 * @param error - Error de axios o error genérico
 * @returns Mensaje de error extraído
 *
 * @example
 * ```typescript
 * try {
 *   await api.get('/endpoint');
 * } catch (err) {
 *   const message = extractApiError(err);
 *   setError(message);
 * }
 * ```
 */
export const extractApiError = (error: unknown): string => {
  if (error && typeof error === "object" && "response" in error) {
    const axiosError = error as { response?: { data?: ApiError } };
    if (axiosError.response?.data) {
      return (
        axiosError.response.data.details ||
        axiosError.response.data.error ||
        "An error occurred"
      );
    }
  }

  if (error instanceof Error) {
    return error.message;
  }

  if (typeof error === "string") {
    return error;
  }

  return "An unexpected error occurred";
};
