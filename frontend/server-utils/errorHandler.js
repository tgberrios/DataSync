/**
 * Utilidades para manejo de errores en el servidor
 */

/**
 * Sanitiza un mensaje de error para no exponer información sensible
 *
 * @param {Error|unknown} error - Error a sanitizar
 * @param {string} defaultMessage - Mensaje por defecto si no se puede sanitizar
 * @param {boolean} isProduction - Si está en producción (default: process.env.NODE_ENV === 'production')
 * @returns {string} Mensaje de error seguro para mostrar al usuario
 */
function sanitizeError(
  error,
  defaultMessage = "An error occurred",
  isProduction = process.env.NODE_ENV === "production"
) {
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
}

/**
 * Middleware para manejo centralizado de errores
 *
 * @param {Error} err - Error capturado
 * @param {Object} req - Request object
 * @param {Object} res - Response object
 * @param {Function} next - Next middleware
 */
function errorHandler(err, req, res, next) {
  console.error("Error:", {
    message: err.message,
    stack: process.env.NODE_ENV === "development" ? err.stack : undefined,
    path: req.path,
    method: req.method,
  });

  const isProduction = process.env.NODE_ENV === "production";
  const sanitizedMessage = sanitizeError(
    err,
    "An error occurred",
    isProduction
  );

  res.status(err.status || 500).json({
    error: sanitizedMessage,
    ...(isProduction ? {} : { details: err.message }),
  });
}

export { sanitizeError, errorHandler };
