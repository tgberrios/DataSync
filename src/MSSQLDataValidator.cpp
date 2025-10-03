#include "MSSQLDataValidator.h"
#include <algorithm>
#include <cctype>
#include <regex>

std::string
MSSQLDataValidator::cleanValueForPostgres(const std::string &value,
                                          const std::string &columnType) {
  std::string cleanValue = value;
  std::string upperType = columnType;
  std::transform(upperType.begin(), upperType.end(), upperType.begin(),
                 ::toupper);

  // Detectar valores NULL de MSSQL - SIMPLIFICADO
  bool isNull =
      (cleanValue.empty() || cleanValue == "NULL" || cleanValue == "null" ||
       cleanValue == "\\N" || cleanValue == "\\0" || cleanValue == "0" ||
       cleanValue.find("0000-") != std::string::npos ||
       cleanValue.find("1900-01-01") != std::string::npos ||
       cleanValue.find("1970-01-01") != std::string::npos);

  // Limpiar caracteres de control y caracteres problemáticos
  for (char &c : cleanValue) {
    if (static_cast<unsigned char>(c) > 127 || c < 32) {
      isNull = true;
      break;
    }
  }

  // Para fechas, cualquier valor que no sea una fecha válida = NULL
  if (upperType.find("TIMESTAMP") != std::string::npos ||
      upperType.find("DATETIME") != std::string::npos ||
      upperType.find("DATE") != std::string::npos) {
    if (cleanValue.length() < 10 || cleanValue.find("-") == std::string::npos ||
        cleanValue.find("0000") != std::string::npos) {
      isNull = true;
    }
  }

  // Si es NULL, generar valor por defecto en lugar de NULL
  if (isNull) {
    if (upperType.find("INTEGER") != std::string::npos ||
        upperType.find("BIGINT") != std::string::npos ||
        upperType.find("SMALLINT") != std::string::npos) {
      return "0"; // Valor por defecto para enteros
    } else if (upperType.find("REAL") != std::string::npos ||
               upperType.find("FLOAT") != std::string::npos ||
               upperType.find("DOUBLE") != std::string::npos ||
               upperType.find("NUMERIC") != std::string::npos) {
      return "0.0"; // Valor por defecto para números decimales
    } else if (upperType == "TEXT") {
      // Fallback para TEXT: devolver NULL para que PostgreSQL use el valor
      // por defecto de la columna
      return "NULL";
    } else if (upperType.find("VARCHAR") != std::string::npos ||
               upperType.find("TEXT") != std::string::npos ||
               upperType.find("CHAR") != std::string::npos) {
      return "DEFAULT"; // Valor por defecto para texto
    } else if (upperType.find("TIMESTAMP") != std::string::npos ||
               upperType.find("DATETIME") != std::string::npos) {
      return "1970-01-01 00:00:00"; // Valor por defecto para fechas
    } else if (upperType.find("DATE") != std::string::npos) {
      return "1970-01-01"; // Valor por defecto para fechas
    } else if (upperType.find("TIME") != std::string::npos) {
      return "00:00:00"; // Valor por defecto para tiempo
    } else if (upperType.find("BOOLEAN") != std::string::npos ||
               upperType.find("BOOL") != std::string::npos) {
      return "false"; // Valor por defecto para booleanos
    } else {
      return "DEFAULT"; // Valor por defecto genérico
    }
  }

  // Limpiar caracteres de control restantes
  cleanValue.erase(std::remove_if(cleanValue.begin(), cleanValue.end(),
                                  [](unsigned char c) {
                                    return c < 32 && c != 9 && c != 10 &&
                                           c != 13;
                                  }),
                   cleanValue.end());

  // Manejar tipos específicos
  if (upperType.find("BOOLEAN") != std::string::npos ||
      upperType.find("BOOL") != std::string::npos) {
    return normalizeBoolean(cleanValue);
  } else if (upperType.find("BIT") != std::string::npos) {
    return normalizeBoolean(cleanValue);
  } else if (upperType.find("NUMERIC") != std::string::npos ||
             upperType.find("DECIMAL") != std::string::npos) {
    return normalizeNumeric(cleanValue, upperType);
  }

  return cleanValue;
}

bool MSSQLDataValidator::compareAndUpdateRecord(
    pqxx::connection &pgConn, const std::string &schemaName,
    const std::string &tableName, const std::vector<std::string> &newRecord,
    const std::vector<std::vector<std::string>> &columnNames,
    const std::string &whereClause) {
  try {
    // Obtener el registro actual de PostgreSQL
    std::string selectQuery = "SELECT * FROM \"" + schemaName + "\".\"" +
                              tableName + "\" WHERE " + whereClause;

    pqxx::work txn(pgConn);
    auto result = txn.exec(selectQuery);
    txn.commit();

    if (result.empty()) {
      return false; // No existe el registro
    }

    const auto &currentRow = result[0];

    // Comparar cada columna (excepto primary keys)
    std::vector<std::string> updateFields;
    bool hasChanges = false;

    for (size_t i = 0; i < columnNames.size(); ++i) {
      std::string columnName = columnNames[i][0];
      std::transform(columnName.begin(), columnName.end(), columnName.begin(),
                     ::tolower);
      std::string newValue = newRecord[i];

      // Obtener valor actual de PostgreSQL
      std::string currentValue =
          currentRow[i].is_null() ? "" : currentRow[i].as<std::string>();

      // Comparar valores (normalizar para comparación)
      if (currentValue != newValue) {
        std::string valueToSet;
        if (newValue.empty()) {
          valueToSet = "NULL";
        } else {
          // Usar cleanValueForPostgres para manejar fechas inválidas y otros
          // valores problemáticos
          // TODO: Necesitamos obtener el tipo real de la columna, por ahora
          // usar TEXT como fallback
          std::string cleanedValue = cleanValueForPostgres(newValue, "TEXT");
          if (cleanedValue == "NULL") {
            valueToSet = "NULL";
          } else {
            valueToSet = "'" + escapeSQL(cleanedValue) + "'";
          }
        }
        updateFields.push_back("\"" + columnName + "\" = " + valueToSet);
        hasChanges = true;
      }
    }

    if (hasChanges) {
      // Ejecutar UPDATE
      std::string updateQuery =
          "UPDATE \"" + schemaName + "\".\"" + tableName + "\" SET ";
      for (size_t i = 0; i < updateFields.size(); ++i) {
        if (i > 0)
          updateQuery += ", ";
        updateQuery += updateFields[i];
      }
      updateQuery += " WHERE " + whereClause;

      pqxx::work updateTxn(pgConn);
      updateTxn.exec(updateQuery);
      updateTxn.commit();

      return true;
    }

    return false; // No había cambios

  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::TRANSFER,
                                "Error comparing/updating record: " +
                                    std::string(e.what()));
    return false;
  }
}

std::string MSSQLDataValidator::escapeSQL(const std::string &value) {
  std::string escaped = value;
  size_t pos = 0;
  while ((pos = escaped.find("'", pos)) != std::string::npos) {
    escaped.replace(pos, 1, "''");
    pos += 2;
  }
  return escaped;
}

bool MSSQLDataValidator::isValidDate(const std::string &value) {
  if (value.empty() || value == "NULL" || value == "null") {
    return false;
  }

  // Basic date validation - check for YYYY-MM-DD format
  std::regex dateRegex(R"(\d{4}-\d{2}-\d{2})");
  return std::regex_match(value, dateRegex);
}

bool MSSQLDataValidator::isValidNumeric(const std::string &value,
                                        const std::string &type) {
  if (value.empty() || value == "NULL" || value == "null") {
    return false;
  }

  try {
    if (type.find("INTEGER") != std::string::npos ||
        type.find("BIGINT") != std::string::npos ||
        type.find("SMALLINT") != std::string::npos) {
      std::stoll(value);
    } else if (type.find("REAL") != std::string::npos ||
               type.find("FLOAT") != std::string::npos ||
               type.find("DOUBLE") != std::string::npos ||
               type.find("NUMERIC") != std::string::npos) {
      std::stod(value);
    }
    return true;
  } catch (const std::exception &) {
    return false;
  }
}

bool MSSQLDataValidator::isValidBoolean(const std::string &value) {
  std::string lowerValue = value;
  std::transform(lowerValue.begin(), lowerValue.end(), lowerValue.begin(),
                 ::tolower);

  return lowerValue == "true" || lowerValue == "false" || lowerValue == "1" ||
         lowerValue == "0" || lowerValue == "y" || lowerValue == "n" ||
         lowerValue == "yes" || lowerValue == "no";
}

std::string MSSQLDataValidator::normalizeBoolean(const std::string &value) {
  std::string lowerValue = value;
  std::transform(lowerValue.begin(), lowerValue.end(), lowerValue.begin(),
                 ::tolower);

  if (lowerValue == "n" || lowerValue == "0" || lowerValue == "false" ||
      lowerValue == "false" || lowerValue == "no") {
    return "false";
  } else if (lowerValue == "y" || lowerValue == "1" || lowerValue == "true" ||
             lowerValue == "true" || lowerValue == "yes") {
    return "true";
  }

  return "false"; // Default to false
}

std::string MSSQLDataValidator::normalizeNumeric(const std::string &value,
                                                 const std::string &type) {
  if (value.empty() || value == "NULL" || value == "null") {
    return "0";
  }

  try {
    if (type.find("INTEGER") != std::string::npos ||
        type.find("BIGINT") != std::string::npos ||
        type.find("SMALLINT") != std::string::npos) {
      return std::to_string(std::stoll(value));
    } else if (type.find("REAL") != std::string::npos ||
               type.find("FLOAT") != std::string::npos ||
               type.find("DOUBLE") != std::string::npos ||
               type.find("NUMERIC") != std::string::npos) {
      return std::to_string(std::stod(value));
    }
  } catch (const std::exception &) {
    return "0";
  }

  return value;
}
