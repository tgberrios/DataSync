#include "MariaDBDataValidator.h"
#include <algorithm>
#include <cctype>

std::string
MariaDBDataValidator::cleanValueForPostgres(const std::string &value,
                                            const std::string &columnType) {
  std::string cleanValue = value;
  std::string upperType = columnType;
  std::transform(upperType.begin(), upperType.end(), upperType.begin(),
                 ::toupper);

  if (isNullValue(cleanValue)) {
    return getDefaultValueForType(upperType);
  }

  // Clean control characters
  for (char &c : cleanValue) {
    if (static_cast<unsigned char>(c) > 127 || c < 32) {
      return getDefaultValueForType(upperType);
    }
  }

  // Handle VARCHAR/CHAR with length limits
  if (upperType.find("VARCHAR") != std::string::npos ||
      upperType.find("CHAR") != std::string::npos) {
    size_t openParen = upperType.find('(');
    size_t closeParen = upperType.find(')');
    if (openParen != std::string::npos && closeParen != std::string::npos) {
      try {
        size_t maxLen = std::stoul(
            upperType.substr(openParen + 1, closeParen - openParen - 1));
        if (cleanValue.length() > maxLen) {
          Logger::getInstance().warning(
              LogCategory::TRANSFER, "MariaDBDataValidator",
              "Value too long for " + upperType + ", truncating from " +
                  std::to_string(cleanValue.length()) + " to " +
                  std::to_string(maxLen) +
                  " characters: " + cleanValue.substr(0, 20) + "...");
          cleanValue = truncateToLength(cleanValue, maxLen);
          if (cleanValue.empty()) {
            return getDefaultValueForType(upperType);
          }
        }
      } catch (const std::exception &e) {
        // If can't parse length, continue without truncating
      }
    }
  }

  // Handle binary data
  if (upperType.find("BYTEA") != std::string::npos ||
      upperType.find("BLOB") != std::string::npos ||
      upperType.find("BIT") != std::string::npos) {
    if (hasInvalidBinaryChars(cleanValue)) {
      Logger::getInstance().warning(
          LogCategory::TRANSFER, "MariaDBDataValidator",
          "Invalid binary data detected, converting to NULL: " +
              cleanValue.substr(0, 50) + "...");
      return getDefaultValueForType(upperType);
    }

    if (!cleanValue.empty() && cleanValue.length() > 1000) {
      Logger::getInstance().warning(
          LogCategory::TRANSFER, "MariaDBDataValidator",
          "Large binary data detected, truncating: " +
              std::to_string(cleanValue.length()) + " bytes");
      cleanValue = truncateToLength(cleanValue, 1000);
    }
  }

  // Handle dates
  if (upperType.find("TIMESTAMP") != std::string::npos ||
      upperType.find("DATETIME") != std::string::npos ||
      upperType.find("DATE") != std::string::npos) {
    if (isNumericValue(cleanValue) || cleanValue.length() < 10 ||
        cleanValue.find("-") == std::string::npos ||
        cleanValue.find("0000") != std::string::npos ||
        cleanValue.find("0000-00-00") != std::string::npos) {
      return getDefaultValueForType(upperType);
    }
  }

  return cleanValue;
}

bool MariaDBDataValidator::isValidPrimaryKey(const std::string &pkValue) {
  return !pkValue.empty() &&
         !std::all_of(pkValue.begin(), pkValue.end(), ::isdigit);
}

std::string MariaDBDataValidator::sanitizeString(const std::string &value) {
  std::string sanitized = value;

  // Remove control characters except tab, newline, carriage return
  sanitized.erase(std::remove_if(sanitized.begin(), sanitized.end(),
                                 [](unsigned char c) {
                                   return c < 32 && c != 9 && c != 10 &&
                                          c != 13;
                                 }),
                  sanitized.end());

  // Replace high ASCII characters with ?
  for (char &c : sanitized) {
    if (static_cast<unsigned char>(c) > 127) {
      c = '?';
    }
  }

  return sanitized;
}

bool MariaDBDataValidator::isNullValue(const std::string &value) {
  return value.empty() || value == "NULL" || value == "null" ||
         value == "\\N" || value == "\\0" || value == "0" || value == "0.0" ||
         value == "0.00" || value == "0.000" ||
         value.find("0000-") != std::string::npos ||
         value.find("0000-00-00") != std::string::npos ||
         value.find("1900-01-01") != std::string::npos ||
         value.find("1970-01-01") != std::string::npos;
}

bool MariaDBDataValidator::isNumericValue(const std::string &value) {
  bool hasDecimal = false;
  for (char c : value) {
    if (!std::isdigit(c) && c != '.' && c != '-') {
      return false;
    }
    if (c == '.') {
      hasDecimal = true;
    }
  }
  return true;
}

bool MariaDBDataValidator::hasInvalidBinaryChars(const std::string &value) {
  for (char c : value) {
    if (!std::isxdigit(c) && c != ' ' && c != '\\' && c != 'x') {
      return true;
    }
  }
  return false;
}

std::string MariaDBDataValidator::truncateToLength(const std::string &value,
                                                   size_t maxLength) {
  if (value.length() <= maxLength) {
    return value;
  }
  return value.substr(0, maxLength);
}

std::string
MariaDBDataValidator::getDefaultValueForType(const std::string &columnType) {
  if (columnType.find("INTEGER") != std::string::npos ||
      columnType.find("BIGINT") != std::string::npos ||
      columnType.find("SMALLINT") != std::string::npos) {
    return "0";
  } else if (columnType.find("REAL") != std::string::npos ||
             columnType.find("FLOAT") != std::string::npos ||
             columnType.find("DOUBLE") != std::string::npos ||
             columnType.find("NUMERIC") != std::string::npos) {
    return "0.0";
  } else if (columnType == "TEXT") {
    return "NULL";
  } else if (columnType.find("VARCHAR") != std::string::npos ||
             columnType.find("TEXT") != std::string::npos ||
             columnType.find("CHAR") != std::string::npos) {
    return "DEFAULT";
  } else if (columnType.find("TIMESTAMP") != std::string::npos ||
             columnType.find("DATETIME") != std::string::npos) {
    return "1970-01-01 00:00:00";
  } else if (columnType.find("DATE") != std::string::npos) {
    return "1970-01-01";
  } else if (columnType.find("TIME") != std::string::npos) {
    return "00:00:00";
  } else {
    return "DEFAULT";
  }
}
