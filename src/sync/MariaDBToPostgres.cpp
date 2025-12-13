#include "sync/MariaDBToPostgres.h"
#include "engines/database_engine.h"
#include <algorithm>
#include <cctype>

std::unordered_map<std::string, std::string> MariaDBToPostgres::dataTypeMap = {
    {"int", "INTEGER"},
    {"bigint", "BIGINT"},
    {"smallint", "SMALLINT"},
    {"tinyint", "SMALLINT"},
    {"decimal", "NUMERIC"},
    {"float", "REAL"},
    {"double", "DOUBLE PRECISION"},
    {"varchar", "VARCHAR"},
    {"char", "CHAR"},
    {"text", "TEXT"},
    {"longtext", "TEXT"},
    {"mediumtext", "TEXT"},
    {"tinytext", "TEXT"},
    {"blob", "BYTEA"},
    {"longblob", "BYTEA"},
    {"mediumblob", "BYTEA"},
    {"tinyblob", "BYTEA"},
    {"json", "JSON"},
    {"boolean", "BOOLEAN"},
    {"bit", "BIT"},
    {"timestamp", "TIMESTAMP"},
    {"datetime", "TIMESTAMP"},
    {"date", "DATE"},
    {"time", "TIME"}};

std::unordered_map<std::string, std::string> MariaDBToPostgres::collationMap = {
    {"utf8_general_ci", "en_US.utf8"},
    {"utf8mb4_general_ci", "en_US.utf8"},
    {"latin1_swedish_ci", "C"},
    {"ascii_general_ci", "C"}};

// Cleans and normalizes a value from MariaDB for insertion into PostgreSQL.
// Handles null detection (empty strings, "NULL", invalid dates like
// "0000-00-00", "1900-01-01", "1970-01-01"), invalid binary characters
// (non-ASCII), and invalid date formats. For VARCHAR/CHAR types, truncates
// values that exceed the maximum length specified in the column type. For
// BYTEA/BLOB/BIT types, validates hexadecimal format and truncates large binary
// data (>1000 bytes). For null values, returns appropriate defaults based on
// column type (0 for integers, 0.0 for floats, "DEFAULT" for strings,
// "1970-01-01 00:00:00" for timestamps). For date/timestamp types, validates
// format and detects invalid dates containing "-00". Returns the cleaned value
// ready for SQL insertion.
std::string
MariaDBToPostgres::cleanValueForPostgres(const std::string &value,
                                         const std::string &columnType) {
  std::string cleanValue = value;
  std::string upperType = columnType;
  std::transform(upperType.begin(), upperType.end(), upperType.begin(),
                 ::toupper);

  bool isNull =
      (cleanValue.empty() || cleanValue == "NULL" || cleanValue == "null" ||
       cleanValue == "\\N" || cleanValue == "\\0" ||
       cleanValue.find("0000-") != std::string::npos ||
       cleanValue.find("1900-01-01") != std::string::npos ||
       cleanValue.find("1970-01-01") != std::string::npos);

  for (char &c : cleanValue) {
    if (static_cast<unsigned char>(c) > 127 || c < 32) {
      isNull = true;
      break;
    }
  }

  if (upperType.find("VARCHAR") != std::string::npos ||
      upperType.find("CHAR") != std::string::npos) {
    size_t openParen = upperType.find('(');
    size_t closeParen = upperType.find(')');
    if (openParen != std::string::npos && closeParen != std::string::npos) {
      try {
        size_t maxLen = std::stoul(
            upperType.substr(openParen + 1, closeParen - openParen - 1));
        if (cleanValue.length() > maxLen) {
          Logger::warning(
              LogCategory::TRANSFER, "cleanValueForPostgres",
              "Value too long for " + upperType + ", truncating from " +
                  std::to_string(cleanValue.length()) + " to " +
                  std::to_string(maxLen) +
                  " characters: " + cleanValue.substr(0, 20) + "...");
          cleanValue = cleanValue.substr(0, maxLen);
        }
      } catch (const std::exception &e) {
      }
    }
  }

  if (upperType.find("BYTEA") != std::string::npos ||
      upperType.find("BLOB") != std::string::npos ||
      upperType.find("BIT") != std::string::npos) {

    bool hasInvalidBinaryChars = false;
    for (char c : cleanValue) {
      if (!std::isxdigit(c) && c != ' ' && c != '\\' && c != 'x') {
        hasInvalidBinaryChars = true;
        break;
      }
    }

    if (hasInvalidBinaryChars) {
      Logger::warning(LogCategory::TRANSFER, "cleanValueForPostgres",
                      "Invalid binary data detected, converting to NULL: " +
                          cleanValue.substr(0, 50) + "...");
      isNull = true;
    } else if (!cleanValue.empty() && cleanValue.length() > 1000) {
      Logger::warning(LogCategory::TRANSFER, "cleanValueForPostgres",
                      "Large binary data detected, truncating: " +
                          std::to_string(cleanValue.length()) + " bytes");
      cleanValue = cleanValue.substr(0, 1000);
    }
  }

  if (upperType.find("TIMESTAMP") != std::string::npos ||
      upperType.find("DATETIME") != std::string::npos ||
      upperType.find("DATE") != std::string::npos) {
    if (cleanValue.length() < 10 || cleanValue.find("-") == std::string::npos ||
        cleanValue.find("0000") != std::string::npos) {
      isNull = true;
    } else {
      if (cleanValue.find("-00") != std::string::npos ||
          cleanValue.find("-00 ") != std::string::npos ||
          cleanValue.find(" 00:00:00") != std::string::npos) {
        isNull = true;
      }
    }
  }

  if (isNull) {
    if (upperType.find("INTEGER") != std::string::npos ||
        upperType.find("BIGINT") != std::string::npos ||
        upperType.find("SMALLINT") != std::string::npos) {
      return "0";
    } else if (upperType.find("REAL") != std::string::npos ||
               upperType.find("FLOAT") != std::string::npos ||
               upperType.find("DOUBLE") != std::string::npos ||
               upperType.find("NUMERIC") != std::string::npos) {
      return "0.0";
    } else if (upperType.find("VARCHAR") != std::string::npos ||
               upperType.find("TEXT") != std::string::npos ||
               upperType.find("CHAR") != std::string::npos) {
      return "DEFAULT";
    } else if (upperType.find("TIMESTAMP") != std::string::npos ||
               upperType.find("DATETIME") != std::string::npos) {
      return "1970-01-01 00:00:00";
    } else if (upperType.find("DATE") != std::string::npos) {
      return "1970-01-01";
    } else if (upperType.find("TIME") != std::string::npos) {
      return "00:00:00";
    } else {
      return "DEFAULT";
    }
  }

  return cleanValue;
}
