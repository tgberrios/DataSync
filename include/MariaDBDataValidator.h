#ifndef MARIADBDATAVALIDATOR_H
#define MARIADBDATAVALIDATOR_H

#include "logger.h"
#include <string>
#include <vector>

class MariaDBDataValidator {
public:
  MariaDBDataValidator() = default;
  ~MariaDBDataValidator() = default;

  std::string cleanValueForPostgres(const std::string &value,
                                    const std::string &columnType);
  bool isValidPrimaryKey(const std::string &pkValue);
  std::string sanitizeString(const std::string &value);
  bool isNullValue(const std::string &value);

private:
  bool isNumericValue(const std::string &value);
  bool hasInvalidBinaryChars(const std::string &value);
  std::string truncateToLength(const std::string &value, size_t maxLength);
  std::string getDefaultValueForType(const std::string &columnType);
};

#endif // MARIADBDATAVALIDATOR_H
