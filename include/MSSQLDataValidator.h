#ifndef MSSQLDATAVALIDATOR_H
#define MSSQLDATAVALIDATOR_H

#include "logger.h"
#include <pqxx/pqxx>
#include <string>
#include <vector>

class MSSQLDataValidator {
public:
  MSSQLDataValidator() = default;
  ~MSSQLDataValidator() = default;

  // Data validation and cleaning
  std::string cleanValueForPostgres(const std::string &value,
                                    const std::string &columnType);

  // Record comparison and update
  bool compareAndUpdateRecord(
      pqxx::connection &pgConn, const std::string &schemaName,
      const std::string &tableName, const std::vector<std::string> &newRecord,
      const std::vector<std::vector<std::string>> &columnNames,
      const std::string &whereClause);

  // Utility functions
  std::string escapeSQL(const std::string &value);

private:
  // Data type validation
  bool isValidDate(const std::string &value);
  bool isValidNumeric(const std::string &value, const std::string &type);
  bool isValidBoolean(const std::string &value);

  // Value normalization
  std::string normalizeBoolean(const std::string &value);
  std::string normalizeNumeric(const std::string &value,
                               const std::string &type);
};

#endif // MSSQLDATAVALIDATOR_H
