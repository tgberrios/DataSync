#ifndef LOGFORMATTER_H
#define LOGFORMATTER_H

#include <chrono>
#include <iomanip>
#include <sstream>
#include <string>

enum class LogLevel {
  DEBUG = 0,
  INFO = 1,
  WARNING = 2,
  ERROR = 3,
  CRITICAL = 4
};

enum class LogCategory {
  SYSTEM = 0,
  DATABASE = 1,
  TRANSFER = 2,
  CONFIG = 3,
  VALIDATION = 4,
  MAINTENANCE = 5,
  MONITORING = 6,
  DDL_EXPORT = 7,
  METRICS = 8,
  GOVERNANCE = 9,
  QUALITY = 10,
  UNKNOWN = 99
};

class LogFormatter {
public:
  LogFormatter() = default;
  ~LogFormatter() = default;

  // Format log message
  std::string formatMessage(LogLevel level, LogCategory category,
                            const std::string &function,
                            const std::string &message);

  // Utility methods
  std::string getCurrentTimestamp();
  std::string getLevelString(LogLevel level);
  std::string getCategoryString(LogCategory category);
  LogLevel stringToLogLevel(const std::string &levelStr);
  LogCategory stringToCategory(const std::string &categoryStr);

private:
  std::string formatTimestamp();
  std::string formatLevel(LogLevel level);
  std::string formatCategory(LogCategory category);
  std::string formatFunction(const std::string &function);
};

#endif // LOGFORMATTER_H
