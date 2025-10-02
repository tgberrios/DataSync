#include "LogFormatter.h"
#include <algorithm>

std::string LogFormatter::formatMessage(LogLevel level, LogCategory category,
                                        const std::string &function,
                                        const std::string &message) {
  std::ostringstream oss;

  // Format: [timestamp] [level] [category] [function] message
  oss << "[" << formatTimestamp() << "] "
      << "[" << formatLevel(level) << "] "
      << "[" << formatCategory(category) << "]";

  if (!function.empty()) {
    oss << " [" << formatFunction(function) << "]";
  }

  oss << " " << message;

  return oss.str();
}

std::string LogFormatter::getCurrentTimestamp() { return formatTimestamp(); }

std::string LogFormatter::getLevelString(LogLevel level) {
  return formatLevel(level);
}

std::string LogFormatter::getCategoryString(LogCategory category) {
  return formatCategory(category);
}

LogLevel LogFormatter::stringToLogLevel(const std::string &levelStr) {
  if (levelStr.empty())
    return LogLevel::INFO;

  std::string upperLevelStr = levelStr;
  std::transform(upperLevelStr.begin(), upperLevelStr.end(),
                 upperLevelStr.begin(), ::toupper);

  if (upperLevelStr == "DEBUG")
    return LogLevel::DEBUG;
  if (upperLevelStr == "INFO")
    return LogLevel::INFO;
  if (upperLevelStr == "WARN" || upperLevelStr == "WARNING")
    return LogLevel::WARNING;
  if (upperLevelStr == "ERROR")
    return LogLevel::ERROR;
  if (upperLevelStr == "FATAL" || upperLevelStr == "CRITICAL")
    return LogLevel::CRITICAL;

  return LogLevel::INFO; // Default
}

LogCategory LogFormatter::stringToCategory(const std::string &categoryStr) {
  if (categoryStr.empty())
    return LogCategory::UNKNOWN;

  std::string upperCategoryStr = categoryStr;
  std::transform(upperCategoryStr.begin(), upperCategoryStr.end(),
                 upperCategoryStr.begin(), ::toupper);

  if (upperCategoryStr == "SYSTEM")
    return LogCategory::SYSTEM;
  if (upperCategoryStr == "DATABASE")
    return LogCategory::DATABASE;
  if (upperCategoryStr == "TRANSFER")
    return LogCategory::TRANSFER;
  if (upperCategoryStr == "CONFIG")
    return LogCategory::CONFIG;
  if (upperCategoryStr == "VALIDATION")
    return LogCategory::VALIDATION;
  if (upperCategoryStr == "MAINTENANCE")
    return LogCategory::MAINTENANCE;
  if (upperCategoryStr == "MONITORING")
    return LogCategory::MONITORING;
  if (upperCategoryStr == "DDL_EXPORT")
    return LogCategory::DDL_EXPORT;
  if (upperCategoryStr == "METRICS")
    return LogCategory::METRICS;
  if (upperCategoryStr == "GOVERNANCE")
    return LogCategory::GOVERNANCE;
  if (upperCategoryStr == "QUALITY")
    return LogCategory::QUALITY;

  return LogCategory::UNKNOWN;
}

std::string LogFormatter::formatTimestamp() {
  auto now = std::chrono::system_clock::now();
  auto time_t = std::chrono::system_clock::to_time_t(now);
  auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                now.time_since_epoch()) %
            1000;

  std::ostringstream oss;
  oss << std::put_time(std::localtime(&time_t), "%Y-%m-%d %H:%M:%S");
  oss << "." << std::setfill('0') << std::setw(3) << ms.count();
  return oss.str();
}

std::string LogFormatter::formatLevel(LogLevel level) {
  switch (level) {
  case LogLevel::DEBUG:
    return "DEBUG";
  case LogLevel::INFO:
    return "INFO";
  case LogLevel::WARNING:
    return "WARNING";
  case LogLevel::ERROR:
    return "ERROR";
  case LogLevel::CRITICAL:
    return "CRITICAL";
  default:
    return "UNKNOWN";
  }
}

std::string LogFormatter::formatCategory(LogCategory category) {
  switch (category) {
  case LogCategory::SYSTEM:
    return "SYSTEM";
  case LogCategory::DATABASE:
    return "DATABASE";
  case LogCategory::TRANSFER:
    return "TRANSFER";
  case LogCategory::CONFIG:
    return "CONFIG";
  case LogCategory::VALIDATION:
    return "VALIDATION";
  case LogCategory::MAINTENANCE:
    return "MAINTENANCE";
  case LogCategory::MONITORING:
    return "MONITORING";
  case LogCategory::DDL_EXPORT:
    return "DDL_EXPORT";
  case LogCategory::METRICS:
    return "METRICS";
  case LogCategory::GOVERNANCE:
    return "GOVERNANCE";
  case LogCategory::QUALITY:
    return "QUALITY";
  default:
    return "UNKNOWN";
  }
}

std::string LogFormatter::formatFunction(const std::string &function) {
  return function; // Simple passthrough for now
}
