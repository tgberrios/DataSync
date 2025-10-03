#include "LogConfigManager.h"
#include "Config.h"
#include <algorithm>
#include <iostream>
#include <pqxx/pqxx>

LogConfigManager::LogConfigManager()
    : currentLogLevel(LogLevel::INFO), showTimestamps(true),
      showThreadId(false), showFileLine(false), configMutex() {
  setDefaultValues();
}

void LogConfigManager::loadFromDatabase() {
  std::lock_guard<std::mutex> lock(configMutex);

  try {
    if (loadConfigFromDatabase()) {
      return; // Successfully loaded from database
    }
  } catch (const std::exception &e) {
    logConfigError("Failed to load config from database: " +
                   std::string(e.what()));
  }

  // Fallback to default config
  setDefaultConfig();
}

void LogConfigManager::setDefaultConfig() {
  std::lock_guard<std::mutex> lock(configMutex);
  setDefaultValues();
}

void LogConfigManager::refreshConfig() { loadFromDatabase(); }

void LogConfigManager::setLogLevel(LogLevel level) {
  std::lock_guard<std::mutex> lock(configMutex);
  currentLogLevel = level;
}

void LogConfigManager::setLogLevel(const std::string &levelStr) {
  if (levelStr.empty()) {
    logConfigError("Empty log level string provided");
    return;
  }

  if (!isValidLogLevel(levelStr)) {
    logConfigError("Invalid log level string: " + levelStr);
    return;
  }

  LogFormatter formatter;
  setLogLevel(formatter.stringToLogLevel(levelStr));
}

LogLevel LogConfigManager::getCurrentLogLevel() const {
  std::lock_guard<std::mutex> lock(configMutex);
  return currentLogLevel;
}

void LogConfigManager::setShowTimestamps(bool show) {
  std::lock_guard<std::mutex> lock(configMutex);
  showTimestamps = show;
}

void LogConfigManager::setShowThreadId(bool show) {
  std::lock_guard<std::mutex> lock(configMutex);
  showThreadId = show;
}

void LogConfigManager::setShowFileLine(bool show) {
  std::lock_guard<std::mutex> lock(configMutex);
  showFileLine = show;
}

bool LogConfigManager::getShowTimestamps() const {
  std::lock_guard<std::mutex> lock(configMutex);
  return showTimestamps;
}

bool LogConfigManager::getShowThreadId() const {
  std::lock_guard<std::mutex> lock(configMutex);
  return showThreadId;
}

bool LogConfigManager::getShowFileLine() const {
  std::lock_guard<std::mutex> lock(configMutex);
  return showFileLine;
}

bool LogConfigManager::isValidLogLevel(const std::string &levelStr) const {
  if (levelStr.empty())
    return false;

  std::string upperLevelStr = levelStr;
  std::transform(upperLevelStr.begin(), upperLevelStr.end(),
                 upperLevelStr.begin(), ::toupper);

  return (upperLevelStr == "DEBUG" || upperLevelStr == "INFO" ||
          upperLevelStr == "WARN" || upperLevelStr == "WARNING" ||
          upperLevelStr == "ERROR" || upperLevelStr == "FATAL" ||
          upperLevelStr == "CRITICAL");
}

bool LogConfigManager::isValidCategory(const std::string &categoryStr) const {
  if (categoryStr.empty())
    return false;

  std::string upperCategoryStr = categoryStr;
  std::transform(upperCategoryStr.begin(), upperCategoryStr.end(),
                 upperCategoryStr.begin(), ::toupper);

  return (upperCategoryStr == "SYSTEM" || upperCategoryStr == "DATABASE" ||
          upperCategoryStr == "TRANSFER" || upperCategoryStr == "CONFIG" ||
          upperCategoryStr == "VALIDATION" ||
          upperCategoryStr == "MAINTENANCE" ||
          upperCategoryStr == "MONITORING" ||
          upperCategoryStr == "DDL_EXPORT" || upperCategoryStr == "METRICS" ||
          upperCategoryStr == "GOVERNANCE" || upperCategoryStr == "QUALITY");
}

void LogConfigManager::loadDebugConfig() {
  // This method is kept for backward compatibility
  loadFromDatabase();
}

void LogConfigManager::setDefaultValues() {
  currentLogLevel = LogLevel::INFO;
  showTimestamps = true;
  showThreadId = false;
  showFileLine = false;
}

bool LogConfigManager::loadConfigFromDatabase() {
  try {
    // Validate connection string
    std::string connStr = DatabaseConfig::getPostgresConnectionString();
    if (connStr.empty()) {
      logConfigError("Empty database connection string");
      return false;
    }

    // Create connection with timeout
    pqxx::connection conn(connStr);
    if (!conn.is_open()) {
      logConfigError("Failed to connect to database");
      return false;
    }

    // Test connection with a simple query
    try {
      pqxx::work testTxn(conn);
      testTxn.exec("SELECT 1");
      testTxn.commit();
    } catch (const std::exception &e) {
      logConfigError("Database connection test failed: " +
                     std::string(e.what()));
      return false;
    }

    pqxx::work txn(conn);

    // Load debug level
    auto result =
        txn.exec("SELECT value FROM metadata.config WHERE key = 'debug_level'");
    if (!result.empty()) {
      std::string levelStr = result[0][0].as<std::string>();
      if (!levelStr.empty() && isValidLogLevel(levelStr)) {
        LogFormatter formatter;
        currentLogLevel = formatter.stringToLogLevel(levelStr);
      }
    }

    // Load timestamp setting
    result = txn.exec("SELECT value FROM metadata.config WHERE key = "
                      "'debug_show_timestamps'");
    if (!result.empty()) {
      std::string value = result[0][0].as<std::string>();
      showTimestamps = (value == "true");
    }

    // Load thread ID setting
    result = txn.exec(
        "SELECT value FROM metadata.config WHERE key = 'debug_show_thread_id'");
    if (!result.empty()) {
      std::string value = result[0][0].as<std::string>();
      showThreadId = (value == "true");
    }

    // Load file/line setting
    result = txn.exec(
        "SELECT value FROM metadata.config WHERE key = 'debug_show_file_line'");
    if (!result.empty()) {
      std::string value = result[0][0].as<std::string>();
      showFileLine = (value == "true");
    }

    txn.commit();
    return true;

  } catch (const pqxx::sql_error &e) {
    logConfigError("SQL error: " + std::string(e.what()) +
                   " [SQL State: " + e.sqlstate() + "]");
    return false;
  } catch (const std::exception &e) {
    logConfigError("Database error: " + std::string(e.what()));
    return false;
  }
}

void LogConfigManager::logConfigError(const std::string &message) {
  std::cerr << "[LogConfigManager] " << message << std::endl;
}
