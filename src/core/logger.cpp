#include "core/logger.h"
#include "core/Config.h"
#include <algorithm>

std::unique_ptr<FileLogWriter> Logger::fileWriter_;
std::unique_ptr<FileLogWriter> Logger::errorWriter_;
std::unique_ptr<DatabaseLogWriter> Logger::dbWriter_;
std::mutex Logger::logMutex;
size_t Logger::messageCount = 0;

// Debug configuration variables
LogLevel Logger::currentLogLevel = LogLevel::INFO;
bool Logger::showTimestamps = true;
bool Logger::showThreadId = false;
bool Logger::showFileLine = false;
std::mutex Logger::configMutex;

const std::unordered_map<std::string, LogCategory> Logger::categoryMap = {
    {"SYSTEM", LogCategory::SYSTEM},
    {"DATABASE", LogCategory::DATABASE},
    {"TRANSFER", LogCategory::TRANSFER},
    {"CONFIG", LogCategory::CONFIG},
    {"VALIDATION", LogCategory::VALIDATION},
    {"MAINTENANCE", LogCategory::MAINTENANCE},
    {"MONITORING", LogCategory::MONITORING},
    {"DDL_EXPORT", LogCategory::DDL_EXPORT},
    {"METRICS", LogCategory::METRICS},
    {"GOVERNANCE", LogCategory::GOVERNANCE},
    {"QUALITY", LogCategory::QUALITY}};

const std::unordered_map<std::string, LogLevel> Logger::levelMap = {
    {"DEBUG", LogLevel::DEBUG},      {"INFO", LogLevel::INFO},
    {"WARN", LogLevel::WARNING},     {"WARNING", LogLevel::WARNING},
    {"ERROR", LogLevel::ERROR},      {"FATAL", LogLevel::CRITICAL},
    {"CRITICAL", LogLevel::CRITICAL}};

void Logger::loadDebugConfig() {
  std::lock_guard<std::mutex> lock(configMutex);

  try {
    std::string connStr = DatabaseConfig::getPostgresConnectionString();
    if (connStr.empty()) {
      setDefaultConfig();
      return;
    }

    pqxx::connection conn(connStr);
    if (!conn.is_open()) {
      setDefaultConfig();
      return;
    }

    try {
      pqxx::work testTxn(conn);
      testTxn.exec("SELECT 1");
      testTxn.commit();
    } catch (const std::exception &e) {
      setDefaultConfig();
      return;
    }

    pqxx::work txn(conn);

    // Load debug level with validation
    auto result =
        txn.exec("SELECT value FROM metadata.config WHERE key = 'debug_level'");
    if (!result.empty()) {
      std::string levelStr = result[0][0].as<std::string>();
      if (!levelStr.empty()) {
        LogLevel newLevel = stringToLogLevel(levelStr);
        currentLogLevel = newLevel;
      }
    }

    // Load timestamp setting with validation
    result = txn.exec("SELECT value FROM metadata.config WHERE key = "
                      "'debug_show_timestamps'");
    if (!result.empty()) {
      std::string value = result[0][0].as<std::string>();
      showTimestamps = (value == "true");
    }

    // Load thread ID setting with validation
    result = txn.exec(
        "SELECT value FROM metadata.config WHERE key = 'debug_show_thread_id'");
    if (!result.empty()) {
      std::string value = result[0][0].as<std::string>();
      showThreadId = (value == "true");
    }

    // Load file/line setting with validation
    result = txn.exec(
        "SELECT value FROM metadata.config WHERE key = 'debug_show_file_line'");
    if (!result.empty()) {
      std::string value = result[0][0].as<std::string>();
      showFileLine = (value == "true");
    }

    txn.commit();

    // Removed debug config load log to reduce noise

  } catch (const pqxx::sql_error &e) {
    setDefaultConfig();
  } catch (const std::exception &e) {
    setDefaultConfig();
  }
}

void Logger::setDefaultConfig() {
  currentLogLevel = LogLevel::INFO;
  showTimestamps = true;
  showThreadId = false;
  showFileLine = false;

  if (fileWriter_ && fileWriter_->isOpen()) {
    fileWriter_->write(
        "-- Using default debug configuration (database unavailable)");
  }
}

void Logger::setLogLevel(LogLevel level) {
  std::lock_guard<std::mutex> lock(configMutex);
  currentLogLevel = level;
}

void Logger::setLogLevel(const std::string &levelStr) {
  if (levelStr.empty()) {
    return;
  }

  std::string upperLevelStr = levelStr;
  std::transform(upperLevelStr.begin(), upperLevelStr.end(),
                 upperLevelStr.begin(), ::toupper);

  if (upperLevelStr != "DEBUG" && upperLevelStr != "INFO" &&
      upperLevelStr != "WARN" && upperLevelStr != "WARNING" &&
      upperLevelStr != "ERROR" && upperLevelStr != "FATAL" &&
      upperLevelStr != "CRITICAL") {
    return;
  }

  setLogLevel(stringToLogLevel(levelStr));
}

LogLevel Logger::getCurrentLogLevel() {
  std::lock_guard<std::mutex> lock(configMutex);
  return currentLogLevel;
}

void Logger::refreshConfig() { loadDebugConfig(); }

void Logger::initialize(const std::string &fileName) {
  std::lock_guard<std::mutex> lock(logMutex);

  messageCount = 0;
  fileWriter_ = std::make_unique<FileLogWriter>(fileName);

  std::string errorFileName =
      fileName.substr(0, fileName.find_last_of('.')) + "Errors.log";
  errorWriter_ = std::make_unique<FileLogWriter>(errorFileName);

  loadDebugConfig();

  try {
    std::string connStr = DatabaseConfig::getPostgresConnectionString();
    if (!connStr.empty()) {
      dbWriter_ = std::make_unique<DatabaseLogWriter>(connStr);
    }
  } catch (const std::exception &) {
  }
}
