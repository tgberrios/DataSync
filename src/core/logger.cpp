#include "core/logger.h"
#include "core/Config.h"
#include <algorithm>

// Static member initialization for Logger class. dbWriter_ holds the database
// log writer instance, logMutex provides thread safety for logging operations.
std::unique_ptr<DatabaseLogWriter> Logger::dbWriter_;
std::mutex Logger::logMutex;

// Debug configuration variables. These control the logging behavior and can
// be configured via the metadata.config table in the database. currentLogLevel
// determines the minimum log level that will be written, showTimestamps
// controls whether timestamps are included, showThreadId controls thread ID
// display, and showFileLine controls file/line number display.
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

// Loads debug configuration settings from the metadata.config table in the
// PostgreSQL database. This function connects to the database, queries for
// configuration keys (debug_level, debug_show_timestamps, debug_show_thread_id,
// debug_show_file_line), and updates the static configuration variables. If
// the database connection fails or any error occurs, the function falls back
// to default configuration values. This function is called during Logger
// initialization and can be called again via refreshConfig() to reload
// settings.
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
    } catch (const std::exception &) {
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

// Sets the logger configuration to default values. This function is called
// when database configuration loading fails or when explicit defaults are
// needed. Defaults: LogLevel::INFO, showTimestamps=true, showThreadId=false,
// showFileLine=false.
void Logger::setDefaultConfig() {
  currentLogLevel = LogLevel::INFO;
  showTimestamps = true;
  showThreadId = false;
  showFileLine = false;
}

// Sets the current log level to the specified level. Only log messages at or
// above this level will be written. This function is thread-safe and updates
// the global log level immediately. Lower levels (DEBUG=0) are more verbose,
// higher levels (CRITICAL=4) are less verbose.
void Logger::setLogLevel(LogLevel level) {
  std::lock_guard<std::mutex> lock(configMutex);
  currentLogLevel = level;
}

// Sets the current log level from a string representation. The function accepts
// "DEBUG", "INFO", "WARN"/"WARNING", "ERROR", "FATAL"/"CRITICAL"
// (case-insensitive). If the string is empty or does not match a valid log
// level, the function returns without making changes. This function is
// thread-safe.
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

// Returns the current log level setting. This function is thread-safe and
// returns the minimum log level that will be written. Log messages below this
// level are filtered out and not written.
LogLevel Logger::getCurrentLogLevel() {
  std::lock_guard<std::mutex> lock(configMutex);
  return currentLogLevel;
}

// Reloads the debug configuration from the database. This function calls
// loadDebugConfig() to refresh all configuration settings (log level,
// timestamp display, etc.) from the metadata.config table. Useful for
// applying configuration changes without restarting the application.
void Logger::refreshConfig() { loadDebugConfig(); }

// Initializes the Logger system. This function must be called before using
// any logging functionality. It loads debug configuration from the database
// and initializes the database log writer if a PostgreSQL connection string
// is available. If database initialization fails, logging will continue to
// work but database logging will be disabled. Errors during initialization
// are logged to stderr since the logger may not be fully initialized yet.
void Logger::initialize() {
  std::lock_guard<std::mutex> lock(logMutex);

  loadDebugConfig();

  try {
    std::string connStr = DatabaseConfig::getPostgresConnectionString();
    if (!connStr.empty()) {
      dbWriter_ = std::make_unique<DatabaseLogWriter>(connStr);
      if (!dbWriter_->isEnabled()) {
        std::cerr << "Warning: Database log writer initialization failed. "
                     "Logging to database will be disabled."
                  << std::endl;
      }
    }
  } catch (const std::exception &e) {
    std::cerr << "Error initializing database log writer: " << e.what()
              << std::endl;
  }
}
