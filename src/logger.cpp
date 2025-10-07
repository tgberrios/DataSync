#include "logger.h"
#include "Config.h"
#include <algorithm>

// Definición de variables estáticas
std::ofstream Logger::logFile;
std::ofstream Logger::errorLogFile;
std::mutex Logger::logMutex;
std::string Logger::logFileName = "DataSync.log";
std::string Logger::errorLogFileName = "DataSyncErrors.log";
size_t Logger::messageCount = 0;
std::unique_ptr<pqxx::connection> Logger::dbConn;
bool Logger::dbLoggingEnabled = false;
bool Logger::dbStatementPrepared = false;

// Debug configuration variables
LogLevel Logger::currentLogLevel = LogLevel::INFO;
bool Logger::showTimestamps = true;
bool Logger::showThreadId = false;
bool Logger::showFileLine = false;
std::mutex Logger::configMutex;

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

  if (logFile.is_open()) {
    logFile << "-- Using default debug configuration (database unavailable)"
            << std::endl;
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

// Update the initialize function to load debug config
void Logger::initialize(const std::string &fileName) {
  std::lock_guard<std::mutex> lock(logMutex);

  messageCount = 0;
  logFileName = fileName;

  if (!logFile.is_open()) {
    logFile.open(logFileName, std::ios::app);
  }

  loadDebugConfig();

  try {
    std::string connStr = DatabaseConfig::getPostgresConnectionString();
    if (!connStr.empty()) {
      dbConn = std::make_unique<pqxx::connection>(connStr);
      if (dbConn && dbConn->is_open()) {
        dbLoggingEnabled = true;
        dbStatementPrepared = false;
      }
    }
  } catch (const std::exception &) {
    dbLoggingEnabled = false;
    dbStatementPrepared = false;
    dbConn.reset();
  }
}
