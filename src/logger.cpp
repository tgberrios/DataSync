#include "logger.h"
#include "Config.h"
#include <algorithm>

// Definición de variables estáticas
std::ofstream Logger::logFile;
std::mutex Logger::logMutex;
std::string Logger::logFileName = "DataSync.log";
size_t Logger::messageCount = 0;

// Debug configuration variables
LogLevel Logger::currentLogLevel = LogLevel::INFO;
bool Logger::showTimestamps = true;
bool Logger::showThreadId = false;
bool Logger::showFileLine = false;
std::mutex Logger::configMutex;

void Logger::loadDebugConfig() {
  std::lock_guard<std::mutex> lock(configMutex);

  try {
    // Validate connection string
    std::string connStr = DatabaseConfig::getPostgresConnectionString();
    if (connStr.empty()) {
      std::cerr << "Logger::loadDebugConfig: Empty database connection string"
                << std::endl;
      setDefaultConfig();
      return;
    }

    // Create connection with timeout
    pqxx::connection conn(connStr);
    if (!conn.is_open()) {
      std::cerr << "Logger::loadDebugConfig: Failed to connect to database"
                << std::endl;
      setDefaultConfig();
      return;
    }

    // Test connection with a simple query
    try {
      pqxx::work testTxn(conn);
      testTxn.exec("SELECT 1");
      testTxn.commit();
    } catch (const std::exception &e) {
      std::cerr << "Logger::loadDebugConfig: Database connection test failed: "
                << e.what() << std::endl;
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

    // Log successful config load
    if (logFile.is_open()) {
      logFile << "-- Debug configuration loaded from database successfully"
              << std::endl;
    }

  } catch (const pqxx::sql_error &e) {
    std::cerr << "Logger::loadDebugConfig: SQL error: " << e.what()
              << " [SQL State: " << e.sqlstate() << "]" << std::endl;
    setDefaultConfig();
  } catch (const std::exception &e) {
    std::cerr << "Logger::loadDebugConfig: Database error: " << e.what()
              << std::endl;
    setDefaultConfig();
  }
}

void Logger::setDefaultConfig() {
  std::cerr << "Logger::loadDebugConfig: Using default configuration (database "
               "unavailable)"
            << std::endl;
  currentLogLevel = LogLevel::INFO; // Use INFO as default for production
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
    std::cerr << "Logger::setLogLevel: Empty log level string provided"
              << std::endl;
    return;
  }

  // Validate the log level string before converting
  std::string upperLevelStr = levelStr;
  std::transform(upperLevelStr.begin(), upperLevelStr.end(),
                 upperLevelStr.begin(), ::toupper);

  if (upperLevelStr != "DEBUG" && upperLevelStr != "INFO" &&
      upperLevelStr != "WARN" && upperLevelStr != "WARNING" &&
      upperLevelStr != "ERROR" && upperLevelStr != "FATAL" &&
      upperLevelStr != "CRITICAL") {
    std::cerr << "Logger::setLogLevel: Invalid log level string: " << levelStr
              << std::endl;
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

  // Use proper filesystem path handling for cross-platform compatibility
  std::filesystem::path executablePath = std::filesystem::current_path();
  std::filesystem::path logPath = executablePath / fileName;
  logFileName = logPath.string();

  // Validate file path and permissions
  std::filesystem::path parentDir = logPath.parent_path();

  // Ensure parent directory exists
  if (!parentDir.empty() && !std::filesystem::exists(parentDir)) {
    try {
      std::filesystem::create_directories(parentDir);
    } catch (const std::filesystem::filesystem_error &e) {
      std::cerr << "Logger::initialize: Failed to create log directory: "
                << parentDir.string() << " - " << e.what() << std::endl;
      return;
    }
  }

  // Check if we can write to the directory
  if (!parentDir.empty() && !std::filesystem::exists(parentDir)) {
    std::cerr << "Logger::initialize: Log directory does not exist and could "
                 "not be created: "
              << parentDir.string() << std::endl;
    return;
  }

  // Try to open the log file
  logFile.open(logFileName, std::ios::app);
  if (!logFile.is_open()) {
    std::cerr << "Logger::initialize: Failed to open log file: " << logFileName
              << std::endl;
    std::cerr << "Logger::initialize: Check file permissions and disk space"
              << std::endl;
    return;
  }

  // Test write permissions
  logFile << "-- Logger initialized at "
          << std::chrono::system_clock::now().time_since_epoch().count()
          << std::endl;
  if (!logFile.good()) {
    std::cerr << "Logger::initialize: Failed to write to log file: "
              << logFileName << std::endl;
    logFile.close();
    return;
  }

  messageCount = 0;

  // Load debug configuration from database
  loadDebugConfig();
}
