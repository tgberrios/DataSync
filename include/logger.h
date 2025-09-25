#ifndef LOGGER_H
#define LOGGER_H

#include <chrono>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <pqxx/pqxx>
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

class Logger {
private:
  static std::ofstream logFile;
  static std::mutex logMutex;
  static std::string logFileName;
  static size_t messageCount;
  static const size_t MAX_MESSAGES_BEFORE_FLUSH = 100;
  static const size_t MAX_FILE_SIZE = 10 * 1024 * 1024; // 10MB
  static const int MAX_BACKUP_FILES = 5;

  // Debug configuration
  static LogLevel currentLogLevel;
  static bool showTimestamps;
  static bool showThreadId;
  static bool showFileLine;
  static std::mutex configMutex;

  static std::string getCurrentTimestamp() {
    auto now = std::chrono::system_clock::now();
    auto time_t = std::chrono::system_clock::to_time_t(now);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                  now.time_since_epoch()) %
              1000;

    std::stringstream ss;
    ss << std::put_time(std::localtime(&time_t), "%Y-%m-%d %H:%M:%S");
    ss << "." << std::setfill('0') << std::setw(3) << ms.count();
    return ss.str();
  }

  static std::string getLevelString(LogLevel level) {
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

  static std::string getCategoryString(LogCategory category) {
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

  static LogCategory stringToCategory(const std::string &categoryStr) {
    if (categoryStr == "SYSTEM")
      return LogCategory::SYSTEM;
    if (categoryStr == "DATABASE")
      return LogCategory::DATABASE;
    if (categoryStr == "TRANSFER")
      return LogCategory::TRANSFER;
    if (categoryStr == "CONFIG")
      return LogCategory::CONFIG;
    if (categoryStr == "VALIDATION")
      return LogCategory::VALIDATION;
    if (categoryStr == "MAINTENANCE")
      return LogCategory::MAINTENANCE;
    if (categoryStr == "MONITORING")
      return LogCategory::MONITORING;
    if (categoryStr == "DDL_EXPORT")
      return LogCategory::DDL_EXPORT;
    if (categoryStr == "METRICS")
      return LogCategory::METRICS;
    if (categoryStr == "GOVERNANCE")
      return LogCategory::GOVERNANCE;
    if (categoryStr == "QUALITY")
      return LogCategory::QUALITY;
    return LogCategory::UNKNOWN;
  }

  static void rotateLogFile() {
    if (logFile.is_open()) {
      logFile.close();
    }

    // Rotar archivos existentes
    for (int i = MAX_BACKUP_FILES - 1; i > 0; --i) {
      std::string oldFile = logFileName + "." + std::to_string(i);
      std::string newFile = logFileName + "." + std::to_string(i + 1);

      if (std::filesystem::exists(oldFile)) {
        if (i == MAX_BACKUP_FILES - 1) {
          std::filesystem::remove(oldFile);
        } else {
          std::filesystem::rename(oldFile, newFile);
        }
      }
    }

    // Mover archivo actual a .1
    if (std::filesystem::exists(logFileName)) {
      std::filesystem::rename(logFileName, logFileName + ".1");
    }

    // Abrir nuevo archivo
    logFile.open(logFileName, std::ios::app);
  }

  static void checkFileSize() {
    if (std::filesystem::exists(logFileName)) {
      auto fileSize = std::filesystem::file_size(logFileName);
      if (fileSize >= MAX_FILE_SIZE) {
        rotateLogFile();
      }
    }
  }

  static void writeLog(LogLevel level, LogCategory category,
                       const std::string &function,
                       const std::string &message) {
    std::lock_guard<std::mutex> lock(logMutex);

    // Check if this log level should be written
    if (level < currentLogLevel) {
      return;
    }

    if (!logFile.is_open()) {
      logFile.open(logFileName, std::ios::app);
    }

    checkFileSize();

    std::string timestamp = getCurrentTimestamp();
    std::string levelStr = getLevelString(level);
    std::string categoryStr = getCategoryString(category);

    logFile << "[" << timestamp << "] [" << levelStr << "] [" << categoryStr
            << "]";
    if (!function.empty()) {
      logFile << " [" << function << "]";
    }
    logFile << " " << message << std::endl;

    messageCount++;
    if (messageCount >= MAX_MESSAGES_BEFORE_FLUSH) {
      logFile.flush();
      messageCount = 0;
    }
  }

  static LogLevel stringToLogLevel(const std::string &levelStr) {
    if (levelStr == "DEBUG")
      return LogLevel::DEBUG;
    if (levelStr == "INFO")
      return LogLevel::INFO;
    if (levelStr == "WARN" || levelStr == "WARNING")
      return LogLevel::WARNING;
    if (levelStr == "ERROR")
      return LogLevel::ERROR;
    if (levelStr == "FATAL" || levelStr == "CRITICAL")
      return LogLevel::CRITICAL;
    return LogLevel::INFO; // Default
  }

public:
  static void initialize(const std::string &fileName = "DataSync.log");

  static void shutdown() {
    std::lock_guard<std::mutex> lock(logMutex);
    if (logFile.is_open()) {
      logFile.flush();
      logFile.close();
    }
  }

  // Convenience methods with categories
  static void debug(const std::string &message) {
    writeLog(LogLevel::DEBUG, LogCategory::SYSTEM, "", message);
  }

  static void info(const std::string &message) {
    writeLog(LogLevel::INFO, LogCategory::SYSTEM, "", message);
  }

  static void warning(const std::string &message) {
    writeLog(LogLevel::WARNING, LogCategory::SYSTEM, "", message);
  }

  static void error(const std::string &message) {
    writeLog(LogLevel::ERROR, LogCategory::SYSTEM, "", message);
  }

  static void critical(const std::string &message) {
    writeLog(LogLevel::CRITICAL, LogCategory::SYSTEM, "", message);
  }

  static void debug(const std::string &function, const std::string &message) {
    writeLog(LogLevel::DEBUG, LogCategory::SYSTEM, function, message);
  }

  static void info(const std::string &function, const std::string &message) {
    writeLog(LogLevel::INFO, LogCategory::SYSTEM, function, message);
  }

  static void warning(const std::string &function, const std::string &message) {
    writeLog(LogLevel::WARNING, LogCategory::SYSTEM, function, message);
  }

  static void error(const std::string &function, const std::string &message) {
    writeLog(LogLevel::ERROR, LogCategory::SYSTEM, function, message);
  }

  static void critical(const std::string &function,
                       const std::string &message) {
    writeLog(LogLevel::CRITICAL, LogCategory::SYSTEM, function, message);
  }

  // Categorized logging methods
  static void debug(LogCategory category, const std::string &message) {
    writeLog(LogLevel::DEBUG, category, "", message);
  }

  static void info(LogCategory category, const std::string &message) {
    writeLog(LogLevel::INFO, category, "", message);
  }

  static void warning(LogCategory category, const std::string &message) {
    writeLog(LogLevel::WARNING, category, "", message);
  }

  static void error(LogCategory category, const std::string &message) {
    writeLog(LogLevel::ERROR, category, "", message);
  }

  static void critical(LogCategory category, const std::string &message) {
    writeLog(LogLevel::CRITICAL, category, "", message);
  }

  static void debug(LogCategory category, const std::string &function,
                    const std::string &message) {
    writeLog(LogLevel::DEBUG, category, function, message);
  }

  static void info(LogCategory category, const std::string &function,
                   const std::string &message) {
    writeLog(LogLevel::INFO, category, function, message);
  }

  static void warning(LogCategory category, const std::string &function,
                      const std::string &message) {
    writeLog(LogLevel::WARNING, category, function, message);
  }

  static void error(LogCategory category, const std::string &function,
                    const std::string &message) {
    writeLog(LogLevel::ERROR, category, function, message);
  }

  static void critical(LogCategory category, const std::string &function,
                       const std::string &message) {
    writeLog(LogLevel::CRITICAL, category, function, message);
  }

  // Generic logging methods
  static void log(LogLevel level, LogCategory category,
                  const std::string &message) {
    writeLog(level, category, "", message);
  }

  static void log(LogLevel level, LogCategory category,
                  const std::string &function, const std::string &message) {
    writeLog(level, category, function, message);
  }

  // Configuration management
  static void loadDebugConfig();
  static void setLogLevel(LogLevel level);
  static void setLogLevel(const std::string &levelStr);
  static LogLevel getCurrentLogLevel();
  static void refreshConfig();
};

// Declaración de variables estáticas (definidas en logger.cpp)

#endif
