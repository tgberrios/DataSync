#include "logger.h"
#include <iostream>

Logger &Logger::getInstance() {
  static Logger instance;
  return instance;
}

void Logger::initialize(const std::string &fileName) {
  std::lock_guard<std::mutex> lock(logMutex);

  // Initialize components
  formatter = std::make_unique<LogFormatter>();
  fileManager = std::make_unique<LogFileManager>(fileName);
  configManager = std::make_unique<LogConfigManager>();

  // Load configuration
  configManager->loadFromDatabase();

  // Initialize file manager
  if (!fileManager->initialize()) {
    std::cerr << "[Logger] Failed to initialize file manager" << std::endl;
    return;
  }
}

void Logger::shutdown() {
  std::lock_guard<std::mutex> lock(logMutex);

  if (fileManager) {
    fileManager->shutdown();
  }

  formatter.reset();
  fileManager.reset();
  configManager.reset();
}

void Logger::log(LogLevel level, LogCategory category,
                 const std::string &message) {
  log(level, category, "", message);
}

void Logger::log(LogLevel level, LogCategory category,
                 const std::string &function, const std::string &message) {
  if (!shouldLog(level)) {
    return;
  }

  writeLog(level, category, function, message);

  // Also write to error file for error and critical levels
  if (level == LogLevel::ERROR || level == LogLevel::CRITICAL) {
    writeErrorLog(level, category, function, message);
  }
}

void Logger::debug(LogCategory category, const std::string &message) {
  log(LogLevel::DEBUG, category, message);
}

void Logger::info(LogCategory category, const std::string &message) {
  log(LogLevel::INFO, category, message);
}

void Logger::warning(LogCategory category, const std::string &message) {
  log(LogLevel::WARNING, category, message);
}

void Logger::error(LogCategory category, const std::string &message) {
  log(LogLevel::ERROR, category, message);
}

void Logger::critical(LogCategory category, const std::string &message) {
  log(LogLevel::CRITICAL, category, message);
}

void Logger::debug(LogCategory category, const std::string &function,
                   const std::string &message) {
  log(LogLevel::DEBUG, category, function, message);
}

void Logger::info(LogCategory category, const std::string &function,
                  const std::string &message) {
  log(LogLevel::INFO, category, function, message);
}

void Logger::warning(LogCategory category, const std::string &function,
                     const std::string &message) {
  log(LogLevel::WARNING, category, function, message);
}

void Logger::error(LogCategory category, const std::string &function,
                   const std::string &message) {
  log(LogLevel::ERROR, category, function, message);
}

void Logger::critical(LogCategory category, const std::string &function,
                      const std::string &message) {
  log(LogLevel::CRITICAL, category, function, message);
}

// Legacy compatibility methods
void Logger::debug(const std::string &message) {
  log(LogLevel::DEBUG, LogCategory::SYSTEM, message);
}

void Logger::info(const std::string &message) {
  log(LogLevel::INFO, LogCategory::SYSTEM, message);
}

void Logger::warning(const std::string &message) {
  log(LogLevel::WARNING, LogCategory::SYSTEM, message);
}

void Logger::error(const std::string &message) {
  log(LogLevel::ERROR, LogCategory::SYSTEM, message);
}

void Logger::critical(const std::string &message) {
  log(LogLevel::CRITICAL, LogCategory::SYSTEM, message);
}

void Logger::debug(const std::string &function, const std::string &message) {
  log(LogLevel::DEBUG, LogCategory::SYSTEM, function, message);
}

void Logger::info(const std::string &function, const std::string &message) {
  log(LogLevel::INFO, LogCategory::SYSTEM, function, message);
}

void Logger::warning(const std::string &function, const std::string &message) {
  log(LogLevel::WARNING, LogCategory::SYSTEM, function, message);
}

void Logger::error(const std::string &function, const std::string &message) {
  log(LogLevel::ERROR, LogCategory::SYSTEM, function, message);
}

void Logger::critical(const std::string &function, const std::string &message) {
  log(LogLevel::CRITICAL, LogCategory::SYSTEM, function, message);
}

void Logger::setLogLevel(LogLevel level) {
  if (configManager) {
    configManager->setLogLevel(level);
  }
}

void Logger::setLogLevel(const std::string &levelStr) {
  if (configManager) {
    configManager->setLogLevel(levelStr);
  }
}

LogLevel Logger::getCurrentLogLevel() const {
  if (configManager) {
    return configManager->getCurrentLogLevel();
  }
  return LogLevel::INFO;
}

void Logger::refreshConfig() {
  if (configManager) {
    configManager->refreshConfig();
  }
}

void Logger::setLogFileName(const std::string &fileName) {
  if (fileManager) {
    fileManager->setLogFileName(fileName);
  }
}

void Logger::setMaxFileSize(size_t maxSize) {
  if (fileManager) {
    fileManager->setMaxFileSize(maxSize);
  }
}

void Logger::setMaxBackupFiles(int maxFiles) {
  if (fileManager) {
    fileManager->setMaxBackupFiles(maxFiles);
  }
}

void Logger::setMaxMessagesBeforeFlush(size_t maxMessages) {
  if (fileManager) {
    fileManager->setMaxMessagesBeforeFlush(maxMessages);
  }
}

bool Logger::shouldLog(LogLevel level) const {
  if (!configManager) {
    return true; // Default to logging if no config manager
  }

  return level >= configManager->getCurrentLogLevel();
}

void Logger::writeLog(LogLevel level, LogCategory category,
                      const std::string &function, const std::string &message) {
  if (!formatter || !fileManager) {
    return;
  }

  std::string formattedMessage =
      formatter->formatMessage(level, category, function, message);
  fileManager->writeLog(formattedMessage);
}

void Logger::writeErrorLog(LogLevel level, LogCategory category,
                           const std::string &function,
                           const std::string &message) {
  if (!formatter || !fileManager) {
    return;
  }

  std::string formattedMessage =
      formatter->formatMessage(level, category, function, message);
  fileManager->writeError(formattedMessage);
}