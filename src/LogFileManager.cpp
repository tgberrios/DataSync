#include "LogFileManager.h"
#include <algorithm>
#include <iostream>

LogFileManager::LogFileManager(const std::string &logFileName)
    : logFileName(logFileName), errorFileName(logFileName), logFile(),
      errorFile(), logMutex(), errorMutex(),
      maxFileSize(10 * 1024 * 1024), // 10MB
      maxBackupFiles(5), maxMessagesBeforeFlush(100), messageCount(0),
      errorCount(0) {

  // Generate error file name
  std::filesystem::path logPath(logFileName);
  std::filesystem::path errorPath =
      logPath.parent_path() /
      (logPath.stem().string() + "_Errors" + logPath.extension().string());
  errorFileName = errorPath.string();
}

LogFileManager::~LogFileManager() { shutdown(); }

bool LogFileManager::initialize() {
  std::lock_guard<std::mutex> logLock(logMutex);
  std::lock_guard<std::mutex> errorLock(errorMutex);

  // Create log directory if it doesn't exist
  if (!createLogDirectory()) {
    return false;
  }

  // Open log files
  if (!openLogFile()) {
    return false;
  }

  if (!openErrorFile()) {
    closeLogFile();
    return false;
  }

  messageCount = 0;
  errorCount = 0;

  return true;
}

void LogFileManager::shutdown() {
  {
    std::lock_guard<std::mutex> lock(logMutex);
    closeLogFile();
    messageCount = 0;
  }

  {
    std::lock_guard<std::mutex> lock(errorMutex);
    closeErrorFile();
    errorCount = 0;
  }
}

bool LogFileManager::writeLog(const std::string &message) {
  std::lock_guard<std::mutex> lock(logMutex);

  if (!logFile.is_open()) {
    if (!openLogFile()) {
      return false;
    }
  }

  // Check file size before writing
  if (!checkFileSize()) {
    return false;
  }

  logFile << message << std::endl;

  if (!logFile.good()) {
    std::cerr << "[LogFileManager] Failed to write to log file" << std::endl;
    closeLogFile();
    return false;
  }

  messageCount++;
  if (messageCount >= maxMessagesBeforeFlush) {
    logFile.flush();
    messageCount = 0;
  }

  return true;
}

bool LogFileManager::writeError(const std::string &message) {
  std::lock_guard<std::mutex> lock(errorMutex);

  if (!errorFile.is_open()) {
    if (!openErrorFile()) {
      return false;
    }
  }

  // Check file size before writing
  if (!checkErrorFileSize()) {
    return false;
  }

  errorFile << message << std::endl;

  if (!errorFile.good()) {
    std::cerr << "[LogFileManager] Failed to write to error file" << std::endl;
    closeErrorFile();
    return false;
  }

  errorCount++;
  if (errorCount >= maxMessagesBeforeFlush) {
    errorFile.flush();
    errorCount = 0;
  }

  return true;
}

void LogFileManager::rotateLogFile() {
  std::lock_guard<std::mutex> lock(logMutex);
  performLogRotation();
}

void LogFileManager::rotateErrorFile() {
  std::lock_guard<std::mutex> lock(errorMutex);
  performErrorRotation();
}

bool LogFileManager::checkFileSize() {
  if (std::filesystem::exists(logFileName)) {
    auto fileSize = std::filesystem::file_size(logFileName);
    if (fileSize >= maxFileSize) {
      performLogRotation();
      return true;
    }
  }
  return true;
}

bool LogFileManager::checkErrorFileSize() {
  if (std::filesystem::exists(errorFileName)) {
    auto fileSize = std::filesystem::file_size(errorFileName);
    if (fileSize >= maxFileSize) {
      performErrorRotation();
      return true;
    }
  }
  return true;
}

void LogFileManager::setLogFileName(const std::string &fileName) {
  std::lock_guard<std::mutex> logLock(logMutex);
  std::lock_guard<std::mutex> errorLock(errorMutex);

  logFileName = fileName;

  // Generate error file name
  std::filesystem::path logPath(fileName);
  std::filesystem::path errorPath =
      logPath.parent_path() /
      (logPath.stem().string() + "_Errors" + logPath.extension().string());
  errorFileName = errorPath.string();
}

void LogFileManager::setMaxFileSize(size_t maxSize) { maxFileSize = maxSize; }

void LogFileManager::setMaxBackupFiles(int maxFiles) {
  maxBackupFiles = std::max(1, maxFiles);
}

void LogFileManager::setMaxMessagesBeforeFlush(size_t maxMessages) {
  maxMessagesBeforeFlush = std::max(1UL, maxMessages);
}

bool LogFileManager::openLogFile() {
  logFile.open(logFileName, std::ios::app);
  if (!logFile.is_open()) {
    std::cerr << "[LogFileManager] Cannot open log file: " << logFileName
              << std::endl;
    return false;
  }
  return true;
}

bool LogFileManager::openErrorFile() {
  errorFile.open(errorFileName, std::ios::app);
  if (!errorFile.is_open()) {
    std::cerr << "[LogFileManager] Cannot open error file: " << errorFileName
              << std::endl;
    return false;
  }
  return true;
}

void LogFileManager::closeLogFile() {
  if (logFile.is_open()) {
    logFile.flush();
    logFile.close();
  }
}

void LogFileManager::closeErrorFile() {
  if (errorFile.is_open()) {
    errorFile.flush();
    errorFile.close();
  }
}

bool LogFileManager::createLogDirectory() {
  std::filesystem::path logPath(logFileName);
  std::filesystem::path parentDir = logPath.parent_path();

  if (!parentDir.empty() && !std::filesystem::exists(parentDir)) {
    try {
      std::filesystem::create_directories(parentDir);
    } catch (const std::filesystem::filesystem_error &e) {
      std::cerr << "[LogFileManager] Failed to create log directory: "
                << parentDir.string() << " - " << e.what() << std::endl;
      return false;
    }
  }

  return true;
}

void LogFileManager::performLogRotation() {
  closeLogFile();

  // Rotate backup files
  for (int i = maxBackupFiles - 1; i > 0; --i) {
    std::string oldFile = logFileName + "." + std::to_string(i);
    std::string newFile = logFileName + "." + std::to_string(i + 1);

    if (std::filesystem::exists(oldFile)) {
      if (i == maxBackupFiles - 1) {
        std::filesystem::remove(oldFile);
      } else {
        std::filesystem::rename(oldFile, newFile);
      }
    }
  }

  // Move current file to backup
  if (std::filesystem::exists(logFileName)) {
    std::filesystem::rename(logFileName, logFileName + ".1");
  }

  // Open new log file
  openLogFile();
}

void LogFileManager::performErrorRotation() {
  closeErrorFile();

  // Rotate backup files
  for (int i = maxBackupFiles - 1; i > 0; --i) {
    std::string oldFile = errorFileName + "." + std::to_string(i);
    std::string newFile = errorFileName + "." + std::to_string(i + 1);

    if (std::filesystem::exists(oldFile)) {
      if (i == maxBackupFiles - 1) {
        std::filesystem::remove(oldFile);
      } else {
        std::filesystem::rename(oldFile, newFile);
      }
    }
  }

  // Move current file to backup
  if (std::filesystem::exists(errorFileName)) {
    std::filesystem::rename(errorFileName, errorFileName + ".1");
  }

  // Open new error file
  openErrorFile();
}
