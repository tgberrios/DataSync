#ifndef LOGGER_NEW_H
#define LOGGER_NEW_H

#include "LogConfigManager.h"
#include "LogFileManager.h"
#include "LogFormatter.h"
#include <memory>
#include <mutex>

class Logger {
public:
  // Singleton pattern
  static Logger &getInstance();

  // Initialization and shutdown
  void initialize(const std::string &fileName = "DataSync.log");
  void shutdown();

  // Core logging methods
  void log(LogLevel level, LogCategory category, const std::string &message);
  void log(LogLevel level, LogCategory category, const std::string &function,
           const std::string &message);

  // Convenience methods
  void debug(LogCategory category, const std::string &message);
  void info(LogCategory category, const std::string &message);
  void warning(LogCategory category, const std::string &message);
  void error(LogCategory category, const std::string &message);
  void critical(LogCategory category, const std::string &message);

  void debug(LogCategory category, const std::string &function,
             const std::string &message);
  void info(LogCategory category, const std::string &function,
            const std::string &message);
  void warning(LogCategory category, const std::string &function,
               const std::string &message);
  void error(LogCategory category, const std::string &function,
             const std::string &message);
  void critical(LogCategory category, const std::string &function,
                const std::string &message);

  // Legacy compatibility methods (for backward compatibility)
  void debug(const std::string &message);
  void info(const std::string &message);
  void warning(const std::string &message);
  void error(const std::string &message);
  void critical(const std::string &message);

  void debug(const std::string &function, const std::string &message);
  void info(const std::string &function, const std::string &message);
  void warning(const std::string &function, const std::string &message);
  void error(const std::string &function, const std::string &message);
  void critical(const std::string &function, const std::string &message);

  // Configuration methods
  void setLogLevel(LogLevel level);
  void setLogLevel(const std::string &levelStr);
  LogLevel getCurrentLogLevel() const;
  void refreshConfig();

  // File management
  void setLogFileName(const std::string &fileName);
  void setMaxFileSize(size_t maxSize);
  void setMaxBackupFiles(int maxFiles);
  void setMaxMessagesBeforeFlush(size_t maxMessages);

private:
  Logger() = default;
  ~Logger() = default;
  Logger(const Logger &) = delete;
  Logger &operator=(const Logger &) = delete;

  // Core components
  std::unique_ptr<LogFormatter> formatter;
  std::unique_ptr<LogFileManager> fileManager;
  std::unique_ptr<LogConfigManager> configManager;

  // Thread safety
  mutable std::mutex logMutex;

  // Helper methods
  bool shouldLog(LogLevel level) const;
  void writeLog(LogLevel level, LogCategory category,
                const std::string &function, const std::string &message);
  void writeErrorLog(LogLevel level, LogCategory category,
                     const std::string &function, const std::string &message);
};

#endif // LOGGER_NEW_H
