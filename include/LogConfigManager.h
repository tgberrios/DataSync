#ifndef LOGCONFIGMANAGER_H
#define LOGCONFIGMANAGER_H

#include "LogFormatter.h"
#include <mutex>
#include <string>

class LogConfigManager {
public:
  LogConfigManager();
  ~LogConfigManager() = default;

  // Configuration management
  void loadFromDatabase();
  void setDefaultConfig();
  void refreshConfig();

  // Log level management
  void setLogLevel(LogLevel level);
  void setLogLevel(const std::string &levelStr);
  LogLevel getCurrentLogLevel() const;

  // Debug settings
  void setShowTimestamps(bool show);
  void setShowThreadId(bool show);
  void setShowFileLine(bool show);

  bool getShowTimestamps() const;
  bool getShowThreadId() const;
  bool getShowFileLine() const;

  // Validation
  bool isValidLogLevel(const std::string &levelStr) const;
  bool isValidCategory(const std::string &categoryStr) const;

private:
  LogLevel currentLogLevel;
  bool showTimestamps;
  bool showThreadId;
  bool showFileLine;
  mutable std::mutex configMutex;

  // Helper methods
  void loadDebugConfig();
  void setDefaultValues();
  bool loadConfigFromDatabase();
  void logConfigError(const std::string &message);
};

#endif // LOGCONFIGMANAGER_H
