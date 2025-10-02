#ifndef CONFIGMANAGER_H
#define CONFIGMANAGER_H

#include "Config.h"
#include "logger.h"
#include <mutex>
#include <pqxx/pqxx>
#include <string>

class ConfigManager {
public:
  ConfigManager() = default;
  ~ConfigManager() = default;

  // Configuration management
  void loadFromDatabase(pqxx::connection &conn);
  void refreshConfig();

  // Configuration access
  size_t getChunkSize() const;
  size_t getSyncInterval() const;

  // Configuration validation
  bool isValidChunkSize(size_t size) const;
  bool isValidSyncInterval(size_t interval) const;

private:
  mutable std::mutex configMutex;

  // Configuration values
  size_t chunkSize{1000};
  size_t syncInterval{60};

  // Helper methods
  void updateChunkSize(size_t newSize);
  void updateSyncInterval(size_t newInterval);
  void validateAndSetConfig(const std::string &key, const std::string &value);
  void logConfigChange(const std::string &key, const std::string &oldValue,
                       const std::string &newValue);
};

#endif // CONFIGMANAGER_H
