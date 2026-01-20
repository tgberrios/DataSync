#ifndef EVENT_TRIGGER_MANAGER_H
#define EVENT_TRIGGER_MANAGER_H

#include "catalog/workflow_repository.h"
#include "core/logger.h"
#include <string>
#include <map>
#include <vector>
#include <functional>
#include <thread>
#include <atomic>
#include <mutex>
#include <filesystem>

class EventTriggerManager {
public:
  enum class EventType {
    FILE_ARRIVAL,
    API_CALL,
    DATABASE_CHANGE,
    SCHEDULE,
    MANUAL
  };

  struct EventTrigger {
    std::string workflow_name;
    EventType event_type;
    std::string event_config;
    bool active;
  };

  static EventTriggerManager& getInstance();
  
  void start();
  void stop();
  bool isRunning() const;
  
  void registerTrigger(const EventTrigger& trigger);
  void unregisterTrigger(const std::string& workflowName);
  std::vector<EventTrigger> getTriggers() const;
  
  void triggerWorkflow(const std::string& workflowName, EventType eventType);
  
  void watchFile(const std::string& filePath, const std::string& workflowName);
  void unwatchFile(const std::string& filePath);

private:
  EventTriggerManager() = default;
  ~EventTriggerManager();
  EventTriggerManager(const EventTriggerManager&) = delete;
  EventTriggerManager& operator=(const EventTriggerManager&) = delete;
  
  std::atomic<bool> running_;
  std::thread fileWatcherThread_;
  std::mutex triggersMutex_;
  std::map<std::string, EventTrigger> triggers_;
  std::map<std::string, std::string> fileWatchers_;
  std::map<std::string, std::filesystem::file_time_type> fileLastModified_;
  
  void fileWatcherLoop();
  void checkFileChanges();
  bool shouldTriggerWorkflow(const EventTrigger& trigger, const std::string& eventData);
};

#endif
