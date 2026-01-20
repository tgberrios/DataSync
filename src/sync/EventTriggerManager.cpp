#include "sync/EventTriggerManager.h"
#include "sync/WorkflowExecutor.h"
#include "core/database_config.h"
#include <chrono>
#include <algorithm>

EventTriggerManager& EventTriggerManager::getInstance() {
  static EventTriggerManager instance;
  return instance;
}

EventTriggerManager::~EventTriggerManager() {
  stop();
}

void EventTriggerManager::start() {
  if (running_.exchange(true)) {
    return;
  }
  
  fileWatcherThread_ = std::thread(&EventTriggerManager::fileWatcherLoop, this);
  Logger::info(LogCategory::MONITORING, "EventTriggerManager",
               "Event trigger manager started");
}

void EventTriggerManager::stop() {
  if (!running_.exchange(false)) {
    return;
  }
  
  if (fileWatcherThread_.joinable()) {
    fileWatcherThread_.join();
  }
  
  Logger::info(LogCategory::MONITORING, "EventTriggerManager",
               "Event trigger manager stopped");
}

bool EventTriggerManager::isRunning() const {
  return running_.load();
}

void EventTriggerManager::registerTrigger(const EventTrigger& trigger) {
  std::lock_guard<std::mutex> lock(triggersMutex_);
  triggers_[trigger.workflow_name] = trigger;
  
  if (trigger.event_type == EventType::FILE_ARRIVAL) {
    try {
      auto config = nlohmann::json::parse(trigger.event_config);
      if (config.contains("file_path")) {
        std::string filePath = config["file_path"].get<std::string>();
        watchFile(filePath, trigger.workflow_name);
      }
    } catch (...) {
      Logger::warning(LogCategory::MONITORING, "registerTrigger",
                      "Invalid event config for workflow: " + trigger.workflow_name);
    }
  }
  
  Logger::info(LogCategory::MONITORING, "registerTrigger",
               "Registered trigger for workflow: " + trigger.workflow_name);
}

void EventTriggerManager::unregisterTrigger(const std::string& workflowName) {
  std::lock_guard<std::mutex> lock(triggersMutex_);
  auto it = triggers_.find(workflowName);
  if (it != triggers_.end()) {
    if (it->second.event_type == EventType::FILE_ARRIVAL) {
      try {
        auto config = nlohmann::json::parse(it->second.event_config);
        if (config.contains("file_path")) {
          std::string filePath = config["file_path"].get<std::string>();
          unwatchFile(filePath);
        }
      } catch (...) {
      }
    }
    triggers_.erase(it);
  }
}

std::vector<EventTriggerManager::EventTrigger> EventTriggerManager::getTriggers() const {
  std::lock_guard<std::mutex> lock(triggersMutex_);
  std::vector<EventTrigger> result;
  for (const auto& [name, trigger] : triggers_) {
    result.push_back(trigger);
  }
  return result;
}

void EventTriggerManager::triggerWorkflow(const std::string& workflowName, EventType eventType) {
  std::lock_guard<std::mutex> lock(triggersMutex_);
  auto it = triggers_.find(workflowName);
  if (it != triggers_.end() && it->second.active && it->second.event_type == eventType) {
    std::string connStr = DatabaseConfig::getPostgresConnectionString();
    WorkflowExecutor executor(connStr);
    executor.executeWorkflowAsync(workflowName, TriggerType::EVENT);
    Logger::info(LogCategory::MONITORING, "triggerWorkflow",
                 "Triggered workflow: " + workflowName + " via event type: " + std::to_string(static_cast<int>(eventType)));
  }
}

void EventTriggerManager::watchFile(const std::string& filePath, const std::string& workflowName) {
  std::lock_guard<std::mutex> lock(triggersMutex_);
  fileWatchers_[filePath] = workflowName;
  
  if (std::filesystem::exists(filePath)) {
    fileLastModified_[filePath] = std::filesystem::last_write_time(filePath);
  }
}

void EventTriggerManager::unwatchFile(const std::string& filePath) {
  std::lock_guard<std::mutex> lock(triggersMutex_);
  fileWatchers_.erase(filePath);
  fileLastModified_.erase(filePath);
}

void EventTriggerManager::fileWatcherLoop() {
  while (running_.load()) {
    try {
      checkFileChanges();
    } catch (const std::exception& e) {
      Logger::error(LogCategory::MONITORING, "fileWatcherLoop",
                    "Error in file watcher loop: " + std::string(e.what()));
    }
    
    std::this_thread::sleep_for(std::chrono::seconds(5));
  }
}

void EventTriggerManager::checkFileChanges() {
  std::lock_guard<std::mutex> lock(triggersMutex_);
  
  for (auto& [filePath, workflowName] : fileWatchers_) {
    if (!std::filesystem::exists(filePath)) {
      continue;
    }
    
    auto currentModified = std::filesystem::last_write_time(filePath);
    auto it = fileLastModified_.find(filePath);
    
    if (it == fileLastModified_.end() || it->second != currentModified) {
      fileLastModified_[filePath] = currentModified;
      
      auto triggerIt = triggers_.find(workflowName);
      if (triggerIt != triggers_.end() && triggerIt->second.active) {
        std::string connStr = DatabaseConfig::getPostgresConnectionString();
        WorkflowExecutor executor(connStr);
        executor.executeWorkflowAsync(workflowName, TriggerType::EVENT);
        Logger::info(LogCategory::MONITORING, "checkFileChanges",
                     "File changed, triggering workflow: " + workflowName + " for file: " + filePath);
      }
    }
  }
}

bool EventTriggerManager::shouldTriggerWorkflow(const EventTrigger& trigger, const std::string& eventData) {
  return trigger.active;
}
