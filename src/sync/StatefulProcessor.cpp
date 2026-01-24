#include "sync/StatefulProcessor.h"
#include "core/logger.h"

StatefulProcessor::StatefulProcessor() {
  Logger::info(LogCategory::SYSTEM, "StatefulProcessor",
               "Initializing StatefulProcessor");
}

StatefulProcessor::~StatefulProcessor() {
  std::lock_guard<std::mutex> lock(stateMutex_);
  state_.clear();
}

json StatefulProcessor::getState(const std::string& key) const {
  std::lock_guard<std::mutex> lock(stateMutex_);
  
  auto it = state_.find(key);
  if (it == state_.end()) {
    return json(nullptr);
  }

  stateGets_++;
  return it->second.value;
}

StatefulProcessor::StateValue StatefulProcessor::getStateValue(const std::string& key) const {
  std::lock_guard<std::mutex> lock(stateMutex_);
  
  auto it = state_.find(key);
  if (it == state_.end()) {
    return StateValue{};
  }

  stateGets_++;
  return it->second;
}

bool StatefulProcessor::updateState(const std::string& key, const json& value) {
  std::lock_guard<std::mutex> lock(stateMutex_);
  
  auto it = state_.find(key);
  if (it == state_.end()) {
    StateValue stateValue;
    stateValue.value = value;
    stateValue.lastUpdated = std::chrono::system_clock::now();
    stateValue.updateCount = 1;
    state_[key] = stateValue;
  } else {
    it->second.value = value;
    it->second.lastUpdated = std::chrono::system_clock::now();
    it->second.updateCount++;
  }

  stateUpdates_++;
  return true;
}

bool StatefulProcessor::updateStateWithFunction(
    const std::string& key,
    std::function<json(const json& current, const json& event)> updateFn,
    const json& event) {
  
  std::lock_guard<std::mutex> lock(stateMutex_);
  
  json currentValue = json(nullptr);
  auto it = state_.find(key);
  if (it != state_.end()) {
    currentValue = it->second.value;
  }

  json newValue = updateFn(currentValue, event);

  if (it == state_.end()) {
    StateValue stateValue;
    stateValue.value = newValue;
    stateValue.lastUpdated = std::chrono::system_clock::now();
    stateValue.updateCount = 1;
    state_[key] = stateValue;
  } else {
    it->second.value = newValue;
    it->second.lastUpdated = std::chrono::system_clock::now();
    it->second.updateCount++;
  }

  stateUpdates_++;
  return true;
}

bool StatefulProcessor::clearState(const std::string& key) {
  std::lock_guard<std::mutex> lock(stateMutex_);
  
  auto it = state_.find(key);
  if (it == state_.end()) {
    return false;
  }

  state_.erase(it);
  stateClears_++;
  return true;
}

void StatefulProcessor::clearAllStates() {
  std::lock_guard<std::mutex> lock(stateMutex_);
  
  state_.clear();
  stateClears_++;
  Logger::info(LogCategory::SYSTEM, "StatefulProcessor",
               "All states cleared");
}

StatefulProcessor::StateSnapshot StatefulProcessor::getStateSnapshot() const {
  std::lock_guard<std::mutex> lock(stateMutex_);
  
  StateSnapshot snapshot;
  snapshot.states = state_;
  snapshot.snapshotTime = std::chrono::system_clock::now();
  snapshot.totalKeys = state_.size();
  
  return snapshot;
}

std::vector<std::string> StatefulProcessor::getAllKeys() const {
  std::lock_guard<std::mutex> lock(stateMutex_);
  
  std::vector<std::string> keys;
  for (const auto& [key, value] : state_) {
    keys.push_back(key);
  }
  return keys;
}

bool StatefulProcessor::hasKey(const std::string& key) const {
  std::lock_guard<std::mutex> lock(stateMutex_);
  return state_.find(key) != state_.end();
}

json StatefulProcessor::getStatistics() const {
  std::lock_guard<std::mutex> lock(stateMutex_);
  
  json stats;
  stats["totalKeys"] = state_.size();
  stats["stateUpdates"] = stateUpdates_;
  stats["stateGets"] = stateGets_;
  stats["stateClears"] = stateClears_;
  
  return stats;
}

void StatefulProcessor::cleanupOldStates(int64_t maxAgeSeconds) {
  std::lock_guard<std::mutex> lock(stateMutex_);
  
  auto now = std::chrono::system_clock::now();
  std::vector<std::string> toRemove;

  for (const auto& [key, value] : state_) {
    auto age = std::chrono::duration_cast<std::chrono::seconds>(
        now - value.lastUpdated).count();
    if (age > maxAgeSeconds) {
      toRemove.push_back(key);
    }
  }

  for (const auto& key : toRemove) {
    state_.erase(key);
  }

  if (!toRemove.empty()) {
    Logger::info(LogCategory::SYSTEM, "StatefulProcessor",
                 "Cleaned up " + std::to_string(toRemove.size()) + " old states");
  }
}
