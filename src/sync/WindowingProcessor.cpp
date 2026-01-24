#include "sync/WindowingProcessor.h"
#include "core/logger.h"
#include <sstream>
#include <algorithm>
#include <random>
#include <iomanip>

WindowingProcessor::WindowingProcessor(const WindowConfig& config)
    : config_(config) {
  Logger::info(LogCategory::SYSTEM, "WindowingProcessor",
               "Initializing WindowingProcessor with type: " +
               std::to_string(static_cast<int>(config_.windowType)));
}

WindowingProcessor::~WindowingProcessor() {
  std::lock_guard<std::mutex> lock(windowsMutex_);
  windows_.clear();
  sessionWindows_.clear();
}

std::string WindowingProcessor::createWindow() {
  std::lock_guard<std::mutex> lock(windowsMutex_);
  
  std::string windowId = generateWindowId();
  auto now = std::chrono::system_clock::now();
  
  Window window;
  window.windowId = windowId;
  window.startTime = now;
  window.endTime = now + std::chrono::seconds(config_.windowSizeSeconds);
  window.isClosed = false;
  
  windows_[windowId] = window;
  windowsCreated_++;
  
  Logger::info(LogCategory::SYSTEM, "WindowingProcessor",
               "Window created: " + windowId);
  return windowId;
}

bool WindowingProcessor::addEvent(const std::string& windowId, const json& event,
                                  int64_t eventTimestamp) {
  std::lock_guard<std::mutex> lock(windowsMutex_);
  
  auto it = windows_.find(windowId);
  if (it == windows_.end()) {
    Logger::warning(LogCategory::SYSTEM, "WindowingProcessor",
                    "Window not found: " + windowId);
    return false;
  }

  if (it->second.isClosed) {
    Logger::warning(LogCategory::SYSTEM, "WindowingProcessor",
                    "Cannot add event to closed window: " + windowId);
    return false;
  }

  it->second.events.push_back(event);
  eventsProcessed_++;
  
  return true;
}

std::vector<json> WindowingProcessor::getWindowedEvents(const std::string& windowId) const {
  std::lock_guard<std::mutex> lock(windowsMutex_);
  
  auto it = windows_.find(windowId);
  if (it == windows_.end()) {
    return {};
  }

  return it->second.events;
}

WindowingProcessor::WindowResult WindowingProcessor::closeWindow(const std::string& windowId) {
  std::lock_guard<std::mutex> lock(windowsMutex_);
  
  auto it = windows_.find(windowId);
  if (it == windows_.end()) {
    Logger::warning(LogCategory::SYSTEM, "WindowingProcessor",
                    "Window not found: " + windowId);
    return WindowResult{};
  }

  Window& window = it->second;
  if (window.isClosed) {
    Logger::warning(LogCategory::SYSTEM, "WindowingProcessor",
                    "Window already closed: " + windowId);
    return WindowResult{};
  }

  window.isClosed = true;
  window.endTime = std::chrono::system_clock::now();
  windowsClosed_++;

  WindowResult result;
  result.windowId = windowId;
  result.events = window.events;
  result.eventCount = window.events.size();
  result.startTime = window.startTime;
  result.endTime = window.endTime;
  result.aggregatedData = window.metadata;

  Logger::info(LogCategory::SYSTEM, "WindowingProcessor",
               "Window closed: " + windowId + " with " + 
               std::to_string(result.eventCount) + " events");

  return result;
}

std::vector<std::string> WindowingProcessor::getActiveWindows() const {
  std::lock_guard<std::mutex> lock(windowsMutex_);
  
  std::vector<std::string> activeWindows;
  for (const auto& [windowId, window] : windows_) {
    if (!window.isClosed) {
      activeWindows.push_back(windowId);
    }
  }
  return activeWindows;
}

std::vector<WindowingProcessor::WindowResult> WindowingProcessor::processEvent(
    const json& event, int64_t eventTimestamp) {
  
  std::vector<WindowResult> closedWindows;
  
  if (eventTimestamp == 0) {
    eventTimestamp = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
  }

  std::lock_guard<std::mutex> lock(windowsMutex_);

  switch (config_.windowType) {
    case WindowType::TUMBLING: {
      // Para tumbling windows, crear nueva ventana si no hay activa o si la actual expiró
      std::string currentWindowId;
      bool needNewWindow = true;

      // Buscar ventana activa que contenga este timestamp
      for (auto& [windowId, window] : windows_) {
        if (!window.isClosed) {
          auto windowStart = std::chrono::duration_cast<std::chrono::seconds>(
              window.startTime.time_since_epoch()).count();
          auto windowEnd = windowStart + config_.windowSizeSeconds;
          
          if (eventTimestamp >= windowStart && eventTimestamp < windowEnd) {
            currentWindowId = windowId;
            needNewWindow = false;
            break;
          } else if (eventTimestamp >= windowEnd) {
            // Cerrar ventana expirada
            window.isClosed = true;
            window.endTime = std::chrono::system_clock::now();
            windowsClosed_++;
            
            WindowResult result;
            result.windowId = windowId;
            result.events = window.events;
            result.eventCount = window.events.size();
            result.startTime = window.startTime;
            result.endTime = window.endTime;
            closedWindows.push_back(result);
          }
        }
      }

      if (needNewWindow) {
        currentWindowId = createWindow();
        auto& window = windows_[currentWindowId];
        window.startTime = std::chrono::system_clock::from_time_t(
            calculateWindowStart(eventTimestamp).time_since_epoch().count());
        window.endTime = window.startTime + std::chrono::seconds(config_.windowSizeSeconds);
      }

      addEvent(currentWindowId, event, eventTimestamp);
      break;
    }

    case WindowType::SLIDING: {
      // Para sliding windows, crear nueva ventana cada slideInterval
      std::string currentWindowId = createWindow();
      auto& window = windows_[currentWindowId];
      window.startTime = std::chrono::system_clock::from_time_t(
          calculateWindowStart(eventTimestamp).time_since_epoch().count());
      window.endTime = window.startTime + std::chrono::seconds(config_.windowSizeSeconds);
      
      addEvent(currentWindowId, event, eventTimestamp);

      // Cerrar ventanas que ya no contienen el timestamp actual
      auto currentTime = std::chrono::system_clock::from_time_t(eventTimestamp);
      for (auto& [windowId, window] : windows_) {
        if (!window.isClosed && window.endTime < currentTime) {
          window.isClosed = true;
          window.endTime = std::chrono::system_clock::now();
          windowsClosed_++;
          
          WindowResult result;
          result.windowId = windowId;
          result.events = window.events;
          result.eventCount = window.events.size();
          result.startTime = window.startTime;
          result.endTime = window.endTime;
          closedWindows.push_back(result);
        }
      }
      break;
    }

    case WindowType::SESSION: {
      // Para session windows, usar clave del evento para agrupar
      std::string sessionKey;
      if (event.contains("sessionId")) {
        sessionKey = event["sessionId"];
      } else if (event.contains("userId")) {
        sessionKey = event["userId"];
      } else {
        sessionKey = "default";
      }

      std::string windowId = findSessionWindow(event, eventTimestamp);
      if (windowId.empty()) {
        windowId = createWindow();
        sessionWindows_[sessionKey] = windowId;
        auto& window = windows_[windowId];
        window.startTime = std::chrono::system_clock::from_time_t(eventTimestamp);
        window.endTime = window.startTime + std::chrono::seconds(config_.sessionTimeoutSeconds);
      } else {
        auto& window = windows_[windowId];
        // Extender ventana si hay actividad
        window.endTime = std::chrono::system_clock::from_time_t(
            eventTimestamp) + std::chrono::seconds(config_.sessionTimeoutSeconds);
      }

      addEvent(windowId, event, eventTimestamp);
      break;
    }
  }

  return closedWindows;
}

void WindowingProcessor::cleanupExpiredWindows() {
  std::lock_guard<std::mutex> lock(windowsMutex_);
  
  auto now = std::chrono::system_clock::now();
  std::vector<std::string> toRemove;

  for (auto& [windowId, window] : windows_) {
    if (window.isClosed || window.endTime < now) {
      if (!window.isClosed) {
        window.isClosed = true;
        windowsClosed_++;
      }
      toRemove.push_back(windowId);
    }
  }

  for (const auto& windowId : toRemove) {
    windows_.erase(windowId);
  }

  // Limpiar session windows
  for (auto it = sessionWindows_.begin(); it != sessionWindows_.end();) {
    if (windows_.find(it->second) == windows_.end()) {
      it = sessionWindows_.erase(it);
    } else {
      ++it;
    }
  }
}

json WindowingProcessor::getStatistics() const {
  std::lock_guard<std::mutex> lock(windowsMutex_);
  
  json stats;
  stats["windowsCreated"] = windowsCreated_;
  stats["windowsClosed"] = windowsClosed_;
  stats["eventsProcessed"] = eventsProcessed_;
  stats["activeWindows"] = windows_.size() - (windowsCreated_ - windowsClosed_);
  stats["windowType"] = static_cast<int>(config_.windowType);
  stats["windowSizeSeconds"] = config_.windowSizeSeconds;
  
  return stats;
}

std::string WindowingProcessor::generateWindowId() const {
  auto now = std::chrono::system_clock::now();
  auto time = std::chrono::duration_cast<std::chrono::milliseconds>(
      now.time_since_epoch()).count();
  
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis(0, 9999);
  
  std::stringstream ss;
  ss << "window_" << time << "_" << std::setfill('0') << std::setw(4) << dis(gen);
  return ss.str();
}

std::string WindowingProcessor::findSessionWindow(const json& event, int64_t eventTimestamp) {
  std::string sessionKey;
  if (event.contains("sessionId")) {
    sessionKey = event["sessionId"];
  } else if (event.contains("userId")) {
    sessionKey = event["userId"];
  } else {
    return "";
  }

  auto it = sessionWindows_.find(sessionKey);
  if (it != sessionWindows_.end()) {
    auto windowIt = windows_.find(it->second);
    if (windowIt != windows_.end() && !windowIt->second.isClosed) {
      // Verificar si la ventana aún es válida (no expirada)
      auto windowEnd = std::chrono::duration_cast<std::chrono::seconds>(
          windowIt->second.endTime.time_since_epoch()).count();
      if (eventTimestamp <= windowEnd) {
        return it->second;
      } else {
        // Ventana expirada, remover
        sessionWindows_.erase(it);
      }
    }
  }

  return "";
}

std::string WindowingProcessor::createNewWindow(int64_t eventTimestamp) {
  return createWindow();
}

bool WindowingProcessor::shouldCloseWindow(const Window& window, int64_t currentTime) const {
  auto windowEnd = std::chrono::duration_cast<std::chrono::seconds>(
      window.endTime.time_since_epoch()).count();
  return currentTime >= windowEnd;
}

std::chrono::system_clock::time_point WindowingProcessor::calculateWindowStart(
    int64_t eventTimestamp) const {
  // Para tumbling windows, alinear al inicio del intervalo
  int64_t windowStart = (eventTimestamp / config_.windowSizeSeconds) * config_.windowSizeSeconds;
  return std::chrono::system_clock::from_time_t(windowStart);
}
