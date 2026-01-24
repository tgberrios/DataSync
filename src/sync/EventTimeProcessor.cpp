#include "sync/EventTimeProcessor.h"
#include "core/logger.h"
#include <algorithm>

EventTimeProcessor::EventTimeProcessor(const EventTimeConfig& config)
    : config_(config) {
  Logger::info(LogCategory::SYSTEM, "EventTimeProcessor",
               "Initializing EventTimeProcessor with time type: " +
               std::to_string(static_cast<int>(config_.timeType)));
  
  currentWatermark_.timestamp = 0;
  currentWatermark_.processingTime = std::chrono::system_clock::now();
}

EventTimeProcessor::~EventTimeProcessor() {
  lateEventBuffer_.clear();
}

int64_t EventTimeProcessor::extractEventTime(const json& event) const {
  if (config_.timeType == TimeType::PROCESSING_TIME) {
    return getProcessingTime();
  }

  // Intentar extraer event time del campo especificado
  if (event.contains(config_.eventTimeField)) {
    const auto& field = event[config_.eventTimeField];
    
    if (field.is_number()) {
      return field.get<int64_t>();
    } else if (field.is_string()) {
      // Intentar parsear como timestamp ISO o Unix timestamp
      try {
        // Asumir formato Unix timestamp en string
        return std::stoll(field.get<std::string>());
      } catch (...) {
        Logger::warning(LogCategory::SYSTEM, "EventTimeProcessor",
                        "Could not parse event time field as number");
      }
    }
  }

  // Fallback a processing time si no se puede extraer
  Logger::warning(LogCategory::SYSTEM, "EventTimeProcessor",
                  "Could not extract event time, using processing time");
  return getProcessingTime();
}

EventTimeProcessor::Watermark EventTimeProcessor::calculateWatermark(
    const std::deque<json>& events) {
  
  if (events.empty()) {
    return currentWatermark_;
  }

  // Encontrar el máximo event time en los eventos recientes
  int64_t maxEventTime = 0;
  for (const auto& event : events) {
    int64_t eventTime = extractEventTime(event);
    maxEventTime = std::max(maxEventTime, eventTime);
  }

  // Calcular watermark: maxEventTime - watermarkDelay
  Watermark watermark;
  watermark.timestamp = maxEventTime - config_.watermarkDelaySeconds;
  watermark.processingTime = std::chrono::system_clock::now();

  // El watermark solo puede avanzar, nunca retroceder
  if (watermark.timestamp > currentWatermark_.timestamp) {
    currentWatermark_ = watermark;
  }

  return currentWatermark_;
}

bool EventTimeProcessor::isLateEvent(const json& event,
                                    const Watermark& currentWatermark) const {
  int64_t eventTime = extractEventTime(event);
  
  // Evento es tardío si su event time es menor que el watermark
  // menos el máximo desorden permitido
  int64_t lateThreshold = currentWatermark.timestamp - config_.maxOutOfOrdernessSeconds;
  
  return eventTime < lateThreshold;
}

bool EventTimeProcessor::handleLateData(const json& event,
                                        LateDataHandling handling) {
  lateEvents_++;

  switch (handling) {
    case LateDataHandling::DROP:
      eventsDropped_++;
      Logger::debug(LogCategory::SYSTEM, "EventTimeProcessor",
                    "Dropping late event");
      return false;

    case LateDataHandling::SIDE_OUTPUT:
      // TODO: Implementar side output
      Logger::info(LogCategory::SYSTEM, "EventTimeProcessor",
                   "Sending late event to side output");
      return true;

    case LateDataHandling::BUFFER:
      lateEventBuffer_.push_back(event);
      eventsBuffered_++;
      Logger::info(LogCategory::SYSTEM, "EventTimeProcessor",
                   "Buffering late event (buffer size: " +
                   std::to_string(lateEventBuffer_.size()) + ")");
      return true;
  }

  return false;
}

json EventTimeProcessor::getStatistics() const {
  json stats;
  stats["eventsProcessed"] = eventsProcessed_;
  stats["lateEvents"] = lateEvents_;
  stats["eventsDropped"] = eventsDropped_;
  stats["eventsBuffered"] = eventsBuffered_;
  stats["lateEventBufferSize"] = lateEventBuffer_.size();
  stats["currentWatermark"] = currentWatermark_.timestamp;
  stats["timeType"] = static_cast<int>(config_.timeType);
  
  return stats;
}

int64_t EventTimeProcessor::getProcessingTime() const {
  return std::chrono::duration_cast<std::chrono::seconds>(
      std::chrono::system_clock::now().time_since_epoch()).count();
}
