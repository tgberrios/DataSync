#ifndef EVENT_TIME_PROCESSOR_H
#define EVENT_TIME_PROCESSOR_H

#include "third_party/json.hpp"
#include "core/logger.h"
#include <string>
#include <chrono>
#include <deque>

using json = nlohmann::json;

// EventTimeProcessor: Maneja event time vs processing time y watermarks
class EventTimeProcessor {
public:
  enum class TimeType {
    EVENT_TIME,      // Usar timestamp del evento
    PROCESSING_TIME  // Usar timestamp de procesamiento
  };

  struct EventTimeConfig {
    std::string eventTimeField{"timestamp"};  // Campo que contiene el event time
    int64_t watermarkDelaySeconds{10};         // Delay del watermark
    int64_t maxOutOfOrdernessSeconds{5};       // Máximo desorden permitido
    TimeType timeType{TimeType::EVENT_TIME};
  };

  struct Watermark {
    int64_t timestamp{0};
    std::chrono::system_clock::time_point processingTime;
  };

  explicit EventTimeProcessor(const EventTimeConfig& config);
  ~EventTimeProcessor();

  // Extraer event time de un evento
  int64_t extractEventTime(const json& event) const;

  // Calcular watermark actual
  Watermark calculateWatermark(const std::deque<json>& events);

  // Verificar si evento es tardío (late event)
  bool isLateEvent(const json& event, const Watermark& currentWatermark) const;

  // Manejar evento tardío
  enum class LateDataHandling {
    DROP,           // Descartar evento
    SIDE_OUTPUT,   // Enviar a side output
    BUFFER         // Buffer para procesar después
  };

  bool handleLateData(const json& event, LateDataHandling handling = LateDataHandling::DROP);

  // Obtener watermark actual
  Watermark getCurrentWatermark() const { return currentWatermark_; }

  // Obtener estadísticas
  json getStatistics() const;

private:
  EventTimeConfig config_;
  Watermark currentWatermark_;
  
  // Contadores
  int64_t eventsProcessed_{0};
  int64_t lateEvents_{0};
  int64_t eventsDropped_{0};
  int64_t eventsBuffered_{0};

  // Buffer para eventos tardíos
  std::deque<json> lateEventBuffer_;

  // Obtener processing time actual
  int64_t getProcessingTime() const;
};

#endif // EVENT_TIME_PROCESSOR_H
