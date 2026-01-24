#ifndef CEP_PROCESSOR_H
#define CEP_PROCESSOR_H

#include "third_party/json.hpp"
#include "core/logger.h"
#include <string>
#include <vector>
#include <map>
#include <memory>
#include <mutex>
#include <functional>

using json = nlohmann::json;

// CEPProcessor: Complex Event Processing para detectar patrones en streams
class CEPProcessor {
public:
  struct CEPRule {
    std::string ruleId;
    std::string name;
    std::string description;
    json pattern;                    // Patrón a detectar (secuencia, temporal, etc.)
    json conditions;                 // Condiciones adicionales
    json actions;                    // Acciones a ejecutar cuando se detecta el patrón
    bool enabled{true};
    int64_t windowSeconds{300};     // Ventana de tiempo para el patrón
  };

  struct PatternMatch {
    std::string matchId;
    std::string ruleId;
    std::vector<json> matchedEvents;
    std::chrono::system_clock::time_point matchTime;
    json metadata;
  };

  explicit CEPProcessor();
  ~CEPProcessor();

  // Agregar regla CEP
  bool addRule(const CEPRule& rule);

  // Eliminar regla
  bool removeRule(const std::string& ruleId);

  // Obtener regla
  CEPRule getRule(const std::string& ruleId) const;

  // Obtener todas las reglas
  std::vector<CEPRule> getRules() const;

  // Procesar evento y detectar patrones
  std::vector<PatternMatch> processEvent(const json& event, int64_t eventTimestamp = 0);

  // Obtener matches detectados
  std::vector<PatternMatch> getMatches(const std::string& ruleId = "") const;

  // Limpiar matches antiguos
  void cleanupOldMatches(int64_t maxAgeSeconds);

  // Obtener estadísticas
  json getStatistics() const;

private:
  mutable std::mutex rulesMutex_;
  std::map<std::string, CEPRule> rules_;

  mutable std::mutex matchesMutex_;
  std::vector<PatternMatch> matches_;

  // Estado para detección de patrones
  struct PatternState {
    std::string ruleId;
    std::vector<json> currentSequence;
    std::chrono::system_clock::time_point sequenceStart;
    int currentStep{0};
  };
  std::map<std::string, PatternState> patternStates_;

  // Contadores
  int64_t eventsProcessed_{0};
  int64_t patternsMatched_{0};

  // Verificar si evento cumple condición
  bool evaluateCondition(const json& event, const json& condition) const;

  // Verificar si secuencia de eventos cumple patrón
  bool matchPattern(const std::vector<json>& events, const json& pattern) const;

  // Ejecutar acciones cuando se detecta patrón
  void executeActions(const std::string& ruleId, const PatternMatch& match);

  // Generar ID único para match
  std::string generateMatchId() const;
};

#endif // CEP_PROCESSOR_H
